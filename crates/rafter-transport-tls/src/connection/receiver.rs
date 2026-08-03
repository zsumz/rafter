//! One authenticated inbound connection worker and its bounded admission loop.

mod admission;
mod classify;
mod frame;
mod scratch;
mod tls;

use std::{
    io::Read,
    net::TcpStream,
    sync::{atomic::Ordering, Arc},
};

use crate::diagnostics::{increment, Counters};
use crate::queue::{InboundQueue, ReceiveMemoryBudget, ReceiveMemoryPermit};
use crate::runtime::{InboundEpochs, RuntimeControl, SessionStoreHandle};
use crate::{
    CertificateDirectory, GroupIdCodec, InboundSequence, PeerFrameCodec, TlsHandshakeConfig,
    TlsIdentity, TlsPeerDirectory, TransportTimeouts,
};

use self::classify::classify_frame_io;
use self::frame::{process_frame, FrameInput, FrameStep};
use self::scratch::{ReceiverScratch, ReceiverScratchError};
use super::io::{read_peer_frame, PeerFrameRead};

pub(crate) struct ReceiverTemplate<G, C> {
    pub(crate) identity: TlsIdentity,
    pub(crate) certificates: CertificateDirectory,
    pub(crate) handshake: TlsHandshakeConfig,
    pub(crate) sessions: SessionStoreHandle,
    pub(crate) codec: Arc<PeerFrameCodec<G, C>>,
    pub(crate) directory: TlsPeerDirectory<G>,
    pub(crate) inbound: Arc<InboundQueue<G>>,
    pub(crate) receive_memory: ReceiveMemoryBudget,
    pub(crate) epochs: Arc<InboundEpochs>,
    pub(crate) control: Arc<RuntimeControl>,
    pub(crate) counters: Arc<Counters>,
    pub(crate) timeouts: TransportTimeouts,
}

impl<G, C> Clone for ReceiverTemplate<G, C> {
    fn clone(&self) -> Self {
        Self {
            identity: self.identity.clone(),
            certificates: self.certificates.clone(),
            handshake: self.handshake.clone(),
            sessions: self.sessions.clone(),
            codec: Arc::clone(&self.codec),
            directory: self.directory.clone(),
            inbound: Arc::clone(&self.inbound),
            receive_memory: self.receive_memory.clone(),
            epochs: Arc::clone(&self.epochs),
            control: Arc::clone(&self.control),
            counters: Arc::clone(&self.counters),
            timeouts: self.timeouts,
        }
    }
}

pub(crate) struct ReceiverContext<G, C> {
    pub(crate) template: ReceiverTemplate<G, C>,
    pub(crate) socket: TcpStream,
    pub(crate) shutdown_socket: Arc<TcpStream>,
    pub(crate) permit: ConnectionPermit,
}

#[derive(Debug)]
pub(crate) struct ConnectionPermit {
    counters: Arc<Counters>,
}

impl ConnectionPermit {
    pub(crate) fn acquire(counters: Arc<Counters>, maximum: usize) -> Option<Self> {
        let mut current = counters.active_inbound.load(Ordering::Relaxed);
        loop {
            if current >= maximum {
                return None;
            }
            match counters.active_inbound.compare_exchange_weak(
                current,
                current.saturating_add(1),
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(Self { counters }),
                Err(actual) => current = actual,
            }
        }
    }
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        let _ = self.counters.active_inbound.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |value| Some(value.saturating_sub(1)),
        );
    }
}

pub(crate) fn receive_loop<G, C>(context: ReceiverContext<G, C>)
where
    G: Ord + Send + Sync + 'static,
    C: GroupIdCodec<G>,
{
    let ReceiverContext {
        template,
        socket,
        shutdown_socket,
        permit: _permit,
    } = context;
    let Some(mut established) = tls::establish(&template, socket, shutdown_socket) else {
        return;
    };
    let mut scratch = match ReceiverScratch::acquire(&template.receive_memory, &template.codec) {
        Ok(scratch) => scratch,
        Err(ReceiverScratchError::MemoryFull) => {
            increment(&template.counters.inbound_full);
            increment(&template.counters.inbound_memory_full);
            return;
        }
        Err(ReceiverScratchError::Allocation(error)) => {
            template.control.fail(format!(
                "could not allocate canonical group scratch for an authenticated receiver: {error}"
            ));
            return;
        }
    };
    let mut expected = InboundSequence::new();

    while !template.control.shutdown_requested() {
        if !epoch_is_current(&template, &established.epoch) {
            return;
        }
        // The wire buffer is frame-scoped. Dropping it after admission keeps
        // idle connections from retaining allocations outside the weighted
        // receive-memory permit.
        let mut encoded = Vec::new();
        let complete = match read_next_frame(
            &template,
            &established.epoch,
            &mut established.stream,
            established.frame_bytes,
            &mut encoded,
        ) {
            FrameReadStep::Complete { bytes, memory } => (bytes, memory),
            FrameReadStep::Idle => continue,
            FrameReadStep::Stop => return,
        };
        let (complete, memory) = complete;
        if matches!(
            process_frame(
                &template,
                &mut expected,
                scratch.frame_mut(),
                FrameInput {
                    peer: &established.peer,
                    epoch: &established.epoch,
                    encoded: &encoded,
                    complete,
                    memory,
                },
            ),
            FrameStep::Stop
        ) {
            return;
        }
    }
}

#[derive(Debug)]
enum FrameReadStep {
    Complete {
        bytes: usize,
        memory: ReceiveMemoryPermit,
    },
    Idle,
    Stop,
}

fn read_next_frame<G, C>(
    template: &ReceiverTemplate<G, C>,
    epoch: &crate::runtime::InboundEpochGuard,
    stream: &mut impl Read,
    frame_bytes: usize,
    encoded: &mut Vec<u8>,
) -> FrameReadStep {
    match read_peer_frame(stream, frame_bytes, &template.receive_memory, encoded) {
        Ok(PeerFrameRead::Complete { bytes, memory }) => FrameReadStep::Complete { bytes, memory },
        Ok(PeerFrameRead::Idle) => FrameReadStep::Idle,
        Ok(PeerFrameRead::Closed) => FrameReadStep::Stop,
        Err(error) => {
            match epoch.is_current() {
                Ok(false) => {}
                Ok(true) => classify_frame_io(&template.counters, &error),
                Err(()) => template.control.fail("inbound epoch state is poisoned"),
            }
            FrameReadStep::Stop
        }
    }
}

fn epoch_is_current<G, C>(
    template: &ReceiverTemplate<G, C>,
    epoch: &crate::runtime::InboundEpochGuard,
) -> bool {
    match epoch.is_current() {
        Ok(true) => true,
        Ok(false) => {
            increment(&template.counters.sequence_violations);
            false
        }
        Err(()) => {
            template.control.fail("inbound epoch state is poisoned");
            false
        }
    }
}
