//! One authenticated inbound connection worker and its bounded admission loop.

mod admission;
mod classify;
mod tls;

use std::{
    net::TcpStream,
    sync::{atomic::Ordering, Arc},
};

use crate::diagnostics::{increment, Counters};
use crate::queue::{InboundQueue, InboundQueueError, InboundQueueFull};
use crate::runtime::{InboundEpochs, RuntimeControl, SessionStoreHandle};
use crate::{
    CertificateDirectory, GroupIdCodec, InboundSequence, PeerFrameCodec, PeerFrameScratch,
    TlsHandshakeConfig, TlsIdentity, TlsPeerDirectory, TransportTimeouts,
};

use self::admission::{admit_frame, AdmissionRefusal};
use self::classify::{classify_decode_error, classify_frame_io};
use super::io::{read_peer_frame, PeerFrameRead};

pub(crate) struct ReceiverTemplate<G, C> {
    pub(crate) identity: TlsIdentity,
    pub(crate) certificates: CertificateDirectory,
    pub(crate) handshake: TlsHandshakeConfig,
    pub(crate) sessions: SessionStoreHandle,
    pub(crate) codec: Arc<PeerFrameCodec<G, C>>,
    pub(crate) directory: TlsPeerDirectory<G>,
    pub(crate) inbound: Arc<InboundQueue<G>>,
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

#[allow(clippy::too_many_lines)]
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
    let mut expected = InboundSequence::new();
    let mut encoded = Vec::new();
    let mut scratch = PeerFrameScratch::new();

    while !template.control.shutdown_requested() {
        match established.epoch.is_current() {
            Ok(true) => {}
            Ok(false) => {
                increment(&template.counters.sequence_violations);
                return;
            }
            Err(()) => {
                template.control.fail("inbound epoch state is poisoned");
                return;
            }
        }
        let complete = match read_peer_frame(
            &mut established.stream,
            established.frame_bytes,
            &mut encoded,
        ) {
            Ok(PeerFrameRead::Complete(complete)) => complete,
            Ok(PeerFrameRead::Idle) => continue,
            Ok(PeerFrameRead::Closed) => return,
            Err(error) => {
                match established.epoch.is_current() {
                    Ok(false) => return,
                    Ok(true) => classify_frame_io(&template.counters, &error),
                    Err(()) => template.control.fail("inbound epoch state is poisoned"),
                }
                return;
            }
        };
        let frame = match template.codec.decode(&encoded, &mut scratch) {
            Ok(frame) => frame,
            Err(error) => {
                classify_decode_error(&template.counters, &error);
                return;
            }
        };
        if expected.accept(frame.sequence()).is_err() {
            increment(&template.counters.sequence_violations);
            increment(&template.counters.frames_dropped);
            return;
        }
        let envelope = match admit_frame(
            &template.directory,
            template.handshake.local_peer_id(),
            &established.peer,
            frame,
        ) {
            Ok(envelope) => envelope,
            Err(AdmissionRefusal::Identity) => {
                increment(&template.counters.identity_mismatches);
                increment(&template.counters.frames_dropped);
                return;
            }
            Err(AdmissionRefusal::Unauthorized) => {
                increment(&template.counters.unauthorized_frames);
                increment(&template.counters.frames_dropped);
                continue;
            }
            Err(AdmissionRefusal::Retired) => {
                increment(&template.counters.retired_peer_frames);
                increment(&template.counters.frames_dropped);
                continue;
            }
            Err(AdmissionRefusal::Terminal) => {
                template.control.fail("peer directory state is poisoned");
                return;
            }
        };
        let admitted = established.epoch.while_current(|| {
            template
                .inbound
                .try_push(established.peer.clone(), complete, envelope)
        });
        match admitted {
            Ok(Some(Ok(()))) => increment(&template.counters.frames_received),
            Ok(Some(Err(InboundQueueError::Full(InboundQueueFull::Peer)))) => {
                increment(&template.counters.inbound_full);
                increment(&template.counters.inbound_peer_full);
                increment(&template.counters.frames_dropped);
            }
            Ok(Some(Err(InboundQueueError::Full(InboundQueueFull::Global)))) => {
                increment(&template.counters.inbound_full);
                increment(&template.counters.inbound_global_full);
                increment(&template.counters.frames_dropped);
            }
            Ok(Some(Err(InboundQueueError::Closed))) => return,
            Ok(Some(Err(InboundQueueError::Poisoned))) => {
                template.control.fail("inbound queue state is poisoned");
                return;
            }
            Ok(None) => {
                increment(&template.counters.frames_dropped);
                return;
            }
            Err(()) => {
                template.control.fail("inbound epoch state is poisoned");
                return;
            }
        }
    }
}
