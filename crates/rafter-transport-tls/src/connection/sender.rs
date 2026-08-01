//! One persistent outbound connection worker per configured physical peer.

use std::{sync::Arc, thread, time::Duration};

use crate::diagnostics::{add, increment, Counters, PeerCounters};
use crate::queue::{OutboundItem, OutboundQueue, OutboundQueueError};
use crate::runtime::{RuntimeControl, SessionStoreHandle};
use crate::snapshot::SnapshotResolverHandle;
use crate::{
    CertificateDirectory, EndpointBook, GroupIdCodec, PeerFrameCodec, PeerFrameScratch, PeerId,
    TlsHandshakeConfig, TlsIdentity, TransportTimeouts,
};

use super::{
    dial::{dial, DialError, OutboundConnection},
    io::write_all_flush,
    snapshot::{prepare_snapshot, SnapshotPreparation},
};

pub(crate) struct SenderContext<G, C> {
    pub(crate) peer: PeerId,
    pub(crate) endpoints: EndpointBook,
    pub(crate) identity: TlsIdentity,
    pub(crate) certificates: CertificateDirectory,
    pub(crate) handshake: TlsHandshakeConfig,
    pub(crate) sessions: SessionStoreHandle,
    pub(crate) codec: Arc<PeerFrameCodec<G, C>>,
    pub(crate) snapshot_resolver: Option<SnapshotResolverHandle<G>>,
    pub(crate) queue: Arc<OutboundQueue<G>>,
    pub(crate) control: Arc<RuntimeControl>,
    pub(crate) counters: Arc<Counters>,
    pub(crate) peer_counters: Arc<PeerCounters>,
    pub(crate) timeouts: TransportTimeouts,
}

pub(crate) fn sender_loop<G, C>(context: &SenderContext<G, C>)
where
    G: Send + 'static,
    C: GroupIdCodec<G>,
{
    let mut connection: Option<OutboundConnection> = None;
    let mut current: Option<OutboundItem<G>> = None;
    let mut encoded = Vec::new();
    let mut scratch = PeerFrameScratch::new();
    let mut connected_once = false;

    loop {
        if should_stop(context) {
            break;
        }
        if context.control.starting() {
            thread::sleep(context.timeouts.poll());
            continue;
        }

        // While serving, keep the physical peer connection established even
        // when no frame is queued. During shutdown, dial only to drain work
        // that was accepted before admission closed.
        if connection.is_none() && (!context.control.shutdown_requested() || current.is_some()) {
            match connect(context, &mut connection, &mut connected_once) {
                WorkerStep::Ready => {}
                WorkerStep::Retry => continue,
                WorkerStep::Stop => break,
            }
        }

        if current.is_none() {
            match poll_work(context, &mut current) {
                WorkerStep::Ready => {}
                WorkerStep::Retry => continue,
                WorkerStep::Stop => break,
            }
        }

        // A shutdown may have begun while this worker was waiting for work. An
        // accepted frame still gets its bounded drain opportunity.
        if connection.is_none() {
            match connect(context, &mut connection, &mut connected_once) {
                WorkerStep::Ready => {}
                WorkerStep::Retry => continue,
                WorkerStep::Stop => break,
            }
        }
        if matches!(
            transmit_current(
                context,
                &mut connection,
                &mut current,
                &mut encoded,
                &mut scratch,
            ),
            WorkerStep::Stop
        ) {
            break;
        }
    }

    drop(connection);
    if current.is_some() {
        drop_current(context, &mut current);
    }
    match context.queue.discard_queued() {
        Ok(discarded) => {
            add(&context.counters.frames_dropped, discarded.frames);
            context.peer_counters.dropped_many(discarded.frames);
        }
        Err(_) => context.control.fail("outbound queue state is poisoned"),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerStep {
    Ready,
    Retry,
    Stop,
}

fn should_stop<G, C>(context: &SenderContext<G, C>) -> bool {
    context.control.terminal()
        || context.control.shutdown_grace_expired()
        || context.control.stopping_while_paused()
}

fn connect<G, C>(
    context: &SenderContext<G, C>,
    connection: &mut Option<OutboundConnection>,
    connected_once: &mut bool,
) -> WorkerStep {
    match dial(context, *connected_once) {
        Ok(open) => {
            *connected_once = true;
            *connection = Some(open);
            WorkerStep::Ready
        }
        Err(DialError::Retry) => {
            sleep_interruptibly(context, context.timeouts.redial());
            WorkerStep::Retry
        }
        Err(DialError::Terminal(message)) => {
            context.control.fail(message);
            WorkerStep::Stop
        }
    }
}

fn poll_work<G, C>(
    context: &SenderContext<G, C>,
    current: &mut Option<OutboundItem<G>>,
) -> WorkerStep {
    match context.queue.pop_timeout(context.timeouts.poll()) {
        Ok(item) => *current = item,
        Err(OutboundQueueError::Closed) => {}
        Err(OutboundQueueError::Full(_)) => {
            context
                .control
                .fail("outbound queue returned a pop-time full error");
            return WorkerStep::Stop;
        }
        Err(OutboundQueueError::Poisoned) => {
            context.control.fail("outbound queue state is poisoned");
            return WorkerStep::Stop;
        }
    }
    if current.is_some() {
        return WorkerStep::Ready;
    }
    match context.queue.is_closed_and_empty() {
        Ok(true) => WorkerStep::Stop,
        Ok(false) => WorkerStep::Retry,
        Err(_) => {
            context.control.fail("outbound queue state is poisoned");
            WorkerStep::Stop
        }
    }
}

fn transmit_current<G, C>(
    context: &SenderContext<G, C>,
    connection: &mut Option<OutboundConnection>,
    current: &mut Option<OutboundItem<G>>,
    encoded: &mut Vec<u8>,
    scratch: &mut PeerFrameScratch,
) -> WorkerStep
where
    C: GroupIdCodec<G>,
{
    let Some(item_bytes) = current.as_ref().map(OutboundItem::bytes) else {
        return WorkerStep::Retry;
    };
    let Some(frame_bytes) = connection.as_ref().map(|open| open.frame_bytes) else {
        return WorkerStep::Retry;
    };
    if item_bytes > frame_bytes {
        increment(&context.counters.frame_too_large);
        drop_current(context, current);
        return WorkerStep::Retry;
    }

    let Some(item) = current.as_mut() else {
        return WorkerStep::Retry;
    };
    match prepare_snapshot(context, item, scratch) {
        SnapshotPreparation::Ready => {}
        SnapshotPreparation::Drop => {
            drop_current(context, current);
            return WorkerStep::Retry;
        }
        SnapshotPreparation::Terminal(message) => {
            context.control.fail(message);
            return WorkerStep::Stop;
        }
    }

    let Some(open) = connection.as_mut() else {
        return WorkerStep::Retry;
    };
    let Ok(sequence) = open.sequence.take_next() else {
        increment(&context.counters.sequence_violations);
        *connection = None;
        return WorkerStep::Retry;
    };
    let Some(frame) = current.as_ref().and_then(OutboundItem::prepared) else {
        context
            .control
            .fail("outbound item is not prepared for transmission");
        return WorkerStep::Stop;
    };
    frame.encode_into(sequence, encoded);
    let Some(open) = connection.as_mut() else {
        return WorkerStep::Retry;
    };
    if write_all_flush(&mut open.stream, encoded).is_err() {
        increment(&context.counters.tls_failures);
        *connection = None;
        return WorkerStep::Retry;
    }

    increment(&context.counters.frames_sent);
    context.peer_counters.sent();
    release_current(context, current);
    WorkerStep::Ready
}

fn release_current<G, C>(context: &SenderContext<G, C>, current: &mut Option<OutboundItem<G>>) {
    let Some(item) = current.take() else {
        return;
    };
    if context.queue.release(&item).is_err() {
        context.control.fail("outbound queue state is poisoned");
    }
}

fn drop_current<G, C>(context: &SenderContext<G, C>, current: &mut Option<OutboundItem<G>>) {
    increment(&context.counters.frames_dropped);
    context.peer_counters.dropped();
    release_current(context, current);
}

fn sleep_interruptibly<G, C>(context: &SenderContext<G, C>, duration: Duration) {
    let mut remaining = duration;
    while !remaining.is_zero()
        && !context.control.terminal()
        && !context.control.shutdown_grace_expired()
    {
        let step = remaining.min(context.timeouts.poll());
        thread::sleep(step);
        remaining = remaining.saturating_sub(step);
    }
}
