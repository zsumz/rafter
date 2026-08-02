//! One persistent outbound connection worker per configured physical peer.

mod retry;

use std::{sync::Arc, thread};

use crate::diagnostics::{add, increment, Counters, PeerCounters};
use crate::queue::{OutboundItem, OutboundQueue, OutboundQueueError, RequeueOutcome};
use crate::runtime::{RuntimeControl, SessionStoreHandle};
use crate::{
    CertificateDirectory, EndpointBook, PeerId, TlsHandshakeConfig, TlsIdentity, TransportTimeouts,
};

use super::{dial::OutboundConnection, io::write_all_flush};

use self::retry::{backoff_after_failure, connect};
use super::dial::DialAttemptState;

pub(crate) struct SenderContext<G> {
    pub(crate) peer: PeerId,
    pub(crate) endpoints: EndpointBook,
    pub(crate) identity: TlsIdentity,
    pub(crate) certificates: CertificateDirectory,
    pub(crate) handshake: TlsHandshakeConfig,
    pub(crate) sessions: SessionStoreHandle,
    pub(crate) queue: Arc<OutboundQueue<G>>,
    pub(crate) control: Arc<RuntimeControl>,
    pub(crate) counters: Arc<Counters>,
    pub(crate) peer_counters: Arc<PeerCounters>,
    pub(crate) timeouts: TransportTimeouts,
}

pub(crate) fn sender_loop<G>(context: &SenderContext<G>)
where
    G: Send + 'static,
{
    let mut connection: Option<OutboundConnection> = None;
    let mut current: Option<OutboundItem<G>> = None;
    let mut encoded = Vec::new();
    let mut connected_once = false;
    let mut retry_attempt = 0_u32;
    let mut dial_attempts = DialAttemptState::default();

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
            match connect(
                context,
                &mut connection,
                &mut connected_once,
                &mut retry_attempt,
                &mut dial_attempts,
            ) {
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
            match connect(
                context,
                &mut connection,
                &mut connected_once,
                &mut retry_attempt,
                &mut dial_attempts,
            ) {
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
                &mut retry_attempt,
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
    match context.queue.stop_sender_and_discard_queued() {
        Ok(discarded) => {
            add(&context.counters.frames_dropped, discarded.frames);
            context.peer_counters.dropped_many(discarded.frames);
        }
        Err(_) => context.control.fail("outbound queue state is poisoned"),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WorkerStep {
    Ready,
    Retry,
    Stop,
}

pub(super) fn should_stop<G>(context: &SenderContext<G>) -> bool {
    context.control.terminal()
        || context.control.shutdown_grace_expired()
        || context.control.stopping_while_paused()
}

fn poll_work<G>(context: &SenderContext<G>, current: &mut Option<OutboundItem<G>>) -> WorkerStep {
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

fn transmit_current<G>(
    context: &SenderContext<G>,
    connection: &mut Option<OutboundConnection>,
    current: &mut Option<OutboundItem<G>>,
    encoded: &mut Vec<u8>,
    retry_attempt: &mut u32,
) -> WorkerStep {
    let Some(endpoint_current) = endpoint_is_current(context, connection.as_ref()) else {
        return WorkerStep::Stop;
    };
    if !endpoint_current {
        *connection = None;
        return WorkerStep::Retry;
    }
    let Some(item_bytes) = current.as_ref().map(OutboundItem::bytes) else {
        return WorkerStep::Retry;
    };
    if current.as_ref().is_some_and(|item| !item.is_authorized()) {
        increment(&context.counters.invalidated_queued_frames);
        drop_current(context, current);
        return WorkerStep::Retry;
    }
    let Some(frame_bytes) = connection.as_ref().map(|open| open.frame_bytes) else {
        return WorkerStep::Retry;
    };
    if item_bytes > frame_bytes {
        increment(&context.counters.frame_too_large);
        drop_current(context, current);
        context.control.fail(format!(
            "accepted outbound frame of {item_bytes} bytes exceeds exact negotiated bound of \
             {frame_bytes} bytes"
        ));
        return WorkerStep::Stop;
    }

    let Some(open) = connection.as_mut() else {
        return WorkerStep::Retry;
    };
    if current.as_ref().is_some_and(|item| !item.is_authorized()) {
        increment(&context.counters.invalidated_queued_frames);
        drop_current(context, current);
        return WorkerStep::Retry;
    }
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
    let Some(endpoint_current) = endpoint_is_current(context, connection.as_ref()) else {
        return WorkerStep::Stop;
    };
    if !endpoint_current {
        *connection = None;
        return WorkerStep::Retry;
    }
    if current.as_ref().is_some_and(|item| !item.is_authorized()) {
        increment(&context.counters.invalidated_queued_frames);
        *connection = None;
        drop_current(context, current);
        return WorkerStep::Retry;
    }
    let Some(open) = connection.as_mut() else {
        return WorkerStep::Retry;
    };
    if write_all_flush(&mut open.stream, encoded).is_err() {
        return handle_write_failure(context, connection, current, retry_attempt);
    }

    if open.stability_proven() {
        *retry_attempt = 0;
    }
    increment(&context.counters.frames_sent);
    context.peer_counters.sent();
    release_current(context, current);
    WorkerStep::Ready
}

fn handle_write_failure<G>(
    context: &SenderContext<G>,
    connection: &mut Option<OutboundConnection>,
    current: &mut Option<OutboundItem<G>>,
    retry_attempt: &mut u32,
) -> WorkerStep {
    increment(&context.counters.tls_failures);
    *connection = None;
    if current
        .as_ref()
        .is_some_and(|item| item.class() != crate::TrafficClass::Control)
    {
        if !current.as_mut().is_some_and(OutboundItem::retry_bulk) {
            increment(&context.counters.retry_exhausted_frames);
            drop_current(context, current);
            backoff_after_failure(context, retry_attempt);
            return WorkerStep::Retry;
        }
        let Some(item) = current.take() else {
            context.control.fail("outbound retry lost its current item");
            return WorkerStep::Stop;
        };
        match context.queue.requeue_ready(item) {
            Ok(RequeueOutcome::Queued) => {}
            Ok(RequeueOutcome::SenderStopped) => {
                increment(&context.counters.frames_dropped);
                context.peer_counters.dropped();
                return WorkerStep::Stop;
            }
            Err(_) => {
                context.control.fail("outbound queue state is poisoned");
                return WorkerStep::Stop;
            }
        }
    }
    backoff_after_failure(context, retry_attempt);
    WorkerStep::Retry
}

fn endpoint_is_current<G>(
    context: &SenderContext<G>,
    connection: Option<&OutboundConnection>,
) -> Option<bool> {
    let Some(open) = connection else {
        return Some(true);
    };
    match context.endpoints.snapshot(&context.peer) {
        Ok(Some(snapshot)) => Some(snapshot.generation() == open.endpoint_generation),
        Ok(None) => Some(false),
        Err(error) => {
            context.control.fail(format!(
                "endpoint book failed for {}: {error}",
                context.peer
            ));
            None
        }
    }
}

fn release_current<G>(context: &SenderContext<G>, current: &mut Option<OutboundItem<G>>) {
    let Some(item) = current.take() else {
        return;
    };
    if context.queue.release(&item).is_err() {
        context.control.fail("outbound queue state is poisoned");
    }
}

fn drop_current<G>(context: &SenderContext<G>, current: &mut Option<OutboundItem<G>>) {
    increment(&context.counters.frames_dropped);
    context.peer_counters.dropped();
    release_current(context, current);
}
