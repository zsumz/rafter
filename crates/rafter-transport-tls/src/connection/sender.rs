//! One persistent outbound connection worker per configured physical peer.

mod retry;
mod transmit;

use std::{sync::Arc, thread};

use crate::diagnostics::{add, increment, Counters, PeerCounters};
use crate::queue::{OutboundItem, OutboundQueue, OutboundQueueError};
use crate::runtime::{RuntimeControl, SessionStoreHandle};
use crate::{
    CertificateDirectory, EndpointBook, PeerId, TlsHandshakeConfig, TlsIdentity, TransportTimeouts,
};

use super::dial::OutboundConnection;

use self::retry::connect;
use self::transmit::transmit_current;
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
