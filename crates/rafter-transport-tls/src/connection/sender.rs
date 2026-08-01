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

#[allow(clippy::too_many_lines)]
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
        if context.control.terminal()
            || context.control.shutdown_grace_expired()
            || context.control.stopping_while_paused()
        {
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
            match dial(context, connected_once) {
                Ok(open) => {
                    connected_once = true;
                    connection = Some(open);
                }
                Err(DialError::Retry) => {
                    sleep_interruptibly(context, context.timeouts.redial());
                    continue;
                }
                Err(DialError::Terminal(message)) => {
                    context.control.fail(message);
                    break;
                }
            }
        }

        if current.is_none() {
            match context.queue.pop_timeout(context.timeouts.poll()) {
                Ok(item) => current = item,
                Err(OutboundQueueError::Closed) => {}
                Err(OutboundQueueError::Full(_)) => {
                    context
                        .control
                        .fail("outbound queue returned a pop-time full error");
                    break;
                }
                Err(OutboundQueueError::Poisoned) => {
                    context.control.fail("outbound queue state is poisoned");
                    break;
                }
            }
        }
        if current.is_none() {
            match context.queue.is_closed_and_empty() {
                Ok(true) => break,
                Ok(false) => continue,
                Err(_) => {
                    context.control.fail("outbound queue state is poisoned");
                    break;
                }
            }
        }

        // A shutdown may have begun while this worker was waiting for work. An
        // accepted frame still gets its bounded drain opportunity.
        if connection.is_none() {
            match dial(context, connected_once) {
                Ok(open) => {
                    connected_once = true;
                    connection = Some(open);
                }
                Err(DialError::Retry) => {
                    sleep_interruptibly(context, context.timeouts.redial());
                    continue;
                }
                Err(DialError::Terminal(message)) => {
                    context.control.fail(message);
                    break;
                }
            }
        }

        let Some(item_bytes) = current.as_ref().map(OutboundItem::bytes) else {
            continue;
        };
        let Some(frame_bytes) = connection.as_ref().map(|open| open.frame_bytes) else {
            continue;
        };
        if item_bytes > frame_bytes {
            increment(&context.counters.frame_too_large);
            drop_current(context, &mut current);
            continue;
        }

        let Some(item) = current.as_mut() else {
            continue;
        };
        match prepare_snapshot(context, item, &mut scratch) {
            SnapshotPreparation::Ready => {}
            SnapshotPreparation::Drop => {
                drop_current(context, &mut current);
                continue;
            }
            SnapshotPreparation::Terminal(message) => {
                context.control.fail(message);
                break;
            }
        }

        let sequence = {
            let Some(open) = connection.as_mut() else {
                continue;
            };
            let Ok(sequence) = open.sequence.take_next() else {
                increment(&context.counters.sequence_violations);
                connection = None;
                continue;
            };
            sequence
        };
        let Some(frame) = current.as_ref().and_then(OutboundItem::prepared) else {
            context
                .control
                .fail("outbound item is not prepared for transmission");
            break;
        };
        frame.encode_into(sequence, &mut encoded);
        let write_result = {
            let Some(open) = connection.as_mut() else {
                continue;
            };
            write_all_flush(&mut open.stream, &encoded)
        };
        if write_result.is_err() {
            increment(&context.counters.tls_failures);
            connection = None;
            continue;
        }

        increment(&context.counters.frames_sent);
        context.peer_counters.sent();
        release_current(context, &mut current);
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
