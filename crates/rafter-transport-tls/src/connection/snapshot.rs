//! Dedicated snapshot-resolution lane for one physical peer.

use std::{sync::Arc, thread, time::Duration};

use rafter::{InstallSnapshotChunk, Message};

use crate::diagnostics::{increment, Counters, PeerCounters};
use crate::queue::{OutboundItem, OutboundQueue, RequeueOutcome};
use crate::runtime::RuntimeControl;
use crate::snapshot::{SnapshotChunkResolveRequest, SnapshotResolverHandle};
use crate::{GroupIdCodec, PeerFrameCodec, PeerFrameScratch};

pub(crate) struct SnapshotContext<G, C> {
    pub(crate) resolver: SnapshotResolverHandle<G>,
    pub(crate) codec: Arc<PeerFrameCodec<G, C>>,
    pub(crate) queue: Arc<OutboundQueue<G>>,
    pub(crate) control: Arc<RuntimeControl>,
    pub(crate) counters: Arc<Counters>,
    pub(crate) peer_counters: Arc<PeerCounters>,
    pub(crate) poll: Duration,
}

pub(crate) fn snapshot_loop<G, C>(context: &SnapshotContext<G, C>)
where
    G: Send + 'static,
    C: GroupIdCodec<G>,
{
    let mut scratch = PeerFrameScratch::new();
    loop {
        if context.control.terminal()
            || context.control.shutdown_grace_expired()
            || context.control.stopping_while_paused()
        {
            break;
        }
        if context.control.starting() {
            thread::sleep(context.poll);
            continue;
        }
        let item = match context.queue.pop_snapshot_timeout(context.poll) {
            Ok(Some(item)) => item,
            Ok(None) => match context.queue.snapshots_closed_and_empty() {
                Ok(true) => break,
                Ok(false) => continue,
                Err(_) => return fail_queue(context),
            },
            Err(_) => return fail_queue(context),
        };
        resolve_one(context, item, &mut scratch);
    }
}

fn resolve_one<G, C>(
    context: &SnapshotContext<G, C>,
    mut item: OutboundItem<G>,
    scratch: &mut PeerFrameScratch,
) where
    C: GroupIdCodec<G>,
{
    if !item.is_authorized() {
        increment(&context.counters.invalidated_queued_frames);
        return drop_item(context, &item);
    }
    let Some((group_id, chunk)) = item.snapshot_parts() else {
        context
            .control
            .fail("snapshot resolver lane received a prepared outbound item");
        return drop_item(context, &item);
    };

    let bytes = match context.resolver.resolve(SnapshotChunkResolveRequest::new(
        group_id,
        item.from(),
        item.to(),
        chunk,
    )) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => {
            increment(&context.counters.snapshot_source_refusals);
            context.peer_counters.snapshot_source_refused();
            return drop_item(context, &item);
        }
        Err(_) => {
            increment(&context.counters.snapshot_resolve_failures);
            context.peer_counters.snapshot_resolve_failed();
            return drop_item(context, &item);
        }
    };
    let Ok(expected_len) = usize::try_from(chunk.len) else {
        context
            .control
            .fail("snapshot chunk length cannot be represented by this target");
        return drop_item(context, &item);
    };
    if bytes.len() != expected_len {
        increment(&context.counters.snapshot_resolution_mismatches);
        context.peer_counters.snapshot_resolution_mismatched();
        return drop_item(context, &item);
    }

    let message = Message::InstallSnapshotChunk(InstallSnapshotChunk {
        term: chunk.term,
        leader_id: chunk.leader_id,
        transfer_id: chunk.transfer_id,
        metadata: chunk.metadata.clone(),
        total_payload_len: chunk.total_payload_len,
        application_payload_crc32: chunk.application_payload_crc32,
        offset: chunk.offset,
        chunk: bytes,
        done: chunk.done,
    });
    let prepared =
        match context
            .codec
            .prepare_message(group_id, item.from(), item.to(), &message, scratch)
        {
            Ok(prepared) => prepared,
            Err(error) => {
                increment(&context.counters.snapshot_resolution_mismatches);
                context.peer_counters.snapshot_resolution_mismatched();
                context.control.fail(format!(
                    "snapshot frame no longer encodes after bounded admission: {error}"
                ));
                return drop_item(context, &item);
            }
        };
    if !item.install_prepared(prepared) {
        increment(&context.counters.snapshot_resolution_mismatches);
        context.peer_counters.snapshot_resolution_mismatched();
        context
            .control
            .fail("resolved snapshot frame changed its admitted byte reservation");
        return drop_item(context, &item);
    }
    if !item.is_authorized() {
        increment(&context.counters.invalidated_queued_frames);
        return drop_item(context, &item);
    }
    if context.control.terminal() || context.control.shutdown_grace_expired() {
        return drop_item(context, &item);
    }

    increment(&context.counters.snapshot_chunks_resolved);
    context.peer_counters.snapshot_resolved();
    match context.queue.requeue_ready(item) {
        Ok(RequeueOutcome::Queued) => {}
        Ok(RequeueOutcome::SenderStopped) => {
            increment(&context.counters.frames_dropped);
            context.peer_counters.dropped();
        }
        Err(_) => fail_queue(context),
    }
}

fn drop_item<G, C>(context: &SnapshotContext<G, C>, item: &OutboundItem<G>) {
    increment(&context.counters.frames_dropped);
    context.peer_counters.dropped();
    if context.queue.release(item).is_err() {
        fail_queue(context);
    }
}

fn fail_queue<G, C>(context: &SnapshotContext<G, C>) {
    context.control.fail("outbound queue state is poisoned");
}
