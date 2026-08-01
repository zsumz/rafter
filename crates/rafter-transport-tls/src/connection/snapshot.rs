//! Sender-worker snapshot directive resolution and frame materialization.

use rafter::{InstallSnapshotChunk, Message};

use crate::diagnostics::increment;
use crate::queue::OutboundItem;
use crate::snapshot::SnapshotChunkResolveRequest;
use crate::{GroupIdCodec, PeerFrameScratch};

use super::sender::SenderContext;

#[derive(Debug)]
pub(crate) enum SnapshotPreparation {
    Ready,
    Drop,
    Terminal(String),
}

pub(crate) fn prepare_snapshot<G, C>(
    context: &SenderContext<G, C>,
    item: &mut OutboundItem<G>,
    scratch: &mut PeerFrameScratch,
) -> SnapshotPreparation
where
    C: GroupIdCodec<G>,
{
    if item.prepared().is_some() {
        return SnapshotPreparation::Ready;
    }
    let Some(resolver) = context.snapshot_resolver.as_ref() else {
        return SnapshotPreparation::Terminal(
            "queued snapshot directive has no installed resolver".to_owned(),
        );
    };
    let Some((group_id, chunk)) = item.snapshot_parts() else {
        return SnapshotPreparation::Terminal(
            "outbound item has neither a prepared frame nor a snapshot directive".to_owned(),
        );
    };

    let bytes = match resolver.resolve(SnapshotChunkResolveRequest::new(
        group_id,
        item.from(),
        item.to(),
        chunk,
    )) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => {
            increment(&context.counters.snapshot_source_refusals);
            context.peer_counters.snapshot_source_refused();
            return SnapshotPreparation::Drop;
        }
        Err(_) => {
            increment(&context.counters.snapshot_resolve_failures);
            context.peer_counters.snapshot_resolve_failed();
            return SnapshotPreparation::Drop;
        }
    };
    let Ok(expected_len) = usize::try_from(chunk.len) else {
        return SnapshotPreparation::Terminal(
            "snapshot chunk length cannot be represented by this target".to_owned(),
        );
    };
    if bytes.len() != expected_len {
        increment(&context.counters.snapshot_resolution_mismatches);
        context.peer_counters.snapshot_resolution_mismatched();
        return SnapshotPreparation::Drop;
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
                return SnapshotPreparation::Terminal(format!(
                    "snapshot frame no longer encodes after bounded admission: {error}"
                ));
            }
        };
    if !item.install_prepared(prepared) {
        increment(&context.counters.snapshot_resolution_mismatches);
        context.peer_counters.snapshot_resolution_mismatched();
        return SnapshotPreparation::Terminal(
            "resolved snapshot frame changed its admitted byte reservation".to_owned(),
        );
    }

    increment(&context.counters.snapshot_chunks_resolved);
    context.peer_counters.snapshot_resolved();
    SnapshotPreparation::Ready
}
