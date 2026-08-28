//! Chunked snapshot-transfer acceptance checks.
//!
//! A node may only accept a chunk that continues the staged prefix it already
//! holds, under one unchanging descriptor, and may only install once the whole
//! advertised byte range has arrived.

use rafter::{InstallSnapshotChunk, NodeId, PendingSnapshotTransfer, RaftSnapshot};

use crate::{Cluster, Envelope};

use super::transitions::snapshot_identity_changed;
use super::{AcceptedSnapshotChunkEffect, SnapshotHistory, SnapshotTransferDescriptor};

impl AcceptedSnapshotChunkEffect {
    const fn before_received(self) -> u64 {
        match self {
            Self::Staged {
                before_received, ..
            }
            | Self::Installed {
                before_received, ..
            } => before_received,
        }
    }
}

pub(super) fn accepted_chunk_effect(
    before: &Cluster,
    after: &Cluster,
    node_id: NodeId,
    leader_id: NodeId,
    request: &InstallSnapshotChunk,
) -> Option<AcceptedSnapshotChunkEffect> {
    let after_node = after.node(node_id);
    if snapshot_identity_changed(before, after, node_id) {
        let before_received = before
            .snapshot_staging
            .get(&node_id)
            .filter(|staged| {
                staged.leader_id == leader_id
                    && staged.transfer_id == request.transfer_id
                    && staged.metadata == request.metadata
                    && staged.total_payload_len == request.total_payload_len
                    && staged.application_payload_crc32 == request.application_payload_crc32
            })
            .map_or(0, |staged| staged.bytes.len() as u64);
        return Some(AcceptedSnapshotChunkEffect::Installed {
            before_received,
            snapshot_index: after_node.snapshot_index(),
        });
    }

    let after_staged = after.snapshot_staging.get(&node_id)?;
    let before_staged = before.snapshot_staging.get(&node_id);
    let same_staged_transfer = before_staged.is_some_and(|staged| {
        staged.leader_id == after_staged.leader_id
            && staged.transfer_id == after_staged.transfer_id
            && staged.metadata == after_staged.metadata
            && staged.total_payload_len == after_staged.total_payload_len
            && staged.application_payload_crc32 == after_staged.application_payload_crc32
    });
    let before_received = if same_staged_transfer {
        before_staged.map_or(0, |staged| staged.bytes.len() as u64)
    } else {
        0
    };
    let after_received = after_staged.bytes.len() as u64;
    let expected_after = before_received.checked_add(request.chunk.len() as u64)?;
    let suffix_offset = usize::try_from(before_received).ok()?;
    let suffix = after_staged.bytes.get(suffix_offset..)?;
    (after_received == expected_after && suffix == request.chunk.as_slice()).then_some(
        AcceptedSnapshotChunkEffect::Staged {
            before_received,
            after_received,
        },
    )
}

pub(super) fn chunk_identity_issue(
    history: &mut SnapshotHistory,
    after: &Cluster,
    envelope: &Envelope,
    request: &InstallSnapshotChunk,
    effect: AcceptedSnapshotChunkEffect,
) -> Option<String> {
    let advertised = SnapshotTransferDescriptor {
        leader_id: request.leader_id,
        snapshot: RaftSnapshot::new(
            request.metadata.clone(),
            request.total_payload_len,
            request.application_payload_crc32,
        ),
    };
    if envelope.from != request.leader_id {
        return Some(format!(
            "{} accepted transfer {} from envelope sender {} but request leader was {}",
            envelope.to, request.transfer_id, envelope.from, request.leader_id
        ));
    }
    if request.transfer_id != advertised.snapshot.transfer_id() {
        return Some(format!(
            "{} accepted transfer id {} with descriptor identity {}",
            envelope.to,
            request.transfer_id,
            advertised.snapshot.transfer_id()
        ));
    }
    let key = (envelope.to, request.transfer_id);
    if let Some(previous) = history.chunk_descriptors.get(&key) {
        if previous != &advertised {
            return Some(format!(
                "{} accepted chunk for transfer {} with a descriptor different from an earlier accepted chunk",
                envelope.to, request.transfer_id
            ));
        }
    } else {
        history.chunk_descriptors.insert(key, advertised.clone());
    }

    let actual_snapshot = match effect {
        AcceptedSnapshotChunkEffect::Staged { .. } => {
            after.snapshot_staging.get(&envelope.to).map(|staged| {
                RaftSnapshot::new(
                    staged.metadata.clone(),
                    staged.total_payload_len,
                    staged.application_payload_crc32,
                )
            })
        }
        AcceptedSnapshotChunkEffect::Installed { .. } => {
            after.node(envelope.to).snapshot().cloned()
        }
    };
    if actual_snapshot.as_ref() != Some(&advertised.snapshot) {
        return Some(format!(
            "{} accepted chunk for transfer {} but retained a different snapshot descriptor",
            envelope.to, request.transfer_id
        ));
    }
    None
}

pub(super) fn chunk_offset_issue(
    after: &Cluster,
    node_id: NodeId,
    request: &InstallSnapshotChunk,
    effect: AcceptedSnapshotChunkEffect,
) -> Option<String> {
    let chunk_len = request.chunk.len() as u64;
    let Some(end) = request.offset.checked_add(chunk_len) else {
        return Some(format!(
            "{node_id} accepted a snapshot chunk whose byte range overflowed"
        ));
    };
    let valid_shape = request.offset <= request.total_payload_len
        && end <= request.total_payload_len
        && if request.done {
            end == request.total_payload_len
        } else {
            chunk_len > 0 && end < request.total_payload_len
        };
    if !valid_shape {
        return Some(format!(
            "{node_id} accepted invalid chunk range {}..{end} for payload length {} (done={})",
            request.offset, request.total_payload_len, request.done
        ));
    }
    if request.offset != effect.before_received() {
        return Some(format!(
            "{node_id} accepted chunk at offset {} while the staged prefix ended at {}",
            request.offset,
            effect.before_received()
        ));
    }
    if let AcceptedSnapshotChunkEffect::Staged { after_received, .. } = effect {
        if after_received != end {
            return Some(format!(
                "{node_id} recorded {after_received} received bytes after accepting range {}..{end}",
                request.offset
            ));
        }
        if after
            .node(node_id)
            .pending_snapshot_transfer()
            .is_none_or(|pending| pending.received_bytes() != after_received)
        {
            return Some(format!(
                "{node_id} staged {after_received} bytes without matching pending-transfer progress"
            ));
        }
    }
    None
}

pub(super) fn install_completeness_issue(
    after: &Cluster,
    node_id: NodeId,
    request: &InstallSnapshotChunk,
    effect: AcceptedSnapshotChunkEffect,
) -> Option<String> {
    let AcceptedSnapshotChunkEffect::Installed {
        before_received, ..
    } = effect
    else {
        return None;
    };
    let end = request.offset.checked_add(request.chunk.len() as u64);
    if !request.done || request.offset != before_received || end != Some(request.total_payload_len)
    {
        return Some(format!(
            "{node_id} installed transfer {} before the complete byte range was present: staged={before_received}, offset={}, chunk={}, total={}, done={}",
            request.transfer_id,
            request.offset,
            request.chunk.len(),
            request.total_payload_len,
            request.done
        ));
    }
    if after.snapshot_staging.contains_key(&node_id)
        || after.node(node_id).pending_snapshot_transfer().is_some()
    {
        return Some(format!(
            "{node_id} installed transfer {} but retained partial transfer state",
            request.transfer_id
        ));
    }
    None
}

pub(super) fn descriptor_from_pending(
    pending: &PendingSnapshotTransfer,
) -> SnapshotTransferDescriptor {
    SnapshotTransferDescriptor {
        leader_id: pending.leader_id,
        snapshot: RaftSnapshot::new(
            pending.metadata.clone(),
            pending.total_payload_len,
            pending.application_payload_crc32,
        ),
    }
}
