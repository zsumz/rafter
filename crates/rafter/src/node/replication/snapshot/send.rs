//! Leader-side snapshot chunk construction.

use crate::{NodeId, RaftSnapshot, SnapshotChunkSend};

use super::super::super::{state::ProgressMode, Node, Output};

/// Bytes of snapshot payload per chunk directive. Comfortably below the
/// default append budget (`DEFAULT_MAX_APPEND_ENTRIES_BYTES`, 512 KiB), so a
/// transport whose frame limit accommodates the append budget accommodates
/// snapshot chunks too.
const INSTALL_SNAPSHOT_CHUNK_BYTES: u32 = 64 * 1024;

impl Node {
    pub(in crate::node::replication) fn install_snapshot_chunk_to(
        &self,
        follower_id: NodeId,
        snapshot: &RaftSnapshot,
    ) -> Output {
        let total_payload_len = snapshot.application_payload_len;
        let offset = self.snapshot_send_offset(follower_id, total_payload_len);
        let len = snapshot_chunk_len(total_payload_len, offset);
        let done = offset + u64::from(len) == total_payload_len;

        Output::SendSnapshotChunk {
            to: follower_id,
            chunk: SnapshotChunkSend {
                term: self.current_term(),
                leader_id: self.id(),
                transfer_id: snapshot.transfer_id(),
                metadata: snapshot.metadata.clone(),
                total_payload_len,
                application_payload_crc32: snapshot.application_payload_crc32,
                offset,
                len,
                done,
            },
        }
    }

    fn snapshot_send_offset(&self, follower_id: NodeId, total_payload_len: u64) -> u64 {
        self.leader
            .progress
            .get(follower_id)
            .and_then(|progress| match progress.mode {
                ProgressMode::Snapshot { next_offset } => Some(next_offset),
                ProgressMode::Probe { .. } | ProgressMode::Replicate => None,
            })
            .unwrap_or(0)
            .min(total_payload_len)
    }
}

fn snapshot_chunk_len(total_payload_len: u64, offset: u64) -> u32 {
    let remaining = total_payload_len.saturating_sub(offset);
    let bounded = remaining.min(u64::from(INSTALL_SNAPSHOT_CHUNK_BYTES));
    u32::try_from(bounded).unwrap_or(INSTALL_SNAPSHOT_CHUNK_BYTES)
}
