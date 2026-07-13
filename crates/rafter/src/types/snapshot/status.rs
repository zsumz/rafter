//! Read-only snapshot transfer status and rejection diagnostics.

use super::super::{LogIndex, NodeId};
use super::SnapshotTransferId;

/// Snapshot transfer observability for one node.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct SnapshotTransferStatus {
    pub leader: Vec<LeaderSnapshotTransferStatus>,
    pub follower: Option<FollowerSnapshotTransferStatus>,
    pub rejected_chunks: SnapshotChunkRejectionCounters,
}

impl SnapshotTransferStatus {
    /// Returns whether no leader transfer, follower transfer, or rejection
    /// counter is active.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.leader.is_empty() && self.follower.is_none() && self.rejected_chunks.is_empty()
    }
}

/// Leader-side snapshot transfer progress for one follower.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LeaderSnapshotTransferStatus {
    pub follower_id: NodeId,
    pub transfer_id: SnapshotTransferId,
    pub last_included_index: LogIndex,
    pub total_bytes: u64,
    pub next_offset: u64,
}

/// Follower-side snapshot transfer progress from one leader.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct FollowerSnapshotTransferStatus {
    pub leader_id: NodeId,
    pub transfer_id: SnapshotTransferId,
    pub last_included_index: LogIndex,
    pub total_bytes: u64,
    pub received_bytes: u64,
}

/// Counters for rejected inbound snapshot chunks.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct SnapshotChunkRejectionCounters {
    pub stale_term: u64,
    pub wrong_transfer: u64,
    pub metadata_mismatch: u64,
    pub out_of_order_offset: u64,
    pub invalid_bounds: u64,
    pub corrupt_persisted_pending_transfer: u64,
}

impl SnapshotChunkRejectionCounters {
    /// Returns whether all rejection counters are zero.
    #[must_use]
    pub fn is_empty(self) -> bool {
        self.stale_term == 0
            && self.wrong_transfer == 0
            && self.metadata_mismatch == 0
            && self.out_of_order_offset == 0
            && self.invalid_bounds == 0
            && self.corrupt_persisted_pending_transfer == 0
    }
}
