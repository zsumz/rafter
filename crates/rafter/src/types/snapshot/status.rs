//! Read-only snapshot transfer status and rejection diagnostics.

use super::super::{LogIndex, NodeId};
use super::SnapshotTransferId;

/// Snapshot transfer observability for one node.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct SnapshotTransferStatus {
    /// Active leader-side transfers, one per follower.
    pub leader: Vec<LeaderSnapshotTransferStatus>,
    /// Active follower-side inbound transfer.
    pub follower: Option<FollowerSnapshotTransferStatus>,
    /// Cumulative reasons inbound chunks were rejected.
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
    /// Follower receiving the snapshot.
    pub follower_id: NodeId,
    /// Stable transfer identity.
    pub transfer_id: SnapshotTransferId,
    /// Snapshot boundary being transferred.
    pub last_included_index: LogIndex,
    /// Complete payload length in bytes.
    pub total_bytes: u64,
    /// Next payload offset to send.
    pub next_offset: u64,
}

/// Follower-side snapshot transfer progress from one leader.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct FollowerSnapshotTransferStatus {
    /// Leader sending the snapshot.
    pub leader_id: NodeId,
    /// Stable transfer identity.
    pub transfer_id: SnapshotTransferId,
    /// Snapshot boundary being transferred.
    pub last_included_index: LogIndex,
    /// Complete payload length in bytes.
    pub total_bytes: u64,
    /// Payload bytes durably staged so far.
    pub received_bytes: u64,
}

/// Counters for rejected inbound snapshot chunks.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct SnapshotChunkRejectionCounters {
    /// Chunks rejected because the sender term was stale.
    pub stale_term: u64,
    /// Chunks rejected because they belonged to another transfer.
    pub wrong_transfer: u64,
    /// Chunks rejected because their descriptor changed mid-transfer.
    pub metadata_mismatch: u64,
    /// Chunks rejected because their offset was not the required next offset.
    pub out_of_order_offset: u64,
    /// Chunks rejected because their byte range or finality was invalid.
    pub invalid_bounds: u64,
    /// Chunks refused because persisted pending-transfer state was corrupt.
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
