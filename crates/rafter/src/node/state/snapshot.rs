//! Follower-side progress for one inbound chunked snapshot transfer.
//!
//! The kernel tracks only transfer identity and byte progress. Payload bytes
//! leave through `Output::StageSnapshotChunk` and remain owned by the embedding
//! snapshot store.

use crate::{NodeId, PendingSnapshotTransfer, RaftSnapshotMetadata, SnapshotTransferId};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(in crate::node) struct IncomingSnapshotTransfer {
    pub leader_id: NodeId,
    pub transfer_id: SnapshotTransferId,
    pub metadata: RaftSnapshotMetadata,
    pub total_payload_len: u64,
    pub application_payload_crc32: u32,
    pub received_len: u64,
}

impl IncomingSnapshotTransfer {
    /// Starts a new inbound snapshot transfer.
    pub(in crate::node) fn new(
        leader_id: NodeId,
        transfer_id: SnapshotTransferId,
        metadata: RaftSnapshotMetadata,
        total_payload_len: u64,
        application_payload_crc32: u32,
    ) -> Self {
        Self {
            leader_id,
            transfer_id,
            metadata,
            total_payload_len,
            application_payload_crc32,
            received_len: 0,
        }
    }

    /// Restores an inbound snapshot transfer from durable pending state.
    pub(in crate::node) fn from_pending(pending: PendingSnapshotTransfer) -> Self {
        Self {
            leader_id: pending.leader_id,
            transfer_id: pending.transfer_id,
            metadata: pending.metadata,
            total_payload_len: pending.total_payload_len,
            application_payload_crc32: pending.application_payload_crc32,
            received_len: pending.received_len,
        }
    }

    /// Converts this in-memory transfer to durable pending state.
    pub(in crate::node) fn to_pending(&self) -> PendingSnapshotTransfer {
        PendingSnapshotTransfer {
            leader_id: self.leader_id,
            transfer_id: self.transfer_id,
            metadata: self.metadata.clone(),
            total_payload_len: self.total_payload_len,
            application_payload_crc32: self.application_payload_crc32,
            received_len: self.received_len,
        }
    }

    /// Returns the next expected byte offset.
    pub(in crate::node) fn next_offset(&self) -> u64 {
        self.received_len
    }
}
