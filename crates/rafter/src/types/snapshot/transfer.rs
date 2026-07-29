//! Snapshot transfer identities, directives, staging, and restart progress.

use std::fmt;

use super::super::{NodeId, Term};
use super::RaftSnapshotMetadata;

/// Stable identifier for one snapshot transfer.
///
/// Values produced by [`super::RaftSnapshot::transfer_id`] are deterministic,
/// non-zero 64-bit routing identities derived from Raft snapshot metadata,
/// payload length, and the payload CRC32. They let receivers reject chunks
/// that do not belong to the advertised transfer, but they are not
/// collision-resistant digests or authentication tags.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SnapshotTransferId(pub u64);

impl fmt::Display for SnapshotTransferId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// A leader-side directive to send one snapshot chunk to a follower.
///
/// Carries everything the wire message needs except the payload bytes; the
/// transport resolves those through a [`super::SnapshotChunkSource`] with
/// [`SnapshotChunkSend::resolve`]. A directive that cannot be resolved is
/// dropped exactly like a lost message — the transfer resumes from the
/// follower's acknowledged offset.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SnapshotChunkSend {
    /// Leader term.
    pub term: Term,
    /// Node sending the snapshot.
    pub leader_id: NodeId,
    /// Stable transfer identity.
    pub transfer_id: SnapshotTransferId,
    /// Raft-visible snapshot descriptor.
    pub metadata: RaftSnapshotMetadata,
    /// Complete payload length.
    pub total_payload_len: u64,
    /// Checksum of the complete payload.
    pub application_payload_crc32: u32,
    /// Starting payload offset.
    pub offset: u64,
    /// Exact number of payload bytes to read and send.
    pub len: u32,
    /// Whether this chunk reaches the payload end.
    pub done: bool,
}

/// A validated inbound snapshot chunk for the receiver's snapshot store.
///
/// Chunks arrive in offset order within a transfer; `offset` is always the
/// staging area's current length for `transfer_id` (a new transfer starts at
/// zero). `done` marks the final chunk: the staged payload is complete and
/// the [`Output::ApplySnapshot`](crate::Output::ApplySnapshot) emitted
/// alongside it refers to the staged content.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct StagedSnapshotChunk {
    /// Leader that sent the chunk.
    pub leader_id: NodeId,
    /// Stable transfer identity.
    pub transfer_id: SnapshotTransferId,
    /// Raft-visible snapshot descriptor.
    pub metadata: RaftSnapshotMetadata,
    /// Complete payload length.
    pub total_payload_len: u64,
    /// Checksum of the complete payload.
    pub application_payload_crc32: u32,
    /// Starting payload offset.
    pub offset: u64,
    /// Opaque application payload bytes.
    pub bytes: Vec<u8>,
    /// Whether this chunk completes the payload.
    pub done: bool,
}

/// Receiver-side progress for a partially staged snapshot transfer.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PendingSnapshotTransfer {
    /// Leader that owns the inbound transfer.
    pub leader_id: NodeId,
    /// Stable transfer identity.
    pub transfer_id: SnapshotTransferId,
    /// Raft-visible snapshot descriptor.
    pub metadata: RaftSnapshotMetadata,
    /// Complete payload length.
    pub total_payload_len: u64,
    /// Checksum of the complete payload.
    pub application_payload_crc32: u32,
    /// Payload bytes durably staged so far.
    pub received_len: u64,
}

impl PendingSnapshotTransfer {
    /// Returns the number of payload bytes already received.
    #[must_use]
    pub fn received_bytes(&self) -> u64 {
        self.received_len
    }

    /// Returns whether the staged payload length has reached the descriptor.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.received_bytes() == self.total_payload_len
    }
}
