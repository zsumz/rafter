//! Snapshot-install request, chunk, and response frames.
//!
//! Leaders stream chunks in the peer protocol. The whole-snapshot frame remains
//! available to direct in-memory embeddings that intentionally carry all bytes.

use crate::{
    LogIndex, NodeId, RaftSnapshotMetadata, SnapshotChunkRequest, SnapshotChunkSend,
    SnapshotChunkSource, SnapshotTransferId, Term,
};

/// A whole snapshot in one message: metadata plus the complete payload.
///
/// The kernel never sends this — leaders stream
/// [`InstallSnapshotChunk`] messages — and `rafter-codec` does not encode it
/// in the current peer wire format. Direct kernel embeddings may still submit
/// it when they intentionally carry a complete snapshot payload in memory.
/// Payload bytes in a message are transient: an accepted whole snapshot is
/// handed to the receiver's store as a single staged chunk, never retained in
/// kernel state.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct InstallSnapshot {
    /// Leader term.
    pub term: Term,
    /// Node sending the snapshot.
    pub leader_id: NodeId,
    /// Raft-visible snapshot descriptor.
    pub metadata: RaftSnapshotMetadata,
    /// Complete opaque application payload.
    pub application_payload: Vec<u8>,
}

/// One chunk of an install-snapshot transfer.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct InstallSnapshotChunk {
    /// Leader term.
    pub term: Term,
    /// Node sending the chunk.
    pub leader_id: NodeId,
    /// Stable identity of this transfer.
    pub transfer_id: SnapshotTransferId,
    /// Raft-visible snapshot descriptor.
    pub metadata: RaftSnapshotMetadata,
    /// Complete application payload length.
    pub total_payload_len: u64,
    /// Checksum of the complete application payload.
    pub application_payload_crc32: u32,
    /// Starting payload offset of this chunk.
    pub offset: u64,
    /// Opaque application payload bytes at `offset`.
    pub chunk: Vec<u8>,
    /// Whether this chunk reaches the declared payload end.
    pub done: bool,
}

impl SnapshotChunkSend {
    /// Materializes the wire message for this directive by reading the chunk
    /// bytes from `source`.
    ///
    /// Returns `None` when the source cannot serve the snapshot or returns a
    /// chunk of the wrong length; the caller drops the directive like a lost
    /// message and the transfer resumes from the follower's acknowledged
    /// offset.
    #[must_use]
    pub fn resolve<S: SnapshotChunkSource + ?Sized>(
        &self,
        source: &S,
    ) -> Option<InstallSnapshotChunk> {
        let chunk = source.snapshot_chunk(SnapshotChunkRequest {
            transfer_id: self.transfer_id,
            metadata: &self.metadata,
            total_payload_len: self.total_payload_len,
            application_payload_crc32: self.application_payload_crc32,
            offset: self.offset,
            len: self.len,
        })?;
        if chunk.len() != self.len as usize {
            return None;
        }
        Some(InstallSnapshotChunk {
            term: self.term,
            leader_id: self.leader_id,
            transfer_id: self.transfer_id,
            metadata: self.metadata.clone(),
            total_payload_len: self.total_payload_len,
            application_payload_crc32: self.application_payload_crc32,
            offset: self.offset,
            chunk,
            done: self.done,
        })
    }
}

/// Response to an install-snapshot message or chunk.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct InstallSnapshotResponse {
    /// Current follower term.
    pub term: Term,
    /// Node sending the response.
    pub follower_id: NodeId,
    /// Whether the chunk or complete snapshot was accepted.
    pub success: bool,
    /// Snapshot boundary named by the response.
    pub last_included_index: LogIndex,
    /// Streaming transfer identity, or `None` for a whole snapshot.
    pub transfer_id: Option<SnapshotTransferId>,
    /// Next payload offset the follower requires.
    pub next_offset: u64,
}
