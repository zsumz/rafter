//! Public snapshot-store behavior and durability contract.
//!
//! Implementations expose current snapshot descriptors, bounded payload
//! streaming, durable inbound staging, and explicit promotion. Filesystem
//! publication mechanics and concrete state live outside this module.

use rafter::{PendingSnapshotTransfer, RaftSnapshot, SnapshotChunkSource, StagedSnapshotChunk};

use crate::PersistedRaftSnapshot;

use super::RaftSnapshotStoreWriteError;

/// Storage contract for durable Raft snapshots and inbound snapshot transfer
/// staging.
///
/// File-backed mutations may fail after the filesystem accepted some or all of
/// an operation. The reference handle then rejects later mutations until reopen.
/// [`RaftSnapshotStoreWriteError::SnapshotCommittedButReopenRequired`] is the
/// exceptional case whose error explicitly says the new current snapshot already
/// crossed its durable manifest commit point.
pub trait RaftSnapshotStore {
    /// Writes a complete durable snapshot and makes it current.
    ///
    /// Suits application snapshots small enough to hold in memory; large
    /// state machines stream through
    /// [`RaftSnapshotStore::write_snapshot_from_source`] instead.
    ///
    /// # Errors
    ///
    /// Returns [`RaftSnapshotStoreWriteError`] if either the immutable snapshot
    /// file or the current manifest cannot be durably written.
    fn write_snapshot(
        &mut self,
        snapshot: PersistedRaftSnapshot,
    ) -> Result<(), RaftSnapshotStoreWriteError>;

    /// Writes a durable snapshot whose payload is pulled from `source` in
    /// bounded chunks, and makes it current. The payload is never
    /// materialized whole.
    ///
    /// # Errors
    ///
    /// Returns [`RaftSnapshotStoreWriteError`] when the source cannot serve
    /// the snapshot identified by `snapshot.transfer_id()` or the snapshot
    /// cannot be durably written.
    fn write_snapshot_from_source(
        &mut self,
        snapshot: &RaftSnapshot,
        source: &dyn SnapshotChunkSource,
    ) -> Result<(), RaftSnapshotStoreWriteError>;

    /// The current snapshot's descriptor: metadata plus payload length.
    /// Payload bytes are read through the store's [`SnapshotChunkSource`]
    /// implementation, never handed out whole.
    fn current_snapshot(&self) -> Option<RaftSnapshot>;

    /// Stages one validated inbound snapshot chunk durably.
    ///
    /// Every chunk must have a checked in-range end offset, consistent
    /// `done` flag, and transfer id derived from its metadata, total length,
    /// and payload checksum. Empty chunks are valid only for an exactly
    /// complete payload. A chunk at offset zero begins staging and replaces
    /// whatever was staged before it. A chunk at a non-zero offset must also
    /// continue the staged transfer exactly: same leader and descriptor, with
    /// `chunk.offset` equal to the staged length. The staged bytes are not
    /// current state — they only become current through
    /// [`RaftSnapshotStore::promote_staged_snapshot`].
    ///
    /// # Errors
    ///
    /// Returns [`RaftSnapshotStoreWriteError`] when the chunk does not
    /// continue the staged transfer (the kernel orders chunks, so a mismatch
    /// is a caller bug) or when the staged bytes cannot be durably written.
    fn stage_snapshot_chunk(
        &mut self,
        chunk: &StagedSnapshotChunk,
    ) -> Result<(), RaftSnapshotStoreWriteError>;

    /// Promotes the completed staged transfer identified by
    /// `snapshot.transfer_id()` to the current snapshot and clears the
    /// staging area.
    ///
    /// # Errors
    ///
    /// Returns [`RaftSnapshotStoreWriteError`] when nothing is staged, the
    /// transfer id or complete staged descriptor differs from `snapshot`, the
    /// staged payload is incomplete, or the promoted snapshot cannot be durably
    /// written. Publication remains committed if only later staging cleanup
    /// fails.
    fn promote_staged_snapshot(
        &mut self,
        snapshot: &RaftSnapshot,
    ) -> Result<(), RaftSnapshotStoreWriteError>;

    /// Clears any partially received incoming snapshot transfer.
    ///
    /// # Errors
    ///
    /// Returns [`RaftSnapshotStoreWriteError`] when the staged transfer marker
    /// cannot be durably removed.
    fn clear_pending_snapshot_transfer(&mut self) -> Result<(), RaftSnapshotStoreWriteError>;

    /// Returns the logical pending inbound snapshot transfer, if one is
    /// resumable.
    fn current_pending_snapshot_transfer(&self) -> Option<&PendingSnapshotTransfer>;
}
