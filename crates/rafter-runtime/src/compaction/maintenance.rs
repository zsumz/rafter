//! File-backed snapshot-retention maintenance after durable compaction.

use rafter_storage::{
    FileRaftSnapshotStore, SnapshotPruneError, SnapshotPruneReport, SnapshotRetention,
};

use crate::DurableRaftNode;

impl<H, L> DurableRaftNode<H, L, FileRaftSnapshotStore> {
    /// Prunes noncurrent file-backed snapshot envelopes according to `retention`.
    ///
    /// This is storage maintenance, not a Raft state transition. The selected
    /// snapshot and its manifest are never removed, the installed kernel
    /// descriptor is unchanged, and a maintenance failure does not poison the
    /// runtime. The operation is safe to retry idempotently, including after a
    /// restart.
    ///
    /// Call this after a successful [`Self::compact_log_with_snapshot`] or
    /// [`Self::compact_log_with_streamed_snapshot`] to bound retained snapshot
    /// history. `CurrentOnly` is the smallest supported retention envelope.
    /// Unknown files and stable inbound-transfer staging are never removed.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotPruneError`] when the snapshot store requires reopen,
    /// inventory cannot establish the selected snapshot, or durable deletion
    /// cannot complete. Any reported deletion prefix may already be absent;
    /// callers may safely retry the same policy.
    pub fn prune_snapshot_files(
        &mut self,
        retention: SnapshotRetention,
    ) -> Result<SnapshotPruneReport, SnapshotPruneError> {
        self.snapshot_store.prune_snapshots(retention)
    }

    /// Removes recognized temporary snapshot files left by an interrupted writer.
    ///
    /// This is storage maintenance, not a Raft state transition. Selected and
    /// noncurrent snapshots, stable inbound-transfer staging, and unknown files
    /// are never removed. The operation is intended for startup after recovery,
    /// or another point where the embedding exclusively owns the node and no
    /// snapshot-store write is in progress.
    ///
    /// A maintenance failure does not poison the runtime. The operation is safe
    /// to retry idempotently, including after another restart.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotPruneError`] when the snapshot store requires reopen,
    /// inventory cannot establish the selected snapshot, or durable deletion
    /// cannot complete. Any reported deletion prefix may already be absent.
    pub fn cleanup_abandoned_snapshot_temporary_files(
        &mut self,
    ) -> Result<SnapshotPruneReport, SnapshotPruneError> {
        self.snapshot_store
            .cleanup_abandoned_snapshot_temporary_files()
    }
}
