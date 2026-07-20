//! Reopen behaviour across the snapshot-persist / log-compaction crash
//! window. The durable write order is snapshot-first, then log compaction; a
//! crash between the two must be repaired at open so the reopened node is
//! indistinguishable from one that never crashed.

use super::file_backed_fixture::TestDirectory;
use super::snapshot::{persisted_entry, raft_snapshot};
use super::*;

mod boundary;
mod reopen;
mod repair;

/// Builds the half-installed on-disk shape a crash in the window leaves
/// behind: a durable snapshot through `snapshot_index`, but a log whose
/// compacted prefix is still behind it and still holds the covered entries.
fn half_installed_stores(
    snapshot_index: u64,
    snapshot_term: u64,
    log: &[PersistedRaftLogEntry],
) -> (InMemoryRaftLogSegment, InMemoryRaftSnapshotStore) {
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .append_entries(log)
        .expect("log entries persist");

    let snapshot = raft_snapshot(snapshot_index, snapshot_term, snapshot_term, b"snapshot");
    let snapshot_store = InMemoryRaftSnapshotStore::with_snapshot(snapshot);
    (log_segment, snapshot_store)
}
