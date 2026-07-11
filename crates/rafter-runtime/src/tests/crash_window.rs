//! Reopen behaviour across the snapshot-persist / log-compaction crash
//! window. The durable write order is snapshot-first, then log compaction; a
//! crash between the two must be repaired at open so the reopened node is
//! indistinguishable from one that never crashed.

use super::snapshot::{persisted_entry, raft_snapshot};
use super::*;
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

mod boundary;
mod reopen;
mod repair;

static TEST_DIRECTORY_ID: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(name: &str) -> Self {
        let id = TEST_DIRECTORY_ID.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("rafter-runtime-{name}-{}-{id}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("test store directory is created");
        Self(path)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

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
