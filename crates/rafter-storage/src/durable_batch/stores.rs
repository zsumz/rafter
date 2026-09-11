//! Exclusive replica ownership for the opt-in shared log/hard-state WAL.
use super::{codec, open, reclamation, WalRaftHardStateStore, WalRaftLogSegment};
use crate::{
    file_node_stores::ownership_error, file_store_ownership::acquire_file_store_ownership,
    FileRaftSnapshotStore, RaftSnapshotStore,
};
use std::{
    io,
    path::Path,
    sync::{Arc, Mutex},
};

/// Opt-in RFWB WAL with shared hard-state/log views and the existing snapshot store.
///
/// No existing directory is migrated. The initial WAL generation occupies
/// `hard-state`, so legacy backends reject its distinct header. Prefix compaction
/// checkpoints the live state into manifest-selected generation files before
/// reclaiming obsolete WAL history.
#[derive(Debug)]
pub struct WalRaftNodeStores {
    hard: WalRaftHardStateStore,
    log: WalRaftLogSegment,
    snapshots: FileRaftSnapshotStore,
}
impl WalRaftNodeStores {
    /// Opens an exclusively owned replica, rejecting legacy log files and corrupt records.
    ///
    /// # Errors
    /// Returns original I/O errors or `InvalidData` for incompatible/corrupt WAL bytes.
    pub fn open(directory: impl AsRef<Path>) -> io::Result<Self> {
        let directory = directory.as_ref();
        let ownership = acquire_file_store_ownership(directory)
            .map_err(|e| io::Error::other(ownership_error(e)))?;
        if directory.join("log").exists() {
            return Err(codec::invalid(
                "legacy log exists; WAL conversion is not implicit",
            ));
        }
        let wal_exists =
            directory.join("hard-state").exists() || reclamation::manifest_path(directory).exists();
        if !wal_exists
            && directory.join("snapshots").exists()
            && std::fs::read_dir(directory.join("snapshots"))?
                .next()
                .is_some()
        {
            return Err(codec::invalid("snapshot data exists without its WAL"));
        }
        let mut snapshots =
            FileRaftSnapshotStore::open(directory.join("snapshots")).map_err(io::Error::other)?;
        let current_snapshot = snapshots.current_snapshot();
        let shared = Arc::new(Mutex::new(open::open(
            &directory.join("hard-state"),
            ownership.clone(),
            current_snapshot.as_ref(),
        )?));
        snapshots.attach_ownership(ownership);
        Ok(Self {
            hard: WalRaftHardStateStore(shared.clone()),
            log: WalRaftLogSegment(shared),
            snapshots,
        })
    }
    /// Splits the bundle; every handle retains the replica's ownership lock.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        WalRaftHardStateStore,
        WalRaftLogSegment,
        FileRaftSnapshotStore,
    ) {
        (self.hard, self.log, self.snapshots)
    }
}
