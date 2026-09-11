//! Owned replica-directory opening with the opt-in hard-state journal.
//!
//! The existing hard-state pathname deliberately stays the same: selecting the
//! wrong backend must reject the format, never treat an existing replica as new.

use crate::{
    durable_fs::ParentDirectorySyncBatch,
    file_node_stores::{io_error, ownership_error},
    file_store_ownership::acquire_file_store_ownership,
    FileRaftLogSegment, FileRaftSnapshotStore, JournalRaftHardStateStore,
    OpenFileRaftNodeStoresError, OpenJournalRaftHardStateStoreError, RaftHardStateStore,
};
use std::{error::Error, fmt, path::Path, sync::Arc};

/// File-backed replica stores with an opt-in RFHJ v1 hard-state journal.
///
/// Uses the same directory layout and exclusive ownership as
/// [`crate::FileRaftNodeStores`]. Existing RFHS hard state is rejected; use a
/// fresh replica directory to evaluate this backend. No migration is implicit.
#[derive(Debug)]
pub struct JournalRaftNodeStores {
    hard_state: JournalRaftHardStateStore,
    log_segment: FileRaftLogSegment,
    snapshot_store: FileRaftSnapshotStore,
}

/// Journal bundle open failures, separated by storage authority.
///
/// This enum is exhaustive so callers can inspect both failure families.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OpenJournalRaftNodeStoresError {
    /// The journal could not be opened or recovered.
    HardState(OpenJournalRaftHardStateStoreError),
    /// Directory ownership, log, snapshot, or directory-sync failure.
    Stores(OpenFileRaftNodeStoresError),
}

impl JournalRaftNodeStores {
    /// Opens stores under an existing replica directory with strict log replay.
    ///
    /// # Errors
    /// Returns an error on ownership conflict, invalid storage, or I/O failure.
    pub fn open(directory: impl AsRef<Path>) -> Result<Self, OpenJournalRaftNodeStoresError> {
        Self::open_inner(directory.as_ref(), false)
    }

    /// Opens stores, repairing only log corruption above the durable commit floor.
    ///
    /// # Errors
    /// Returns an error on ownership conflict, invalid storage, or I/O failure.
    pub fn open_repairing_uncommitted_log_tail(
        directory: impl AsRef<Path>,
    ) -> Result<Self, OpenJournalRaftNodeStoresError> {
        Self::open_inner(directory.as_ref(), true)
    }

    fn open_inner(
        directory: &Path,
        repair_log: bool,
    ) -> Result<Self, OpenJournalRaftNodeStoresError> {
        use OpenFileRaftNodeStoresError as StoresError;
        use OpenJournalRaftNodeStoresError as OpenError;
        let ownership = acquire_file_store_ownership(directory)
            .map_err(|error| OpenError::Stores(ownership_error(error)))?;
        let mut hard_state = JournalRaftHardStateStore::open(directory.join("hard-state"))
            .map_err(OpenError::HardState)?;
        let mut sync_batch = ParentDirectorySyncBatch::new();
        let log_path = directory.join("log");
        let mut log_segment = if repair_log {
            FileRaftLogSegment::open_with_parent_sync_batch_repairing_uncommitted_tail(
                log_path,
                &mut sync_batch,
                hard_state.current().commit_index,
            )
        } else {
            FileRaftLogSegment::open_with_parent_sync_batch(log_path, &mut sync_batch)
        }
        .map_err(|error| OpenError::Stores(StoresError::Log(error)))?;
        let mut snapshot_store = FileRaftSnapshotStore::open_with_parent_sync_batch(
            directory.join("snapshots"),
            &mut sync_batch,
        )
        .map_err(|error| OpenError::Stores(StoresError::Snapshot(error)))?;
        sync_batch.flush().map_err(|error| {
            OpenError::Stores(io_error("sync raft node store directory", directory, error))
        })?;
        hard_state.attach_ownership(Arc::clone(&ownership));
        log_segment.attach_ownership(Arc::clone(&ownership));
        snapshot_store.attach_ownership(ownership);
        Ok(Self {
            hard_state,
            log_segment,
            snapshot_store,
        })
    }

    /// Splits the bundle; each returned store retains the shared ownership lock.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        JournalRaftHardStateStore,
        FileRaftLogSegment,
        FileRaftSnapshotStore,
    ) {
        (self.hard_state, self.log_segment, self.snapshot_store)
    }
}

impl fmt::Display for OpenJournalRaftNodeStoresError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HardState(error) => write!(f, "could not open hard-state journal: {error}"),
            Self::Stores(error) => fmt::Display::fmt(error, f),
        }
    }
}

impl Error for OpenJournalRaftNodeStoresError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::HardState(error) => Some(error),
            Self::Stores(error) => Some(error),
        }
    }
}

#[cfg(test)]
#[path = "journal_node_stores_test.rs"]
mod tests;
