//! Standard file-backed store bundle construction.
//!
//! This module owns the replica-directory layout and coordinated opening of
//! hard state, retained log, and snapshots; each store owns its own recovery.

use std::{
    error::Error,
    fmt, fs, io,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    durable_fs::ParentDirectorySyncBatch,
    file_store_ownership::{acquire_file_store_ownership, AcquireFileStoreOwnershipError},
    raft_hard_state_store::{
        FileRaftHardStateStore, OpenRaftHardStateStoreError, RaftHardStateStore,
    },
    raft_log_segment::{FileRaftLogSegment, OpenRaftLogSegmentError},
    raft_snapshot_store::{FileRaftSnapshotStore, OpenRaftSnapshotStoreError},
    StorageIoError,
};

/// File-backed store bundle for one Raft replica.
///
/// The bundle owns the standard durable hard-state, log, and snapshot stores
/// under one replica directory and can be split back into those stores with
/// [`FileRaftNodeStores::into_parts`]. Opening the bundle acquires exclusive
/// cooperating-process ownership of the directory before any store can repair
/// or publish bytes.
#[derive(Debug)]
pub struct FileRaftNodeStores {
    hard_state: FileRaftHardStateStore,
    log_segment: FileRaftLogSegment,
    snapshot_store: FileRaftSnapshotStore,
}

/// Errors returned while opening the standard file-backed store bundle.
///
/// This enum is exhaustive so callers can distinguish which durable store or
/// directory operation failed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OpenFileRaftNodeStoresError {
    AlreadyOpen {
        directory: PathBuf,
    },
    HardState(OpenRaftHardStateStoreError),
    Log(OpenRaftLogSegmentError),
    Snapshot(OpenRaftSnapshotStoreError),
    Io {
        operation: &'static str,
        path: PathBuf,
        source: StorageIoError,
    },
}

impl FileRaftNodeStores {
    /// Opens the standard file-backed stores under an existing replica
    /// directory.
    ///
    /// The layout is:
    ///
    /// ```text
    /// <directory>/.rafter-storage.lock
    /// <directory>/hard-state
    /// <directory>/log
    /// <directory>/snapshots/
    /// ```
    ///
    /// Fresh log-file and snapshot-directory creation syncs are batched and
    /// flushed once before this function returns. Existing single-store
    /// constructors keep their immediate-sync behavior.
    ///
    /// # Errors
    ///
    /// Returns [`OpenFileRaftNodeStoresError::AlreadyOpen`] when another bundle
    /// owns the directory. Otherwise returns [`OpenFileRaftNodeStoresError`]
    /// when a store cannot be opened, replayed, verified, or when the batched
    /// parent-directory sync fails.
    pub fn open(directory: impl AsRef<Path>) -> Result<Self, OpenFileRaftNodeStoresError> {
        Self::open_with_log_repair(directory, LogOpenMode::Strict)
    }

    /// Opens the standard file-backed stores, repairing a corrupt, partial,
    /// or non-contiguous uncommitted log tail when the durable hard-state
    /// commit index proves the retained prefix is sufficient.
    ///
    /// Exclusive directory ownership is acquired before any repair. Hard state
    /// then supplies the durable commit floor, and log open remains fail-loud
    /// for corruption at or below that floor.
    ///
    /// # Errors
    ///
    /// Returns [`OpenFileRaftNodeStoresError::AlreadyOpen`] when another bundle
    /// owns the directory. Otherwise returns [`OpenFileRaftNodeStoresError`]
    /// when a store cannot be opened, replayed, verified, repaired, or when the
    /// batched parent-directory sync fails.
    pub fn open_repairing_uncommitted_log_tail(
        directory: impl AsRef<Path>,
    ) -> Result<Self, OpenFileRaftNodeStoresError> {
        Self::open_with_log_repair(directory, LogOpenMode::RepairUncommittedTail)
    }

    fn open_with_log_repair(
        directory: impl AsRef<Path>,
        log_open_mode: LogOpenMode,
    ) -> Result<Self, OpenFileRaftNodeStoresError> {
        let directory = directory.as_ref().to_path_buf();
        let metadata = fs::metadata(&directory)
            .map_err(|error| io_error("open raft node store directory", &directory, error))?;
        if !metadata.is_dir() {
            return Err(OpenFileRaftNodeStoresError::Io {
                operation: "open raft node store directory",
                path: directory,
                source: io::Error::new(io::ErrorKind::InvalidInput, "not a directory").into(),
            });
        }

        let ownership = acquire_file_store_ownership(&directory).map_err(ownership_error)?;
        let mut hard_state = FileRaftHardStateStore::open(directory.join("hard-state"))
            .map_err(OpenFileRaftNodeStoresError::HardState)?;

        let mut sync_batch = ParentDirectorySyncBatch::new();
        let log_path = directory.join("log");
        let mut log_segment = match log_open_mode {
            LogOpenMode::Strict => {
                FileRaftLogSegment::open_with_parent_sync_batch(log_path, &mut sync_batch)
            }
            LogOpenMode::RepairUncommittedTail => {
                let commit_index = hard_state.current().commit_index;
                FileRaftLogSegment::open_with_parent_sync_batch_repairing_uncommitted_tail(
                    log_path,
                    &mut sync_batch,
                    commit_index,
                )
            }
        }
        .map_err(OpenFileRaftNodeStoresError::Log)?;
        let mut snapshot_store = FileRaftSnapshotStore::open_with_parent_sync_batch(
            directory.join("snapshots"),
            &mut sync_batch,
        )
        .map_err(OpenFileRaftNodeStoresError::Snapshot)?;

        sync_batch
            .flush()
            .map_err(|error| sync_error(&directory, error))?;

        hard_state.attach_ownership(Arc::clone(&ownership));
        log_segment.attach_ownership(Arc::clone(&ownership));
        snapshot_store.attach_ownership(ownership);

        Ok(Self {
            hard_state,
            log_segment,
            snapshot_store,
        })
    }

    /// Splits the bundle into its hard-state, log, and snapshot stores.
    ///
    /// This is the normal handoff point for runtimes that want the standard
    /// on-disk layout but still own the concrete store instances separately.
    /// The three stores share the directory-ownership guard; another bundle can
    /// open the directory only after every returned store has been dropped.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        FileRaftHardStateStore,
        FileRaftLogSegment,
        FileRaftSnapshotStore,
    ) {
        (self.hard_state, self.log_segment, self.snapshot_store)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LogOpenMode {
    Strict,
    RepairUncommittedTail,
}

fn ownership_error(error: AcquireFileStoreOwnershipError) -> OpenFileRaftNodeStoresError {
    match error {
        AcquireFileStoreOwnershipError::AlreadyHeld { directory } => {
            OpenFileRaftNodeStoresError::AlreadyOpen { directory }
        }
        AcquireFileStoreOwnershipError::Io {
            operation,
            path,
            source,
        } => OpenFileRaftNodeStoresError::Io {
            operation,
            path,
            source,
        },
    }
}

fn io_error(operation: &'static str, path: &Path, error: io::Error) -> OpenFileRaftNodeStoresError {
    OpenFileRaftNodeStoresError::Io {
        operation,
        path: path.to_path_buf(),
        source: error.into(),
    }
}

fn sync_error(directory: &Path, error: io::Error) -> OpenFileRaftNodeStoresError {
    io_error("sync raft node store directory", directory, error)
}

impl fmt::Display for OpenFileRaftNodeStoresError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyOpen { directory } => write!(
                formatter,
                "Raft node store directory {} is already open by another owner",
                directory.display()
            ),
            Self::HardState(error) => write!(formatter, "could not open hard-state store: {error}"),
            Self::Log(error) => write!(formatter, "could not open log segment: {error}"),
            Self::Snapshot(error) => write!(formatter, "could not open snapshot store: {error}"),
            Self::Io {
                operation,
                path,
                source,
            } => write!(
                formatter,
                "could not {operation} at {}: {source}",
                path.display()
            ),
        }
    }
}

impl Error for OpenFileRaftNodeStoresError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::AlreadyOpen { .. } => None,
            Self::HardState(error) => Some(error),
            Self::Log(error) => Some(error),
            Self::Snapshot(error) => Some(error),
            Self::Io { source, .. } => Some(source.as_io_error()),
        }
    }
}

#[cfg(test)]
#[path = "file_node_stores_test.rs"]
mod tests;
