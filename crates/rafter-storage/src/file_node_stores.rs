use std::{
    error::Error,
    fmt, fs, io,
    path::{Path, PathBuf},
};

use crate::{
    durable_fs::ParentDirectorySyncBatch,
    raft_hard_state_store::{
        FileRaftHardStateStore, OpenRaftHardStateStoreError, RaftHardStateStore,
    },
    raft_log_segment::{FileRaftLogSegment, OpenRaftLogSegmentError},
    raft_snapshot_store::{FileRaftSnapshotStore, OpenRaftSnapshotStoreError},
};

/// File-backed store bundle for one Raft replica.
///
/// The bundle owns the standard durable hard-state, log, and snapshot stores
/// under one replica directory and can be split back into those stores with
/// [`FileRaftNodeStores::into_parts`].
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
    HardState(OpenRaftHardStateStoreError),
    Log(OpenRaftLogSegmentError),
    Snapshot(OpenRaftSnapshotStoreError),
    Io {
        operation: &'static str,
        path: PathBuf,
        message: String,
    },
}

impl FileRaftNodeStores {
    /// Opens the standard file-backed stores under an existing replica
    /// directory.
    ///
    /// The layout is:
    ///
    /// ```text
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
    /// Returns [`OpenFileRaftNodeStoresError`] when any store cannot be opened,
    /// replayed, verified, or when the batched parent-directory sync fails.
    pub fn open(directory: impl AsRef<Path>) -> Result<Self, OpenFileRaftNodeStoresError> {
        Self::open_with_log_repair(directory, LogOpenMode::Strict)
    }

    /// Opens the standard file-backed stores, repairing a corrupt, partial,
    /// or non-contiguous uncommitted log tail when the durable hard-state
    /// commit index proves the retained prefix is sufficient.
    ///
    /// Hard-state is opened first and supplies the durable commit floor. The
    /// log open remains fail-loud for corruption at or below that floor.
    ///
    /// # Errors
    ///
    /// Returns [`OpenFileRaftNodeStoresError`] when any store cannot be opened,
    /// replayed, verified, repaired, or when the batched parent-directory sync
    /// fails.
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
            .map_err(|error| io_error("open raft node store directory", &directory, &error))?;
        if !metadata.is_dir() {
            return Err(OpenFileRaftNodeStoresError::Io {
                operation: "open raft node store directory",
                path: directory,
                message: "not a directory".to_string(),
            });
        }

        let hard_state = FileRaftHardStateStore::open(directory.join("hard-state"))
            .map_err(OpenFileRaftNodeStoresError::HardState)?;

        let mut sync_batch = ParentDirectorySyncBatch::new();
        let log_path = directory.join("log");
        let log_segment = match log_open_mode {
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
        let snapshot_store = FileRaftSnapshotStore::open_with_parent_sync_batch(
            directory.join("snapshots"),
            &mut sync_batch,
        )
        .map_err(OpenFileRaftNodeStoresError::Snapshot)?;

        sync_batch
            .flush()
            .map_err(|error| sync_error(&directory, &error))?;

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

fn io_error(
    operation: &'static str,
    path: &Path,
    error: &io::Error,
) -> OpenFileRaftNodeStoresError {
    OpenFileRaftNodeStoresError::Io {
        operation,
        path: path.to_path_buf(),
        message: error.to_string(),
    }
}

fn sync_error(directory: &Path, error: &io::Error) -> OpenFileRaftNodeStoresError {
    io_error("sync raft node store directory", directory, error)
}

impl fmt::Display for OpenFileRaftNodeStoresError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HardState(error) => write!(formatter, "could not open hard-state store: {error}"),
            Self::Log(error) => write!(formatter, "could not open log segment: {error}"),
            Self::Snapshot(error) => write!(formatter, "could not open snapshot store: {error}"),
            Self::Io {
                operation,
                path,
                message,
            } => write!(
                formatter,
                "could not {operation} at {}: {message}",
                path.display()
            ),
        }
    }
}

impl Error for OpenFileRaftNodeStoresError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::HardState(error) => Some(error),
            Self::Log(error) => Some(error),
            Self::Snapshot(error) => Some(error),
            Self::Io { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        PersistedRaftLogEntry, RaftHardState, RaftHardStateStore, RaftLogSegment, RaftSnapshotStore,
    };
    use rafter::{LogIndex, Term};
    use std::{
        fs,
        io::Write,
        sync::atomic::{AtomicU64, Ordering},
    };

    static TEST_DIRECTORY_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn file_raft_node_stores_open_standard_layout() {
        let directory = test_directory("standard-layout");
        fs::create_dir_all(&directory).expect("replica directory creates");

        let stores = FileRaftNodeStores::open(&directory).expect("node stores open");
        let (hard_state, log_segment, snapshot_store) = stores.into_parts();

        assert_eq!(hard_state.current(), RaftHardState::default());
        assert_eq!(log_segment.next_index(), rafter::LogIndex(1));
        assert!(snapshot_store.current_snapshot().is_none());
        assert!(directory.join("log").is_file());
        assert!(directory.join("snapshots").is_dir());

        fs::remove_dir_all(directory).expect("test directory removes");
    }

    #[test]
    fn file_raft_node_stores_requires_existing_directory() {
        let directory = test_directory("missing-directory");

        let error =
            FileRaftNodeStores::open(&directory).expect_err("missing directory is rejected");

        assert!(matches!(
            error,
            OpenFileRaftNodeStoresError::Io {
                operation: "open raft node store directory",
                ..
            }
        ));
    }

    #[test]
    fn file_raft_node_stores_repair_uses_hard_state_commit_floor() {
        let directory = test_directory("repair-uses-commit");
        fs::create_dir_all(&directory).expect("replica directory creates");

        {
            let mut hard_state = FileRaftHardStateStore::open(directory.join("hard-state"))
                .expect("hard state opens");
            hard_state
                .write_hard_state(RaftHardState {
                    commit_index: LogIndex(1),
                    ..RaftHardState::default()
                })
                .expect("hard state writes");

            let mut log_segment =
                FileRaftLogSegment::open(directory.join("log")).expect("log opens");
            log_segment
                .append_entries(&[PersistedRaftLogEntry::application(
                    LogIndex(1),
                    Term(7),
                    b"committed".to_vec(),
                )])
                .expect("committed entry appends");
        }
        let mut log = fs::OpenOptions::new()
            .append(true)
            .open(directory.join("log"))
            .expect("log opens for partial tail append");
        log.write_all(&[0, 0])
            .expect("uncommitted partial tail writes");

        let stores = FileRaftNodeStores::open_repairing_uncommitted_log_tail(&directory)
            .expect("node stores repair uncommitted log tail");
        let (hard_state, log_segment, snapshot_store) = stores.into_parts();

        assert_eq!(hard_state.current().commit_index, LogIndex(1));
        assert_eq!(log_segment.next_index(), LogIndex(2));
        assert_eq!(
            log_segment.replay_entries(),
            vec![PersistedRaftLogEntry::application(
                LogIndex(1),
                Term(7),
                b"committed".to_vec(),
            )]
        );
        assert!(snapshot_store.current_snapshot().is_none());
        FileRaftNodeStores::open(&directory).expect("repaired stores open strictly");

        fs::remove_dir_all(directory).expect("test directory removes");
    }

    fn test_directory(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "rafter-storage-node-stores-{name}-{}-{}",
            std::process::id(),
            TEST_DIRECTORY_ID.fetch_add(1, Ordering::Relaxed)
        ))
    }
}
