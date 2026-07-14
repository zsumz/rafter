use std::{
    error::Error,
    fmt,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use crate::{
    decode_raft_hard_state, durable_fs::sync_parent_directory, encode_raft_hard_state,
    DecodeRaftHardStateError, RaftHardState,
};

/// Errors returned while durably writing Raft hard state.
///
/// This enum is exhaustive because writes currently fail only through the
/// underlying filesystem operations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RaftHardStateStoreWriteError {
    Io {
        operation: &'static str,
        path: PathBuf,
        message: String,
    },
}

/// Errors returned while opening a Raft hard-state store.
///
/// This enum is exhaustive so callers can distinguish I/O from corrupt
/// persisted bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OpenRaftHardStateStoreError {
    Io {
        operation: &'static str,
        path: PathBuf,
        message: String,
    },
    Decode(DecodeRaftHardStateError),
}

impl fmt::Display for RaftHardStateStoreWriteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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

impl Error for RaftHardStateStoreWriteError {}

impl fmt::Display for OpenRaftHardStateStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io {
                operation,
                path,
                message,
            } => write!(
                formatter,
                "could not {operation} at {}: {message}",
                path.display()
            ),
            Self::Decode(error) => {
                write!(formatter, "stored Raft hard state is corrupt: {error}")
            }
        }
    }
}

impl Error for OpenRaftHardStateStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Decode(error) => Some(error),
            Self::Io { .. } => None,
        }
    }
}

/// Storage contract for the durable Raft hard state.
///
/// Implementations must make successful writes durable before returning and
/// must report the latest durable state through [`RaftHardStateStore::current`].
pub trait RaftHardStateStore {
    /// Writes the latest Raft hard state.
    ///
    /// # Errors
    ///
    /// Returns [`RaftHardStateStoreWriteError`] when the state cannot be
    /// durably written.
    fn write_hard_state(
        &mut self,
        state: RaftHardState,
    ) -> Result<(), RaftHardStateStoreWriteError>;

    /// Returns the latest hard state known to this store.
    fn current(&self) -> RaftHardState;
}

/// In-memory hard-state store for tests and volatile runtimes.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InMemoryRaftHardStateStore {
    current: RaftHardState,
}

impl InMemoryRaftHardStateStore {
    /// Creates an empty in-memory hard-state store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl RaftHardStateStore for InMemoryRaftHardStateStore {
    fn write_hard_state(
        &mut self,
        state: RaftHardState,
    ) -> Result<(), RaftHardStateStoreWriteError> {
        self.current = state;
        Ok(())
    }

    fn current(&self) -> RaftHardState {
        self.current
    }
}

/// File-backed hard-state store using temp-file replace plus parent sync.
#[derive(Debug)]
pub struct FileRaftHardStateStore {
    path: PathBuf,
    current: RaftHardState,
}

impl FileRaftHardStateStore {
    /// Opens a Raft hard-state store at `path`.
    ///
    /// # Errors
    ///
    /// Returns [`OpenRaftHardStateStoreError::Decode`] when existing bytes are
    /// corrupt, and [`OpenRaftHardStateStoreError::Io`] when the file cannot be
    /// opened or read.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, OpenRaftHardStateStoreError> {
        let path = path.as_ref().to_path_buf();
        if !path.exists() {
            return Ok(Self {
                path,
                current: RaftHardState::default(),
            });
        }

        let mut file = File::open(&path).map_err(|error| OpenRaftHardStateStoreError::Io {
            operation: "open raft hard state",
            path: path.clone(),
            message: error.to_string(),
        })?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|error| OpenRaftHardStateStoreError::Io {
                operation: "read raft hard state",
                path: path.clone(),
                message: error.to_string(),
            })?;
        let current =
            decode_raft_hard_state(&bytes).map_err(OpenRaftHardStateStoreError::Decode)?;
        Ok(Self { path, current })
    }

    fn temp_path(&self) -> PathBuf {
        let mut temp = self.path.clone().into_os_string();
        temp.push(".tmp");
        PathBuf::from(temp)
    }
}

impl RaftHardStateStore for FileRaftHardStateStore {
    fn write_hard_state(
        &mut self,
        state: RaftHardState,
    ) -> Result<(), RaftHardStateStoreWriteError> {
        let encoded = encode_raft_hard_state(&state);
        let temp_path = self.temp_path();
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp_path)
            .map_err(|error| RaftHardStateStoreWriteError::Io {
                operation: "open raft hard state temp file",
                path: temp_path.clone(),
                message: error.to_string(),
            })?;

        file.write_all(&encoded)
            .and_then(|()| file.sync_data())
            .map_err(|error| RaftHardStateStoreWriteError::Io {
                operation: "write raft hard state temp file",
                path: temp_path.clone(),
                message: error.to_string(),
            })?;
        drop(file);

        fs::rename(&temp_path, &self.path).map_err(|error| RaftHardStateStoreWriteError::Io {
            operation: "replace raft hard state",
            path: self.path.clone(),
            message: error.to_string(),
        })?;
        sync_parent_directory(&self.path).map_err(|error| RaftHardStateStoreWriteError::Io {
            operation: "sync raft hard state directory",
            path: self.path.clone(),
            message: error.to_string(),
        })?;
        self.current = state;
        Ok(())
    }

    fn current(&self) -> RaftHardState {
        self.current
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rafter::{LogIndex, NodeId, Term};
    use rafter_invariant_test::oracle_assert_eq;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_FILE_ID: AtomicU64 = AtomicU64::new(0);

    fn hard_state(term: u64, voted_for: Option<u64>) -> RaftHardState {
        RaftHardState {
            current_term: Term(term),
            voted_for: voted_for.map(NodeId),
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
        }
    }

    #[test]
    fn empty_file_store_opens_as_default_hard_state() {
        let path = test_store_path("empty");

        let store = FileRaftHardStateStore::open(&path).expect("store opens");

        assert_eq!(store.current(), RaftHardState::default());
        remove_test_file(path);
    }

    #[test]
    fn in_memory_store_returns_latest_written_hard_state() {
        let mut store = InMemoryRaftHardStateStore::new();

        store
            .write_hard_state(hard_state(1, Some(7)))
            .expect("state writes");
        store
            .write_hard_state(hard_state(2, None))
            .expect("state writes");

        assert_eq!(store.current(), hard_state(2, None));
    }

    #[test]
    fn file_store_reopens_latest_written_hard_state() {
        let path = test_store_path("latest");
        {
            let mut store = FileRaftHardStateStore::open(&path).expect("store opens");
            store
                .write_hard_state(hard_state(1, Some(7)))
                .expect("state writes");
            store
                .write_hard_state(hard_state(2, Some(8)))
                .expect("state writes");
        }

        let reopened = FileRaftHardStateStore::open(&path).expect("store reopens");

        oracle_assert_eq!(reopened.current(), hard_state(2, Some(8)));
        remove_test_file(path);
    }

    #[test]
    fn file_store_replaces_state_through_temp_file() {
        let path = test_store_path("replace");
        let temp_path = {
            let store = FileRaftHardStateStore::open(&path).expect("store opens");
            store.temp_path()
        };
        let mut store = FileRaftHardStateStore::open(&path).expect("store opens");

        store
            .write_hard_state(hard_state(3, Some(9)))
            .expect("state writes");

        assert!(!temp_path.exists());
        assert_eq!(
            FileRaftHardStateStore::open(&path).unwrap().current(),
            hard_state(3, Some(9))
        );
        remove_test_file(path);
    }

    #[test]
    fn file_store_ignores_abandoned_temp_file_before_rename() {
        let path = test_store_path("abandoned-temp");
        let temp_path = {
            let mut store = FileRaftHardStateStore::open(&path).expect("store opens");
            store
                .write_hard_state(hard_state(1, Some(7)))
                .expect("initial state writes");
            store.temp_path()
        };
        fs::write(&temp_path, encode_raft_hard_state(&hard_state(2, Some(8))))
            .expect("abandoned temp state is written");

        let reopened = FileRaftHardStateStore::open(&path).expect("store reopens");

        assert_eq!(reopened.current(), hard_state(1, Some(7)));
        remove_test_file(path);
    }

    #[test]
    fn corrupt_hard_state_fails_loudly_on_open() {
        let path = test_store_path("corrupt");
        fs::write(&path, b"bad").expect("corrupt store is written");

        assert_eq!(
            FileRaftHardStateStore::open(&path).map(|store| store.current()),
            Err(OpenRaftHardStateStoreError::Decode(
                DecodeRaftHardStateError::UnexpectedEof {
                    needed: 4,
                    remaining: 3,
                }
            ))
        );
        remove_test_file(path);
    }

    fn test_store_path(name: &str) -> PathBuf {
        let id = TEST_FILE_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "rafter-storage-{name}-{}-{id}.rafthard",
            std::process::id()
        ))
    }

    fn remove_test_file(path: PathBuf) {
        let _ = fs::remove_file(&path);
        let mut temp = path.into_os_string();
        temp.push(".tmp");
        let _ = fs::remove_file(PathBuf::from(temp));
    }
}
