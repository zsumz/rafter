//! File-backed hard-state publication and reopen-required lifecycle.
//!
//! This module owns strict open, temp-file replacement, parent-directory sync,
//! cached acknowledged state, and poisoning after ambiguous mutating I/O. It
//! delegates the versioned envelope grammar to the format layer.

use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use crate::{
    decode_raft_hard_state, durable_fs::sync_parent_directory, encode_raft_hard_state,
    file_store_health::FileStoreHealth, file_store_ownership::SharedFileStoreOwnership,
    RaftHardState,
};

use super::{
    contract::RaftHardStateStore,
    error::{OpenRaftHardStateStoreError, RaftHardStateStoreWriteError},
};

/// File-backed hard-state store using temp-file replace plus parent sync.
#[derive(Debug)]
pub struct FileRaftHardStateStore {
    path: PathBuf,
    current: RaftHardState,
    health: FileStoreHealth,
    ownership: Option<SharedFileStoreOwnership>,
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
            return Ok(Self::empty(path));
        }

        let mut file = File::open(&path).map_err(|error| OpenRaftHardStateStoreError::Io {
            operation: "open raft hard state",
            path: path.clone(),
            source: error.into(),
        })?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|error| OpenRaftHardStateStoreError::Io {
                operation: "read raft hard state",
                path: path.clone(),
                source: error.into(),
            })?;
        let current =
            decode_raft_hard_state(&bytes).map_err(OpenRaftHardStateStoreError::Decode)?;
        Ok(Self {
            path,
            current,
            health: FileStoreHealth::Healthy,
            ownership: None,
        })
    }

    /// Returns whether this handle must be dropped and reopened before a
    /// later mutation.
    ///
    /// A mutating I/O error can arrive after publication, so the cached value
    /// must not be treated as a recovery oracle once this returns `true`.
    #[must_use]
    pub const fn requires_reopen(&self) -> bool {
        self.health.is_reopen_required()
    }

    pub(crate) fn attach_ownership(&mut self, ownership: SharedFileStoreOwnership) {
        debug_assert!(self.ownership.is_none());
        self.ownership = Some(ownership);
    }

    fn empty(path: PathBuf) -> Self {
        Self {
            path,
            current: RaftHardState::default(),
            health: FileStoreHealth::Healthy,
            ownership: None,
        }
    }

    fn ensure_writable(&self) -> Result<(), RaftHardStateStoreWriteError> {
        if self.requires_reopen() {
            Err(RaftHardStateStoreWriteError::StoreRequiresReopen)
        } else {
            Ok(())
        }
    }

    fn io_failure(
        &mut self,
        operation: &'static str,
        path: &Path,
        error: std::io::Error,
    ) -> RaftHardStateStoreWriteError {
        self.health.require_reopen();
        RaftHardStateStoreWriteError::Io {
            operation,
            path: path.to_path_buf(),
            source: error.into(),
        }
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
        self.ensure_writable()?;

        let encoded = encode_raft_hard_state(&state);
        let temp_path = self.temp_path();
        let mut file = match OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp_path)
        {
            Ok(file) => file,
            Err(error) => {
                return Err(self.io_failure("open raft hard state temp file", &temp_path, error));
            }
        };

        if let Err(error) = file.write_all(&encoded).and_then(|()| file.sync_data()) {
            return Err(self.io_failure("write raft hard state temp file", &temp_path, error));
        }
        #[cfg(test)]
        if let Err(error) = crate::storage_failpoint_test::check(
            crate::storage_failpoint_test::DurabilityPoint::HardStateAfterTempSync,
        ) {
            return Err(self.io_failure("write raft hard state temp file", &temp_path, error));
        }
        drop(file);

        if let Err(error) = fs::rename(&temp_path, &self.path) {
            let path = self.path.clone();
            return Err(self.io_failure("replace raft hard state", &path, error));
        }
        #[cfg(test)]
        if let Err(error) = crate::storage_failpoint_test::check(
            crate::storage_failpoint_test::DurabilityPoint::HardStateAfterRename,
        ) {
            let path = self.path.clone();
            return Err(self.io_failure("replace raft hard state", &path, error));
        }
        if let Err(error) = sync_parent_directory(&self.path) {
            let path = self.path.clone();
            return Err(self.io_failure("sync raft hard state directory", &path, error));
        }
        #[cfg(test)]
        if let Err(error) = crate::storage_failpoint_test::check(
            crate::storage_failpoint_test::DurabilityPoint::HardStateAfterDirectorySync,
        ) {
            let path = self.path.clone();
            return Err(self.io_failure("sync raft hard state directory", &path, error));
        }

        self.current = state;
        Ok(())
    }

    fn current(&self) -> RaftHardState {
        self.current
    }
}
