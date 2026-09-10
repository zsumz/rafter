//! Append-only hard-state publication with one data sync per write.
//!
//! This module owns the open file, acknowledged cache, and poison boundary.
//! The recovery module validates bytes before this handle can append again.

use super::{
    journal_error::OpenJournalRaftHardStateStoreError, journal_recovery, RaftHardStateStore,
    RaftHardStateStoreWriteError,
};
use crate::{
    encode_raft_hard_state, file_store_health::FileStoreHealth,
    file_store_ownership::SharedFileStoreOwnership, RaftHardState,
};
use std::{
    fs::File,
    io::Write,
    path::{Path, PathBuf},
};

/// Opt-in append-only hard-state storage using the RFHJ v1 journal format.
///
/// A write appends one 51-byte checksummed state and calls `sync_data` once.
/// The cached state advances only after that sync succeeds. Mutating I/O errors
/// poison the handle; drop and reopen it before using storage again.
///
/// This format is incompatible with [`super::FileRaftHardStateStore`]; neither
/// backend converts the other's files. Journals currently grow by 51 bytes per
/// write and replay linearly at open. There is no automatic compaction yet.
/// Direct open requires caller-enforced exclusive ownership of the path.
#[derive(Debug)]
pub struct JournalRaftHardStateStore {
    path: PathBuf,
    file: File,
    current: RaftHardState,
    health: FileStoreHealth,
    ownership: Option<SharedFileStoreOwnership>,
}

impl JournalRaftHardStateStore {
    /// Opens or creates a journal, discarding only an incomplete final record.
    ///
    /// Complete corrupt records and incomplete/unknown headers fail closed.
    /// Recovered bytes and the parent directory are synced before returning.
    ///
    /// # Errors
    /// Returns an error if validation, I/O, or durable tail repair fails.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, OpenJournalRaftHardStateStoreError> {
        let path = path.as_ref().to_path_buf();
        let (file, current) = journal_recovery::open(&path)?;
        Ok(Self {
            path,
            file,
            current,
            health: FileStoreHealth::Healthy,
            ownership: None,
        })
    }

    /// Whether an ambiguous write failure requires dropping and reopening.
    #[must_use]
    pub const fn requires_reopen(&self) -> bool {
        self.health.is_reopen_required()
    }

    pub(crate) fn attach_ownership(&mut self, ownership: SharedFileStoreOwnership) {
        debug_assert!(self.ownership.is_none());
        self.ownership = Some(ownership);
    }

    fn io_failure(
        &mut self,
        operation: &'static str,
        error: std::io::Error,
    ) -> RaftHardStateStoreWriteError {
        self.health.require_reopen();
        RaftHardStateStoreWriteError::Io {
            operation,
            path: self.path.clone(),
            source: error.into(),
        }
    }
}

impl RaftHardStateStore for JournalRaftHardStateStore {
    fn write_hard_state(
        &mut self,
        state: RaftHardState,
    ) -> Result<(), RaftHardStateStoreWriteError> {
        if self.requires_reopen() {
            return Err(RaftHardStateStoreWriteError::StoreRequiresReopen);
        }
        let encoded = encode_raft_hard_state(&state);
        if let Err(error) = self.file.write_all(&encoded) {
            return Err(self.io_failure("append hard-state journal", error));
        }
        #[cfg(test)]
        if let Err(error) = crate::storage_failpoint_test::check(
            crate::storage_failpoint_test::DurabilityPoint::JournalAfterAppend,
        ) {
            return Err(self.io_failure("append hard-state journal", error));
        }
        if let Err(error) = self.file.sync_data() {
            return Err(self.io_failure("sync hard-state journal", error));
        }
        #[cfg(test)]
        if let Err(error) = crate::storage_failpoint_test::check(
            crate::storage_failpoint_test::DurabilityPoint::JournalAfterSync,
        ) {
            return Err(self.io_failure("sync hard-state journal", error));
        }
        self.current = state;
        Ok(())
    }

    fn current(&self) -> RaftHardState {
        self.current
    }
}
