//! File-backed retained-log state, acknowledged views, and handle health.
//!
//! This module owns the concrete state shared by open, mutation, and rewrite
//! paths. It does not decide continuity, publication order, or replay policy.

use std::{fs::File, path::PathBuf};

use rafter::LogIndex;

use crate::{
    file_store_health::FileStoreHealth, file_store_ownership::SharedFileStoreOwnership,
    StorageIoError,
};

use super::ContiguousLogEntries;

/// File-backed [`super::RaftLogSegment`] implementation.
#[derive(Debug)]
pub struct FileRaftLogSegment {
    pub(super) file: File,
    pub(super) path: PathBuf,
    pub(super) compacted_through: LogIndex,
    pub(super) entries: ContiguousLogEntries,
    pub(super) health: FileStoreHealth,
    pub(super) ownership: Option<SharedFileStoreOwnership>,
}

#[derive(Debug)]
pub(super) struct LogIoFailure {
    pub(super) operation: &'static str,
    pub(super) source: StorageIoError,
}

impl FileRaftLogSegment {
    /// Returns the last successfully acknowledged compacted-prefix boundary.
    #[must_use]
    pub fn compacted_through(&self) -> LogIndex {
        self.compacted_through
    }

    /// Returns whether this handle must be dropped and reopened before another
    /// mutation.
    ///
    /// An I/O error can arrive after a frame append, replacement rename, or
    /// compaction marker publication. Reopen is the recovery oracle once this
    /// returns `true`.
    #[must_use]
    pub const fn requires_reopen(&self) -> bool {
        self.health.is_reopen_required()
    }

    pub(crate) fn attach_ownership(&mut self, ownership: SharedFileStoreOwnership) {
        debug_assert!(self.ownership.is_none());
        self.ownership = Some(ownership);
    }

    pub(super) fn record_io_failure(
        &mut self,
        operation: &'static str,
        error: std::io::Error,
    ) -> LogIoFailure {
        self.health.require_reopen();
        LogIoFailure {
            operation,
            source: error.into(),
        }
    }
}
