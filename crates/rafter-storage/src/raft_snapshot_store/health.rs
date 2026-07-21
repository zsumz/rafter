//! Post-I/O health and committed-publication error mapping.
//!
//! Snapshot publication has a manifest commit point followed by optional
//! staging cleanup. This module keeps ordinary ambiguous I/O failures distinct
//! from failures that happen after the new current manifest is already durable.

use std::path::Path;

use super::{FileRaftSnapshotStore, RaftSnapshotStoreWriteError};

impl FileRaftSnapshotStore {
    /// Returns whether this handle must be dropped and reopened before a later
    /// mutation.
    ///
    /// Read-only inspection still reports the last state acknowledged by this
    /// handle, but reopen is the only recovery oracle after this becomes true.
    #[must_use]
    pub const fn requires_reopen(&self) -> bool {
        self.health.is_reopen_required()
    }

    pub(super) fn ensure_writable(&self) -> Result<(), RaftSnapshotStoreWriteError> {
        if self.requires_reopen() {
            Err(RaftSnapshotStoreWriteError::StoreRequiresReopen)
        } else {
            Ok(())
        }
    }

    pub(super) fn io_failure(
        &mut self,
        operation: &'static str,
        path: &Path,
        error: std::io::Error,
    ) -> RaftSnapshotStoreWriteError {
        self.health.require_reopen();
        RaftSnapshotStoreWriteError::Io {
            operation,
            path: path.to_path_buf(),
            source: error.into(),
        }
    }

    /// Marks errors from a helper that may have mutated snapshot-store files.
    /// Encoding and caller-validation errors remain reusable because their
    /// helpers prepare those decisions before touching authoritative storage.
    pub(super) fn poison_if_io(
        &mut self,
        error: RaftSnapshotStoreWriteError,
    ) -> RaftSnapshotStoreWriteError {
        if matches!(&error, RaftSnapshotStoreWriteError::Io { .. }) {
            self.health.require_reopen();
        }
        error
    }

    /// Converts a cleanup failure after current-manifest publication into an
    /// error that preserves the already-committed snapshot outcome.
    pub(super) fn snapshot_committed_cleanup_failure(
        &mut self,
        file_name: String,
        error: RaftSnapshotStoreWriteError,
    ) -> RaftSnapshotStoreWriteError {
        self.health.require_reopen();
        match error {
            RaftSnapshotStoreWriteError::Io {
                operation,
                path,
                source,
            } => RaftSnapshotStoreWriteError::SnapshotCommittedButReopenRequired {
                file_name,
                operation,
                path,
                source,
            },
            other => RaftSnapshotStoreWriteError::SnapshotCommittedButReopenRequired {
                file_name,
                operation: "finish current snapshot cleanup",
                path: self.directory.clone(),
                source: std::io::Error::other(other).into(),
            },
        }
    }
}
