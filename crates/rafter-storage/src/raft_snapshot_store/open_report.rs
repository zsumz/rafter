//! Observable snapshot-store opening and optional-staging recovery outcomes.
//!
//! Opening may create the snapshot directory or durably discard interrupted
//! optional transfer progress. This module exposes those nonfatal actions
//! without mixing them into the hard open-error vocabulary.

use super::FileRaftSnapshotStore;

/// Result of opening a file-backed snapshot store with recovery observability.
#[derive(Debug)]
pub struct OpenedFileRaftSnapshotStore {
    store: FileRaftSnapshotStore,
    report: FileRaftSnapshotStoreOpenReport,
}

impl OpenedFileRaftSnapshotStore {
    pub(super) const fn new(
        store: FileRaftSnapshotStore,
        report: FileRaftSnapshotStoreOpenReport,
    ) -> Self {
        Self { store, report }
    }

    /// Returns the opened store without consuming the outcome.
    #[must_use]
    pub const fn store(&self) -> &FileRaftSnapshotStore {
        &self.store
    }

    /// Returns the nonfatal actions observed while opening.
    #[must_use]
    pub const fn report(&self) -> &FileRaftSnapshotStoreOpenReport {
        &self.report
    }

    /// Splits the outcome into the opened store and its report.
    #[must_use]
    pub fn into_parts(self) -> (FileRaftSnapshotStore, FileRaftSnapshotStoreOpenReport) {
        (self.store, self.report)
    }

    /// Discards the report and returns only the opened store.
    #[must_use]
    pub fn into_store(self) -> FileRaftSnapshotStore {
        self.store
    }
}

/// Nonfatal filesystem and recovery actions performed while opening a store.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FileRaftSnapshotStoreOpenReport {
    /// Whether opening created the snapshot-store directory.
    pub created_directory: bool,
    /// Optional inbound-transfer recovery or ignored crash residue.
    pub pending_transfer_recovery: Option<PendingSnapshotTransferRecovery>,
}

/// Observable handling of an interrupted pending snapshot transfer.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PendingSnapshotTransferRecovery {
    /// A valid manifest existed without its body, so optional staging was
    /// durably cleared and the leader must restart from offset zero.
    DiscardedMissingBody,
    /// The body was shorter than the manifest's published prefix, so both
    /// staging files were durably cleared.
    DiscardedShortBody {
        /// Prefix length selected by the manifest.
        expected_bytes: u64,
        /// Body length observed at restart.
        actual_bytes: u64,
    },
    /// The published body prefix did not match the manifest checksum, so both
    /// staging files were durably cleared.
    DiscardedChecksumMismatch {
        /// Prefix checksum selected by the manifest.
        expected: u32,
        /// Checksum computed over the published body prefix.
        actual: u32,
    },
    /// The body contains an unpublished suffix beyond the manifest-selected
    /// prefix. The suffix is ignored until the next staged write truncates it.
    IgnoredUnpublishedSuffix {
        /// Body bytes selected by the manifest.
        published_bytes: u64,
        /// Total body bytes observed at restart.
        actual_bytes: u64,
    },
}
