//! Public snapshot-inventory, retention, report, and error vocabulary.

use std::{error::Error, fmt, path::PathBuf};

use rafter::{LogIndex, NodeId, Term};

use crate::StorageIoError;

use super::super::PendingSnapshotTransferStagingStatus;

/// Parsed identity carried by a snapshot filename written by this store.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotFileIdentity {
    /// Monotonic publication sequence assigned by the snapshot store.
    pub sequence: u64,
    /// Compacted log index embedded in the generated filename.
    pub last_included_index: LogIndex,
    /// Compacted log term embedded in the generated filename.
    pub last_included_term: Term,
    /// Node that wrote the snapshot metadata.
    pub writer_id: NodeId,
}

/// One snapshot file visible in the snapshot directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotFileInfo {
    /// Plain filename relative to the snapshot-store directory.
    pub file_name: String,
    /// Current file length in bytes.
    pub bytes: u64,
    /// Parsed generated-filename identity, when the selected filename follows
    /// the store's canonical naming grammar.
    pub identity: Option<SnapshotFileIdentity>,
}

/// Kind of recognized crash-residue temporary file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SnapshotTemporaryFileKind {
    /// Temporary immutable `RFSN` envelope.
    SnapshotEnvelope,
    /// Temporary `RFSM` current-snapshot manifest.
    CurrentManifest,
    /// Temporary `RFPT` pending-transfer manifest.
    PendingTransferManifest,
}

/// One recognized temporary file visible in the snapshot directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotTemporaryFileInfo {
    /// Plain filename relative to the snapshot-store directory.
    pub file_name: String,
    /// Current file length in bytes.
    pub bytes: u64,
    /// Publication stage that owns this temporary file.
    pub kind: SnapshotTemporaryFileKind,
    /// Process id encoded by the temporary-file naming grammar.
    pub process_id: u64,
}

/// Deterministic operator view of snapshot-directory artifacts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotInventory {
    /// Manifest-selected snapshot, when one is current.
    pub current: Option<SnapshotFileInfo>,
    /// Canonically named snapshots older than the current snapshot, sorted
    /// from oldest to newest publication sequence.
    pub retained: Vec<SnapshotFileInfo>,
    /// Canonically named files that are not an older predecessor of the current
    /// snapshot, typically a crash-orphaned future publication. When no current
    /// snapshot exists, every canonical snapshot file is unreferenced.
    pub unreferenced: Vec<SnapshotFileInfo>,
    /// Recognized temporary files, sorted by filename.
    pub temporary: Vec<SnapshotTemporaryFileInfo>,
    /// Directory entries this implementation does not own and will never prune.
    pub unrecognized: Vec<String>,
    /// Stable inbound-transfer manifest and body status.
    pub pending_transfer: PendingSnapshotTransferStagingStatus,
}

/// Policy for retaining complete snapshots that are not current.
///
/// This enum is exhaustive because these are the complete maintenance policies
/// supported by the current snapshot store.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotRetention {
    /// Keep every complete snapshot file.
    KeepAll,
    /// Keep the newest `count` snapshots preceding the current one.
    ///
    /// When there is no current snapshot, no file is considered previous; all
    /// canonical files are unreferenced cleanup candidates.
    KeepPrevious(usize),
    /// Keep only the current snapshot and remove every canonical unreferenced
    /// snapshot file.
    CurrentOnly,
}

/// Files removed by one successful or partially successful maintenance pass.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SnapshotPruneReport {
    /// Canonical noncurrent snapshot files removed oldest-first.
    pub removed_snapshots: Vec<SnapshotFileInfo>,
    /// Recognized temporary files removed in filename order.
    pub removed_temporary_files: Vec<SnapshotTemporaryFileInfo>,
}

/// Errors returned while inspecting snapshot-directory artifacts.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SnapshotInventoryError {
    /// An earlier mutating I/O failure requires authoritative reopen before an
    /// inventory can safely identify the selected snapshot.
    StoreRequiresReopen,
    /// The cached current manifest selects a file that is no longer present.
    CurrentSnapshotMissing { path: PathBuf },
    /// The cached current manifest selects a non-regular directory entry.
    CurrentSnapshotNotRegularFile { path: PathBuf },
    /// A filesystem operation failed during inspection.
    Io {
        operation: &'static str,
        path: PathBuf,
        source: StorageIoError,
    },
}

/// Errors returned while pruning snapshot-directory artifacts.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SnapshotPruneError {
    /// An earlier mutating I/O failure requires authoritative reopen before
    /// cleanup may rely on the selected-snapshot cache.
    StoreRequiresReopen,
    /// Inventory failed before cleanup began.
    Inventory(SnapshotInventoryError),
    /// A removal or directory sync failed after zero or more files were
    /// removed. `removed` records the in-process removal prefix for
    /// observability and idempotent retry; a directory sync may not have
    /// completed.
    Io {
        operation: &'static str,
        path: PathBuf,
        source: StorageIoError,
        removed: SnapshotPruneReport,
    },
}

impl fmt::Display for SnapshotInventoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StoreRequiresReopen => formatter.write_str(
                "snapshot inventory requires reopening the file-backed store after an earlier I/O failure",
            ),
            Self::CurrentSnapshotMissing { path } => write!(
                formatter,
                "current Raft snapshot file {} is missing",
                path.display()
            ),
            Self::CurrentSnapshotNotRegularFile { path } => write!(
                formatter,
                "current Raft snapshot path {} is not a regular file",
                path.display()
            ),
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

impl Error for SnapshotInventoryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source.as_io_error()),
            Self::StoreRequiresReopen
            | Self::CurrentSnapshotMissing { .. }
            | Self::CurrentSnapshotNotRegularFile { .. } => None,
        }
    }
}

impl fmt::Display for SnapshotPruneError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StoreRequiresReopen => formatter.write_str(
                "snapshot cleanup requires reopening the file-backed store after an earlier I/O failure",
            ),
            Self::Inventory(error) => write!(formatter, "snapshot inventory failed: {error}"),
            Self::Io {
                operation,
                path,
                source,
                removed,
            } => write!(
                formatter,
                "could not {operation} at {} after removing {} snapshot files and {} temporary files: {source}",
                path.display(),
                removed.removed_snapshots.len(),
                removed.removed_temporary_files.len(),
            ),
        }
    }
}

impl Error for SnapshotPruneError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Inventory(error) => Some(error),
            Self::Io { source, .. } => Some(source.as_io_error()),
            Self::StoreRequiresReopen => None,
        }
    }
}
