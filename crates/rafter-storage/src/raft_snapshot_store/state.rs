//! File-backed snapshot state, operator inspection, and path vocabulary.
//!
//! This module owns the concrete handle and its cached current and staged
//! views. Opening, mutation, publication, and chunk I/O are implemented by
//! sibling modules over this state.

use std::path::{Path, PathBuf};

use rafter::{PendingSnapshotTransfer, RaftSnapshot};

use crate::{
    checksum::RunningCrc32, file_store_health::FileStoreHealth,
    file_store_ownership::SharedFileStoreOwnership,
};

use super::{pending_transfer::staging_status, PendingSnapshotTransferStagingStatus};

/// File-backed snapshot store with durable current-manifest and staging files.
#[derive(Debug)]
pub struct FileRaftSnapshotStore {
    pub(super) directory: PathBuf,
    pub(super) current: Option<CurrentSnapshot>,
    pub(super) pending: Option<StagedTransfer>,
    pub(super) next_sequence: Option<u64>,
    pub(super) health: FileStoreHealth,
    pub(super) ownership: Option<SharedFileStoreOwnership>,
}

/// The store's view of the current snapshot: descriptor plus where the
/// payload bytes start inside the envelope file. Payload bytes stay on disk
/// and are served by positioned reads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CurrentSnapshot {
    pub(super) file_name: String,
    pub(super) descriptor: RaftSnapshot,
    pub(super) payload_offset: u64,
}

/// The store's in-memory view of the staged transfer: the len-based kernel
/// state plus a running checksum of the staged body, so appends keep the
/// manifest's body checksum correct without re-reading the body file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct StagedTransfer {
    pub(super) transfer: PendingSnapshotTransfer,
    pub(super) body_crc: RunningCrc32,
}

impl FileRaftSnapshotStore {
    /// Returns the plain file name selected by the current manifest.
    #[must_use]
    pub fn current_snapshot_file_name(&self) -> Option<&str> {
        self.current
            .as_ref()
            .map(|current| current.file_name.as_str())
    }

    /// Returns file-level status for pending snapshot transfer staging files.
    ///
    /// This is operator-facing inspection data. It deliberately reports file
    /// presence even when no logical pending transfer is resumable.
    #[must_use]
    pub fn pending_snapshot_transfer_staging_status(&self) -> PendingSnapshotTransferStagingStatus {
        staging_status(&self.directory)
    }

    pub(crate) fn attach_ownership(&mut self, ownership: SharedFileStoreOwnership) {
        debug_assert!(self.ownership.is_none());
        self.ownership = Some(ownership);
    }

    pub(super) fn snapshot_path_for_write(
        &mut self,
        snapshot: &RaftSnapshot,
    ) -> Result<(PathBuf, String, u64), super::RaftSnapshotStoreWriteError> {
        let mut sequence = self
            .next_sequence
            .ok_or(super::RaftSnapshotStoreWriteError::SnapshotSequenceExhausted)?;
        loop {
            let file_name = snapshot_file_name(sequence, snapshot);
            let path = snapshot_path(&self.directory, &file_name);
            match path.try_exists() {
                Ok(false) => return Ok((path, file_name, sequence)),
                Ok(true) => {}
                Err(error) => {
                    return Err(self.io_failure("stat candidate raft snapshot path", &path, error));
                }
            }
            sequence = sequence
                .checked_add(1)
                .ok_or(super::RaftSnapshotStoreWriteError::SnapshotSequenceExhausted)?;
        }
    }

    pub(super) fn temp_snapshot_path(&self) -> PathBuf {
        self.directory
            .join(format!(".snapshot-{}.tmp", std::process::id()))
    }

    pub(super) fn temp_manifest_path(&self) -> PathBuf {
        self.directory
            .join(format!(".current.snapshot-{}.tmp", std::process::id()))
    }

    pub(super) fn temp_pending_snapshot_transfer_path(&self) -> PathBuf {
        self.directory.join(format!(
            ".pending.snapshot-transfer-{}.tmp",
            std::process::id()
        ))
    }
}

pub(super) fn snapshot_path(directory: &Path, file_name: &str) -> PathBuf {
    directory.join(file_name)
}

fn snapshot_file_name(sequence: u64, snapshot: &RaftSnapshot) -> String {
    format!(
        "snapshot-{}-{}-{}-{}.rfsn",
        sequence,
        snapshot.metadata.last_included_index.0,
        snapshot.metadata.last_included_term.0,
        snapshot.metadata.writer_id.0
    )
}
