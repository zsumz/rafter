//! Directory-synced deletion of unreferenced snapshots and abandoned temps.

use std::{fs, path::PathBuf};

use crate::durable_fs::sync_parent_directory;

use super::super::FileRaftSnapshotStore;
use super::model::{
    SnapshotFileInfo, SnapshotInventory, SnapshotInventoryError, SnapshotPruneError,
    SnapshotPruneReport, SnapshotRetention, SnapshotTemporaryFileInfo,
};

impl FileRaftSnapshotStore {
    /// Removes canonical unreferenced snapshot files according to `retention`.
    ///
    /// The current manifest and selected snapshot are never removed. Unknown
    /// files and recognized temporary files are also left untouched. Unless the
    /// policy is [`SnapshotRetention::KeepAll`], future crash-orphaned snapshot
    /// files are removed independently of the previous-snapshot count.
    /// Deletions are followed by one snapshot-directory sync.
    ///
    /// A cleanup I/O error does not poison the store because maintenance never
    /// changes logical Raft state. The returned error records the observed
    /// deletion prefix; because the directory sync may not have completed, the
    /// operation remains safe to retry idempotently after restart.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotPruneError::StoreRequiresReopen`] when the handle's
    /// selected-snapshot cache is not authoritative, or a typed inventory/I/O
    /// error when cleanup cannot complete.
    pub fn prune_snapshots(
        &mut self,
        retention: SnapshotRetention,
    ) -> Result<SnapshotPruneReport, SnapshotPruneError> {
        if retention == SnapshotRetention::KeepAll {
            self.ensure_maintenance_ready()?;
            return Ok(SnapshotPruneReport::default());
        }
        let inventory = self.inventory_for_maintenance()?;
        let keep = match retention {
            SnapshotRetention::KeepAll => unreachable!("handled above"),
            SnapshotRetention::KeepPrevious(count) => count,
            SnapshotRetention::CurrentOnly => 0,
        };
        let remove_count = inventory.retained.len().saturating_sub(keep);
        let mut snapshots = inventory
            .retained
            .into_iter()
            .take(remove_count)
            .chain(inventory.unreferenced)
            .collect::<Vec<_>>();
        snapshots.sort_by(|left, right| {
            snapshot_sequence(left)
                .cmp(&snapshot_sequence(right))
                .then(left.file_name.cmp(&right.file_name))
        });
        self.remove_inventory_artifacts(snapshots, Vec::new())
    }

    /// Removes recognized snapshot, current-manifest, and pending-manifest temp
    /// files left by interrupted publication.
    ///
    /// Stable pending-transfer files and unrecognized directory entries are not
    /// touched. The removals are followed by one snapshot-directory sync.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotPruneError`] with the observed deletion prefix when a
    /// removal or directory sync fails.
    pub fn cleanup_abandoned_snapshot_temporary_files(
        &mut self,
    ) -> Result<SnapshotPruneReport, SnapshotPruneError> {
        let inventory = self.inventory_for_maintenance()?;
        self.remove_inventory_artifacts(Vec::new(), inventory.temporary)
    }

    fn ensure_maintenance_ready(&self) -> Result<(), SnapshotPruneError> {
        if self.requires_reopen() {
            Err(SnapshotPruneError::StoreRequiresReopen)
        } else {
            Ok(())
        }
    }

    fn inventory_for_maintenance(&self) -> Result<SnapshotInventory, SnapshotPruneError> {
        match self.snapshot_inventory() {
            Ok(inventory) => Ok(inventory),
            Err(SnapshotInventoryError::StoreRequiresReopen) => {
                Err(SnapshotPruneError::StoreRequiresReopen)
            }
            Err(error) => Err(SnapshotPruneError::Inventory(error)),
        }
    }

    pub(in crate::raft_snapshot_store) fn remove_inventory_artifacts(
        &self,
        snapshots: Vec<SnapshotFileInfo>,
        temporary_files: Vec<SnapshotTemporaryFileInfo>,
    ) -> Result<SnapshotPruneReport, SnapshotPruneError> {
        let mut report = SnapshotPruneReport::default();
        let mut first_removed_path = None;

        for snapshot in snapshots {
            let path = self.directory.join(&snapshot.file_name);
            if let Err(error) = fs::remove_file(&path) {
                return Err(prune_io_error(
                    "remove unreferenced raft snapshot",
                    path,
                    error,
                    report,
                ));
            }
            first_removed_path.get_or_insert_with(|| path.clone());
            report.removed_snapshots.push(snapshot);
        }
        for temporary in temporary_files {
            let path = self.directory.join(&temporary.file_name);
            if let Err(error) = fs::remove_file(&path) {
                return Err(prune_io_error(
                    "remove abandoned raft snapshot temporary file",
                    path,
                    error,
                    report,
                ));
            }
            first_removed_path.get_or_insert_with(|| path.clone());
            report.removed_temporary_files.push(temporary);
        }

        if let Some(removed_path) = first_removed_path {
            if let Err(error) = sync_parent_directory(&removed_path) {
                return Err(prune_io_error(
                    "sync raft snapshot directory after maintenance",
                    self.directory.clone(),
                    error,
                    report,
                ));
            }
        }
        Ok(report)
    }
}

fn prune_io_error(
    operation: &'static str,
    path: PathBuf,
    error: std::io::Error,
    removed: SnapshotPruneReport,
) -> SnapshotPruneError {
    SnapshotPruneError::Io {
        operation,
        path,
        source: error.into(),
        removed,
    }
}

fn snapshot_sequence(snapshot: &SnapshotFileInfo) -> Option<u64> {
    snapshot.identity.map(|identity| identity.sequence)
}
