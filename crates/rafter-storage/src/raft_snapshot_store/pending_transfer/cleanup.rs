//! Durable removal of pending-transfer manifests and abandoned body files.
//!
//! Cleanup owns file deletion and parent-directory synchronization, not transfer
//! validation or current-snapshot publication.

use std::{fs, path::Path};

use crate::durable_fs::sync_parent_directory;

use super::{
    super::RaftSnapshotStoreWriteError,
    filesystem::remove_file_if_exists,
    paths::{pending_snapshot_transfer_body_path, pending_snapshot_transfer_path, staging_status},
};

pub(in crate::raft_snapshot_store) fn clear_pending_snapshot_transfer(
    directory: &Path,
) -> Result<(), RaftSnapshotStoreWriteError> {
    let manifest_path = pending_snapshot_transfer_path(directory);
    let body_path = pending_snapshot_transfer_body_path(directory);
    let removed_manifest =
        remove_file_if_exists(&manifest_path, "remove pending snapshot transfer manifest")?;
    #[cfg(test)]
    if removed_manifest {
        crate::storage_failpoint_test::check(
            crate::storage_failpoint_test::DurabilityPoint::PendingClearAfterManifestRemoval,
        )
        .map_err(|error| RaftSnapshotStoreWriteError::Io {
            operation: "remove pending snapshot transfer manifest",
            path: manifest_path.clone(),
            source: error.into(),
        })?;
    }
    let removed_body = remove_file_if_exists(&body_path, "remove pending snapshot transfer body")?;
    #[cfg(test)]
    if removed_body {
        crate::storage_failpoint_test::check(
            crate::storage_failpoint_test::DurabilityPoint::PendingClearAfterBodyRemoval,
        )
        .map_err(|error| RaftSnapshotStoreWriteError::Io {
            operation: "remove pending snapshot transfer body",
            path: body_path.clone(),
            source: error.into(),
        })?;
    }
    if removed_manifest || removed_body {
        sync_parent_directory(&manifest_path).map_err(|error| RaftSnapshotStoreWriteError::Io {
            operation: "sync pending snapshot transfer directory after clear",
            path: directory.to_path_buf(),
            source: error.into(),
        })?;
        #[cfg(test)]
        crate::storage_failpoint_test::check(
            crate::storage_failpoint_test::DurabilityPoint::PendingClearAfterDirectorySync,
        )
        .map_err(|error| RaftSnapshotStoreWriteError::Io {
            operation: "sync pending snapshot transfer directory after clear",
            path: directory.to_path_buf(),
            source: error.into(),
        })?;
    }
    Ok(())
}

pub(in crate::raft_snapshot_store) fn remove_abandoned_pending_snapshot_transfer_staging(
    directory: &Path,
) -> Result<bool, RaftSnapshotStoreWriteError> {
    let status = staging_status(directory);
    if !status.abandoned_body {
        return Ok(false);
    }

    let body_path = pending_snapshot_transfer_body_path(directory);
    fs::remove_file(&body_path).map_err(|error| RaftSnapshotStoreWriteError::Io {
        operation: "remove abandoned pending snapshot transfer body",
        path: body_path.clone(),
        source: error.into(),
    })?;
    sync_parent_directory(&body_path).map_err(|error| RaftSnapshotStoreWriteError::Io {
        operation: "sync pending snapshot transfer directory after abandoned body removal",
        path: directory.to_path_buf(),
        source: error.into(),
    })?;
    Ok(true)
}
