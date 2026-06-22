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
    let removed_body = remove_file_if_exists(&body_path, "remove pending snapshot transfer body")?;
    if removed_manifest || removed_body {
        sync_parent_directory(&manifest_path).map_err(|error| RaftSnapshotStoreWriteError::Io {
            operation: "sync pending snapshot transfer directory after clear",
            path: directory.to_path_buf(),
            message: error.to_string(),
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
        message: error.to_string(),
    })?;
    sync_parent_directory(&body_path).map_err(|error| RaftSnapshotStoreWriteError::Io {
        operation: "sync pending snapshot transfer directory after abandoned body removal",
        path: directory.to_path_buf(),
        message: error.to_string(),
    })?;
    Ok(true)
}
