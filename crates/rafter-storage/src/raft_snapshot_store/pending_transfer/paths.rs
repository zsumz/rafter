//! Pending-transfer artifact paths and operator-facing staging inspection.
//!
//! This module maps the fixed staging filenames to one snapshot-store directory.

use std::path::{Path, PathBuf};

use super::{
    constants::{PENDING_SNAPSHOT_TRANSFER_BODY_PATH, PENDING_SNAPSHOT_TRANSFER_PATH},
    status::PendingSnapshotTransferStagingStatus,
};

pub(in crate::raft_snapshot_store) fn staging_status(
    directory: &Path,
) -> PendingSnapshotTransferStagingStatus {
    let manifest_present = pending_snapshot_transfer_path(directory).exists();
    let body_path = pending_snapshot_transfer_body_path(directory);
    let body_bytes = body_path.metadata().ok().map(|metadata| metadata.len());
    PendingSnapshotTransferStagingStatus {
        manifest_present,
        body_present: body_bytes.is_some(),
        body_bytes,
        abandoned_body: body_bytes.is_some() && !manifest_present,
    }
}

pub(super) fn pending_snapshot_transfer_path(directory: &Path) -> PathBuf {
    directory.join(PENDING_SNAPSHOT_TRANSFER_PATH)
}

pub(in crate::raft_snapshot_store) fn pending_snapshot_transfer_body_path(
    directory: &Path,
) -> PathBuf {
    directory.join(PENDING_SNAPSHOT_TRANSFER_BODY_PATH)
}
