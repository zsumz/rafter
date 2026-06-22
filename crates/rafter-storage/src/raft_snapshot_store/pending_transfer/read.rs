use std::{fs::File, io::Read, path::Path};

use rafter::PendingSnapshotTransfer;

use crate::checksum::RunningCrc32;

use super::{
    super::{OpenRaftSnapshotStoreError, RaftSnapshotStoreWriteError},
    cleanup::clear_pending_snapshot_transfer,
    codec::decode_pending_snapshot_transfer_manifest,
    error::DecodePendingSnapshotTransferError,
    manifest::PendingTransferManifest,
    paths::{pending_snapshot_transfer_body_path, pending_snapshot_transfer_path},
};

/// Reads and validates the staged transfer left by an earlier process.
///
/// The body bytes are read once to validate the manifest's checksum and
/// rebuild the running checksum state, then dropped: the store keeps only the
/// staged length and checksum in memory and re-reads the body file at
/// promotion time.
///
/// A body that does not hold what the manifest describes — too short, or
/// failing the manifest's body checksum — is the leftover of a crash between
/// the body replace and the manifest replace of a transfer restarted at
/// offset zero. Staged transfer progress is resumable-but-optional state, so
/// the staging is discarded and the store opens with no pending transfer;
/// the leader restarts the transfer from offset zero through the normal
/// rejection path. Corruption of the manifest itself stays a hard error: it
/// signals damaged storage, not an interrupted two-file swap.
pub(in crate::raft_snapshot_store) fn read_pending_snapshot_transfer(
    directory: &Path,
) -> Result<Option<(PendingSnapshotTransfer, RunningCrc32)>, OpenRaftSnapshotStoreError> {
    let manifest_path = pending_snapshot_transfer_path(directory);
    let Some(manifest) = read_pending_snapshot_transfer_manifest(&manifest_path)? else {
        return Ok(None);
    };
    let body_path = pending_snapshot_transfer_body_path(directory);
    let mut file = File::open(&body_path).map_err(|error| OpenRaftSnapshotStoreError::Io {
        operation: "open pending snapshot transfer body",
        path: body_path.clone(),
        message: error.to_string(),
    })?;
    let mut body = Vec::new();
    file.read_to_end(&mut body)
        .map_err(|error| OpenRaftSnapshotStoreError::Io {
            operation: "read pending snapshot transfer body",
            path: body_path,
            message: error.to_string(),
        })?;
    let received_len = usize::try_from(manifest.received_payload_len).map_err(|_| {
        OpenRaftSnapshotStoreError::PendingTransfer(
            DecodePendingSnapshotTransferError::SnapshotEnvelopeTooLarge {
                len: manifest.received_payload_len,
            },
        )
    })?;
    if body.len() < received_len {
        discard_inconsistent_staging(directory)?;
        return Ok(None);
    }
    let mut body_crc = RunningCrc32::new();
    body_crc.update(&body[..received_len]);
    if body_crc.value() != manifest.body_checksum {
        discard_inconsistent_staging(directory)?;
        return Ok(None);
    }
    if manifest.received_payload_len > manifest.total_payload_len {
        return Err(OpenRaftSnapshotStoreError::PendingTransfer(
            DecodePendingSnapshotTransferError::ReceivedPayloadTooLong {
                received_bytes: manifest.received_payload_len,
                total_payload_len: manifest.total_payload_len,
            },
        ));
    }
    Ok(Some((
        PendingSnapshotTransfer {
            leader_id: manifest.leader_id,
            transfer_id: manifest.transfer_id,
            metadata: manifest.metadata,
            total_payload_len: manifest.total_payload_len,
            application_payload_crc32: manifest.application_payload_crc32,
            received_len: manifest.received_payload_len,
        },
        body_crc,
    )))
}

/// Removes both staging files (manifest and body, syncing the parent) so the
/// store opens with no pending transfer. Reuses the durable clear helper the
/// write path uses; its only failure mode is I/O, surfaced as an open error.
fn discard_inconsistent_staging(directory: &Path) -> Result<(), OpenRaftSnapshotStoreError> {
    clear_pending_snapshot_transfer(directory).map_err(|error| match error {
        RaftSnapshotStoreWriteError::Io {
            operation,
            path,
            message,
        } => OpenRaftSnapshotStoreError::Io {
            operation,
            path,
            message,
        },
        other => OpenRaftSnapshotStoreError::Io {
            operation: "discard inconsistent pending snapshot transfer staging",
            path: directory.to_path_buf(),
            message: other.to_string(),
        },
    })
}

fn read_pending_snapshot_transfer_manifest(
    path: &Path,
) -> Result<Option<PendingTransferManifest>, OpenRaftSnapshotStoreError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(OpenRaftSnapshotStoreError::Io {
                operation: "open pending snapshot transfer manifest",
                path: path.to_path_buf(),
                message: error.to_string(),
            });
        }
    };
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|error| OpenRaftSnapshotStoreError::Io {
            operation: "read pending snapshot transfer manifest",
            path: path.to_path_buf(),
            message: error.to_string(),
        })?;
    decode_pending_snapshot_transfer_manifest(&bytes)
        .map(Some)
        .map_err(OpenRaftSnapshotStoreError::PendingTransfer)
}
