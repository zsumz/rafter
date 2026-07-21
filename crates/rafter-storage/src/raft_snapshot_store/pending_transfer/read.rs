//! Open-time recovery of optional inbound snapshot-transfer progress.
//!
//! Manifest corruption fails loudly. Missing, short, or checksum-inconsistent
//! bodies are discarded as interrupted optional progress. Valid staged bodies
//! are verified in bounded reads and are never materialized whole at restart.

use std::{fs::File, io::Read, path::Path};

use rafter::PendingSnapshotTransfer;

use crate::checksum::RunningCrc32;

use super::{
    super::{
        OpenRaftSnapshotStoreError, PendingSnapshotTransferRecovery, RaftSnapshotStoreWriteError,
    },
    cleanup::clear_pending_snapshot_transfer,
    codec::decode_pending_snapshot_transfer_manifest,
    manifest::PendingTransferManifest,
    paths::{pending_snapshot_transfer_body_path, pending_snapshot_transfer_path},
};

const VERIFY_STAGED_BODY_CHUNK_BYTES: usize = 256 * 1024;

pub(in crate::raft_snapshot_store) type OpenedPendingSnapshotTransfer = (
    Option<(PendingSnapshotTransfer, RunningCrc32)>,
    Option<PendingSnapshotTransferRecovery>,
);

/// Reads and validates the staged transfer left by an earlier process.
///
/// The manifest has already pinned the transfer descriptor, received length,
/// and body checksum. Recovery verifies exactly that body prefix in bounded
/// reads and rebuilds the incremental checksum state without keeping the bytes
/// resident. A longer suffix is harmless crash residue and remains ignored.
///
/// Missing, short, or checksum-mismatched body bytes are the recoverable shape
/// of an interrupted two-file staging update. Because staged progress is
/// optional, recovery durably discards both files and opens with no pending
/// transfer. Manifest corruption remains a hard open error.
pub(in crate::raft_snapshot_store) fn read_pending_snapshot_transfer(
    directory: &Path,
) -> Result<OpenedPendingSnapshotTransfer, OpenRaftSnapshotStoreError> {
    let manifest_path = pending_snapshot_transfer_path(directory);
    let Some(manifest) = read_pending_snapshot_transfer_manifest(&manifest_path)? else {
        return Ok((None, None));
    };
    let body_path = pending_snapshot_transfer_body_path(directory);
    let mut file = match File::open(&body_path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            discard_inconsistent_staging(directory)?;
            return Ok((
                None,
                Some(PendingSnapshotTransferRecovery::DiscardedMissingBody),
            ));
        }
        Err(error) => {
            return Err(OpenRaftSnapshotStoreError::Io {
                operation: "open pending snapshot transfer body",
                path: body_path,
                source: error.into(),
            });
        }
    };
    let body_len = file
        .metadata()
        .map_err(|error| OpenRaftSnapshotStoreError::Io {
            operation: "stat pending snapshot transfer body",
            path: body_path.clone(),
            source: error.into(),
        })?
        .len();
    if body_len < manifest.received_payload_len {
        discard_inconsistent_staging(directory)?;
        return Ok((
            None,
            Some(PendingSnapshotTransferRecovery::DiscardedShortBody {
                expected_bytes: manifest.received_payload_len,
                actual_bytes: body_len,
            }),
        ));
    }

    let body_crc =
        checksum_staged_body_prefix(&mut file, manifest.received_payload_len, &body_path)?;
    if body_crc.value() != manifest.body_checksum {
        let actual = body_crc.value();
        discard_inconsistent_staging(directory)?;
        return Ok((
            None,
            Some(PendingSnapshotTransferRecovery::DiscardedChecksumMismatch {
                expected: manifest.body_checksum,
                actual,
            }),
        ));
    }

    let recovery = (body_len > manifest.received_payload_len).then_some(
        PendingSnapshotTransferRecovery::IgnoredUnpublishedSuffix {
            published_bytes: manifest.received_payload_len,
            actual_bytes: body_len,
        },
    );
    Ok((
        Some((
            PendingSnapshotTransfer {
                leader_id: manifest.leader_id,
                transfer_id: manifest.transfer_id,
                metadata: manifest.metadata,
                total_payload_len: manifest.total_payload_len,
                application_payload_crc32: manifest.application_payload_crc32,
                received_len: manifest.received_payload_len,
            },
            body_crc,
        )),
        recovery,
    ))
}

fn checksum_staged_body_prefix(
    file: &mut File,
    mut remaining: u64,
    body_path: &Path,
) -> Result<RunningCrc32, OpenRaftSnapshotStoreError> {
    let mut checksum = RunningCrc32::new();
    let mut buffer = vec![0_u8; bounded_usize_len(remaining, VERIFY_STAGED_BODY_CHUNK_BYTES)];
    while remaining > 0 {
        let len = bounded_usize_len(remaining, buffer.len());
        file.read_exact(&mut buffer[..len])
            .map_err(|error| OpenRaftSnapshotStoreError::Io {
                operation: "read pending snapshot transfer body",
                path: body_path.to_path_buf(),
                source: error.into(),
            })?;
        checksum.update(&buffer[..len]);
        remaining -= u64::try_from(len).unwrap_or(remaining);
    }
    Ok(checksum)
}

fn bounded_usize_len(remaining: u64, max_len: usize) -> usize {
    let max_len_u64 = u64::try_from(max_len).unwrap_or(u64::MAX);
    let bounded = remaining.min(max_len_u64);
    usize::try_from(bounded).unwrap_or(max_len)
}

/// Removes both staging files (manifest and body, syncing the parent) so the
/// store opens with no pending transfer. Reuses the durable clear helper the
/// write path uses; its only failure mode is I/O, surfaced as an open error.
fn discard_inconsistent_staging(directory: &Path) -> Result<(), OpenRaftSnapshotStoreError> {
    clear_pending_snapshot_transfer(directory).map_err(|error| match error {
        RaftSnapshotStoreWriteError::Io {
            operation,
            path,
            source,
        } => OpenRaftSnapshotStoreError::Io {
            operation,
            path,
            source,
        },
        other => OpenRaftSnapshotStoreError::Io {
            operation: "discard inconsistent pending snapshot transfer staging",
            path: directory.to_path_buf(),
            source: std::io::Error::other(other).into(),
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
                source: error.into(),
            });
        }
    };
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|error| OpenRaftSnapshotStoreError::Io {
            operation: "read pending snapshot transfer manifest",
            path: path.to_path_buf(),
            source: error.into(),
        })?;
    decode_pending_snapshot_transfer_manifest(&bytes)
        .map(Some)
        .map_err(OpenRaftSnapshotStoreError::PendingTransfer)
}
