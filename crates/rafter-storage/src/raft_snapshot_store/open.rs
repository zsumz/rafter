//! File-backed snapshot opening and streaming envelope verification.

use std::{fs, io, path::Path};

use rafter::RaftSnapshot;

use super::{
    manifest_path, read_manifest, read_pending_snapshot_transfer, snapshot_path, CurrentSnapshot,
    FileRaftSnapshotStore, FileRaftSnapshotStoreOpenReport, OpenRaftSnapshotStoreError,
    OpenedFileRaftSnapshotStore, PendingSnapshotTransferRecovery, StagedTransfer,
};
use crate::{
    durable_fs::{sync_parent_directory, ParentDirectorySyncBatch},
    file_store_health::FileStoreHealth,
};

mod verify;

use verify::verify_snapshot_envelope;

fn read_staged_transfer(
    directory: &std::path::Path,
) -> Result<
    (
        Option<StagedTransfer>,
        Option<PendingSnapshotTransferRecovery>,
    ),
    OpenRaftSnapshotStoreError,
> {
    let (pending, recovery) = read_pending_snapshot_transfer(directory)?;
    Ok((
        pending.map(|(transfer, body_crc)| StagedTransfer { transfer, body_crc }),
        recovery,
    ))
}

impl FileRaftSnapshotStore {
    /// Opens a durable snapshot store rooted at `directory`.
    ///
    /// The current snapshot's envelope is verified in one streaming pass —
    /// header parse, payload and envelope checksums — without materializing
    /// the payload; only the descriptor and the payload's file offset stay
    /// resident.
    ///
    /// # Errors
    ///
    /// Returns [`OpenRaftSnapshotStoreError`] when the current manifest is
    /// corrupt, references a missing snapshot, or selects snapshot bytes that
    /// fail envelope validation.
    pub fn open(directory: impl AsRef<Path>) -> Result<Self, OpenRaftSnapshotStoreError> {
        Self::open_with_report(directory).map(OpenedFileRaftSnapshotStore::into_store)
    }

    /// Opens a durable snapshot store and reports nonfatal recovery actions.
    ///
    /// # Errors
    ///
    /// Returns [`OpenRaftSnapshotStoreError`] for corrupt authoritative state or
    /// filesystem failures that prevent a complete open.
    pub fn open_with_report(
        directory: impl AsRef<Path>,
    ) -> Result<OpenedFileRaftSnapshotStore, OpenRaftSnapshotStoreError> {
        Self::open_with_creation_sync(directory.as_ref(), CreationSync::Immediate)
    }

    pub(crate) fn open_with_parent_sync_batch(
        directory: impl AsRef<Path>,
        batch: &mut ParentDirectorySyncBatch,
    ) -> Result<Self, OpenRaftSnapshotStoreError> {
        Self::open_with_creation_sync(directory.as_ref(), CreationSync::Batched(batch))
            .map(OpenedFileRaftSnapshotStore::into_store)
    }

    fn open_with_creation_sync(
        directory: &Path,
        creation_sync: CreationSync<'_>,
    ) -> Result<OpenedFileRaftSnapshotStore, OpenRaftSnapshotStoreError> {
        let directory = directory.to_path_buf();
        let created_directory = ensure_directory(&directory, creation_sync)?;

        let manifest_path = manifest_path(&directory);
        let Some(manifest) = read_manifest(&manifest_path)? else {
            let (pending, pending_transfer_recovery) = read_staged_transfer(&directory)?;
            return Ok(OpenedFileRaftSnapshotStore::new(
                Self {
                    directory,
                    current: None,
                    pending,
                    next_sequence: Some(1),
                    health: FileStoreHealth::Healthy,
                    ownership: None,
                },
                FileRaftSnapshotStoreOpenReport {
                    created_directory,
                    pending_transfer_recovery,
                },
            ));
        };

        let snapshot_path = snapshot_path(&directory, &manifest.file_name);
        let verified = verify_snapshot_envelope(&snapshot_path)?;
        let descriptor = RaftSnapshot::new(
            verified.header.metadata,
            verified.header.payload_len,
            verified.payload_crc32,
        );
        let next_sequence = manifest.sequence.checked_add(1);
        let (pending, pending_transfer_recovery) = read_staged_transfer(&directory)?;
        Ok(OpenedFileRaftSnapshotStore::new(
            Self {
                directory,
                current: Some(CurrentSnapshot {
                    file_name: manifest.file_name,
                    descriptor,
                    payload_offset: verified.header.header_len,
                }),
                pending,
                next_sequence,
                health: FileStoreHealth::Healthy,
                ownership: None,
            },
            FileRaftSnapshotStoreOpenReport {
                created_directory,
                pending_transfer_recovery,
            },
        ))
    }
}

enum CreationSync<'a> {
    Immediate,
    Batched(&'a mut ParentDirectorySyncBatch),
}

fn ensure_directory(
    directory: &Path,
    creation_sync: CreationSync<'_>,
) -> Result<bool, OpenRaftSnapshotStoreError> {
    match fs::metadata(directory) {
        Ok(metadata) if metadata.is_dir() => return Ok(false),
        Ok(_) => {
            return Err(OpenRaftSnapshotStoreError::Io {
                operation: "open raft snapshot directory",
                path: directory.to_path_buf(),
                source: io::Error::new(io::ErrorKind::InvalidInput, "not a directory").into(),
            });
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(OpenRaftSnapshotStoreError::Io {
                operation: "stat raft snapshot directory",
                path: directory.to_path_buf(),
                source: error.into(),
            });
        }
    }
    fs::create_dir_all(directory).map_err(|error| OpenRaftSnapshotStoreError::Io {
        operation: "create raft snapshot directory",
        path: directory.to_path_buf(),
        source: error.into(),
    })?;
    match creation_sync {
        CreationSync::Immediate => {
            sync_parent_directory(directory).map_err(|error| OpenRaftSnapshotStoreError::Io {
                operation: "sync raft snapshot parent directory",
                path: directory.to_path_buf(),
                source: error.into(),
            })?;
        }
        CreationSync::Batched(batch) => batch.record_parent_of(directory),
    }
    Ok(true)
}
