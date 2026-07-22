//! File-backed snapshot opening and streaming envelope verification.

use std::{
    fs::{self, File},
    io::{self, Read},
    path::Path,
};

use rafter::RaftSnapshot;

use super::{
    manifest_path, read_manifest, read_pending_snapshot_transfer, snapshot_path, CurrentSnapshot,
    FileRaftSnapshotStore, FileRaftSnapshotStoreOpenReport, OpenRaftSnapshotStoreError,
    OpenedFileRaftSnapshotStore, PendingSnapshotTransferRecovery, StagedTransfer,
};
use crate::{
    checksum::RunningCrc32,
    durable_fs::{sync_parent_directory, ParentDirectorySyncBatch},
    file_store_health::FileStoreHealth,
    raft_snapshot_codec::{decode_raft_snapshot_header, SnapshotEnvelopeHeader},
    DecodeRaftSnapshotError,
};

/// Bytes of envelope prefix read to parse the header. Sized above the
/// largest header the encoder can produce (a joint configuration of four
/// full u16 member sets is just over 2 MiB), so anything past this bound
/// really is corrupt rather than merely large.
const SNAPSHOT_HEADER_PREFIX_BYTES: u64 = 4 * 1024 * 1024;

/// Bytes per read while stream-verifying an envelope.
const VERIFY_CHUNK_BYTES: usize = 256 * 1024;

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
        let header = verify_snapshot_envelope(&snapshot_path)?;
        let descriptor =
            RaftSnapshot::new(header.metadata, header.payload_len, header.payload_crc32);
        let next_sequence = manifest.sequence.checked_add(1);
        let (pending, pending_transfer_recovery) = read_staged_transfer(&directory)?;
        Ok(OpenedFileRaftSnapshotStore::new(
            Self {
                directory,
                current: Some(CurrentSnapshot {
                    file_name: manifest.file_name,
                    descriptor,
                    payload_offset: header.header_len,
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

/// Parses and fully verifies the envelope at `path` in one streaming pass,
/// returning its header. The envelope checksum is checked before the payload
/// checksum, mirroring whole-buffer decoding.
fn verify_snapshot_envelope(
    path: &Path,
) -> Result<SnapshotEnvelopeHeader, OpenRaftSnapshotStoreError> {
    let io_error = |operation: &'static str| {
        let path = path.to_path_buf();
        move |error: std::io::Error| OpenRaftSnapshotStoreError::Io {
            operation,
            path: path.clone(),
            source: error.into(),
        }
    };
    let corrupt = OpenRaftSnapshotStoreError::Snapshot;

    let file_len = snapshot_file_len(path)?;
    let mut file = File::open(path).map_err(io_error("open raft snapshot"))?;

    let mut prefix = Vec::new();
    file.by_ref()
        .take(SNAPSHOT_HEADER_PREFIX_BYTES.min(file_len))
        .read_to_end(&mut prefix)
        .map_err(io_error("read raft snapshot"))?;
    let mut header = decode_raft_snapshot_header(&prefix).map_err(corrupt)?;

    let expected_len = header
        .header_len
        .checked_add(header.payload_len)
        .and_then(|len| len.checked_add(8));
    match expected_len {
        Some(expected) if file_len == expected => {}
        Some(expected) if file_len > expected => {
            return Err(corrupt(DecodeRaftSnapshotError::TrailingBytes(
                usize::try_from(file_len - expected).unwrap_or(usize::MAX),
            )));
        }
        _ => {
            return Err(corrupt(DecodeRaftSnapshotError::UnexpectedEof {
                needed: usize::try_from(header.payload_len).unwrap_or(usize::MAX),
                remaining: usize::try_from(file_len.saturating_sub(header.header_len))
                    .unwrap_or(usize::MAX),
            }));
        }
    }

    let mut envelope_crc = RunningCrc32::new();
    let header_len = match usize::try_from(header.header_len) {
        Ok(header_len) if header_len <= prefix.len() => header_len,
        Ok(header_len) => {
            return Err(corrupt(DecodeRaftSnapshotError::UnexpectedEof {
                needed: header_len,
                remaining: prefix.len(),
            }));
        }
        Err(_) => {
            return Err(corrupt(DecodeRaftSnapshotError::UnexpectedEof {
                needed: usize::MAX,
                remaining: prefix.len(),
            }));
        }
    };
    envelope_crc.update(&prefix[..header_len]);

    let mut payload_crc = RunningCrc32::new();
    let mut remaining = header.payload_len;
    let mut already_read = &prefix[header_len..];
    let mut buffer = vec![0u8; VERIFY_CHUNK_BYTES];
    while remaining > 0 {
        let bytes = if already_read.is_empty() {
            let want = bounded_usize_len(remaining, VERIFY_CHUNK_BYTES);
            file.read_exact(&mut buffer[..want])
                .map_err(io_error("read raft snapshot"))?;
            &buffer[..want]
        } else {
            let take = bounded_usize_len(remaining, already_read.len());
            let (bytes, rest) = already_read.split_at(take);
            already_read = rest;
            bytes
        };
        payload_crc.update(bytes);
        envelope_crc.update(bytes);
        remaining -= bytes.len() as u64;
    }

    // The prefix may already hold part of the trailer for small envelopes.
    let mut trailer = [0u8; 8];
    let from_prefix = already_read.len().min(8);
    trailer[..from_prefix].copy_from_slice(&already_read[..from_prefix]);
    file.read_exact(&mut trailer[from_prefix..])
        .map_err(io_error("read raft snapshot"))?;
    let expected_payload_crc = u32::from_be_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]);
    let expected_envelope_crc =
        u32::from_be_bytes([trailer[4], trailer[5], trailer[6], trailer[7]]);

    envelope_crc.update(&trailer[..4]);
    if envelope_crc.value() != expected_envelope_crc {
        return Err(corrupt(DecodeRaftSnapshotError::EnvelopeChecksumMismatch {
            expected: expected_envelope_crc,
            actual: envelope_crc.value(),
        }));
    }
    if payload_crc.value() != expected_payload_crc {
        return Err(corrupt(DecodeRaftSnapshotError::PayloadChecksumMismatch {
            expected: expected_payload_crc,
            actual: payload_crc.value(),
        }));
    }
    header.payload_crc32 = expected_payload_crc;
    Ok(header)
}

fn snapshot_file_len(path: &Path) -> Result<u64, OpenRaftSnapshotStoreError> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            Err(OpenRaftSnapshotStoreError::MissingSnapshot {
                path: path.to_path_buf(),
            })
        }
        Err(error) => Err(OpenRaftSnapshotStoreError::Io {
            operation: "stat raft snapshot",
            path: path.to_path_buf(),
            source: error.into(),
        }),
    }
}

fn bounded_usize_len(remaining: u64, max_len: usize) -> usize {
    let max_len_u64 = u64::try_from(max_len).unwrap_or(u64::MAX);
    let bounded = remaining.min(max_len_u64);
    usize::try_from(bounded).unwrap_or(max_len)
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
