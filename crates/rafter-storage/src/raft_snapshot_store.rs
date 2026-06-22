use std::{
    error::Error,
    fmt,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use rafter::{
    PendingSnapshotTransfer, RaftSnapshot, SnapshotChunkRequest, SnapshotChunkSource,
    SnapshotTransferId, StagedSnapshotChunk,
};

mod in_memory;
mod manifest;
mod open;
mod pending_transfer;

pub use self::{
    in_memory::InMemoryRaftSnapshotStore,
    manifest::{RaftSnapshotManifestDecodeError, RaftSnapshotManifestEncodeError},
    pending_transfer::{DecodePendingSnapshotTransferError, PendingSnapshotTransferStagingStatus},
};

use self::{
    manifest::{encode_manifest, manifest_path, read_manifest, SnapshotManifest},
    pending_transfer::{
        clear_pending_snapshot_transfer, open_staged_body, pending_snapshot_transfer_body_path,
        read_pending_snapshot_transfer, remove_abandoned_pending_snapshot_transfer_staging,
        stage_pending_snapshot_chunk, staging_status,
    },
};
use crate::{
    checksum::RunningCrc32, durable_fs::sync_parent_directory,
    raft_snapshot_codec::encode_raft_snapshot_header, DecodeRaftSnapshotError,
    EncodeRaftSnapshotError, PersistedRaftSnapshot,
};

/// Errors returned while writing, staging, or promoting Raft snapshots.
///
/// This enum is exhaustive so callers can distinguish source-contract
/// failures, staging protocol mistakes, checksum mismatches, manifest encoding
/// failures, and filesystem errors.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RaftSnapshotStoreWriteError {
    /// Snapshot envelope metadata or membership cannot be represented on disk.
    EncodeSnapshot(EncodeRaftSnapshotError),
    /// The current snapshot manifest could not be encoded.
    EncodeManifest(RaftSnapshotManifestEncodeError),
    /// A source could not serve a chunk of the snapshot being written.
    SourceChunkUnavailable {
        transfer_id: SnapshotTransferId,
        offset: u64,
    },
    /// A chunk with a non-zero offset arrived while nothing was staged.
    StagedChunkWithoutTransfer {
        transfer_id: SnapshotTransferId,
        offset: u64,
    },
    /// A continuation chunk does not belong to the staged transfer.
    StagedChunkTransferMismatch {
        staged_leader_id: rafter::NodeId,
        staged_transfer_id: SnapshotTransferId,
        leader_id: rafter::NodeId,
        transfer_id: SnapshotTransferId,
    },
    /// A continuation chunk does not start at the staged length.
    StagedChunkOffsetMismatch { expected_offset: u64, offset: u64 },
    /// A snapshot was promoted while nothing was staged.
    PromoteWithoutStagedTransfer { requested: SnapshotTransferId },
    /// The promoted snapshot is not the staged transfer.
    PromoteTransferIdMismatch {
        staged: SnapshotTransferId,
        requested: SnapshotTransferId,
    },
    /// The staged transfer has not received its complete payload.
    PromoteIncompleteStagedTransfer {
        received_len: u64,
        total_payload_len: u64,
    },
    /// The bytes read or staged for a snapshot did not match its descriptor.
    SnapshotPayloadChecksumMismatch { expected: u32, actual: u32 },
    Io {
        operation: &'static str,
        path: PathBuf,
        message: String,
    },
}

/// Errors returned while opening a file-backed snapshot store.
///
/// This enum is exhaustive so callers can distinguish corrupt manifests,
/// missing snapshot files, corrupt snapshot envelopes, pending-transfer
/// corruption, and filesystem errors.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OpenRaftSnapshotStoreError {
    Io {
        operation: &'static str,
        path: PathBuf,
        message: String,
    },
    Manifest(RaftSnapshotManifestDecodeError),
    MissingSnapshot {
        path: PathBuf,
    },
    Snapshot(DecodeRaftSnapshotError),
    PendingTransfer(DecodePendingSnapshotTransferError),
}

impl fmt::Display for RaftSnapshotStoreWriteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EncodeSnapshot(error) => write!(
                formatter,
                "Raft snapshot envelope could not be encoded: {error}"
            ),
            Self::SourceChunkUnavailable {
                transfer_id,
                offset,
            } => write!(
                formatter,
                "snapshot chunk source could not serve transfer {transfer_id} at offset {offset}"
            ),
            Self::EncodeManifest(error) => write!(
                formatter,
                "Raft snapshot manifest could not be encoded: {error}"
            ),
            Self::StagedChunkWithoutTransfer {
                transfer_id,
                offset,
            } => write!(
                formatter,
                "snapshot chunk of transfer {transfer_id} at offset {offset} arrived while no transfer was staged"
            ),
            Self::StagedChunkTransferMismatch {
                staged_leader_id,
                staged_transfer_id,
                leader_id,
                transfer_id,
            } => write!(
                formatter,
                "snapshot chunk of transfer {transfer_id} from {leader_id} does not continue the staged transfer {staged_transfer_id} from {staged_leader_id}"
            ),
            Self::StagedChunkOffsetMismatch {
                expected_offset,
                offset,
            } => write!(
                formatter,
                "snapshot chunk at offset {offset} does not continue the staged transfer at offset {expected_offset}"
            ),
            Self::PromoteWithoutStagedTransfer { requested } => write!(
                formatter,
                "snapshot transfer {requested} cannot be promoted: no transfer is staged"
            ),
            Self::PromoteTransferIdMismatch { staged, requested } => write!(
                formatter,
                "snapshot transfer {requested} cannot be promoted: the staged transfer is {staged}"
            ),
            Self::PromoteIncompleteStagedTransfer {
                received_len,
                total_payload_len,
            } => write!(
                formatter,
                "staged snapshot transfer cannot be promoted: {received_len} of {total_payload_len} payload bytes received"
            ),
            Self::SnapshotPayloadChecksumMismatch { expected, actual } => write!(
                formatter,
                "snapshot payload checksum {actual:#010x} does not match descriptor checksum {expected:#010x}"
            ),
            Self::Io {
                operation,
                path,
                message,
            } => write!(
                formatter,
                "could not {operation} at {}: {message}",
                path.display()
            ),
        }
    }
}

impl Error for RaftSnapshotStoreWriteError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::EncodeManifest(error) => Some(error),
            Self::SourceChunkUnavailable { .. }
            | Self::StagedChunkWithoutTransfer { .. }
            | Self::StagedChunkTransferMismatch { .. }
            | Self::StagedChunkOffsetMismatch { .. }
            | Self::PromoteWithoutStagedTransfer { .. }
            | Self::PromoteTransferIdMismatch { .. }
            | Self::PromoteIncompleteStagedTransfer { .. }
            | Self::SnapshotPayloadChecksumMismatch { .. }
            | Self::Io { .. } => None,
            Self::EncodeSnapshot(error) => Some(error),
        }
    }
}

impl fmt::Display for OpenRaftSnapshotStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io {
                operation,
                path,
                message,
            } => write!(
                formatter,
                "could not {operation} at {}: {message}",
                path.display()
            ),
            Self::Manifest(error) => write!(
                formatter,
                "stored Raft snapshot manifest is corrupt: {error}"
            ),
            Self::MissingSnapshot { path } => write!(
                formatter,
                "Raft snapshot manifest points at missing snapshot file {}",
                path.display()
            ),
            Self::Snapshot(error) => {
                write!(formatter, "stored Raft snapshot is corrupt: {error}")
            }
            Self::PendingTransfer(error) => write!(
                formatter,
                "stored pending Raft snapshot transfer is corrupt: {error}"
            ),
        }
    }
}

impl Error for OpenRaftSnapshotStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Manifest(error) => Some(error),
            Self::Snapshot(error) => Some(error),
            Self::PendingTransfer(error) => Some(error),
            Self::Io { .. } | Self::MissingSnapshot { .. } => None,
        }
    }
}

/// Storage contract for durable Raft snapshots and inbound snapshot transfer
/// staging.
pub trait RaftSnapshotStore {
    /// Writes a complete durable snapshot and makes it current.
    ///
    /// Suits application snapshots small enough to hold in memory; large
    /// state machines stream through
    /// [`RaftSnapshotStore::write_snapshot_from_source`] instead.
    ///
    /// # Errors
    ///
    /// Returns [`RaftSnapshotStoreWriteError`] if either the immutable snapshot
    /// file or the current manifest cannot be durably written.
    fn write_snapshot(
        &mut self,
        snapshot: PersistedRaftSnapshot,
    ) -> Result<(), RaftSnapshotStoreWriteError>;

    /// Writes a durable snapshot whose payload is pulled from `source` in
    /// bounded chunks, and makes it current. The payload is never
    /// materialized whole.
    ///
    /// # Errors
    ///
    /// Returns [`RaftSnapshotStoreWriteError`] when the source cannot serve
    /// the snapshot identified by `snapshot.transfer_id()` or the snapshot
    /// cannot be durably written.
    fn write_snapshot_from_source(
        &mut self,
        snapshot: &RaftSnapshot,
        source: &dyn SnapshotChunkSource,
    ) -> Result<(), RaftSnapshotStoreWriteError>;

    /// The current snapshot's descriptor: metadata plus payload length.
    /// Payload bytes are read through the store's [`SnapshotChunkSource`]
    /// implementation, never handed out whole.
    fn current_snapshot(&self) -> Option<RaftSnapshot>;

    /// Stages one validated inbound snapshot chunk durably.
    ///
    /// A chunk at offset zero begins staging for its transfer identity and
    /// replaces whatever was staged before it. A chunk at a non-zero offset
    /// must continue the staged transfer exactly: same leader, transfer id,
    /// metadata, and total length, with `chunk.offset` equal to the staged
    /// length. The staged bytes are not current state — they only become the
    /// current snapshot through [`RaftSnapshotStore::promote_staged_snapshot`].
    ///
    /// # Errors
    ///
    /// Returns [`RaftSnapshotStoreWriteError`] when the chunk does not
    /// continue the staged transfer (the kernel orders chunks, so a mismatch
    /// is a caller bug) or when the staged bytes cannot be durably written.
    fn stage_snapshot_chunk(
        &mut self,
        chunk: &StagedSnapshotChunk,
    ) -> Result<(), RaftSnapshotStoreWriteError>;

    /// Promotes the completed staged transfer identified by
    /// `snapshot.transfer_id()` to the current snapshot and clears the
    /// staging area.
    ///
    /// # Errors
    ///
    /// Returns [`RaftSnapshotStoreWriteError`] when nothing is staged, the
    /// staged transfer is not the one identified by the snapshot, the staged
    /// payload is incomplete, or the promoted snapshot cannot be durably
    /// written.
    fn promote_staged_snapshot(
        &mut self,
        snapshot: &RaftSnapshot,
    ) -> Result<(), RaftSnapshotStoreWriteError>;

    /// Clears any partially received incoming snapshot transfer.
    ///
    /// # Errors
    ///
    /// Returns [`RaftSnapshotStoreWriteError`] when the staged transfer marker
    /// cannot be durably removed.
    fn clear_pending_snapshot_transfer(&mut self) -> Result<(), RaftSnapshotStoreWriteError>;

    /// Returns the logical pending inbound snapshot transfer, if one is
    /// resumable.
    fn current_pending_snapshot_transfer(&self) -> Option<&PendingSnapshotTransfer>;
}

/// Validates that `chunk` may be staged over the currently staged transfer.
///
/// Shared by every store implementation so file-backed and in-memory staging
/// reject exactly the same caller bugs.
fn validate_staged_chunk(
    chunk: &StagedSnapshotChunk,
    current: Option<&PendingSnapshotTransfer>,
) -> Result<(), RaftSnapshotStoreWriteError> {
    if chunk.offset == 0 {
        return Ok(());
    }
    let Some(current) = current else {
        return Err(RaftSnapshotStoreWriteError::StagedChunkWithoutTransfer {
            transfer_id: chunk.transfer_id,
            offset: chunk.offset,
        });
    };
    if chunk.leader_id != current.leader_id
        || chunk.transfer_id != current.transfer_id
        || chunk.metadata != current.metadata
        || chunk.total_payload_len != current.total_payload_len
        || chunk.application_payload_crc32 != current.application_payload_crc32
    {
        return Err(RaftSnapshotStoreWriteError::StagedChunkTransferMismatch {
            staged_leader_id: current.leader_id,
            staged_transfer_id: current.transfer_id,
            leader_id: chunk.leader_id,
            transfer_id: chunk.transfer_id,
        });
    }
    if chunk.offset != current.received_len {
        return Err(RaftSnapshotStoreWriteError::StagedChunkOffsetMismatch {
            expected_offset: current.received_len,
            offset: chunk.offset,
        });
    }
    Ok(())
}

/// Validates that the staged transfer is the complete content of `snapshot`.
fn validate_staged_promotion(
    snapshot: &RaftSnapshot,
    current: Option<&PendingSnapshotTransfer>,
) -> Result<(), RaftSnapshotStoreWriteError> {
    let requested = snapshot.transfer_id();
    let Some(current) = current else {
        return Err(RaftSnapshotStoreWriteError::PromoteWithoutStagedTransfer { requested });
    };
    if current.transfer_id != requested {
        return Err(RaftSnapshotStoreWriteError::PromoteTransferIdMismatch {
            staged: current.transfer_id,
            requested,
        });
    }
    if !current.is_complete() {
        return Err(
            RaftSnapshotStoreWriteError::PromoteIncompleteStagedTransfer {
                received_len: current.received_len,
                total_payload_len: current.total_payload_len,
            },
        );
    }
    Ok(())
}

/// The staged transfer state after `chunk` lands: identity from the chunk,
/// received length extended through its bytes.
fn pending_transfer_after_chunk(chunk: &StagedSnapshotChunk) -> PendingSnapshotTransfer {
    PendingSnapshotTransfer {
        leader_id: chunk.leader_id,
        transfer_id: chunk.transfer_id,
        metadata: chunk.metadata.clone(),
        total_payload_len: chunk.total_payload_len,
        application_payload_crc32: chunk.application_payload_crc32,
        received_len: chunk.offset + chunk.bytes.len() as u64,
    }
}

/// Pulls one payload chunk of `snapshot` from `source`, holding the source
/// to its contract: the chunk must exist and be exactly `len` bytes.
fn source_chunk(
    source: &dyn SnapshotChunkSource,
    snapshot: &RaftSnapshot,
    transfer_id: SnapshotTransferId,
    offset: u64,
    len: u32,
) -> Result<Vec<u8>, RaftSnapshotStoreWriteError> {
    let bytes = source
        .snapshot_chunk(SnapshotChunkRequest {
            transfer_id,
            metadata: &snapshot.metadata,
            total_payload_len: snapshot.application_payload_len,
            application_payload_crc32: snapshot.application_payload_crc32,
            offset,
            len,
        })
        .ok_or(RaftSnapshotStoreWriteError::SourceChunkUnavailable {
            transfer_id,
            offset,
        })?;
    if bytes.len() == len as usize {
        Ok(bytes)
    } else {
        Err(RaftSnapshotStoreWriteError::SourceChunkUnavailable {
            transfer_id,
            offset,
        })
    }
}

fn stream_chunk_len(remaining: u64, max_len: u32) -> u32 {
    let bounded = remaining.min(u64::from(max_len));
    u32::try_from(bounded).unwrap_or(max_len)
}

/// File-backed snapshot store with durable current-manifest and staging files.
#[derive(Debug)]
pub struct FileRaftSnapshotStore {
    directory: PathBuf,
    current: Option<CurrentSnapshot>,
    pending: Option<StagedTransfer>,
    next_sequence: u64,
}

/// The store's view of the current snapshot: descriptor plus where the
/// payload bytes start inside the envelope file. Payload bytes stay on disk
/// and are served by positioned reads.
#[derive(Clone, Debug, Eq, PartialEq)]
struct CurrentSnapshot {
    file_name: String,
    descriptor: RaftSnapshot,
    payload_offset: u64,
}

/// The store's in-memory view of the staged transfer: the len-based kernel
/// state plus a running checksum of the staged body, so appends keep the
/// manifest's body checksum correct without re-reading the body file.
#[derive(Clone, Debug, Eq, PartialEq)]
struct StagedTransfer {
    transfer: PendingSnapshotTransfer,
    body_crc: RunningCrc32,
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

    /// Removes an abandoned pending snapshot transfer body when no manifest is
    /// present.
    ///
    /// Returns `true` when a staging body was removed. Valid resumable pending
    /// transfers are left untouched.
    ///
    /// # Errors
    ///
    /// Returns [`RaftSnapshotStoreWriteError`] when the abandoned staging body
    /// or its parent directory cannot be durably updated.
    pub fn remove_abandoned_pending_snapshot_transfer_staging(
        &mut self,
    ) -> Result<bool, RaftSnapshotStoreWriteError> {
        remove_abandoned_pending_snapshot_transfer_staging(&self.directory)
    }

    fn snapshot_path_for_write(&mut self, snapshot: &RaftSnapshot) -> (PathBuf, String) {
        loop {
            let sequence = self.next_sequence;
            let file_name = snapshot_file_name(sequence, snapshot);
            let path = snapshot_path(&self.directory, &file_name);
            self.next_sequence = self.next_sequence.saturating_add(1);
            if !path.exists() {
                return (path, file_name);
            }
        }
    }

    /// Streams one snapshot envelope to disk and makes it current: header,
    /// payload chunks pulled from `read_chunk`, then the payload and envelope
    /// checksums, followed by the manifest update. When
    /// `expected_payload_crc` is given, the assembled payload checksum must
    /// match it before the file becomes visible.
    fn write_snapshot_streamed(
        &mut self,
        descriptor: &RaftSnapshot,
        expected_payload_crc: Option<u32>,
        mut read_chunk: impl FnMut(u64, u32) -> Result<Vec<u8>, RaftSnapshotStoreWriteError>,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        let payload_len = descriptor.application_payload_len;
        let header = encode_raft_snapshot_header(&descriptor.metadata, payload_len)
            .map_err(RaftSnapshotStoreWriteError::EncodeSnapshot)?;
        let (snapshot_path, file_name) = self.snapshot_path_for_write(descriptor);
        let sequence = self.next_sequence.saturating_sub(1);
        let temp_path = self.temp_snapshot_path();

        {
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&temp_path)
                .map_err(|error| RaftSnapshotStoreWriteError::Io {
                    operation: "open raft snapshot temp file",
                    path: temp_path.clone(),
                    message: error.to_string(),
                })?;
            let write_error = |error: std::io::Error| RaftSnapshotStoreWriteError::Io {
                operation: "write raft snapshot temp file",
                path: temp_path.clone(),
                message: error.to_string(),
            };
            let mut envelope_crc = RunningCrc32::new();
            let mut payload_crc = RunningCrc32::new();
            file.write_all(&header).map_err(write_error)?;
            envelope_crc.update(&header);

            let mut offset = 0u64;
            while offset < payload_len {
                let len = stream_chunk_len(payload_len - offset, SNAPSHOT_STREAM_CHUNK_BYTES);
                let bytes = read_chunk(offset, len)?;
                file.write_all(&bytes).map_err(write_error)?;
                payload_crc.update(&bytes);
                envelope_crc.update(&bytes);
                offset += u64::from(len);
            }

            if let Some(expected) = expected_payload_crc {
                if payload_crc.value() != expected {
                    return Err(
                        RaftSnapshotStoreWriteError::SnapshotPayloadChecksumMismatch {
                            expected,
                            actual: payload_crc.value(),
                        },
                    );
                }
            }

            let payload_crc_bytes = payload_crc.value().to_be_bytes();
            file.write_all(&payload_crc_bytes).map_err(write_error)?;
            envelope_crc.update(&payload_crc_bytes);
            file.write_all(&envelope_crc.value().to_be_bytes())
                .map_err(write_error)?;
            file.sync_data().map_err(write_error)?;
        }

        fs::rename(&temp_path, &snapshot_path).map_err(|error| {
            RaftSnapshotStoreWriteError::Io {
                operation: "replace raft snapshot",
                path: snapshot_path.clone(),
                message: error.to_string(),
            }
        })?;
        sync_parent_directory(&snapshot_path).map_err(|error| RaftSnapshotStoreWriteError::Io {
            operation: "sync raft snapshot directory",
            path: snapshot_path.clone(),
            message: error.to_string(),
        })?;

        let manifest = SnapshotManifest {
            sequence,
            file_name: file_name.clone(),
        };
        let encoded_manifest =
            encode_manifest(&manifest).map_err(RaftSnapshotStoreWriteError::EncodeManifest)?;
        write_temp_and_rename(
            &self.temp_manifest_path(),
            &manifest_path(&self.directory),
            &encoded_manifest,
            "open raft snapshot manifest temp file",
            "write raft snapshot manifest temp file",
            "replace raft snapshot manifest",
            "sync raft snapshot manifest directory",
        )?;

        self.current = Some(CurrentSnapshot {
            file_name,
            descriptor: descriptor.clone(),
            payload_offset: header.len() as u64,
        });
        self.clear_pending_snapshot_transfer()
    }

    fn temp_snapshot_path(&self) -> PathBuf {
        self.directory
            .join(format!(".snapshot-{}.tmp", std::process::id()))
    }

    fn temp_manifest_path(&self) -> PathBuf {
        self.directory
            .join(format!(".current.snapshot-{}.tmp", std::process::id()))
    }

    fn temp_pending_snapshot_transfer_path(&self) -> PathBuf {
        self.directory.join(format!(
            ".pending.snapshot-transfer-{}.tmp",
            std::process::id()
        ))
    }
}

impl RaftSnapshotStore for FileRaftSnapshotStore {
    fn write_snapshot(
        &mut self,
        snapshot: PersistedRaftSnapshot,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        let descriptor =
            RaftSnapshot::from_payload(snapshot.metadata.clone(), &snapshot.application_payload);
        let payload = snapshot.application_payload;
        self.write_snapshot_streamed(
            &descriptor,
            Some(descriptor.application_payload_crc32),
            |offset, len| {
                let start = usize::try_from(offset).map_err(|_| {
                    RaftSnapshotStoreWriteError::SourceChunkUnavailable {
                        transfer_id: descriptor.transfer_id(),
                        offset,
                    }
                })?;
                Ok(payload[start..start + len as usize].to_vec())
            },
        )
    }

    fn write_snapshot_from_source(
        &mut self,
        snapshot: &RaftSnapshot,
        source: &dyn SnapshotChunkSource,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        let transfer_id = snapshot.transfer_id();
        let descriptor = snapshot.clone();
        self.write_snapshot_streamed(
            snapshot,
            Some(snapshot.application_payload_crc32),
            move |offset, len| source_chunk(source, &descriptor, transfer_id, offset, len),
        )
    }

    fn current_snapshot(&self) -> Option<RaftSnapshot> {
        self.current
            .as_ref()
            .map(|current| current.descriptor.clone())
    }

    fn stage_snapshot_chunk(
        &mut self,
        chunk: &StagedSnapshotChunk,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        validate_staged_chunk(chunk, self.current_pending_snapshot_transfer())?;
        let body_crc_before_chunk = if chunk.offset == 0 {
            RunningCrc32::new()
        } else {
            let Some(pending) = self.pending.as_ref() else {
                return Err(RaftSnapshotStoreWriteError::StagedChunkWithoutTransfer {
                    transfer_id: chunk.transfer_id,
                    offset: chunk.offset,
                });
            };
            pending.body_crc
        };
        let body_crc = stage_pending_snapshot_chunk(
            &self.directory,
            &self.temp_pending_snapshot_transfer_path(),
            chunk,
            body_crc_before_chunk,
        )?;
        self.pending = Some(StagedTransfer {
            transfer: pending_transfer_after_chunk(chunk),
            body_crc,
        });
        Ok(())
    }

    fn promote_staged_snapshot(
        &mut self,
        snapshot: &RaftSnapshot,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        validate_staged_promotion(snapshot, self.current_pending_snapshot_transfer())?;
        let requested = snapshot.transfer_id();
        let Some(staged) = self.pending.as_ref() else {
            return Err(RaftSnapshotStoreWriteError::PromoteWithoutStagedTransfer { requested });
        };
        let expected_payload_crc = snapshot.application_payload_crc32;
        let mut body = open_staged_body(&self.directory, staged.transfer.received_len)?;
        let body_path = pending_snapshot_transfer_body_path(&self.directory);
        self.write_snapshot_streamed(snapshot, Some(expected_payload_crc), |_, len| {
            let mut bytes = vec![0u8; len as usize];
            body.read_exact(&mut bytes)
                .map_err(|error| RaftSnapshotStoreWriteError::Io {
                    operation: "read pending snapshot transfer body for promotion",
                    path: body_path.clone(),
                    message: error.to_string(),
                })?;
            Ok(bytes)
        })
    }

    fn clear_pending_snapshot_transfer(&mut self) -> Result<(), RaftSnapshotStoreWriteError> {
        clear_pending_snapshot_transfer(&self.directory)?;
        self.pending = None;
        Ok(())
    }

    fn current_pending_snapshot_transfer(&self) -> Option<&PendingSnapshotTransfer> {
        self.pending.as_ref().map(|pending| &pending.transfer)
    }
}

impl SnapshotChunkSource for FileRaftSnapshotStore {
    /// Serves payload chunks of the current snapshot by positioned reads of
    /// the envelope file — the payload is never resident. Any read failure
    /// yields `None`, which callers treat as a lost message.
    fn snapshot_chunk(&self, request: SnapshotChunkRequest<'_>) -> Option<Vec<u8>> {
        let current = self.current.as_ref()?;
        if current.descriptor.transfer_id() != request.transfer_id
            || current.descriptor.application_payload_len != request.total_payload_len
            || current.descriptor.application_payload_crc32 != request.application_payload_crc32
        {
            return None;
        }
        let end = request.offset.checked_add(u64::from(request.len))?;
        if end > current.descriptor.application_payload_len {
            return None;
        }
        let path = snapshot_path(&self.directory, &current.file_name);
        let mut file = File::open(path).ok()?;
        file.seek(SeekFrom::Start(current.payload_offset + request.offset))
            .ok()?;
        let mut bytes = vec![0u8; request.len as usize];
        file.read_exact(&mut bytes).ok()?;
        Some(bytes)
    }
}

/// Bytes pulled per chunk while streaming an envelope to disk.
const SNAPSHOT_STREAM_CHUNK_BYTES: u32 = 256 * 1024;

fn snapshot_path(directory: &Path, file_name: &str) -> PathBuf {
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

fn write_temp_and_rename(
    temp_path: &Path,
    final_path: &Path,
    bytes: &[u8],
    open_operation: &'static str,
    write_operation: &'static str,
    rename_operation: &'static str,
    sync_operation: &'static str,
) -> Result<(), RaftSnapshotStoreWriteError> {
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(temp_path)
            .map_err(|error| RaftSnapshotStoreWriteError::Io {
                operation: open_operation,
                path: temp_path.to_path_buf(),
                message: error.to_string(),
            })?;
        file.write_all(bytes)
            .and_then(|()| file.sync_data())
            .map_err(|error| RaftSnapshotStoreWriteError::Io {
                operation: write_operation,
                path: temp_path.to_path_buf(),
                message: error.to_string(),
            })?;
    }

    fs::rename(temp_path, final_path).map_err(|error| RaftSnapshotStoreWriteError::Io {
        operation: rename_operation,
        path: final_path.to_path_buf(),
        message: error.to_string(),
    })?;
    sync_parent_directory(final_path).map_err(|error| RaftSnapshotStoreWriteError::Io {
        operation: sync_operation,
        path: final_path.to_path_buf(),
        message: error.to_string(),
    })
}

#[cfg(test)]
#[path = "raft_snapshot_store/pending_transfer_test.rs"]
mod pending_transfer_test;
#[cfg(test)]
#[path = "raft_snapshot_store_test.rs"]
mod raft_snapshot_store_test;
#[cfg(test)]
#[path = "raft_snapshot_store/test_support.rs"]
mod test_support;
