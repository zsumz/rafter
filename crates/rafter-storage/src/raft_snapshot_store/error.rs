//! Snapshot-store operational and recovery error vocabulary.
//!
//! This module names caller mistakes, format failures, ambiguous filesystem
//! outcomes, and the post-manifest committed-cleanup outcome.

use std::{error::Error, fmt, path::PathBuf};

use rafter::{RaftSnapshot, SnapshotTransferId};

use crate::{DecodeRaftSnapshotError, EncodeRaftSnapshotError, StorageIoError};

use super::{
    manifest::{RaftSnapshotManifestDecodeError, RaftSnapshotManifestEncodeError},
    pending_transfer::DecodePendingSnapshotTransferError,
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
        /// Snapshot transfer whose source refused the read.
        transfer_id: SnapshotTransferId,
        /// Requested payload offset.
        offset: u64,
    },
    /// A chunk with a non-zero offset arrived while nothing was staged.
    StagedChunkWithoutTransfer {
        /// Transfer identifier carried by the unexpected chunk.
        transfer_id: SnapshotTransferId,
        /// Payload offset carried by the unexpected chunk.
        offset: u64,
    },
    /// A continuation chunk does not belong to the staged transfer.
    StagedChunkTransferMismatch {
        /// Leader that owns the staged transfer.
        staged_leader_id: rafter::NodeId,
        /// Identifier of the staged transfer.
        staged_transfer_id: SnapshotTransferId,
        /// Leader that sent the continuation chunk.
        leader_id: rafter::NodeId,
        /// Transfer identifier carried by the continuation chunk.
        transfer_id: SnapshotTransferId,
    },
    /// A chunk's offset plus byte length overflowed the snapshot length domain.
    StagedChunkRangeOverflow {
        /// Starting payload offset.
        offset: u64,
        /// Chunk length in bytes.
        len: usize,
    },
    /// A chunk extends beyond the advertised snapshot payload.
    StagedChunkPastEnd {
        /// Starting payload offset.
        offset: u64,
        /// Chunk length in bytes.
        len: u64,
        /// Complete payload length declared by the transfer.
        total_payload_len: u64,
    },
    /// An empty chunk appears before the advertised payload end.
    StagedChunkEmptyBeforeEnd {
        /// Payload offset at which the empty chunk arrived.
        offset: u64,
        /// Complete payload length declared by the transfer.
        total_payload_len: u64,
    },
    /// The chunk's finality flag disagrees with its exact end offset.
    StagedChunkDoneMismatch {
        /// Finality flag carried by the chunk.
        done: bool,
        /// Offset immediately after the chunk.
        end_offset: u64,
        /// Complete payload length declared by the transfer.
        total_payload_len: u64,
    },
    /// The chunk's routing identity is not derived from its descriptor.
    StagedChunkTransferIdMismatch {
        /// Transfer identifier derived from the chunk descriptor.
        expected: SnapshotTransferId,
        /// Transfer identifier carried by the chunk.
        actual: SnapshotTransferId,
    },
    /// A continuation chunk does not start at the staged length.
    StagedChunkOffsetMismatch {
        /// Next offset required by staged progress.
        expected_offset: u64,
        /// Offset carried by the continuation chunk.
        offset: u64,
    },
    /// A snapshot was promoted while nothing was staged.
    PromoteWithoutStagedTransfer {
        /// Transfer identifier requested for promotion.
        requested: SnapshotTransferId,
    },
    /// The promoted snapshot is not the staged transfer.
    PromoteTransferIdMismatch {
        /// Identifier of the staged transfer.
        staged: SnapshotTransferId,
        /// Identifier requested for promotion.
        requested: SnapshotTransferId,
    },
    /// Transfer ids match, but the complete staged and requested
    /// descriptors differ. Transfer ids are routing identities rather than
    /// collision-resistant digests, so equality is not sufficient by itself.
    PromoteSnapshotDescriptorMismatch {
        /// Complete descriptor retained with the staged transfer.
        staged: Box<RaftSnapshot>,
        /// Complete descriptor supplied for promotion.
        requested: Box<RaftSnapshot>,
    },
    /// The staged transfer has not received its complete payload.
    PromoteIncompleteStagedTransfer {
        /// Payload bytes durably staged.
        received_len: u64,
        /// Complete payload length declared by the transfer.
        total_payload_len: u64,
    },
    /// The bytes read or staged for a snapshot did not match its descriptor.
    SnapshotPayloadChecksumMismatch {
        /// Checksum declared by the snapshot descriptor.
        expected: u32,
        /// Checksum computed from the payload bytes.
        actual: u32,
    },
    /// Every representable snapshot publication sequence has been consumed.
    /// Reusing a sequence would make manifest ordering ambiguous, so no file is
    /// created and the healthy handle remains usable for reads and maintenance.
    SnapshotSequenceExhausted,
    /// A filesystem operation failed. The file-backed handle now requires a
    /// fresh [`super::FileRaftSnapshotStore::open`] before another mutation.
    Io {
        /// Stable name of the failed filesystem operation.
        operation: &'static str,
        /// Path on which the operation failed.
        path: PathBuf,
        /// Preserved I/O failure.
        source: StorageIoError,
    },
    /// An earlier mutating I/O failure made this file-backed handle unsafe to
    /// reuse without reopening its authoritative manifests and files.
    StoreRequiresReopen,
    /// The current manifest is durable and selects `file_name`, but later
    /// pending-transfer cleanup failed. The snapshot is committed; reopen is
    /// required before another mutation.
    SnapshotCommittedButReopenRequired {
        /// Immutable snapshot file selected by the durable manifest.
        file_name: String,
        /// Stable name of the cleanup operation that failed.
        operation: &'static str,
        /// Path on which cleanup failed.
        path: PathBuf,
        /// Preserved I/O failure.
        source: StorageIoError,
    },
}

/// Errors returned while opening a file-backed snapshot store.
///
/// This enum is exhaustive so callers can distinguish corrupt manifests,
/// missing snapshot files, corrupt snapshot envelopes, pending-transfer
/// corruption, and filesystem errors.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OpenRaftSnapshotStoreError {
    /// A filesystem operation failed while opening the store.
    Io {
        /// Stable name of the failed filesystem operation.
        operation: &'static str,
        /// Path on which the operation failed.
        path: PathBuf,
        /// Preserved I/O failure.
        source: StorageIoError,
    },
    /// The current-snapshot manifest was corrupt or unsupported.
    Manifest(RaftSnapshotManifestDecodeError),
    /// The manifest-selected immutable snapshot file was absent.
    MissingSnapshot {
        /// Expected path of the selected snapshot file.
        path: PathBuf,
    },
    /// The selected snapshot envelope was corrupt or unsupported.
    Snapshot(DecodeRaftSnapshotError),
    /// Pending-transfer metadata was corrupt or unsupported.
    PendingTransfer(DecodePendingSnapshotTransferError),
}

impl fmt::Display for RaftSnapshotStoreWriteError {
    #[allow(clippy::too_many_lines)]
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
            Self::StagedChunkRangeOverflow { offset, len } => write!(
                formatter,
                "snapshot chunk at offset {offset} with {len} bytes overflows the snapshot payload range"
            ),
            Self::StagedChunkPastEnd {
                offset,
                len,
                total_payload_len,
            } => write!(
                formatter,
                "snapshot chunk at offset {offset} with {len} bytes extends beyond total payload length {total_payload_len}"
            ),
            Self::StagedChunkEmptyBeforeEnd {
                offset,
                total_payload_len,
            } => write!(
                formatter,
                "snapshot chunk at offset {offset} is empty before total payload length {total_payload_len}"
            ),
            Self::StagedChunkDoneMismatch {
                done,
                end_offset,
                total_payload_len,
            } => write!(
                formatter,
                "snapshot chunk finality {done} disagrees with end offset {end_offset} and total payload length {total_payload_len}"
            ),
            Self::StagedChunkTransferIdMismatch { expected, actual } => write!(
                formatter,
                "snapshot chunk transfer id {actual} does not match descriptor-derived transfer id {expected}"
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
            Self::PromoteSnapshotDescriptorMismatch { requested, .. } => write!(
                formatter,
                "snapshot transfer {} cannot be promoted: the complete staged descriptor differs from the requested descriptor",
                requested.transfer_id()
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
            Self::SnapshotSequenceExhausted => formatter.write_str(
                "Raft snapshot publication sequence is exhausted",
            ),
            Self::Io {
                operation,
                path,
                source,
            } => write!(
                formatter,
                "could not {operation} at {}: {source}",
                path.display()
            ),
            Self::StoreRequiresReopen => formatter.write_str(
                "Raft snapshot store requires reopen after an earlier I/O failure",
            ),
            Self::SnapshotCommittedButReopenRequired {
                file_name,
                operation,
                path,
                source,
            } => write!(
                formatter,
                "Raft snapshot {file_name} is current, but cleanup could not {operation} at {}: {source}; the store requires reopen",
                path.display()
            ),
        }
    }
}

impl Error for RaftSnapshotStoreWriteError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::EncodeManifest(error) => Some(error),
            Self::EncodeSnapshot(error) => Some(error),
            Self::Io { source, .. } | Self::SnapshotCommittedButReopenRequired { source, .. } => {
                Some(source.as_io_error())
            }
            Self::SourceChunkUnavailable { .. }
            | Self::StagedChunkWithoutTransfer { .. }
            | Self::StagedChunkTransferMismatch { .. }
            | Self::StagedChunkRangeOverflow { .. }
            | Self::StagedChunkPastEnd { .. }
            | Self::StagedChunkEmptyBeforeEnd { .. }
            | Self::StagedChunkDoneMismatch { .. }
            | Self::StagedChunkTransferIdMismatch { .. }
            | Self::StagedChunkOffsetMismatch { .. }
            | Self::PromoteWithoutStagedTransfer { .. }
            | Self::PromoteTransferIdMismatch { .. }
            | Self::PromoteSnapshotDescriptorMismatch { .. }
            | Self::PromoteIncompleteStagedTransfer { .. }
            | Self::SnapshotPayloadChecksumMismatch { .. }
            | Self::SnapshotSequenceExhausted
            | Self::StoreRequiresReopen => None,
        }
    }
}

impl fmt::Display for OpenRaftSnapshotStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io {
                operation,
                path,
                source,
            } => write!(
                formatter,
                "could not {operation} at {}: {source}",
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
            Self::Io { source, .. } => Some(source.as_io_error()),
            Self::MissingSnapshot { .. } => None,
        }
    }
}
