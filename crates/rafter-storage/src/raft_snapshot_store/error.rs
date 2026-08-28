//! Snapshot-store operational and recovery error vocabulary.
//!
//! This module names caller mistakes, format failures, ambiguous filesystem
//! outcomes, and the post-manifest committed-cleanup outcome.

use std::path::PathBuf;

use rafter::{RaftSnapshot, SnapshotTransferId};

use crate::{DecodeRaftSnapshotError, EncodeRaftSnapshotError, StorageIoError};

use super::{
    manifest::{RaftSnapshotManifestDecodeError, RaftSnapshotManifestEncodeError},
    pending_transfer::DecodePendingSnapshotTransferError,
};

mod render;

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
