//! Display and source rendering for the snapshot-store error vocabulary.
//!
//! Operator-facing text and error-source chaining live here so the vocabulary
//! itself stays a readable list of the outcomes a caller must distinguish.

use std::{error::Error, fmt};

use super::{OpenRaftSnapshotStoreError, RaftSnapshotStoreWriteError};

impl fmt::Display for RaftSnapshotStoreWriteError {
    #[allow(
        clippy::too_many_lines,
        reason = "the rendering match enumerates every write-error variant in one reviewed place"
    )]
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
