//! Inbound snapshot chunk and promotion validation.
//!
//! This module proves public chunk ranges, finality, routing identity, exact
//! continuation, and full-descriptor promotion before either store mutates
//! staged bytes.

use rafter::{PendingSnapshotTransfer, RaftSnapshot, StagedSnapshotChunk};

use super::RaftSnapshotStoreWriteError;

/// Proof that one public staged chunk has a checked, in-range end offset and
/// satisfies every descriptor and continuation rule.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ValidatedStagedChunk {
    received_len: u64,
}

impl ValidatedStagedChunk {
    /// Returns the exact staged payload length after this chunk.
    pub(super) const fn received_len(self) -> u64 {
        self.received_len
    }
}

/// Validates that `chunk` may be staged over the currently staged transfer.
///
/// Shared by every store implementation so file-backed and in-memory staging
/// reject exactly the same caller bugs before mutating bytes.
pub(super) fn validate_staged_chunk(
    chunk: &StagedSnapshotChunk,
    current: Option<&PendingSnapshotTransfer>,
) -> Result<ValidatedStagedChunk, RaftSnapshotStoreWriteError> {
    let chunk_len = u64::try_from(chunk.bytes.len()).map_err(|_| {
        RaftSnapshotStoreWriteError::StagedChunkRangeOverflow {
            offset: chunk.offset,
            len: chunk.bytes.len(),
        }
    })?;
    let end_offset = chunk.offset.checked_add(chunk_len).ok_or(
        RaftSnapshotStoreWriteError::StagedChunkRangeOverflow {
            offset: chunk.offset,
            len: chunk.bytes.len(),
        },
    )?;
    if end_offset > chunk.total_payload_len {
        return Err(RaftSnapshotStoreWriteError::StagedChunkPastEnd {
            offset: chunk.offset,
            len: chunk_len,
            total_payload_len: chunk.total_payload_len,
        });
    }
    if chunk_len == 0 && end_offset < chunk.total_payload_len {
        return Err(RaftSnapshotStoreWriteError::StagedChunkEmptyBeforeEnd {
            offset: chunk.offset,
            total_payload_len: chunk.total_payload_len,
        });
    }
    if chunk.done != (end_offset == chunk.total_payload_len) {
        return Err(RaftSnapshotStoreWriteError::StagedChunkDoneMismatch {
            done: chunk.done,
            end_offset,
            total_payload_len: chunk.total_payload_len,
        });
    }

    let expected_transfer_id = RaftSnapshot::new(
        chunk.metadata.clone(),
        chunk.total_payload_len,
        chunk.application_payload_crc32,
    )
    .transfer_id();
    if chunk.transfer_id != expected_transfer_id {
        return Err(RaftSnapshotStoreWriteError::StagedChunkTransferIdMismatch {
            expected: expected_transfer_id,
            actual: chunk.transfer_id,
        });
    }

    if chunk.offset != 0 {
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
    }

    Ok(ValidatedStagedChunk {
        received_len: end_offset,
    })
}

/// Validates that the staged transfer is the exact complete content of
/// `snapshot`, not merely a matching non-cryptographic transfer id.
pub(super) fn validate_staged_promotion(
    snapshot: &RaftSnapshot,
    current: Option<&PendingSnapshotTransfer>,
) -> Result<(), RaftSnapshotStoreWriteError> {
    let requested_transfer_id = snapshot.transfer_id();
    let Some(current) = current else {
        return Err(RaftSnapshotStoreWriteError::PromoteWithoutStagedTransfer {
            requested: requested_transfer_id,
        });
    };
    if current.transfer_id != requested_transfer_id {
        return Err(RaftSnapshotStoreWriteError::PromoteTransferIdMismatch {
            staged: current.transfer_id,
            requested: requested_transfer_id,
        });
    }

    let staged = RaftSnapshot::new(
        current.metadata.clone(),
        current.total_payload_len,
        current.application_payload_crc32,
    );
    if &staged != snapshot {
        return Err(
            RaftSnapshotStoreWriteError::PromoteSnapshotDescriptorMismatch {
                staged: Box::new(staged),
                requested: Box::new(snapshot.clone()),
            },
        );
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

/// Builds store-visible progress from a chunk whose range and descriptor have
/// already been validated.
pub(super) fn pending_transfer_after_chunk(
    chunk: &StagedSnapshotChunk,
    validated: ValidatedStagedChunk,
) -> PendingSnapshotTransfer {
    PendingSnapshotTransfer {
        leader_id: chunk.leader_id,
        transfer_id: chunk.transfer_id,
        metadata: chunk.metadata.clone(),
        total_payload_len: chunk.total_payload_len,
        application_payload_crc32: chunk.application_payload_crc32,
        received_len: validated.received_len(),
    }
}
