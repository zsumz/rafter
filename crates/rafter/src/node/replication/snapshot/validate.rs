//! Snapshot identity, authorization, shape, and rejection accounting.

use crate::{
    types::snapshot_transfer_id_from_parts, InstallSnapshotChunk, NodeId, RaftSnapshotMetadata,
    Term,
};

use super::super::super::Node;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SnapshotTransferHeaderRejection {
    InvalidMetadata,
    LeaderNotAuthorized,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SnapshotChunkRejection {
    StaleTerm,
    WrongTransfer,
    MetadataMismatch,
    LeaderNotAuthorized,
    OutOfOrderOffset,
    InvalidBounds,
}

impl Node {
    pub(super) fn validate_snapshot_transfer_header(
        &self,
        leader_id: NodeId,
        metadata: &RaftSnapshotMetadata,
        term: Term,
    ) -> Result<(), SnapshotTransferHeaderRejection> {
        if !self.valid_snapshot_metadata(metadata, term) {
            return Err(SnapshotTransferHeaderRejection::InvalidMetadata);
        }
        if !self.valid_snapshot_transfer_leader(leader_id, metadata) {
            return Err(SnapshotTransferHeaderRejection::LeaderNotAuthorized);
        }
        Ok(())
    }

    pub(super) fn validate_install_snapshot_chunk_header(
        &self,
        leader_id: NodeId,
        request: &InstallSnapshotChunk,
    ) -> Result<(), SnapshotChunkRejection> {
        self.validate_snapshot_transfer_header(leader_id, &request.metadata, request.term)
            .map_err(|rejection| match rejection {
                SnapshotTransferHeaderRejection::InvalidMetadata => {
                    SnapshotChunkRejection::MetadataMismatch
                }
                SnapshotTransferHeaderRejection::LeaderNotAuthorized => {
                    SnapshotChunkRejection::LeaderNotAuthorized
                }
            })?;

        let expected_transfer_id = snapshot_transfer_id_from_parts(
            &request.metadata,
            request.total_payload_len,
            request.application_payload_crc32,
        );
        if request.transfer_id != expected_transfer_id {
            return Err(SnapshotChunkRejection::WrongTransfer);
        }

        validate_snapshot_chunk_shape(request)
    }

    pub(super) fn record_snapshot_chunk_rejection(&mut self, rejection: SnapshotChunkRejection) {
        match rejection {
            SnapshotChunkRejection::StaleTerm => {
                self.volatile.snapshot_chunk_rejections.stale_term += 1;
            }
            SnapshotChunkRejection::WrongTransfer => {
                self.volatile.snapshot_chunk_rejections.wrong_transfer += 1;
            }
            SnapshotChunkRejection::MetadataMismatch
            | SnapshotChunkRejection::LeaderNotAuthorized => {
                self.volatile.snapshot_chunk_rejections.metadata_mismatch += 1;
            }
            SnapshotChunkRejection::OutOfOrderOffset => {
                self.volatile.snapshot_chunk_rejections.out_of_order_offset += 1;
            }
            SnapshotChunkRejection::InvalidBounds => {
                self.volatile.snapshot_chunk_rejections.invalid_bounds += 1;
            }
        }
    }

    fn valid_snapshot_metadata(&self, metadata: &RaftSnapshotMetadata, term: Term) -> bool {
        metadata.hard_state_term <= term
            && metadata.committed_membership().map_or_else(
                || {
                    self.config
                        .voters()
                        .any(|voter| voter == metadata.writer_id)
                },
                |membership| membership.contains_voter(metadata.writer_id),
            )
    }

    /// Snapshot senders are authorized by the snapshot boundary membership
    /// when it is present. This intentionally rejects older-boundary
    /// snapshots from leaders that joined after that boundary; those leaders
    /// must serve a snapshot whose Raft metadata includes them as voters.
    fn valid_snapshot_transfer_leader(
        &self,
        leader_id: NodeId,
        metadata: &RaftSnapshotMetadata,
    ) -> bool {
        metadata.committed_membership().map_or_else(
            || self.config.is_peer(leader_id),
            |membership| membership.contains_voter(leader_id),
        )
    }
}

pub(super) fn validate_snapshot_chunk_shape(
    request: &InstallSnapshotChunk,
) -> Result<(), SnapshotChunkRejection> {
    let chunk_len = request.chunk.len() as u64;
    let Some(end) = request.offset.checked_add(chunk_len) else {
        return Err(SnapshotChunkRejection::InvalidBounds);
    };
    if request.offset > request.total_payload_len || end > request.total_payload_len {
        return Err(SnapshotChunkRejection::InvalidBounds);
    }
    if request.done {
        if end == request.total_payload_len {
            Ok(())
        } else {
            Err(SnapshotChunkRejection::InvalidBounds)
        }
    } else if chunk_len > 0 && end < request.total_payload_len {
        Ok(())
    } else {
        Err(SnapshotChunkRejection::InvalidBounds)
    }
}
