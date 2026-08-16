//! Snapshot identity, authorization, shape, and rejection accounting.

use crate::{
    types::snapshot_transfer_id_from_parts, InstallSnapshotChunk, NodeId, RaftSnapshotMetadata,
    Term,
};

use crate::node::Node;

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
        metadata.hard_state_term <= term && self.valid_snapshot_author(metadata)
    }

    /// A snapshot's author must have been a **replica** — voter or learner — of
    /// the membership committed at the snapshot's own boundary.
    ///
    /// Learner, not voter, because a learner replicates the same committed
    /// prefix a voter does and so can capture it faithfully; excluding it made
    /// the natural call — a learner compacting at its own applied index, signing
    /// with its own id — produce a descriptor this node would install and then
    /// refuse to hydrate at the next restart. The membership is the descriptor's
    /// own, not this node's current one, because who wrote a snapshot is a fact
    /// about the snapshot. Without a declared boundary there is nothing to check
    /// against but the configuration this process was started with.
    ///
    /// [`Node::install_local_snapshot`](crate::Node::install_local_snapshot) and
    /// bootstrap validation apply the same rule, so no path admits a descriptor
    /// another would refuse.
    fn valid_snapshot_author(&self, metadata: &RaftSnapshotMetadata) -> bool {
        metadata.committed_membership().map_or_else(
            || {
                self.config
                    .static_membership_ref()
                    .contains_replica(metadata.writer_id)
            },
            |membership| membership.contains_replica(metadata.writer_id),
        )
    }

    /// A snapshot's sender is judged on its standing **now**, never on the
    /// historical boundary it happens to be relaying.
    ///
    /// The transfer's real authorization is already established before this
    /// runs: `message_sender_matches` in dispatch proves the frame's `leader_id`
    /// is the node it arrived from, and the receive handlers reject any term
    /// below this node's and adopt any term at or above it — so the sender is
    /// the one leader that term can have, exactly as for `AppendEntries`.
    ///
    /// What remains is a membership check, and it asks whether this node
    /// recognizes the sender at all. It accepts two answers, because a receiver
    /// has two ways to know who the cluster is and either alone wedges:
    ///
    /// * its **current** effective membership — the answer for a node with real
    ///   Raft-derived state, and the one that fixes the livelock. Requiring the
    ///   sender to appear in the *snapshot's* boundary meant a leader that had
    ///   merely installed an older snapshot could never relay it, and the
    ///   leader-side response path rewinds a rejected transfer to offset zero
    ///   and restreams it with no give-up.
    /// * the **descriptor's** boundary membership — the answer for a replica
    ///   that is still joining. Its bootstrap peers may predate the current
    ///   leader, and the log that would name it is exactly the log this snapshot
    ///   replaces, so the descriptor is the only cluster roster it can read. See
    ///   [`NodeConfig::new_non_voter`](crate::NodeConfig::new_non_voter).
    ///
    /// A sender in neither is refused. That is the residual guard: a node this
    /// receiver cannot place in any membership it can see does not get to hand
    /// it a state-machine image.
    fn valid_snapshot_transfer_leader(
        &self,
        leader_id: NodeId,
        metadata: &RaftSnapshotMetadata,
    ) -> bool {
        // Descriptor first, though it is the secondary rule: it is a borrow,
        // and this runs once per inbound chunk, while `effective_membership`
        // materializes the whole set — which a membership at the wire maximum
        // (65,535 voters and as many learners) makes worth not doing per chunk.
        metadata
            .committed_membership()
            .is_some_and(|membership| membership.contains_replica(leader_id))
            || self.effective_membership().contains_replica(leader_id)
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
