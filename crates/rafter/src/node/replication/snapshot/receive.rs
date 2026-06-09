use crate::{
    types::snapshot_transfer_id_from_parts, InstallSnapshot, InstallSnapshotChunk, NodeId,
    RaftSnapshot, StagedSnapshotChunk,
};

use super::super::super::{state::IncomingSnapshotTransfer, Node, Output, Role};
use super::{
    validate_snapshot_chunk_shape, SnapshotChunkRejection, SnapshotTransferHeaderRejection,
};

impl Node {
    pub(in crate::node) fn handle_install_snapshot(
        &mut self,
        leader_id: NodeId,
        request: InstallSnapshot,
    ) -> Vec<Output> {
        if request.term < self.current_term() {
            return vec![self.install_snapshot_response(
                leader_id,
                false,
                self.snapshot_index(),
                None,
                0,
            )];
        }

        let mut outputs = if request.term > self.current_term() || self.role() != Role::Follower {
            self.become_follower(request.term)
        } else {
            Vec::new()
        };

        let snapshot = RaftSnapshot::from_payload(request.metadata, &request.application_payload);
        let snapshot_index = snapshot.metadata.last_included_index;
        if self
            .validate_snapshot_transfer_header(leader_id, &snapshot.metadata, request.term)
            .is_err()
        {
            outputs.push(self.install_snapshot_response(
                leader_id,
                false,
                self.snapshot_index(),
                None,
                0,
            ));
            return outputs;
        }
        self.election_elapsed = 0;
        // Accepted leader traffic refreshes the pre-vote stickiness hint.
        self.volatile.leader_hint = Some(leader_id);
        self.volatile.incoming_snapshot = None;

        // Everything at or below the commit index is already covered; an
        // older snapshot must never rewind the applied state machine. Report
        // the covered boundary so the leader advances past it.
        let covered_through = self.snapshot_index().max(self.commit_index());
        if snapshot_index <= covered_through {
            outputs.push(self.install_snapshot_response(
                leader_id,
                true,
                covered_through,
                Some(snapshot.transfer_id()),
                snapshot.application_payload_len,
            ));
            return outputs;
        }

        let transfer_id = snapshot.transfer_id();
        let total_payload_len = snapshot.application_payload_len;
        outputs.extend(self.install_snapshot_state(snapshot.clone()));

        outputs.extend([
            // A whole-snapshot message stages as one final chunk, so the
            // receiving store follows a single path for both transfer shapes.
            Output::StageSnapshotChunk {
                chunk: StagedSnapshotChunk {
                    leader_id,
                    transfer_id,
                    metadata: snapshot.metadata.clone(),
                    total_payload_len,
                    application_payload_crc32: snapshot.application_payload_crc32,
                    offset: 0,
                    bytes: request.application_payload,
                    done: true,
                },
            },
            Output::ApplySnapshot { snapshot },
            self.install_snapshot_response(
                leader_id,
                true,
                snapshot_index,
                Some(transfer_id),
                total_payload_len,
            ),
        ]);
        outputs
    }

    pub(in crate::node) fn handle_install_snapshot_chunk(
        &mut self,
        leader_id: NodeId,
        request: InstallSnapshotChunk,
    ) -> Vec<Output> {
        if request.term < self.current_term() {
            self.record_snapshot_chunk_rejection(SnapshotChunkRejection::StaleTerm);
            return vec![self.install_snapshot_response(
                leader_id,
                false,
                self.snapshot_index(),
                None,
                0,
            )];
        }

        let mut outputs = if request.term > self.current_term() || self.role() != Role::Follower {
            self.become_follower(request.term)
        } else {
            Vec::new()
        };

        if let Err(rejection) = self.validate_install_snapshot_chunk_header(leader_id, &request) {
            self.record_snapshot_chunk_rejection(rejection);
            outputs.push(self.install_snapshot_response(
                leader_id,
                false,
                self.snapshot_index(),
                None,
                0,
            ));
            return outputs;
        }
        self.election_elapsed = 0;
        // Accepted leader traffic refreshes the pre-vote stickiness hint.
        self.volatile.leader_hint = Some(leader_id);

        let covered_through = self.snapshot_index().max(self.commit_index());
        if request.metadata.last_included_index <= covered_through {
            // A stale duplicate of an already-covered transfer says nothing
            // about a different transfer that may be live right now; only
            // clear tracking that belongs to the covered transfer itself.
            if self
                .volatile
                .incoming_snapshot
                .as_ref()
                .is_some_and(|transfer| transfer.transfer_id == request.transfer_id)
            {
                self.volatile.incoming_snapshot = None;
            }
            outputs.push(self.install_snapshot_response(
                leader_id,
                true,
                covered_through,
                Some(request.transfer_id),
                request.total_payload_len,
            ));
            return outputs;
        }

        let expected_offset = match self.prepare_incoming_snapshot_transfer(leader_id, &request) {
            Ok(expected_offset) => expected_offset,
            Err(rejection) => {
                self.record_snapshot_chunk_rejection(rejection);
                outputs.push(self.install_snapshot_response(
                    leader_id,
                    false,
                    self.snapshot_index(),
                    Some(request.transfer_id),
                    0,
                ));
                return outputs;
            }
        };

        if request.offset < expected_offset {
            // A retransmitted prefix chunk of the matching transfer: the
            // bytes are already staged, so acknowledge the staged length
            // without restaging. The transfer identity pins metadata and
            // total length; verifying retransmitted content is the staging
            // store's concern, not the kernel's — the kernel keeps no bytes
            // to compare against.
            outputs.push(self.install_snapshot_response(
                leader_id,
                true,
                self.snapshot_index(),
                Some(request.transfer_id),
                expected_offset,
            ));
            return outputs;
        }

        if request.offset > expected_offset {
            self.record_snapshot_chunk_rejection(SnapshotChunkRejection::OutOfOrderOffset);
            outputs.push(self.install_snapshot_response(
                leader_id,
                false,
                self.snapshot_index(),
                Some(request.transfer_id),
                expected_offset,
            ));
            return outputs;
        }

        self.accept_current_snapshot_chunk(leader_id, request, outputs)
    }

    fn accept_current_snapshot_chunk(
        &mut self,
        leader_id: NodeId,
        request: InstallSnapshotChunk,
        mut outputs: Vec<Output>,
    ) -> Vec<Output> {
        let Some(received_len) = self
            .volatile
            .incoming_snapshot
            .as_ref()
            .map(|transfer| transfer.received_len)
        else {
            return self.reject_missing_current_snapshot_transfer(
                leader_id,
                request.transfer_id,
                0,
                outputs,
            );
        };
        let next_offset = received_len + request.chunk.len() as u64;

        // Chunk shape validation already pins done chunks to the payload
        // end; this guards the accounting if that ever drifts.
        if request.done && next_offset != request.total_payload_len {
            self.record_snapshot_chunk_rejection(SnapshotChunkRejection::InvalidBounds);
            outputs.push(self.install_snapshot_response(
                leader_id,
                false,
                self.snapshot_index(),
                Some(request.transfer_id),
                received_len,
            ));
            return outputs;
        }

        if !request.done {
            let Some(transfer) = self.volatile.incoming_snapshot.as_mut() else {
                return self.reject_missing_current_snapshot_transfer(
                    leader_id,
                    request.transfer_id,
                    received_len,
                    outputs,
                );
            };
            transfer.received_len = next_offset;
            let response = self.install_snapshot_response(
                leader_id,
                true,
                self.snapshot_index(),
                Some(request.transfer_id),
                next_offset,
            );
            outputs.extend([
                Output::StageSnapshotChunk {
                    chunk: StagedSnapshotChunk {
                        leader_id,
                        transfer_id: request.transfer_id,
                        metadata: request.metadata,
                        total_payload_len: request.total_payload_len,
                        application_payload_crc32: request.application_payload_crc32,
                        offset: request.offset,
                        bytes: request.chunk,
                        done: false,
                    },
                },
                response,
            ]);
            return outputs;
        }

        // Final chunk: the staged payload is complete. Install the snapshot
        // boundary and hand the store the last chunk plus the descriptor the
        // staged content now backs.
        let Some(transfer) = self.volatile.incoming_snapshot.take() else {
            return self.reject_missing_current_snapshot_transfer(
                leader_id,
                request.transfer_id,
                received_len,
                outputs,
            );
        };
        let snapshot = RaftSnapshot::new(
            transfer.metadata,
            request.total_payload_len,
            request.application_payload_crc32,
        );
        let snapshot_index = snapshot.metadata.last_included_index;
        outputs.extend(self.install_snapshot_state(snapshot.clone()));
        let response = self.install_snapshot_response(
            leader_id,
            true,
            snapshot_index,
            Some(request.transfer_id),
            next_offset,
        );

        outputs.extend([
            Output::StageSnapshotChunk {
                chunk: StagedSnapshotChunk {
                    leader_id,
                    transfer_id: request.transfer_id,
                    metadata: request.metadata,
                    total_payload_len: request.total_payload_len,
                    application_payload_crc32: request.application_payload_crc32,
                    offset: request.offset,
                    bytes: request.chunk,
                    done: true,
                },
            },
            Output::ApplySnapshot { snapshot },
            response,
        ]);
        outputs
    }

    fn reject_missing_current_snapshot_transfer(
        &mut self,
        leader_id: NodeId,
        transfer_id: crate::SnapshotTransferId,
        next_offset: u64,
        mut outputs: Vec<Output>,
    ) -> Vec<Output> {
        self.record_snapshot_chunk_rejection(SnapshotChunkRejection::WrongTransfer);
        outputs.push(self.install_snapshot_response(
            leader_id,
            false,
            self.snapshot_index(),
            Some(transfer_id),
            next_offset,
        ));
        outputs
    }

    fn validate_install_snapshot_chunk_header(
        &self,
        leader_id: NodeId,
        request: &InstallSnapshotChunk,
    ) -> Result<(), SnapshotChunkRejection> {
        if let Err(rejection) =
            self.validate_snapshot_transfer_header(leader_id, &request.metadata, request.term)
        {
            return Err(match rejection {
                SnapshotTransferHeaderRejection::InvalidMetadata => {
                    SnapshotChunkRejection::MetadataMismatch
                }
                SnapshotTransferHeaderRejection::LeaderNotAuthorized => {
                    SnapshotChunkRejection::LeaderNotAuthorized
                }
            });
        }
        if request.transfer_id
            != snapshot_transfer_id_from_parts(
                &request.metadata,
                request.total_payload_len,
                request.application_payload_crc32,
            )
        {
            return Err(SnapshotChunkRejection::WrongTransfer);
        }
        validate_snapshot_chunk_shape(request)
    }

    fn record_snapshot_chunk_rejection(&mut self, rejection: SnapshotChunkRejection) {
        match rejection {
            SnapshotChunkRejection::StaleTerm => {
                self.snapshot_chunk_rejections.stale_term += 1;
            }
            SnapshotChunkRejection::WrongTransfer => {
                self.snapshot_chunk_rejections.wrong_transfer += 1;
            }
            SnapshotChunkRejection::MetadataMismatch
            | SnapshotChunkRejection::LeaderNotAuthorized => {
                self.snapshot_chunk_rejections.metadata_mismatch += 1;
            }
            SnapshotChunkRejection::OutOfOrderOffset => {
                self.snapshot_chunk_rejections.out_of_order_offset += 1;
            }
            SnapshotChunkRejection::InvalidBounds => {
                self.snapshot_chunk_rejections.invalid_bounds += 1;
            }
        }
    }

    fn prepare_incoming_snapshot_transfer(
        &mut self,
        leader_id: NodeId,
        request: &InstallSnapshotChunk,
    ) -> Result<u64, SnapshotChunkRejection> {
        let current_matches = self
            .volatile
            .incoming_snapshot
            .as_ref()
            .is_some_and(|transfer| {
                transfer.leader_id == leader_id
                    && transfer.transfer_id == request.transfer_id
                    && transfer.metadata == request.metadata
                    && transfer.total_payload_len == request.total_payload_len
                    && transfer.application_payload_crc32 == request.application_payload_crc32
            });

        if !current_matches {
            if request.offset == 0 {
                self.volatile.incoming_snapshot = Some(IncomingSnapshotTransfer::new(
                    leader_id,
                    request.transfer_id,
                    request.metadata.clone(),
                    request.total_payload_len,
                    request.application_payload_crc32,
                ));
            } else if let Some(transfer) = self.volatile.incoming_snapshot.as_ref() {
                return Err(
                    if transfer.leader_id != leader_id
                        || transfer.transfer_id != request.transfer_id
                    {
                        SnapshotChunkRejection::WrongTransfer
                    } else {
                        SnapshotChunkRejection::MetadataMismatch
                    },
                );
            } else {
                return Err(SnapshotChunkRejection::OutOfOrderOffset);
            }
        }

        self.volatile
            .incoming_snapshot
            .as_ref()
            .map(IncomingSnapshotTransfer::next_offset)
            .ok_or(SnapshotChunkRejection::WrongTransfer)
    }
}
