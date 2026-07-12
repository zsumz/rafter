//! Reception and application of one snapshot transfer chunk.

use crate::{
    InstallSnapshotChunk, LogIndex, NodeId, RaftSnapshot, SnapshotTransferId, StagedSnapshotChunk,
};

use super::super::super::super::{Node, Output};
use super::super::reply::SnapshotReply;
use super::super::validate::SnapshotChunkRejection;
use super::disposition::ChunkDisposition;

impl Node {
    pub(in crate::node) fn handle_install_snapshot_chunk(
        &mut self,
        leader_id: NodeId,
        request: InstallSnapshotChunk,
    ) -> Vec<Output> {
        if request.term < self.current_term() {
            self.record_snapshot_chunk_rejection(SnapshotChunkRejection::StaleTerm);
            return vec![self.snapshot_reply(
                leader_id,
                SnapshotReply::rejected_current(self.snapshot_index()),
            )];
        }

        let mut outputs = self.adopt_snapshot_term(request.term);
        if let Err(rejection) = self.validate_install_snapshot_chunk_header(leader_id, &request) {
            self.record_snapshot_chunk_rejection(rejection);
            outputs.push(self.snapshot_reply(
                leader_id,
                SnapshotReply::rejected_current(self.snapshot_index()),
            ));
            return outputs;
        }

        self.record_accepted_snapshot_leader(leader_id);

        let disposition = match self.classify_snapshot_chunk(leader_id, &request) {
            Ok(disposition) => disposition,
            Err(rejection) => {
                self.record_snapshot_chunk_rejection(rejection);
                outputs.push(self.snapshot_reply(
                    leader_id,
                    SnapshotReply::rejected_transfer(self.snapshot_index(), request.transfer_id, 0),
                ));
                return outputs;
            }
        };

        match disposition {
            ChunkDisposition::AlreadyCovered { covered_through } => self
                .acknowledge_covered_snapshot_chunk(leader_id, &request, covered_through, outputs),
            ChunkDisposition::Retransmission { next_offset } => {
                outputs.push(self.snapshot_reply(
                    leader_id,
                    SnapshotReply::accepted_transfer(
                        self.snapshot_index(),
                        request.transfer_id,
                        next_offset,
                    ),
                ));
                outputs
            }
            ChunkDisposition::OutOfOrder { expected_offset } => {
                self.record_snapshot_chunk_rejection(SnapshotChunkRejection::OutOfOrderOffset);
                outputs.push(self.snapshot_reply(
                    leader_id,
                    SnapshotReply::rejected_transfer(
                        self.snapshot_index(),
                        request.transfer_id,
                        expected_offset,
                    ),
                ));
                outputs
            }
            ChunkDisposition::Accept { received_len } => {
                self.accept_current_snapshot_chunk(leader_id, request, received_len, outputs)
            }
        }
    }

    fn acknowledge_covered_snapshot_chunk(
        &mut self,
        leader_id: NodeId,
        request: &InstallSnapshotChunk,
        covered_through: LogIndex,
        mut outputs: Vec<Output>,
    ) -> Vec<Output> {
        // A stale duplicate says nothing about another transfer that may be
        // live now, so clear only tracking for this exact transfer.
        if self
            .volatile
            .incoming_snapshot
            .as_ref()
            .is_some_and(|transfer| transfer.transfer_id == request.transfer_id)
        {
            self.volatile.incoming_snapshot = None;
        }

        outputs.push(self.snapshot_reply(
            leader_id,
            SnapshotReply::accepted_transfer(
                covered_through,
                request.transfer_id,
                request.total_payload_len,
            ),
        ));
        outputs
    }

    fn accept_current_snapshot_chunk(
        &mut self,
        leader_id: NodeId,
        request: InstallSnapshotChunk,
        received_len: u64,
        mut outputs: Vec<Output>,
    ) -> Vec<Output> {
        let next_offset = received_len + request.chunk.len() as u64;

        // Shape validation pins a final chunk to the payload end. Keep the
        // accounting guard here so a future validator change fails closed.
        if request.done && next_offset != request.total_payload_len {
            self.record_snapshot_chunk_rejection(SnapshotChunkRejection::InvalidBounds);
            outputs.push(self.snapshot_reply(
                leader_id,
                SnapshotReply::rejected_transfer(
                    self.snapshot_index(),
                    request.transfer_id,
                    received_len,
                ),
            ));
            return outputs;
        }

        if request.done {
            return self.install_final_snapshot_chunk(
                leader_id,
                request,
                received_len,
                next_offset,
                outputs,
            );
        }

        let Some(transfer) = self.volatile.incoming_snapshot.as_mut() else {
            return self.reject_missing_snapshot_transfer(
                leader_id,
                request.transfer_id,
                received_len,
                outputs,
            );
        };
        transfer.received_len = next_offset;
        let transfer_id = transfer.transfer_id;

        outputs.extend([
            stage_snapshot_chunk(leader_id, request),
            self.snapshot_reply(
                leader_id,
                SnapshotReply::accepted_transfer(self.snapshot_index(), transfer_id, next_offset),
            ),
        ]);
        outputs
    }

    fn install_final_snapshot_chunk(
        &mut self,
        leader_id: NodeId,
        request: InstallSnapshotChunk,
        received_len: u64,
        next_offset: u64,
        mut outputs: Vec<Output>,
    ) -> Vec<Output> {
        let Some(transfer) = self.volatile.incoming_snapshot.take() else {
            return self.reject_missing_snapshot_transfer(
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
        let transfer_id = request.transfer_id;

        outputs.extend(self.install_snapshot_state(snapshot.clone()));
        outputs.extend([
            stage_snapshot_chunk(leader_id, request),
            Output::ApplySnapshot { snapshot },
            self.snapshot_reply(
                leader_id,
                SnapshotReply::accepted_transfer(snapshot_index, transfer_id, next_offset),
            ),
        ]);
        outputs
    }

    fn reject_missing_snapshot_transfer(
        &mut self,
        leader_id: NodeId,
        transfer_id: SnapshotTransferId,
        next_offset: u64,
        mut outputs: Vec<Output>,
    ) -> Vec<Output> {
        self.record_snapshot_chunk_rejection(SnapshotChunkRejection::WrongTransfer);
        outputs.push(self.snapshot_reply(
            leader_id,
            SnapshotReply::rejected_transfer(self.snapshot_index(), transfer_id, next_offset),
        ));
        outputs
    }
}

fn stage_snapshot_chunk(leader_id: NodeId, request: InstallSnapshotChunk) -> Output {
    Output::StageSnapshotChunk {
        chunk: StagedSnapshotChunk {
            leader_id,
            transfer_id: request.transfer_id,
            metadata: request.metadata,
            total_payload_len: request.total_payload_len,
            application_payload_crc32: request.application_payload_crc32,
            offset: request.offset,
            bytes: request.chunk,
            done: request.done,
        },
    }
}
