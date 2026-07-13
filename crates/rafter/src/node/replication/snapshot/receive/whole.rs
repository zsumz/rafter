//! Reception of a complete snapshot carried in one message.

use crate::{InstallSnapshot, NodeId, RaftSnapshot, StagedSnapshotChunk};

use super::super::reply::SnapshotReply;
use crate::node::{Node, Output};

impl Node {
    pub(in crate::node) fn handle_install_snapshot(
        &mut self,
        leader_id: NodeId,
        request: InstallSnapshot,
    ) -> Vec<Output> {
        if request.term < self.current_term() {
            return vec![self.snapshot_reply(
                leader_id,
                SnapshotReply::rejected_current(self.snapshot_index()),
            )];
        }

        let mut outputs = self.adopt_snapshot_term(request.term);
        let snapshot = RaftSnapshot::from_payload(request.metadata, &request.application_payload);

        if self
            .validate_snapshot_transfer_header(leader_id, &snapshot.metadata, request.term)
            .is_err()
        {
            outputs.push(self.snapshot_reply(
                leader_id,
                SnapshotReply::rejected_current(self.snapshot_index()),
            ));
            return outputs;
        }

        self.record_accepted_snapshot_leader(leader_id);
        self.volatile.incoming_snapshot = None;

        let covered_through = self.snapshot_covered_through();
        let snapshot_index = snapshot.metadata.last_included_index;
        if snapshot_index <= covered_through {
            outputs.push(self.snapshot_reply(
                leader_id,
                SnapshotReply::accepted_transfer(
                    covered_through,
                    snapshot.transfer_id(),
                    snapshot.application_payload_len,
                ),
            ));
            return outputs;
        }

        self.install_whole_snapshot(leader_id, snapshot, request.application_payload, outputs)
    }

    fn install_whole_snapshot(
        &mut self,
        leader_id: NodeId,
        snapshot: RaftSnapshot,
        application_payload: Vec<u8>,
        mut outputs: Vec<Output>,
    ) -> Vec<Output> {
        let transfer_id = snapshot.transfer_id();
        let snapshot_index = snapshot.metadata.last_included_index;
        let total_payload_len = snapshot.application_payload_len;

        outputs.extend(self.install_snapshot_state(snapshot.clone()));
        outputs.extend([
            // A whole-snapshot message stages as one final chunk, so the
            // receiving store follows one path for both transfer shapes.
            Output::StageSnapshotChunk {
                chunk: StagedSnapshotChunk {
                    leader_id,
                    transfer_id,
                    metadata: snapshot.metadata.clone(),
                    total_payload_len,
                    application_payload_crc32: snapshot.application_payload_crc32,
                    offset: 0,
                    bytes: application_payload,
                    done: true,
                },
            },
            Output::ApplySnapshot { snapshot },
            self.snapshot_reply(
                leader_id,
                SnapshotReply::accepted_transfer(snapshot_index, transfer_id, total_payload_len),
            ),
        ]);
        outputs
    }
}
