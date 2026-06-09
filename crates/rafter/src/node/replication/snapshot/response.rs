use crate::{InstallSnapshotResponse, NodeId, RaftSnapshot};

use super::super::super::state::ProgressMode;
use super::super::super::{Node, Output, Role};

impl Node {
    pub(in crate::node) fn handle_install_snapshot_response(
        &mut self,
        follower_id: NodeId,
        response: InstallSnapshotResponse,
    ) -> Vec<Output> {
        if response.term > self.current_term() {
            return self.become_follower(response.term);
        }

        if self.role() != Role::Leader || response.term != self.current_term() {
            return Vec::new();
        }

        // A same-term snapshot response still proves the follower recognizes
        // this leader for check-quorum purposes.
        self.leader.quorum_acks.insert(follower_id);

        // A response naming a transfer is only meaningful for the snapshot
        // the leader currently holds; delayed duplicates from older
        // transfers must not restream or regress replication progress.
        if let Some(transfer_id) = response.transfer_id {
            let current_transfer = self
                .persistent
                .snapshot
                .as_ref()
                .map(RaftSnapshot::transfer_id);
            if current_transfer != Some(transfer_id) {
                return Vec::new();
            }
        }

        if response.success {
            if let Some(snapshot) = self.persistent.snapshot.as_ref() {
                let total_payload_len = snapshot.application_payload_len;
                let snapshot_index = snapshot.metadata.last_included_index;
                let expected_transfer_id = snapshot.transfer_id();
                if response.last_included_index < snapshot_index
                    || response.next_offset < total_payload_len
                {
                    let acked_offset = if response.transfer_id == Some(expected_transfer_id) {
                        response.next_offset.min(total_payload_len)
                    } else {
                        0
                    };
                    let progress = self.follower_progress_mut(follower_id);
                    // Acks may arrive out of order; the send offset for the
                    // current transfer only ever advances.
                    let current_offset = match progress.mode {
                        ProgressMode::Snapshot { next_offset } => next_offset,
                        _ => 0,
                    };
                    progress.mode = ProgressMode::Snapshot {
                        next_offset: acked_offset.max(current_offset),
                    };
                    progress.inflights.clear();
                    progress.next_index = snapshot_index;
                    let mut outputs = Vec::new();
                    self.replicate_to_peer(follower_id, true, &mut outputs);
                    return outputs;
                }
            }

            let reported_snapshot_index =
                std::cmp::min(response.last_included_index, self.last_log_index());
            let progress = self.follower_progress_mut(follower_id);
            progress.match_index = progress.match_index.max(reported_snapshot_index);
            let acknowledged = progress.match_index;
            progress.confirm_replicating();
            progress.next_index = progress.next_index.max(acknowledged.next());
            let mut outputs = self.maybe_complete_leadership_transfer(follower_id);
            outputs.extend(self.advance_commit_index());
            // The installed snapshot confirmed the follower's position; fill
            // its window with the suffix immediately.
            self.replicate_to_peer(follower_id, false, &mut outputs);
            return outputs;
        }

        let rewound_offset = self.persistent.snapshot.as_ref().map(|snapshot| {
            if response.transfer_id == Some(snapshot.transfer_id()) {
                response.next_offset.min(snapshot.application_payload_len)
            } else {
                0
            }
        });
        let snapshot_index = self.snapshot_index();
        let progress = self.follower_progress_mut(follower_id);
        if let Some(next_offset) = rewound_offset {
            // The follower rejected the transfer state we assumed: resume
            // from the offset it reports (or from scratch for a different
            // transfer), and stay in snapshot mode.
            progress.mode = ProgressMode::Snapshot { next_offset };
            progress.inflights.clear();
        }
        progress.next_index = snapshot_index;
        let mut outputs = Vec::new();
        self.replicate_to_peer(follower_id, true, &mut outputs);
        outputs
    }
}
