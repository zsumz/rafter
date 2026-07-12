//! Leader-side handling of `AppendEntriesResponse` frames.
//!
//! Same-term responses refresh authority evidence, update follower progress,
//! trigger commit or transfer completion, and refill the follower window.

use crate::{AppendEntriesResponse, LogIndex, NodeId};

use super::super::state::ProgressMode;
use super::super::{Node, Output, Role};
use super::ReplicationDemand;

impl Node {
    pub(in crate::node) fn handle_append_entries_response(
        &mut self,
        follower_id: NodeId,
        response: AppendEntriesResponse,
    ) -> Vec<Output> {
        if response.term > self.current_term() {
            return self.become_follower(response.term);
        }
        if self.role() != Role::Leader || response.term != self.current_term() {
            return Vec::new();
        }

        // Any same-term response proves the follower still recognizes this
        // leader. It counts for check-quorum, the read lease, and pending read
        // barriers through the echoed heartbeat sequence.
        self.record_quorum_ack(follower_id);
        self.acknowledge_read_lease(follower_id, response.sequence);
        let mut outputs = self.acknowledge_read_barriers(follower_id, response.sequence);

        if response.success {
            self.accept_append_response(follower_id, response.match_index, &mut outputs);
        } else {
            self.reject_append_response(follower_id, &mut outputs);
        }

        outputs
    }

    fn accept_append_response(
        &mut self,
        follower_id: NodeId,
        reported_match_index: LogIndex,
        outputs: &mut Vec<Output>,
    ) {
        let snapshot_index = self.snapshot_index();
        let reported_match_index = reported_match_index.min(self.last_log_index());
        let commit_index = self.commit_index();

        let Some(can_advance_commit) =
            self.try_follower_progress_mut(follower_id).map(|progress| {
                let old_match_index = progress.match_index;
                progress.match_index = progress.match_index.max(reported_match_index);
                let acknowledged = progress.match_index;
                progress.inflights.free_through(acknowledged);

                // An acknowledgement at or beyond the snapshot boundary proves
                // the follower has the log. A stale pre-snapshot response says
                // nothing about a transfer already in progress.
                if !matches!(progress.mode, ProgressMode::Snapshot { .. })
                    || acknowledged >= snapshot_index
                {
                    progress.confirm_replicating();
                }
                progress.next_index = progress.next_index.max(acknowledged.next());

                successful_ack_can_advance_commit(old_match_index, acknowledged, commit_index)
            })
        else {
            return;
        };

        outputs.extend(self.maybe_complete_leadership_transfer(follower_id));
        if can_advance_commit {
            self.advance_commit_index_into(outputs);
        }

        // A freed window slot pulls the next batch immediately. Catch-up is
        // acknowledgement-paced rather than heartbeat-paced.
        self.replicate_to_follower(follower_id, ReplicationDemand::ProgressOnly, outputs);
    }

    fn reject_append_response(&mut self, follower_id: NodeId, outputs: &mut Vec<Output>) {
        let snapshot_index = self.snapshot_index();
        let Some(progress) = self.try_follower_progress_mut(follower_id) else {
            return;
        };
        if !matches!(progress.mode, ProgressMode::Snapshot { .. }) {
            progress.collapse_into_probe(snapshot_index);
        }

        self.replicate_to_follower(follower_id, ReplicationDemand::EnsureContact, outputs);
    }
}

pub(super) fn successful_ack_can_advance_commit(
    old_match_index: LogIndex,
    acknowledged: LogIndex,
    commit_index: LogIndex,
) -> bool {
    acknowledged > old_match_index && acknowledged > commit_index
}
