use crate::{LogEntry, Term};

use super::state::LeaderState;
use super::{LocalProposalDropReason, Node, Output, ReadIndexCancelReason, Role};

impl Node {
    pub(super) fn become_follower(&mut self, term: Term) -> Vec<Output> {
        let mut outputs = self.drain_local_proposals(LocalProposalDropReason::LeadershipLost);
        outputs.extend(self.drain_pending_reads(ReadIndexCancelReason::LeadershipLost));
        if term > self.persistent.current_term {
            self.persistent.voted_for = None;
            // A term increase invalidates who we believed led the old term;
            // accepted traffic from the new leader will refresh the hint.
            self.volatile.leader_hint = None;
        }
        self.volatile.role = Role::Follower;
        self.persistent.current_term = std::cmp::max(self.persistent.current_term, term);
        self.election_elapsed = 0;
        self.granted_votes.clear();
        self.granted_pre_votes.clear();
        self.leader = LeaderState::default();
        outputs
    }

    pub(super) fn become_leader(&mut self) -> Vec<Output> {
        self.volatile.role = Role::Leader;
        // The leader believes in itself: with a hint set and election_elapsed
        // pinned at zero, the pre-vote stickiness rule (thesis 4.2.3) makes a
        // healthy leader deny pre-votes instead of helping depose itself.
        self.volatile.leader_hint = Some(self.id());
        self.election_elapsed = 0;
        // Every term's leadership starts from clean leader state: pending
        // transfers, read barriers, quorum bookkeeping, and the heartbeat
        // round must never leak across terms (responses are term-gated, so
        // resetting the round is safe).
        self.leader = LeaderState::default();
        self.granted_pre_votes.clear();
        self.volatile.incoming_snapshot = None;

        self.append_log_entry(LogEntry::noop(self.current_term()));
        let noop_index = self.last_log_index();

        self.leader.progress.reset(
            &self.effective_membership(),
            self.id(),
            noop_index,
            noop_index,
        );
        self.advance_commit_index()
    }

    pub(super) fn drain_local_proposals(&mut self, reason: LocalProposalDropReason) -> Vec<Output> {
        std::mem::take(&mut self.volatile.local_proposals)
            .into_iter()
            .map(|(index, proposal)| Output::LocalProposalDropped {
                proposal_id: proposal.id,
                index,
                term: proposal.term,
                reason,
            })
            .collect()
    }

    pub(super) fn drain_pending_reads(&mut self, reason: ReadIndexCancelReason) -> Vec<Output> {
        self.leader
            .pending_reads
            .drain(..)
            .flat_map(|pending| {
                pending
                    .read_ids
                    .into_iter()
                    .map(move |read_id| Output::ReadIndexCanceled { read_id, reason })
            })
            .collect()
    }
}
