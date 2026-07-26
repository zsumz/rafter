//! Election timeouts, pre-vote, binding votes, and leadership acquisition.
//!
//! This module owns campaign transitions and vote eligibility. Durable term
//! and vote changes remain explicit in [`Node`](super::Node) state, while
//! [`ElectionState`](super::state::ElectionState) owns only the local timer and
//! grants collected during active rounds.

use crate::{
    LogIndex, Message, NodeId, PreVote, PreVoteResponse, RequestVote, RequestVoteResponse, Term,
};

use super::{Node, Output, Role};

impl Node {
    pub(super) fn tick(&mut self) -> Vec<Output> {
        if self.role() == Role::Leader {
            self.leader.ticks += 1;
            self.leader.heartbeat_elapsed = self.leader.heartbeat_elapsed.saturating_add(1);
            self.tick_read_lease();
            self.tick_leadership_transfer();
            if let Some(outputs) = self.tick_check_quorum() {
                return outputs;
            }
            if self.leader.heartbeat_elapsed < self.config.heartbeat_interval_ticks() {
                return Vec::new();
            }
            return self.broadcast_append_entries();
        }

        if self.election.advance_timeout() < self.effective_election_timeout() {
            return Vec::new();
        }

        if self.config.pre_vote() {
            // Thesis 9.6: every election is preceded by a pre-vote round, so
            // a node that cannot win (partitioned, stale log) never inflates
            // its term. This also covers a timed-out Candidate: its next
            // attempt starts with a fresh pre-vote round rather than another
            // term increment.
            return self.start_pre_vote_round();
        }

        self.start_election()
    }

    fn start_pre_vote_round(&mut self) -> Vec<Output> {
        if !self.is_effective_voter(self.id()) {
            self.election.reset_timeout();
            return Vec::new();
        }
        // A poll proposes the successor term, and the maximum term has none.
        // Refusing here keeps the node a follower rather than polling at its
        // own term forever, which no voter could grant anyway.
        let Some(proposed_term) = self.current_term().checked_next() else {
            self.election.reset_timeout();
            return Vec::new();
        };

        // The round proposes current + 1 WITHOUT mutating persistent state:
        // no term increment, no voted_for, nothing persisted. Timing out
        // again re-broadcasts at the same proposed term, which is the point
        // of the feature (thesis 9.6).
        self.volatile.role = Role::PreCandidate;
        self.volatile.leader_hint = None;
        let self_id = self.id();
        self.election.begin_pre_vote(self_id);

        let request = PreVote {
            term: proposed_term,
            candidate_id: self.id(),
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        };

        let mut outputs: Vec<Output> = self
            .effective_membership()
            .voter_ids()
            .into_iter()
            .filter(|voter| *voter != self.id())
            .map(|to| Output::Send {
                to,
                message: Message::PreVote(request),
            })
            .collect();

        // A single-voter membership wins its own pre-vote poll immediately.
        if self.has_effective_quorum(self.election.pre_votes()) {
            outputs.extend(self.start_election());
        }

        outputs
    }

    pub(super) fn start_election(&mut self) -> Vec<Output> {
        if !self.is_effective_voter(self.id()) {
            self.election.reset_timeout();
            return Vec::new();
        }
        // Term exhaustion stops elections rather than restarting history. The
        // increment is the first thing a campaign does, and at `Term::MAX`
        // there is nothing to increment to: this node stays a follower, and
        // says so by changing no state at all.
        let Some(campaign_term) = self.persistent.current_term.checked_next() else {
            self.election.reset_timeout();
            return Vec::new();
        };

        self.volatile.role = Role::Candidate;
        self.volatile.leader_hint = None;
        self.persistent.current_term = campaign_term;
        let self_id = self.id();
        self.persistent.voted_for = Some(self_id);
        self.election.begin_election(self_id);

        let request = RequestVote {
            term: self.current_term(),
            candidate_id: self.id(),
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        };

        let mut outputs: Vec<Output> = self
            .effective_membership()
            .voter_ids()
            .into_iter()
            .filter(|voter| *voter != self.id())
            .map(|to| Output::Send {
                to,
                message: Message::RequestVote(request),
            })
            .collect();

        if self.has_effective_quorum(self.election.votes()) {
            outputs.extend(self.become_leader());
            outputs.extend(self.broadcast_append_entries());
        }

        outputs
    }

    pub(super) fn handle_request_vote(
        &mut self,
        candidate_id: NodeId,
        request: RequestVote,
    ) -> Vec<Output> {
        let mut outputs = if request.term > self.current_term() {
            self.become_follower(request.term)
        } else {
            Vec::new()
        };

        let vote_granted = request.term == self.current_term()
            && self.effective_membership().contains_voter(candidate_id)
            && self.candidate_log_is_up_to_date(request.last_log_term, request.last_log_index)
            && self
                .persistent
                .voted_for
                .is_none_or(|voted_for| voted_for == candidate_id);

        if vote_granted {
            self.persistent.voted_for = Some(candidate_id);
            self.election.reset_timeout();
        }

        outputs.push(Output::Send {
            to: candidate_id,
            message: Message::RequestVoteResponse(RequestVoteResponse {
                term: self.current_term(),
                voter_id: self.id(),
                vote_granted,
            }),
        });
        outputs
    }

    pub(super) fn handle_request_vote_response(
        &mut self,
        voter_id: NodeId,
        response: RequestVoteResponse,
    ) -> Vec<Output> {
        if response.term > self.current_term() {
            return self.become_follower(response.term);
        }

        if self.role() != Role::Candidate || response.term != self.current_term() {
            return Vec::new();
        }

        if !self.is_effective_voter(self.id()) {
            return self.become_follower(self.current_term());
        }

        if response.vote_granted {
            self.election.record_vote(voter_id);
        }

        if self.has_effective_quorum(self.election.votes()) {
            let mut outputs = self.become_leader();
            outputs.extend(self.broadcast_append_entries());
            return outputs;
        }

        Vec::new()
    }

    pub(super) fn handle_pre_vote(
        &mut self,
        candidate_id: NodeId,
        request: PreVote,
    ) -> Vec<Output> {
        // Leader stickiness (thesis 4.2.3): while this node has heard from a
        // leader within its election timeout, a pre-vote poll must fail so a
        // rejoining partitioned node cannot depose a healthy leader.
        let leader_believed_current = self.volatile.leader_hint.is_some()
            && self.election.elapsed() < self.config.election_timeout_ticks();

        let vote_granted = request.term > self.current_term()
            && self.effective_membership().contains_voter(candidate_id)
            && self.candidate_log_is_up_to_date(request.last_log_term, request.last_log_index)
            && !leader_believed_current;

        // Granting mutates nothing: term, voted_for, and the election timer all
        // stay put, and nothing is persisted. Multiple pre-vote grants in one
        // term are allowed by design; only a real RequestVote binds a vote.
        //
        // Grants echo the proposed term. Denials carry this node's own term
        // so a candidate that has fallen behind learns about newer terms
        // instead of polling at a stale proposal forever.
        let response_term = if vote_granted {
            request.term
        } else {
            self.current_term()
        };
        vec![Output::Send {
            to: candidate_id,
            message: Message::PreVoteResponse(PreVoteResponse {
                term: response_term,
                voter_id: self.id(),
                vote_granted,
            }),
        }]
    }

    pub(super) fn handle_pre_vote_response(
        &mut self,
        voter_id: NodeId,
        response: PreVoteResponse,
    ) -> Vec<Output> {
        // At `Term::MAX` this node never polled, so nothing can confirm a
        // proposal it did not make; a response carrying a newer term is
        // impossible there, since no term is newer.
        let Some(proposed_term) = self.current_term().checked_next() else {
            return Vec::new();
        };
        if response.term > proposed_term {
            return self.become_follower(response.term);
        }

        // A denial carrying a term newer than ours reveals the poll is
        // stale; step to that term so the next round proposes past it.
        if !response.vote_granted && response.term > self.current_term() {
            return self.become_follower(response.term);
        }

        if self.role() != Role::PreCandidate || response.term != proposed_term {
            return Vec::new();
        }

        if response.vote_granted {
            self.election.record_pre_vote(voter_id);
        }

        // A quorum at the proposed term predicts a winnable election, so the
        // real, term-incrementing election path starts now (thesis 9.6).
        if self.has_effective_quorum(self.election.pre_votes()) {
            return self.start_election();
        }

        Vec::new()
    }

    /// Base timeout plus this term's deterministic jitter offset. The
    /// offset mixes (node id, current term) so ties break differently each
    /// term without any source of nondeterminism.
    fn effective_election_timeout(&self) -> u64 {
        const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
        const FNV_PRIME: u64 = 0x0100_0000_01b3;

        let base = self.config.election_timeout_ticks();
        let jitter = self.config.election_jitter_ticks();
        if jitter == 0 {
            return base;
        }
        let mut hash = FNV_OFFSET_BASIS;
        for value in [self.id().0, self.current_term().0] {
            for byte in value.to_be_bytes() {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(FNV_PRIME);
            }
        }
        let offset = if jitter == u64::MAX {
            hash
        } else {
            hash % (jitter + 1)
        };
        base.saturating_add(offset)
    }

    fn candidate_log_is_up_to_date(&self, last_log_term: Term, last_log_index: LogIndex) -> bool {
        (last_log_term, last_log_index) >= (self.last_log_term(), self.last_log_index())
    }
}
