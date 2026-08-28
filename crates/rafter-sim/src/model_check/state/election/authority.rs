//! Authority and pre-vote transition observation.
//!
//! A delivered message may only raise a node's authority. Every transition
//! that fences a higher term incorrectly, or that lets a stale term create or
//! lower authority, is recorded as an explicit violation.

use rafter::{Message, Term};

use crate::{Cluster, Envelope};

use super::super::super::observations::Observation;
use super::super::ExplorationState;
use super::{
    AuthorityTransitionViolation, AuthorityTransitionViolationKind, PreVoteViolation,
    PreVoteViolationKind,
};

impl ExplorationState {
    pub(in crate::model_check) fn observe_election_authority(&mut self) {
        let observations = self.election_history.observe_authority_state(&self.cluster);
        self.observations.union_with(observations);
    }

    pub(super) fn record_pre_vote_transition(&mut self, before: &Cluster, envelope: &Envelope) {
        let Some(before_node) = before.nodes.get(&envelope.to) else {
            return;
        };
        match &envelope.message {
            Message::PreVote(_) => {
                self.mark_observation(Observation::PreVoteRequestDeliveries);
                if before_node.role() == rafter::Role::Leader {
                    self.mark_observation(Observation::LeaderPreVoteRequestDeliveries);
                }
            }
            Message::PreVoteResponse(response) if response.term <= before_node.current_term() => {
                self.mark_observation(Observation::StalePreVoteResponses);
            }
            _ => {}
        }
        let Some(after_node) = self.cluster.nodes.get(&envelope.to) else {
            return;
        };
        match &envelope.message {
            Message::PreVote(request) => {
                if before_node.current_term() != after_node.current_term()
                    || before_node.voted_for() != after_node.voted_for()
                {
                    self.election_history
                        .pre_vote_violations
                        .push(PreVoteViolation {
                            node_id: envelope.to,
                            message_kind: "PreVote",
                            message_term: request.term,
                            before_term: before_node.current_term(),
                            after_term: after_node.current_term(),
                            before_vote: before_node.voted_for(),
                            after_vote: after_node.voted_for(),
                            before_role: before_node.role(),
                            after_role: after_node.role(),
                            reason: PreVoteViolationKind::RequestMutatedAuthority,
                        });
                }
                if before_node.role() == rafter::Role::Leader
                    && after_node.role() != rafter::Role::Leader
                {
                    self.election_history
                        .pre_vote_violations
                        .push(PreVoteViolation {
                            node_id: envelope.to,
                            message_kind: "PreVote",
                            message_term: request.term,
                            before_term: before_node.current_term(),
                            after_term: after_node.current_term(),
                            before_vote: before_node.voted_for(),
                            after_vote: after_node.voted_for(),
                            before_role: before_node.role(),
                            after_role: after_node.role(),
                            reason: PreVoteViolationKind::RequestDisruptedLeader,
                        });
                }
            }
            Message::PreVoteResponse(response) => {
                let response_is_stale = response.term <= before_node.current_term();
                let created_campaign_authority = !matches!(
                    before_node.role(),
                    rafter::Role::Candidate | rafter::Role::Leader
                ) && matches!(
                    after_node.role(),
                    rafter::Role::Candidate | rafter::Role::Leader
                );
                let advanced_authority = after_node.current_term() != before_node.current_term()
                    || after_node.voted_for() != before_node.voted_for()
                    || created_campaign_authority;
                if response.vote_granted && response_is_stale && advanced_authority {
                    self.election_history
                        .pre_vote_violations
                        .push(PreVoteViolation {
                            node_id: envelope.to,
                            message_kind: "PreVoteResponse",
                            message_term: response.term,
                            before_term: before_node.current_term(),
                            after_term: after_node.current_term(),
                            before_vote: before_node.voted_for(),
                            after_vote: after_node.voted_for(),
                            before_role: before_node.role(),
                            after_role: after_node.role(),
                            reason: PreVoteViolationKind::StaleResponseAdvancedAuthority,
                        });
                }
            }
            _ => {}
        }
    }

    pub(super) fn record_authority_transition(&mut self, before: &Cluster, envelope: &Envelope) {
        let Some(message_authority) = authority_message(&envelope.message) else {
            return;
        };
        let Some(before_node) = before.nodes.get(&envelope.to) else {
            return;
        };

        if message_authority.must_fence_higher_term
            && message_authority.term > before_node.current_term()
        {
            self.mark_observation(Observation::HigherTermAuthorityDeliveries);
        }
        if message_authority.is_response && message_authority.term < before_node.current_term() {
            self.mark_observation(Observation::StaleAuthorityResponses);
        }
        let Some(after_node) = self.cluster.nodes.get(&envelope.to) else {
            return;
        };
        let after_term = after_node.current_term();
        let after_vote = after_node.voted_for();
        let after_role = after_node.role();
        let stale_term = message_authority.term < before_node.current_term();
        if stale_term {
            self.mark_observation(Observation::StaleAuthorityStateComparisons);
        }

        let higher_term_not_fenced = message_authority.must_fence_higher_term
            && message_authority.term > before_node.current_term()
            && (after_term < message_authority.term
                || (before_node.role() == rafter::Role::Leader
                    && after_role == rafter::Role::Leader));
        if higher_term_not_fenced {
            self.election_history.authority_transition_violations.push(
                AuthorityTransitionViolation {
                    node_id: envelope.to,
                    message_kind: message_authority.kind,
                    message_term: message_authority.term,
                    before_term: before_node.current_term(),
                    after_term,
                    before_vote: before_node.voted_for(),
                    after_vote,
                    before_role: before_node.role(),
                    after_role,
                    reason: AuthorityTransitionViolationKind::HigherTermNotFenced,
                },
            );
        }

        let stale_term_created_leader = message_authority.term < before_node.current_term()
            && before_node.role() != rafter::Role::Leader
            && after_role == rafter::Role::Leader;
        if stale_term_created_leader {
            self.election_history.authority_transition_violations.push(
                AuthorityTransitionViolation {
                    node_id: envelope.to,
                    message_kind: message_authority.kind,
                    message_term: message_authority.term,
                    before_term: before_node.current_term(),
                    after_term,
                    before_vote: before_node.voted_for(),
                    after_vote,
                    before_role: before_node.role(),
                    after_role,
                    reason: AuthorityTransitionViolationKind::StaleTermCreatedLeader,
                },
            );
        }

        let stale_term_lowered_authority = stale_term
            && (after_term < before_node.current_term()
                || (after_term == before_node.current_term()
                    && after_vote != before_node.voted_for()));
        if stale_term_lowered_authority {
            self.election_history.authority_transition_violations.push(
                AuthorityTransitionViolation {
                    node_id: envelope.to,
                    message_kind: message_authority.kind,
                    message_term: message_authority.term,
                    before_term: before_node.current_term(),
                    after_term,
                    before_vote: before_node.voted_for(),
                    after_vote,
                    before_role: before_node.role(),
                    after_role,
                    reason: AuthorityTransitionViolationKind::StaleTermLoweredAuthority,
                },
            );
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct AuthorityMessage {
    kind: &'static str,
    term: Term,
    must_fence_higher_term: bool,
    is_response: bool,
}

fn authority_message(message: &Message) -> Option<AuthorityMessage> {
    let (kind, term, must_fence_higher_term, is_response) = match message {
        Message::AppendEntries(request) => ("AppendEntries", request.term, true, false),
        Message::AppendEntriesResponse(response) => {
            ("AppendEntriesResponse", response.term, true, true)
        }
        Message::InstallSnapshot(request) => ("InstallSnapshot", request.term, true, false),
        Message::InstallSnapshotChunk(request) => {
            ("InstallSnapshotChunk", request.term, true, false)
        }
        Message::InstallSnapshotResponse(response) => {
            ("InstallSnapshotResponse", response.term, true, true)
        }
        Message::TimeoutNow(request) => ("TimeoutNow", request.term, false, false),
        Message::RequestVote(request) => ("RequestVote", request.term, true, false),
        Message::RequestVoteResponse(response) => {
            ("RequestVoteResponse", response.term, true, true)
        }
        Message::PreVote(_) | Message::PreVoteResponse(_) => return None,
    };
    Some(AuthorityMessage {
        kind,
        term,
        must_fence_higher_term,
        is_response,
    })
}
