//! Vote-grant and leader-transition observation.
//!
//! A grant is witnessed only when the voter actually emitted the matching
//! response, and a new leader's certificate captures the exact granting set
//! and the logical prefix that node held at election time.

use rafter::{MembershipConfig, Message, NodeId, RequestVoteResponse, Term};

use crate::{Cluster, Envelope};

use super::super::super::observations::Observation;
use super::super::ExplorationState;
use super::{last_log_term_from_bootstrap, ElectionCertificate, VoteGrantObservation};

impl ExplorationState {
    pub(in crate::model_check) fn record_election_observation(
        &mut self,
        before: &Cluster,
        delivered: Option<&Envelope>,
        emitted: &[Envelope],
    ) {
        self.election_history.transition_contexts_observed += 1;
        if let Some(envelope) = delivered {
            self.record_pre_vote_transition(before, envelope);
            self.record_authority_transition(before, envelope);
            self.record_request_vote_response_grant(envelope);
            self.record_request_vote_grant(before, envelope, emitted);
        }
        self.record_leader_transitions(before);
    }

    fn record_request_vote_response_grant(&mut self, envelope: &Envelope) {
        let Message::RequestVoteResponse(response) = &envelope.message else {
            return;
        };
        if !response.vote_granted || response.voter_id != envelope.from {
            return;
        }
        self.election_history
            .grants_by_candidate
            .entry((response.term, envelope.to))
            .or_default()
            .insert(envelope.from);
    }

    fn record_request_vote_grant(
        &mut self,
        before: &Cluster,
        envelope: &Envelope,
        emitted: &[Envelope],
    ) {
        let Message::RequestVote(request) = &envelope.message else {
            return;
        };
        let Some(response) =
            vote_response_to_candidate(emitted, envelope.to, request.candidate_id, request.term)
        else {
            return;
        };
        let voter_bootstrap = before.bootstrap_state(envelope.to);
        let voter_last_log_term = last_log_term_from_bootstrap(&voter_bootstrap);
        let voter_membership = before.effective_membership(envelope.to);
        if !voter_membership.contains_voter(request.candidate_id) {
            self.mark_observation(Observation::NonvoterVoteDecisions);
        }
        let candidate_log_is_stale = request.last_log_term < voter_last_log_term
            || (request.last_log_term == voter_last_log_term
                && request.last_log_index < before.last_log_index(envelope.to));
        if candidate_log_is_stale {
            self.mark_observation(Observation::StaleLogVoteDecisions);
        }
        if !response.vote_granted {
            return;
        }
        let Some(response) = granted_vote_response_to_candidate(
            emitted,
            envelope.to,
            request.candidate_id,
            request.term,
        ) else {
            return;
        };
        if response.voter_id != envelope.to {
            return;
        }
        let durable_vote = self
            .cluster
            .nodes
            .get(&envelope.to)
            .and_then(|node| (node.current_term() == response.term).then(|| node.voted_for()))
            .flatten();
        self.election_history
            .vote_grants
            .push(VoteGrantObservation {
                voter_id: envelope.to,
                candidate_id: request.candidate_id,
                term: response.term,
                candidate_last_log_index: request.last_log_index,
                candidate_last_log_term: request.last_log_term,
                voter_last_log_index: before.last_log_index(envelope.to),
                voter_last_log_term,
                voter_membership,
                durable_vote,
            });
    }

    fn record_leader_transitions(&mut self, before: &Cluster) {
        for (node_id, before_node) in &before.nodes {
            let Some(after_node) = self.cluster.nodes.get(node_id) else {
                continue;
            };
            if before_node.role() == rafter::Role::Leader
                || after_node.role() != rafter::Role::Leader
            {
                continue;
            }

            let term = after_node.current_term();
            let mut granted_by = self
                .election_history
                .grants_by_candidate
                .get(&(term, *node_id))
                .cloned()
                .unwrap_or_default();
            if self
                .election_history
                .votes_by_node_term
                .get(&(*node_id, term))
                == Some(node_id)
            {
                granted_by.insert(*node_id);
            }

            let certificate = ElectionCertificate {
                leader_id: *node_id,
                term,
                membership: before_node.effective_membership(),
                granted_by,
                last_log_index: before_node.last_log_index(),
                last_log_term: last_log_term_from_bootstrap(&before.bootstrap_state(*node_id)),
                logical_prefix_at_election: self.logical_log_history.prefix_from_view(
                    &self.logical_log_history.observed_view(before, *node_id),
                    before_node.last_log_index(),
                ),
            };
            let leader_is_eligible = certificate.membership.contains_voter(*node_id);
            let stable_membership = matches!(&certificate.membership, MembershipConfig::Stable(_));
            self.election_history.record_election(certificate);
            self.mark_observation(Observation::ElectionCertificates);
            if leader_is_eligible {
                self.mark_observation(Observation::EligibleLeaderCertificates);
            }
            self.mark_observation(if stable_membership {
                Observation::StableElectionCertificates
            } else {
                Observation::JointElectionCertificates
            });
        }
    }
}

fn vote_response_to_candidate(
    emitted: &[Envelope],
    voter_id: NodeId,
    candidate_id: NodeId,
    term: Term,
) -> Option<&RequestVoteResponse> {
    emitted.iter().find_map(|envelope| {
        if envelope.from != voter_id || envelope.to != candidate_id {
            return None;
        }
        let Message::RequestVoteResponse(response) = &envelope.message else {
            return None;
        };
        (response.term == term).then_some(response)
    })
}

fn granted_vote_response_to_candidate(
    emitted: &[Envelope],
    voter_id: NodeId,
    candidate_id: NodeId,
    term: Term,
) -> Option<&RequestVoteResponse> {
    vote_response_to_candidate(emitted, voter_id, candidate_id, term)
        .filter(|response| response.vote_granted)
}
