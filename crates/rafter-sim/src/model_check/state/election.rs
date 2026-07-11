use std::collections::{btree_map::Entry, BTreeMap, BTreeSet};

use rafter::{
    BootstrapState, LogIndex, MembershipConfig, Message, NodeId, RequestVoteResponse, Term,
};

use crate::{Cluster, Envelope};

use super::ExplorationState;

#[derive(Clone, Debug, Default, Hash)]
pub(crate) struct ElectionHistory {
    pub(crate) term_floor_by_node: BTreeMap<NodeId, Term>,
    pub(crate) votes_by_node_term: BTreeMap<(NodeId, Term), NodeId>,
    pub(crate) term_regressions: BTreeSet<TermRegression>,
    pub(crate) vote_conflicts: BTreeSet<VoteConflict>,
    pub(crate) vote_losses: BTreeSet<VoteLoss>,
    pub(crate) vote_grants: Vec<VoteGrantObservation>,
    pub(crate) authority_transition_violations: Vec<AuthorityTransitionViolation>,
    pub(crate) grants_by_candidate: BTreeMap<(Term, NodeId), BTreeSet<NodeId>>,
    pub(crate) elected_by_term: BTreeMap<Term, ElectionCertificate>,
    pub(crate) conflicting_elections: BTreeSet<ElectionConflict>,
}

impl ElectionHistory {
    pub(in crate::model_check) fn observe_authority_state(&mut self, cluster: &Cluster) {
        for (node_id, node) in &cluster.nodes {
            let observed_term = node.current_term();
            match self.term_floor_by_node.entry(*node_id) {
                Entry::Vacant(entry) => {
                    entry.insert(observed_term);
                }
                Entry::Occupied(mut entry) => {
                    let floor = *entry.get();
                    if observed_term < floor {
                        self.term_regressions.insert(TermRegression {
                            node_id: *node_id,
                            previous_floor: floor,
                            observed: observed_term,
                        });
                    } else if observed_term > floor {
                        entry.insert(observed_term);
                    }
                }
            }

            let vote_key = (*node_id, observed_term);
            if let Some(voted_for) = node.voted_for() {
                match self.votes_by_node_term.entry(vote_key) {
                    Entry::Vacant(entry) => {
                        entry.insert(voted_for);
                    }
                    Entry::Occupied(entry) if *entry.get() != voted_for => {
                        self.vote_conflicts.insert(VoteConflict {
                            node_id: *node_id,
                            term: observed_term,
                            first_vote: *entry.get(),
                            second_vote: voted_for,
                        });
                    }
                    Entry::Occupied(_) => {}
                }
            } else if let Some(previous_vote) = self.votes_by_node_term.get(&vote_key) {
                self.vote_losses.insert(VoteLoss {
                    node_id: *node_id,
                    term: observed_term,
                    previous_vote: *previous_vote,
                });
            }
        }
    }

    pub(in crate::model_check) fn record_election(&mut self, certificate: ElectionCertificate) {
        if let Some(previous) = self.elected_by_term.get(&certificate.term) {
            if previous.leader_id != certificate.leader_id {
                self.conflicting_elections.insert(ElectionConflict {
                    term: certificate.term,
                    first_leader: previous.leader_id,
                    second_leader: certificate.leader_id,
                });
            }
            return;
        }
        self.elected_by_term.insert(certificate.term, certificate);
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct TermRegression {
    pub(crate) node_id: NodeId,
    pub(crate) previous_floor: Term,
    pub(crate) observed: Term,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct VoteConflict {
    pub(crate) node_id: NodeId,
    pub(crate) term: Term,
    pub(crate) first_vote: NodeId,
    pub(crate) second_vote: NodeId,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct VoteLoss {
    pub(crate) node_id: NodeId,
    pub(crate) term: Term,
    pub(crate) previous_vote: NodeId,
}

#[derive(Clone, Debug, Hash)]
pub(crate) struct VoteGrantObservation {
    pub(crate) voter_id: NodeId,
    pub(crate) candidate_id: NodeId,
    pub(crate) term: Term,
    pub(crate) candidate_last_log_index: LogIndex,
    pub(crate) candidate_last_log_term: Term,
    pub(crate) voter_last_log_index: LogIndex,
    pub(crate) voter_last_log_term: Term,
    pub(crate) voter_membership: MembershipConfig,
    pub(crate) durable_vote: Option<NodeId>,
}

#[derive(Clone, Debug, Hash)]
pub(crate) struct AuthorityTransitionViolation {
    pub(crate) node_id: NodeId,
    pub(crate) message_kind: &'static str,
    pub(crate) message_term: Term,
    pub(crate) before_term: Term,
    pub(crate) after_term: Term,
    pub(crate) before_role: rafter::Role,
    pub(crate) after_role: rafter::Role,
    pub(crate) reason: AuthorityTransitionViolationKind,
}

#[derive(Clone, Copy, Debug, Hash)]
pub(crate) enum AuthorityTransitionViolationKind {
    HigherTermNotFenced,
    StaleTermCreatedLeader,
}

#[derive(Clone, Debug, Hash)]
pub(crate) struct ElectionCertificate {
    pub(crate) leader_id: NodeId,
    pub(crate) term: Term,
    pub(crate) membership: MembershipConfig,
    pub(crate) granted_by: BTreeSet<NodeId>,
    pub(crate) last_log_index: LogIndex,
    pub(crate) last_log_term: Term,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct ElectionConflict {
    pub(crate) term: Term,
    pub(crate) first_leader: NodeId,
    pub(crate) second_leader: NodeId,
}

impl ExplorationState {
    pub(in crate::model_check) fn observe_election_authority(&mut self) {
        self.election_history.observe_authority_state(&self.cluster);
    }

    pub(in crate::model_check) fn record_election_observation(
        &mut self,
        before: &Cluster,
        delivered: Option<&Envelope>,
        emitted: &[Envelope],
    ) {
        if let Some(envelope) = delivered {
            self.record_authority_transition(before, envelope);
            self.record_request_vote_response_grant(envelope);
            self.record_request_vote_grant(before, envelope, emitted);
        }
        self.record_leader_transitions(before);
    }

    fn record_authority_transition(&mut self, before: &Cluster, envelope: &Envelope) {
        let Some(message_authority) = authority_message(&envelope.message) else {
            return;
        };
        let Some(before_node) = before.nodes.get(&envelope.to) else {
            return;
        };
        let Some(after_node) = self.cluster.nodes.get(&envelope.to) else {
            return;
        };

        let higher_term_not_fenced = message_authority.must_fence_higher_term
            && message_authority.term > before_node.current_term()
            && (after_node.current_term() < message_authority.term
                || (before_node.role() == rafter::Role::Leader
                    && after_node.role() == rafter::Role::Leader));
        if higher_term_not_fenced {
            self.election_history.authority_transition_violations.push(
                AuthorityTransitionViolation {
                    node_id: envelope.to,
                    message_kind: message_authority.kind,
                    message_term: message_authority.term,
                    before_term: before_node.current_term(),
                    after_term: after_node.current_term(),
                    before_role: before_node.role(),
                    after_role: after_node.role(),
                    reason: AuthorityTransitionViolationKind::HigherTermNotFenced,
                },
            );
        }

        let stale_term_created_leader = message_authority.term < before_node.current_term()
            && before_node.role() != rafter::Role::Leader
            && after_node.role() == rafter::Role::Leader;
        if stale_term_created_leader {
            self.election_history.authority_transition_violations.push(
                AuthorityTransitionViolation {
                    node_id: envelope.to,
                    message_kind: message_authority.kind,
                    message_term: message_authority.term,
                    before_term: before_node.current_term(),
                    after_term: after_node.current_term(),
                    before_role: before_node.role(),
                    after_role: after_node.role(),
                    reason: AuthorityTransitionViolationKind::StaleTermCreatedLeader,
                },
            );
        }
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
        let voter_bootstrap = before.bootstrap_state(envelope.to);
        let voter_last_log_term = last_log_term_from_bootstrap(&voter_bootstrap);
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
                voter_membership: before.effective_membership(envelope.to),
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
            };
            self.election_history.record_election(certificate);
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct AuthorityMessage {
    kind: &'static str,
    term: Term,
    must_fence_higher_term: bool,
}

fn authority_message(message: &Message) -> Option<AuthorityMessage> {
    let (kind, term, must_fence_higher_term) = match message {
        Message::AppendEntries(request) => ("AppendEntries", request.term, true),
        Message::AppendEntriesResponse(response) => ("AppendEntriesResponse", response.term, true),
        Message::InstallSnapshot(request) => ("InstallSnapshot", request.term, true),
        Message::InstallSnapshotChunk(request) => ("InstallSnapshotChunk", request.term, true),
        Message::InstallSnapshotResponse(response) => {
            ("InstallSnapshotResponse", response.term, true)
        }
        Message::TimeoutNow(request) => ("TimeoutNow", request.term, false),
        Message::RequestVote(request) => ("RequestVote", request.term, true),
        Message::RequestVoteResponse(response) => ("RequestVoteResponse", response.term, true),
        Message::PreVote(_) | Message::PreVoteResponse(_) => return None,
    };
    Some(AuthorityMessage {
        kind,
        term,
        must_fence_higher_term,
    })
}

fn granted_vote_response_to_candidate(
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
        (response.vote_granted && response.term == term).then_some(response)
    })
}

fn last_log_term_from_bootstrap(bootstrap: &BootstrapState) -> Term {
    bootstrap.log.last().map_or_else(
        || {
            bootstrap
                .snapshot
                .as_ref()
                .map_or(Term::default(), |snapshot| {
                    snapshot.metadata.last_included_term
                })
        },
        |entry| entry.term,
    )
}
