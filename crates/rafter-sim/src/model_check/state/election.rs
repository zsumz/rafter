use std::collections::{BTreeMap, BTreeSet};

use rafter::{BootstrapState, LogIndex, MembershipConfig, Message, NodeId, Term};

use crate::{Cluster, Envelope};

use super::ExplorationState;

#[derive(Clone, Debug, Default, Hash)]
pub(crate) struct ElectionHistory {
    pub(crate) grants_by_candidate: BTreeMap<(Term, NodeId), BTreeSet<NodeId>>,
    pub(crate) elected_by_term: BTreeMap<Term, ElectionCertificate>,
    pub(crate) conflicting_elections: BTreeSet<ElectionConflict>,
}

impl ElectionHistory {
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
    pub(in crate::model_check) fn record_election_observation(
        &mut self,
        before: &Cluster,
        delivered: Option<&Envelope>,
    ) {
        if let Some(envelope) = delivered {
            self.record_request_vote_grant(envelope);
        }
        self.record_leader_transitions(before);
    }

    fn record_request_vote_grant(&mut self, envelope: &Envelope) {
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
            if after_node.voted_for() == Some(*node_id) {
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
