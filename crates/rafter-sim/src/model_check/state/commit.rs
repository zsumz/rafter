use std::{
    collections::{BTreeMap, BTreeSet},
    hash::{Hash, Hasher},
};

use rafter::{LogEntry, LogIndex, MembershipConfig, NodeId, Role, Term};

use crate::Cluster;

use super::super::catalog;
use super::election::ElectionHistory;
use super::logical_log::{LogPrefixWitness, LogicalLogHistory, LogicalLogView};
use super::ExplorationState;

#[derive(Clone, Debug, Default)]
pub(crate) struct CommitHistory {
    pub(crate) certificates: BTreeMap<(NodeId, Term, LogIndex), CommitCertificate>,
    pub(crate) committed_prefixes: BTreeMap<LogIndex, LogPrefixWitness>,
    pub(crate) leader_completeness_checked_through: BTreeMap<(NodeId, Term), LogIndex>,
    pub(crate) violations: BTreeSet<CommitHistoryViolation>,
}

impl Hash for CommitHistory {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.committed_prefixes.hash(state);
        self.leader_completeness_checked_through.hash(state);
        self.violations.hash(state);
    }
}

impl CommitHistory {
    pub(super) fn record_commit_transitions(
        &mut self,
        before: &BTreeMap<NodeId, CommitTransitionContext>,
        cluster: &Cluster,
        _logical_logs: &LogicalLogHistory,
    ) {
        for context in before.values() {
            if context.role != Role::Leader {
                continue;
            }
            let Some(node) = cluster.nodes.get(&context.node_id) else {
                continue;
            };
            let new_commit = node.commit_index();
            if new_commit <= context.old_commit {
                continue;
            }
            if node.current_term() != context.term {
                continue;
            }

            let view = LogicalLogView::from_cluster(cluster, context.node_id);
            let Some(candidate_term) = view.term_at(new_commit) else {
                self.violations.insert(CommitHistoryViolation {
                    invariant: catalog::CM_02_COMMIT_REQUIRES_EFFECTIVE_QUORUM,
                    message: format!(
                        "{} committed through {new_commit} without a local candidate term",
                        context.node_id
                    ),
                });
                continue;
            };
            let candidate_entry = view.entry_at(new_commit).cloned();

            if candidate_term != context.term {
                self.violations.insert(CommitHistoryViolation {
                    invariant: catalog::CM_03_LEADERS_ONLY_COMMIT_CURRENT_TERM_ENTRIES,
                    message: format!(
                        "{} advanced commit to {new_commit} for term {candidate_term} while leading term {}",
                        context.node_id, context.term
                    ),
                });
            }

            let membership = context.effective_membership.clone();
            let stored_by = nodes_storing_entry(
                cluster,
                new_commit,
                candidate_term,
                candidate_entry.as_ref(),
            );
            if !membership.has_quorum(stored_by.iter().copied()) {
                self.violations.insert(CommitHistoryViolation {
                    invariant: catalog::CM_02_COMMIT_REQUIRES_EFFECTIVE_QUORUM,
                    message: format!(
                        "{} committed {new_commit} without an effective quorum; stored_by={stored_by:?}, membership={membership:?}",
                        context.node_id
                    ),
                });
            }

            self.certificates.insert(
                (context.node_id, context.term, new_commit),
                CommitCertificate {
                    leader_id: context.node_id,
                    leader_term: context.term,
                    committed_through: new_commit,
                    candidate_term,
                    membership,
                    stored_by,
                },
            );
        }
    }

    pub(super) fn observe_committed_prefixes(
        &mut self,
        cluster: &Cluster,
        logical_logs: &LogicalLogHistory,
    ) {
        for (node_id, node) in &cluster.nodes {
            let view = LogicalLogView::from_cluster(cluster, *node_id);
            for raw_index in 1..=node.commit_index().0 {
                let index = LogIndex(raw_index);
                let Some(prefix) = logical_logs.prefix_from_view(&view, index) else {
                    continue;
                };
                self.insert_committed_prefix(*node_id, prefix);
            }
        }
    }

    fn insert_committed_prefix(&mut self, node_id: NodeId, prefix: LogPrefixWitness) {
        if let Some(previous) = self.committed_prefixes.get(&prefix.through) {
            if previous != &prefix {
                self.violations.insert(CommitHistoryViolation {
                    invariant: catalog::LG_04_COMMITTED_PREFIX_STABILITY,
                    message: format!(
                        "{node_id} observed a committed prefix mismatch at {}",
                        prefix.through
                    ),
                });
            }
            return;
        }
        self.committed_prefixes.insert(prefix.through, prefix);
    }

    pub(super) fn record_leader_completeness(
        &mut self,
        cluster: &Cluster,
        logical_logs: &LogicalLogHistory,
        election_history: &ElectionHistory,
    ) {
        for certificate in election_history.elected_by_term.values() {
            let key = (certificate.leader_id, certificate.term);
            let checked_through = self
                .leader_completeness_checked_through
                .get(&key)
                .copied()
                .unwrap_or_default();
            let mut rechecked_through = checked_through;
            let leader_view = LogicalLogView::from_cluster(cluster, certificate.leader_id);
            for prefix in self.committed_prefixes.values() {
                if prefix.through <= checked_through {
                    continue;
                }
                rechecked_through = rechecked_through.max(prefix.through);
                let Some(committed_term) = prefix.entries.last().map(|entry| entry.term) else {
                    continue;
                };
                if committed_term >= certificate.term {
                    continue;
                }
                if logical_logs.prefix_from_view(&leader_view, prefix.through)
                    == Some(prefix.clone())
                {
                    continue;
                }
                self.violations.insert(CommitHistoryViolation {
                    invariant: catalog::LG_05_LEADER_COMPLETENESS,
                    message: format!(
                        "{} became leader in term {} without committed prefix through {}",
                        certificate.leader_id, certificate.term, prefix.through
                    ),
                });
            }
            self.leader_completeness_checked_through
                .insert(key, rechecked_through);
        }
    }
}

fn nodes_storing_entry(
    cluster: &Cluster,
    index: LogIndex,
    term: Term,
    expected_entry: Option<&LogEntry>,
) -> BTreeSet<NodeId> {
    cluster
        .nodes
        .keys()
        .filter_map(|node_id| {
            let view = LogicalLogView::from_cluster(cluster, *node_id);
            if view.term_at(index) != Some(term) {
                return None;
            }
            if let Some(expected) = expected_entry {
                if view.entry_at(index) != Some(expected) {
                    return None;
                }
            }
            Some(*node_id)
        })
        .collect()
}

#[derive(Clone, Debug, Hash)]
pub(crate) struct CommitTransitionContext {
    pub(crate) node_id: NodeId,
    pub(crate) term: Term,
    pub(crate) role: Role,
    pub(crate) effective_membership: MembershipConfig,
    pub(crate) old_commit: LogIndex,
}

#[derive(Clone, Debug, Hash)]
pub(crate) struct CommitCertificate {
    pub(crate) leader_id: NodeId,
    pub(crate) leader_term: Term,
    pub(crate) committed_through: LogIndex,
    pub(crate) candidate_term: Term,
    pub(crate) membership: MembershipConfig,
    pub(crate) stored_by: BTreeSet<NodeId>,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct CommitHistoryViolation {
    pub(crate) invariant: &'static str,
    pub(crate) message: String,
}

impl ExplorationState {
    pub(in crate::model_check) fn commit_transition_context(
        &self,
    ) -> BTreeMap<NodeId, CommitTransitionContext> {
        self.cluster
            .nodes
            .iter()
            .map(|(node_id, node)| {
                (
                    *node_id,
                    CommitTransitionContext {
                        node_id: *node_id,
                        term: node.current_term(),
                        role: node.role(),
                        effective_membership: node.effective_membership(),
                        old_commit: node.commit_index(),
                    },
                )
            })
            .collect()
    }

    pub(in crate::model_check) fn record_commit_observation(
        &mut self,
        before: &BTreeMap<NodeId, CommitTransitionContext>,
    ) {
        self.commit_history.record_commit_transitions(
            before,
            &self.cluster,
            &self.logical_log_history,
        );
        self.refresh_committed_prefixes();
    }

    pub(in crate::model_check) fn refresh_committed_prefixes(&mut self) {
        self.commit_history
            .observe_committed_prefixes(&self.cluster, &self.logical_log_history);
    }

    pub(in crate::model_check) fn record_leader_completeness_observation(&mut self) {
        self.commit_history.record_leader_completeness(
            &self.cluster,
            &self.logical_log_history,
            &self.election_history,
        );
    }

    pub(in crate::model_check) fn reset_commit_floor(&mut self, node_id: NodeId) {
        if let Some(node) = self.cluster.nodes.get(&node_id) {
            self.commit_floor_by_node
                .insert(node_id, node.commit_index());
            self.committed_configuration_floor_by_node
                .insert(node_id, node.committed_configuration_state());
        }
    }
}
