use std::collections::{BTreeMap, BTreeSet};

use rafter::{LogEntry, LogIndex, MembershipConfig, NodeId, Term};

use crate::Cluster;

use super::super::catalog;
use super::election::ElectionHistory;
use super::logical_log::{LogPrefixWitness, LogicalLogHistory, LogicalLogView};
use super::ExplorationState;

#[derive(Clone, Debug, Default, Hash)]
pub(crate) struct CommitHistory {
    pub(crate) certificates: BTreeMap<(NodeId, Term, LogIndex), CommitCertificate>,
    pub(crate) committed_prefixes: BTreeMap<LogIndex, LogPrefixWitness>,
    pub(crate) leader_completeness_checked: BTreeSet<(NodeId, Term)>,
    pub(crate) violations: BTreeSet<CommitHistoryViolation>,
}

impl CommitHistory {
    pub(super) fn record_commit_transitions(
        &mut self,
        before_commit: &BTreeMap<NodeId, LogIndex>,
        cluster: &Cluster,
        _logical_logs: &LogicalLogHistory,
    ) {
        for (node_id, node) in &cluster.nodes {
            if node.role() != rafter::Role::Leader {
                continue;
            }
            let old_commit = before_commit.get(node_id).copied().unwrap_or_default();
            let new_commit = node.commit_index();
            if new_commit <= old_commit {
                continue;
            }

            let view = LogicalLogView::from_cluster(cluster, *node_id);
            let Some(candidate_term) = view.term_at(new_commit) else {
                self.violations.insert(CommitHistoryViolation {
                    invariant: catalog::CM_02_COMMIT_REQUIRES_EFFECTIVE_QUORUM,
                    message: format!(
                        "{node_id} committed through {new_commit} without a local candidate term"
                    ),
                });
                continue;
            };
            let candidate_entry = view.entry_at(new_commit).cloned();

            if candidate_term != node.current_term() {
                self.violations.insert(CommitHistoryViolation {
                    invariant: catalog::CM_03_LEADERS_ONLY_COMMIT_CURRENT_TERM_ENTRIES,
                    message: format!(
                        "{node_id} advanced commit to {new_commit} for term {candidate_term} while leading term {}",
                        node.current_term()
                    ),
                });
            }

            let membership = node.membership_at_index(new_commit);
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
                        "{node_id} committed {new_commit} without an effective quorum; stored_by={stored_by:?}, membership={membership:?}"
                    ),
                });
            }

            self.certificates.insert(
                (*node_id, node.current_term(), new_commit),
                CommitCertificate {
                    leader_id: *node_id,
                    leader_term: node.current_term(),
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
            if self.leader_completeness_checked.contains(&key) {
                continue;
            }
            let leader_view = LogicalLogView::from_cluster(cluster, certificate.leader_id);
            for prefix in self.committed_prefixes.values() {
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
            self.leader_completeness_checked.insert(key);
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
    pub(in crate::model_check) fn commit_observation_floor(&self) -> BTreeMap<NodeId, LogIndex> {
        self.cluster
            .nodes
            .iter()
            .map(|(node_id, node)| (*node_id, node.commit_index()))
            .collect()
    }

    pub(in crate::model_check) fn record_commit_observation(
        &mut self,
        before_commit: &BTreeMap<NodeId, LogIndex>,
    ) {
        self.commit_history.record_commit_transitions(
            before_commit,
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
