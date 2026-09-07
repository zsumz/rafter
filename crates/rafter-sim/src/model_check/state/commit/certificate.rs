//! Commit-certificate observation for each advancing commit index.
//!
//! Every commit advance is bound to the membership actually in force at that
//! index and to the exact set of nodes storing the committed entry, so a
//! certificate can only be recorded when an effective quorum really held it.

use std::collections::{BTreeMap, BTreeSet};

use rafter::{LogEntry, LogIndex, MembershipConfig, NodeId, Role, Term};

use crate::Cluster;

use super::super::super::catalog;
use super::super::super::observations::{Observation, ObservationSet};
use super::super::logical_log::{LogicalLogHistory, LogicalLogView};
use super::{
    CommitCertificate, CommitHistory, CommitHistoryViolation, CommitTransitionContext,
    ConfigurationAppend,
};

impl CommitHistory {
    pub(in crate::model_check::state) fn record_commit_transitions(
        &mut self,
        before: &BTreeMap<NodeId, CommitTransitionContext>,
        cluster: &Cluster,
        configuration_append: Option<ConfigurationAppend>,
        follower_commit_authority: Option<(NodeId, Term)>,
        _logical_logs: &LogicalLogHistory,
    ) -> ObservationSet {
        let mut observations = ObservationSet::default();
        self.record_follower_commit_terms(before, cluster, follower_commit_authority);
        for context in before.values() {
            let Some(node) = cluster.nodes.get(&context.node_id) else {
                continue;
            };
            let (authority_term, authority_membership) = if context.role == Role::Leader {
                (context.term, context.effective_membership.clone())
            } else if node.role() == Role::Leader {
                (node.current_term(), node.effective_membership())
            } else {
                continue;
            };
            let new_commit = node.commit_index();
            if new_commit <= context.old_commit {
                continue;
            }
            if node.current_term() != authority_term {
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
            self.record_commit_terms(context.old_commit, new_commit, authority_term);
            let candidate_is_current_term = candidate_term == authority_term;
            let covers_prior_term_prefix = candidate_is_current_term
                && (context.old_commit.0.saturating_add(1)..new_commit.0).any(|index| {
                    view.term_at(LogIndex(index))
                        .is_some_and(|term| term < authority_term)
                });
            if !candidate_is_current_term {
                self.violations.insert(CommitHistoryViolation {
                    invariant: catalog::CM_03_LEADERS_ONLY_COMMIT_CURRENT_TERM_ENTRIES,
                    message: format!(
                        "{} advanced commit to {new_commit} for term {candidate_term} while leading term {}",
                        context.node_id, authority_term
                    ),
                });
            }

            let used_post_append_membership = configuration_append.is_some_and(|append| {
                append.proposer == context.node_id && new_commit >= append.index
            });
            let membership = if used_post_append_membership {
                node.effective_membership()
            } else {
                authority_membership
            };
            let stored_by = nodes_storing_entry(
                cluster,
                new_commit,
                candidate_term,
                candidate_entry.as_ref(),
            );
            let has_effective_quorum = membership.has_quorum(stored_by.iter().copied());
            if has_effective_quorum {
                mark_commit_certificate_observations(
                    &mut observations,
                    &membership,
                    used_post_append_membership,
                    candidate_is_current_term,
                    covers_prior_term_prefix,
                );
            } else {
                self.violations.insert(CommitHistoryViolation {
                    invariant: catalog::CM_02_COMMIT_REQUIRES_EFFECTIVE_QUORUM,
                    message: format!(
                        "{} committed {new_commit} without an effective quorum; stored_by={stored_by:?}, membership={membership:?}",
                        context.node_id
                    ),
                });
            }

            self.certificates.insert(
                (context.node_id, authority_term, new_commit),
                CommitCertificate {
                    leader_id: context.node_id,
                    leader_term: authority_term,
                    committed_through: new_commit,
                    candidate_term,
                    membership,
                    stored_by,
                },
            );
        }
        observations
    }

    fn record_follower_commit_terms(
        &mut self,
        before: &BTreeMap<NodeId, CommitTransitionContext>,
        cluster: &Cluster,
        follower_commit_authority: Option<(NodeId, Term)>,
    ) {
        let Some((node_id, authority_term)) = follower_commit_authority else {
            return;
        };
        let (Some(context), Some(node)) = (before.get(&node_id), cluster.nodes.get(&node_id))
        else {
            return;
        };
        if node.commit_index() > context.old_commit {
            self.record_commit_terms(context.old_commit, node.commit_index(), authority_term);
        }
    }
}

fn mark_commit_certificate_observations(
    observations: &mut ObservationSet,
    membership: &MembershipConfig,
    used_post_append_membership: bool,
    candidate_is_current_term: bool,
    covers_prior_term_prefix: bool,
) {
    match membership {
        MembershipConfig::Stable(_) => observations.mark(Observation::StableCommitCertificates),
        MembershipConfig::Joint(_) => {
            observations.mark(if used_post_append_membership {
                Observation::PostAppendJointCommitCertificates
            } else {
                Observation::PreTransitionJointCommitCertificates
            });
        }
    }
    if candidate_is_current_term {
        observations.mark(Observation::CurrentTermCommitCertificates);
        if covers_prior_term_prefix {
            observations.mark(Observation::CurrentTermCommitCoveringPriorTermPrefix);
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
