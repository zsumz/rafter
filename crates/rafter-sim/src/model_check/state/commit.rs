use std::{
    collections::{BTreeMap, BTreeSet},
    hash::{Hash, Hasher},
};

use rafter::{LogEntry, LogIndex, MembershipConfig, NodeId, Role, Term};

use crate::Cluster;

use super::super::catalog;
use super::super::observations::{Observation, ObservationSet};
use super::election::ElectionHistory;
use super::logical_log::{LogPrefixWitness, LogicalLogHistory, LogicalLogView};
use super::ExplorationState;

#[derive(Clone, Debug, Default)]
pub(crate) struct CommitHistory {
    pub(crate) certificates: BTreeMap<(NodeId, Term, LogIndex), CommitCertificate>,
    pub(crate) committed_prefix: Option<LogPrefixWitness>,
    committed_prefix_owner: Option<NodeId>,
    pub(crate) committed_in_terms: Vec<Term>,
    pub(crate) unwitnessed_committed_prefixes: BTreeSet<(NodeId, LogIndex)>,
    pub(crate) unwitnessed_commit_terms: BTreeSet<LogIndex>,
    pub(crate) leader_completeness_checked_through: BTreeMap<(NodeId, Term, usize), LogIndex>,
    pub(crate) violations: BTreeSet<CommitHistoryViolation>,
}

/// One configuration entry appended by the transition currently being
/// observed. Commits below `index` still use the frozen pre-transition
/// membership; commits at or above it use the post-append membership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::model_check) struct ConfigurationAppend {
    pub(in crate::model_check) proposer: NodeId,
    pub(in crate::model_check) index: LogIndex,
}

impl Hash for CommitHistory {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.certificates.hash(state);
        self.committed_prefix.hash(state);
        self.committed_prefix_owner.hash(state);
        self.committed_in_terms.hash(state);
        self.unwitnessed_committed_prefixes.hash(state);
        self.unwitnessed_commit_terms.hash(state);
        self.leader_completeness_checked_through.hash(state);
        self.violations.hash(state);
    }
}

impl CommitHistory {
    pub(super) fn record_commit_transitions(
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

    pub(super) fn observe_committed_prefixes(
        &mut self,
        cluster: &Cluster,
        logical_logs: &LogicalLogHistory,
    ) -> ObservationSet {
        let mut observations = ObservationSet::default();
        for (node_id, node) in &cluster.nodes {
            self.unwitnessed_committed_prefixes
                .retain(|(owner, _)| owner != node_id);
            let committed_through = node.commit_index();
            if committed_through == LogIndex::ZERO {
                continue;
            }
            let view = logical_logs.observed_view(cluster, *node_id);
            let Some(prefix) = logical_logs.prefix_from_view(&view, committed_through) else {
                self.unwitnessed_committed_prefixes
                    .insert((*node_id, committed_through));
                continue;
            };
            observations.union_with(self.insert_committed_prefix(*node_id, prefix));
        }
        observations
    }

    pub(super) fn record_seeded_commit_authority(
        &mut self,
        old_commit: LogIndex,
        new_commit: LogIndex,
        term: Term,
    ) {
        if term != Term::default() {
            self.record_commit_terms(old_commit, new_commit, term);
        }
    }

    pub(super) fn record_seeded_commit_terms(
        &mut self,
        cluster: &Cluster,
        logical_logs: &LogicalLogHistory,
    ) {
        for (node_id, node) in &cluster.nodes {
            let Ok(len) = usize::try_from(node.commit_index().0) else {
                continue;
            };
            let view = logical_logs.observed_view(cluster, *node_id);
            if logical_logs
                .prefix_from_view(&view, node.commit_index())
                .is_some()
                && self.committed_in_terms.len() < len
            {
                self.committed_in_terms.resize(len, Term::default());
            }
        }
        self.refresh_commit_term_coverage();
    }

    fn insert_committed_prefix(
        &mut self,
        node_id: NodeId,
        prefix: LogPrefixWitness,
    ) -> ObservationSet {
        let mut observations = ObservationSet::default();
        let Some(committed) = self.committed_prefix.as_ref() else {
            self.committed_prefix = Some(prefix);
            self.committed_prefix_owner = Some(node_id);
            self.refresh_commit_term_coverage();
            return observations;
        };

        let comparison_through = committed.through().min(prefix.through());
        let committed_slice = committed.slice_through(comparison_through);
        let observed_slice = prefix.slice_through(comparison_through);
        if committed_slice != observed_slice {
            self.violations.insert(CommitHistoryViolation {
                invariant: catalog::LG_04_COMMITTED_PREFIX_STABILITY,
                message: format!(
                    "{node_id} observed a committed prefix mismatch at or before {comparison_through}"
                ),
            });
            return observations;
        }

        observations.mark(Observation::CommittedPrefixHistoryComparisons);
        if self
            .committed_prefix_owner
            .is_some_and(|owner| owner != node_id)
        {
            observations.mark(Observation::CrossNodeCommittedPrefixAgreementChecks);
        }

        if prefix.through() > committed.through() {
            self.committed_prefix = Some(prefix);
            self.committed_prefix_owner = Some(node_id);
        }
        self.refresh_commit_term_coverage();
        observations
    }

    fn record_commit_terms(&mut self, old_commit: LogIndex, new_commit: LogIndex, term: Term) {
        let (Ok(old_len), Ok(new_len)) =
            (usize::try_from(old_commit.0), usize::try_from(new_commit.0))
        else {
            return;
        };
        if self.committed_in_terms.len() < new_len {
            self.committed_in_terms.resize(new_len, Term::default());
        }
        let mut first_newly_authoritative = None;
        for (offset, committed_in_term) in self.committed_in_terms[old_len..new_len]
            .iter_mut()
            .enumerate()
        {
            if term != Term::default()
                && (*committed_in_term == Term::default() || term < *committed_in_term)
            {
                *committed_in_term = term;
                first_newly_authoritative.get_or_insert(LogIndex((old_len + offset + 1) as u64));
            }
        }
        if let Some(first_newly_authoritative) = first_newly_authoritative {
            let recheck_after = LogIndex(first_newly_authoritative.0.saturating_sub(1));
            for ((_, election_term, _), checked_through) in
                &mut self.leader_completeness_checked_through
            {
                if term < *election_term && *checked_through >= first_newly_authoritative {
                    *checked_through = recheck_after;
                }
            }
        }
        self.refresh_commit_term_coverage();
    }

    fn refresh_commit_term_coverage(&mut self) {
        let Some(committed) = self.committed_prefix.as_ref() else {
            return;
        };
        for offset in 0..committed.len() {
            let index = LogIndex(offset as u64 + 1);
            if self
                .committed_in_terms
                .get(offset)
                .is_some_and(|term| *term != Term::default())
            {
                self.unwitnessed_commit_terms.remove(&index);
            } else {
                self.unwitnessed_commit_terms.insert(index);
            }
        }
    }

    pub(super) fn record_leader_completeness(
        &mut self,
        election_history: &ElectionHistory,
    ) -> ObservationSet {
        let mut observations = ObservationSet::default();
        for certificates in election_history.elected_by_term.values() {
            for (certificate_ordinal, certificate) in certificates.iter().enumerate() {
                let key = (certificate.leader_id, certificate.term, certificate_ordinal);
                let checked_through = self
                    .leader_completeness_checked_through
                    .get(&key)
                    .copied()
                    .unwrap_or_default();
                let Some(committed) = self.committed_prefix.as_ref() else {
                    self.leader_completeness_checked_through
                        .insert(key, checked_through);
                    continue;
                };
                if committed.through() <= checked_through {
                    continue;
                }
                let checked_entries = usize::try_from(checked_through.0)
                    .unwrap_or(committed.len())
                    .min(committed.len());
                let relevant_through = (checked_entries..committed.len())
                    .filter(|offset| {
                        self.committed_in_terms
                            .get(*offset)
                            .is_some_and(|term| *term < certificate.term)
                    })
                    .map(|offset| LogIndex(offset as u64 + 1))
                    .next_back();
                if let Some(relevant_through) = relevant_through {
                    observations.mark(Observation::LaterTermLeaderPriorPrefixChecks);
                    let expected = committed.slice_through(relevant_through);
                    let election_prefix = certificate
                        .logical_prefix_at_election
                        .as_ref()
                        .and_then(|prefix| prefix.slice_through(relevant_through));
                    if election_prefix != expected {
                        self.violations.insert(CommitHistoryViolation {
                            invariant: catalog::LG_05_LEADER_COMPLETENESS,
                            message: format!(
                                "{} became leader in term {} without committed prefix through {}",
                                certificate.leader_id, certificate.term, relevant_through
                            ),
                        });
                    }
                }
                self.leader_completeness_checked_through
                    .insert(key, committed.through());
            }
        }
        observations
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
        configuration_append: Option<ConfigurationAppend>,
        follower_commit_authority: Option<(NodeId, Term)>,
    ) {
        let observations = self.commit_history.record_commit_transitions(
            before,
            &self.cluster,
            configuration_append,
            follower_commit_authority,
            &self.logical_log_history,
        );
        self.observations.union_with(observations);
        self.refresh_committed_prefixes();
    }

    pub(in crate::model_check) fn refresh_committed_prefixes(&mut self) {
        let observations = self
            .commit_history
            .observe_committed_prefixes(&self.cluster, &self.logical_log_history);
        self.observations.union_with(observations);
    }

    pub(in crate::model_check) fn refresh_seeded_commit_history(&mut self) {
        self.commit_history
            .record_seeded_commit_terms(&self.cluster, &self.logical_log_history);
        let _ = self
            .commit_history
            .observe_committed_prefixes(&self.cluster, &self.logical_log_history);
    }

    pub(in crate::model_check) fn witness_seeded_commit_authority(
        &mut self,
        old_commit: LogIndex,
        new_commit: LogIndex,
        term: Term,
    ) {
        self.commit_history
            .record_seeded_commit_authority(old_commit, new_commit, term);
    }

    pub(in crate::model_check) fn record_leader_completeness_observation(&mut self) {
        let observations = self
            .commit_history
            .record_leader_completeness(&self.election_history);
        self.observations.union_with(observations);
    }
}
