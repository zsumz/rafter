//! Committed-prefix agreement and leader-completeness bookkeeping.
//!
//! One canonical committed prefix is kept across nodes; every newly observed
//! prefix must match it through the shared length, and each election
//! certificate is rechecked against the prefix that was committed before it.

use rafter::{LogIndex, NodeId, Term};

use crate::Cluster;

use super::super::super::catalog;
use super::super::super::observations::{Observation, ObservationSet};
use super::super::election::ElectionHistory;
use super::super::logical_log::{LogPrefixWitness, LogicalLogHistory};
use super::{CommitHistory, CommitHistoryViolation};

impl CommitHistory {
    pub(in crate::model_check::state) fn observe_committed_prefixes(
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

    pub(in crate::model_check::state) fn record_seeded_commit_authority(
        &mut self,
        old_commit: LogIndex,
        new_commit: LogIndex,
        term: Term,
    ) {
        if term != Term::default() {
            self.record_commit_terms(old_commit, new_commit, term);
        }
    }

    pub(in crate::model_check::state) fn record_seeded_commit_terms(
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

    pub(super) fn record_commit_terms(
        &mut self,
        old_commit: LogIndex,
        new_commit: LogIndex,
        term: Term,
    ) {
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

    pub(in crate::model_check::state) fn record_leader_completeness(
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
