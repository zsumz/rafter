use std::collections::BTreeSet;

use rafter::LogIndex;

use super::{
    logical_log::LogicalLogHistory, ClientReadOutcome, ClientWriteStatus, ExplorationState,
};
use crate::model_check::observations::Observation;

impl ExplorationState {
    pub(in crate::model_check) fn mark_observation(&mut self, observation: Observation) {
        self.observations.mark(observation);
    }

    pub(in crate::model_check) fn observe_state_coverage(&mut self) {
        self.observe_application_coverage();
        self.observe_membership_coverage();
        self.observe_read_coverage();
        self.observe_snapshot_coverage();
        self.observe_cross_node_log_coverage();
    }

    fn observe_application_coverage(&mut self) {
        if !self.cluster.applied().is_empty() || !self.cluster.snapshot_installs().is_empty() {
            self.mark_observation(Observation::AppliesOrSnapshotBoundaries);
        }

        let mut indexes = BTreeSet::new();
        if self
            .cluster
            .applied()
            .iter()
            .any(|applied| !indexes.insert(applied.index))
        {
            self.mark_observation(Observation::SameIndexApplyPairs);
        }
    }

    fn observe_membership_coverage(&mut self) {
        let exactly_one = self.cluster.nodes.keys().any(|node_id| {
            let bootstrap = self.cluster.bootstrap_state(*node_id);
            bootstrap
                .log
                .iter()
                .filter(|entry| {
                    entry.index > bootstrap.commit_index && entry.kind.is_configuration()
                })
                .count()
                == 1
        });
        if exactly_one {
            self.mark_observation(Observation::StatesWithOneUncommittedConfiguration);
        }
    }

    fn observe_read_coverage(&mut self) {
        if !self.cluster.read_grants().is_empty() {
            self.mark_observation(Observation::RegisteredReadGrants);
        }
        let completed_reads = self
            .client_history
            .reads
            .values()
            .filter_map(|read| match read.outcome {
                ClientReadOutcome::Completed { .. } => Some(read.started_at),
                ClientReadOutcome::Pending | ClientReadOutcome::ProofGranted { .. } => None,
            })
            .collect::<Vec<_>>();
        if completed_reads.is_empty() {
            return;
        }
        self.mark_observation(Observation::CompletedReads);
        let write_completed_before_read = self.client_history.writes.values().any(|write| {
            let ClientWriteStatus::Completed { completed_at, .. } = write.status else {
                return false;
            };
            completed_reads
                .iter()
                .any(|started_at| completed_at < *started_at)
        });
        if write_completed_before_read {
            self.mark_observation(Observation::CompletedWriteBeforeReadHistories);
        }
    }

    fn observe_snapshot_coverage(&mut self) {
        if self
            .cluster
            .nodes
            .values()
            .any(|node| node.snapshot_index() > LogIndex::ZERO)
        {
            self.mark_observation(Observation::NodesWithNonzeroSnapshotIndex);
        }
        if self.cluster.nodes.values().any(|node| {
            node.pending_snapshot_transfer()
                .is_some_and(|pending| pending.received_bytes() > 0 && !pending.is_complete())
        }) {
            self.mark_observation(Observation::PartialSnapshotTransfersChecked);
        }
        let mut boundaries = BTreeSet::new();
        if self
            .cluster
            .snapshot_installs()
            .iter()
            .any(|install| !boundaries.insert(install.last_included_index))
        {
            self.mark_observation(Observation::SameBoundarySnapshotInstallPairs);
        }
    }

    fn observe_cross_node_log_coverage(&mut self) {
        let nodes = self.cluster.nodes.keys().copied().collect::<Vec<_>>();
        let mut compared_prefix = false;
        let mut compared_committed = false;
        for (offset, left_id) in nodes.iter().enumerate() {
            let left = self
                .logical_log_history
                .observed_view(&self.cluster, *left_id);
            for right_id in &nodes[offset + 1..] {
                let right = self
                    .logical_log_history
                    .observed_view(&self.cluster, *right_id);
                for (index, entry) in &left.entries {
                    if right.term_at(*index) != Some(entry.term) {
                        continue;
                    }
                    compared_prefix |= LogicalLogHistory::prefix_from_view(&left, *index)
                        == LogicalLogHistory::prefix_from_view(&right, *index)
                        && LogicalLogHistory::prefix_from_view(&left, *index).is_some();
                    compared_committed |= *index <= self.cluster.commit_index(*left_id)
                        && *index <= self.cluster.commit_index(*right_id);
                }
            }
        }
        if compared_prefix {
            self.mark_observation(Observation::CrossNodeIndexTermPrefixComparisons);
        }
        if compared_committed {
            self.mark_observation(Observation::CrossNodeCommittedIndexComparisons);
        }
    }
}
