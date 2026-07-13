use std::collections::{BTreeMap, BTreeSet};

use rafter::{LogEntryKind, LogIndex};

use super::super::observations::{Observation, ObservationSet};
use crate::network::ExecutionInstrumentationError;
use crate::{Cluster, ExecutionWitness};

/// Model-check-owned immutable copy of the simulator's execution ledger.
///
/// Keeping this history outside live node/log state makes AP-02 evidence
/// survive compaction, process restart, and explicit application epoch changes.
#[derive(Clone, Debug, Default, Hash)]
pub(super) struct ApplicationHistory {
    witnesses: Vec<ExecutionWitness>,
    instrumentation_errors: BTreeSet<ExecutionInstrumentationError>,
}

impl ApplicationHistory {
    pub(super) fn from_cluster(cluster: &Cluster) -> Self {
        Self {
            witnesses: cluster.execution_history().to_vec(),
            instrumentation_errors: cluster
                .execution_instrumentation_errors()
                .into_iter()
                .collect(),
        }
    }

    pub(super) fn observe_cluster(&mut self, cluster: &Cluster) -> ObservationSet {
        let actual = cluster.execution_history();
        assert!(
            actual.starts_with(&self.witnesses),
            "simulator execution history must be append-only"
        );
        self.witnesses
            .extend_from_slice(&actual[self.witnesses.len()..]);
        self.instrumentation_errors
            .extend(cluster.execution_instrumentation_errors());
        self.coverage()
    }

    pub(super) fn witnesses(&self) -> &[ExecutionWitness] {
        &self.witnesses
    }

    pub(super) const fn instrumentation_errors(&self) -> &BTreeSet<ExecutionInstrumentationError> {
        &self.instrumentation_errors
    }

    fn coverage(&self) -> ObservationSet {
        let mut observations = ObservationSet::default();
        let mut applications = BTreeMap::<LogIndex, BTreeSet<_>>::new();
        let mut configurations = BTreeMap::<LogIndex, BTreeSet<_>>::new();

        for witness in &self.witnesses {
            let class = match witness.entry.kind {
                LogEntryKind::Application(_) => Some(&mut applications),
                LogEntryKind::Configuration(_) => Some(&mut configurations),
                LogEntryKind::Noop => None,
            };
            if let Some(class) = class {
                class
                    .entry(witness.entry.index)
                    .or_default()
                    .insert(witness.node_id);
            }
        }

        if applications.values().any(|nodes| nodes.len() >= 2) {
            observations.mark(Observation::SameIndexApplicationWitnessPairs);
            observations.mark(Observation::SameIndexApplicationResultPairs);
        }
        if configurations.values().any(|nodes| nodes.len() >= 2) {
            observations.mark(Observation::SameIndexConfigurationWitnessPairs);
            observations.mark(Observation::SameIndexConfigurationResultPairs);
        }
        observations
    }
}

#[cfg(test)]
mod tests {
    use rafter::{
        ConfigurationEntry, ConfigurationId, LogEntryKind, MembershipConfig, MembershipSet, NodeId,
        Term,
    };

    use super::{ApplicationHistory, Observation};
    use crate::{ExecutedLogEntry, ExecutionWitness, ReferenceState};

    #[test]
    fn same_node_replay_does_not_qualify_cross_replica_coverage() {
        let mut history = ApplicationHistory::default();
        for epoch in [0, 1] {
            history.witnesses.push(witness(
                1,
                epoch,
                1,
                LogEntryKind::Application(b"command".to_vec().into()),
            ));
            history
                .witnesses
                .push(witness(1, epoch, 2, configuration_kind()));
        }

        let coverage = history.coverage();
        assert!(!coverage.contains(Observation::SameIndexApplicationWitnessPairs));
        assert!(!coverage.contains(Observation::SameIndexConfigurationWitnessPairs));
        assert!(!coverage.contains(Observation::SameIndexApplicationResultPairs));
        assert!(!coverage.contains(Observation::SameIndexConfigurationResultPairs));
    }

    #[test]
    fn distinct_nodes_qualify_each_execution_witness_class() {
        let mut history = ApplicationHistory::default();
        for node_id in [1, 2] {
            history.witnesses.push(witness(
                node_id,
                0,
                1,
                LogEntryKind::Application(b"command".to_vec().into()),
            ));
            history
                .witnesses
                .push(witness(node_id, 0, 2, configuration_kind()));
        }

        let coverage = history.coverage();
        assert!(coverage.contains(Observation::SameIndexApplicationWitnessPairs));
        assert!(coverage.contains(Observation::SameIndexConfigurationWitnessPairs));
        assert!(coverage.contains(Observation::SameIndexApplicationResultPairs));
        assert!(coverage.contains(Observation::SameIndexConfigurationResultPairs));
    }

    #[test]
    fn application_pairs_do_not_qualify_configuration_results() {
        let mut history = ApplicationHistory::default();
        for node_id in [1, 2] {
            history.witnesses.push(witness(
                node_id,
                0,
                1,
                LogEntryKind::Application(b"command".to_vec().into()),
            ));
        }

        let coverage = history.coverage();
        assert!(coverage.contains(Observation::SameIndexApplicationResultPairs));
        assert!(!coverage.contains(Observation::SameIndexConfigurationResultPairs));
    }

    #[test]
    fn configuration_pairs_do_not_qualify_application_results() {
        let mut history = ApplicationHistory::default();
        for node_id in [1, 2] {
            history
                .witnesses
                .push(witness(node_id, 0, 2, configuration_kind()));
        }

        let coverage = history.coverage();
        assert!(coverage.contains(Observation::SameIndexConfigurationResultPairs));
        assert!(!coverage.contains(Observation::SameIndexApplicationResultPairs));
    }

    fn witness(
        node_id: u64,
        application_epoch: u64,
        index: u64,
        kind: LogEntryKind,
    ) -> ExecutionWitness {
        let state = ReferenceState {
            application_value: Vec::new().into(),
            committed_membership: membership(&[1, 2, 3]),
            committed_configuration: None,
        };
        ExecutionWitness {
            node_id: NodeId(node_id),
            application_epoch,
            commit_index_at_emit: rafter::LogIndex(index),
            entry: ExecutedLogEntry {
                index: rafter::LogIndex(index),
                term: Term(1),
                kind,
            },
            prior_state: state.clone(),
            resulting_state: state,
        }
    }

    fn configuration_kind() -> LogEntryKind {
        LogEntryKind::Configuration(ConfigurationEntry::stable(
            ConfigurationId(7),
            MembershipSet::new(vec![NodeId(1), NodeId(2)], Vec::new())
                .expect("fixture membership is valid"),
        ))
    }

    fn membership(voters: &[u64]) -> MembershipConfig {
        MembershipConfig::stable(
            MembershipSet::new(voters.iter().copied().map(NodeId).collect(), Vec::new())
                .expect("fixture membership is valid"),
        )
    }
}
