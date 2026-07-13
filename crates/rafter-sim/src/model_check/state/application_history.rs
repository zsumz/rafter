use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    hash::{DefaultHasher, Hash, Hasher},
};

use rafter::{LogEntryKind, LogIndex, NodeId};

use super::super::observations::{Observation, ObservationSet};
use crate::network::ExecutionInstrumentationError;
use crate::{Cluster, ExecutionWitness};

/// Incremental verifier index over the simulator's immutable execution ledger.
///
/// The cluster remains the sole owner of exact witnesses. This index keeps a
/// cursor, rolling digest, and one canonical identity per logical index so AP-02
/// checks do not clone or rescan the full ledger after every transition.
#[derive(Clone, Debug, Default, Hash)]
pub(super) struct ApplicationHistory {
    cursor: usize,
    digest: ExecutionLedgerDigest,
    canonical_by_index: BTreeMap<LogIndex, ExecutionWitness>,
    application_nodes_by_index: BTreeMap<LogIndex, BTreeSet<NodeId>>,
    configuration_nodes_by_index: BTreeMap<LogIndex, BTreeSet<NodeId>>,
    epochs_by_node_index: BTreeMap<(NodeId, LogIndex), BTreeSet<u64>>,
    violations: BTreeSet<ExecutionHistoryViolation>,
    instrumentation_errors: BTreeSet<ExecutionHistoryInstrumentationError>,
    coverage: ObservationSet,
}

impl ApplicationHistory {
    pub(super) fn from_cluster(cluster: &Cluster) -> Self {
        let mut history = Self::default();
        let _ = history.observe_cluster(cluster);
        history
    }

    pub(super) fn observe_cluster(&mut self, cluster: &Cluster) -> ObservationSet {
        let actual = cluster.execution_history();
        if actual.len() < self.cursor {
            self.instrumentation_errors.insert(
                ExecutionHistoryInstrumentationError::LedgerTruncated {
                    observed: self.cursor,
                    actual: actual.len(),
                },
            );
            return self.coverage;
        }
        for witness in &actual[self.cursor..] {
            self.record_witness(witness);
        }
        self.cursor = actual.len();
        self.instrumentation_errors.extend(
            cluster
                .execution_instrumentation_errors()
                .into_iter()
                .map(ExecutionHistoryInstrumentationError::Simulator),
        );
        self.coverage
    }

    pub(super) const fn violations(&self) -> &BTreeSet<ExecutionHistoryViolation> {
        &self.violations
    }

    pub(super) const fn instrumentation_errors(
        &self,
    ) -> &BTreeSet<ExecutionHistoryInstrumentationError> {
        &self.instrumentation_errors
    }

    fn record_witness(&mut self, witness: &ExecutionWitness) {
        self.digest.push(witness);
        let expected = independently_derive_reference_result(witness);
        if witness.resulting_state != expected {
            self.violations.insert(ExecutionHistoryViolation {
                kind: ExecutionHistoryViolationKind::InvalidReferenceResult,
                message: format!(
                    "{} epoch {} recorded an invalid reference-state result at log index {}",
                    witness.node_id, witness.application_epoch, witness.entry.index
                ),
            });
        }

        if let Some(previous) = self.canonical_by_index.get(&witness.entry.index) {
            if previous.entry.term != witness.entry.term
                || previous.entry.kind != witness.entry.kind
            {
                self.violations.insert(ExecutionHistoryViolation {
                    kind: ExecutionHistoryViolationKind::EntryIdentityMismatch,
                    message: format!(
                        "{} and {} applied different term/kind/input identities at log index {}",
                        previous.node_id, witness.node_id, witness.entry.index
                    ),
                });
            } else if previous.prior_state != witness.prior_state
                || previous.resulting_state != witness.resulting_state
            {
                self.violations.insert(ExecutionHistoryViolation {
                    kind: ExecutionHistoryViolationKind::StateIdentityMismatch,
                    message: format!(
                        "{} and {} obtained different prior/result state identities at log index {}",
                        previous.node_id, witness.node_id, witness.entry.index
                    ),
                });
            }
        } else {
            self.canonical_by_index
                .insert(witness.entry.index, witness.clone());
        }

        let nodes = match witness.entry.kind {
            LogEntryKind::Application(_) => Some((
                &mut self.application_nodes_by_index,
                Observation::SameIndexApplicationWitnessPairs,
                Observation::SameIndexApplicationResultPairs,
            )),
            LogEntryKind::Configuration(_) => Some((
                &mut self.configuration_nodes_by_index,
                Observation::SameIndexConfigurationWitnessPairs,
                Observation::SameIndexConfigurationResultPairs,
            )),
            LogEntryKind::Noop => None,
        };
        if let Some((nodes_by_index, witness_observation, result_observation)) = nodes {
            let nodes = nodes_by_index.entry(witness.entry.index).or_default();
            nodes.insert(witness.node_id);
            if nodes.len() >= 2 {
                self.coverage.mark(witness_observation);
                self.coverage.mark(result_observation);
            }

            let epochs = self
                .epochs_by_node_index
                .entry((witness.node_id, witness.entry.index))
                .or_default();
            epochs.insert(witness.application_epoch);
            if epochs.len() >= 2 {
                self.coverage
                    .mark(Observation::CrossEpochExecutionWitnessPairs);
            }
        }
    }

    #[cfg(test)]
    fn record_fixture(&mut self, witness: &ExecutionWitness) {
        self.record_witness(witness);
        self.cursor += 1;
    }

    #[cfg(test)]
    const fn coverage(&self) -> ObservationSet {
        self.coverage
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
struct ExecutionLedgerDigest {
    lane_a: u64,
    lane_b: u64,
}

impl ExecutionLedgerDigest {
    fn push(&mut self, witness: &ExecutionWitness) {
        let mut hasher = DefaultHasher::new();
        witness.hash(&mut hasher);
        let item = hasher.finish();
        self.lane_a = self.lane_a.rotate_left(7) ^ item.wrapping_mul(0x9e37_79b1_85eb_ca87);
        self.lane_b = self
            .lane_b
            .rotate_right(11)
            .wrapping_add(item ^ self.lane_a);
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::model_check) struct ExecutionHistoryViolation {
    kind: ExecutionHistoryViolationKind,
    pub(in crate::model_check) message: String,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum ExecutionHistoryViolationKind {
    InvalidReferenceResult,
    EntryIdentityMismatch,
    StateIdentityMismatch,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::model_check) enum ExecutionHistoryInstrumentationError {
    Simulator(ExecutionInstrumentationError),
    LedgerTruncated { observed: usize, actual: usize },
}

impl fmt::Display for ExecutionHistoryInstrumentationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Simulator(error) => error.fmt(formatter),
            Self::LedgerTruncated { observed, actual } => write!(
                formatter,
                "immutable execution ledger shrank from {observed} to {actual} witnesses"
            ),
        }
    }
}

fn independently_derive_reference_result(witness: &ExecutionWitness) -> crate::ReferenceState {
    let mut result = witness.prior_state.clone();
    match &witness.entry.kind {
        LogEntryKind::Application(payload) => result.application_value.clone_from(payload),
        LogEntryKind::Configuration(configuration) => {
            result.committed_membership = configuration.membership_config();
            result.committed_configuration = Some(rafter::CommittedConfiguration {
                index: witness.entry.index,
                config_id: configuration.config_id(),
            });
        }
        LogEntryKind::Noop => {}
    }
    result
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
            history.record_fixture(&witness(
                1,
                epoch,
                1,
                LogEntryKind::Application(b"command".to_vec().into()),
            ));
            history.record_fixture(&witness(1, epoch, 2, configuration_kind()));
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
            history.record_fixture(&witness(
                node_id,
                0,
                1,
                LogEntryKind::Application(b"command".to_vec().into()),
            ));
            history.record_fixture(&witness(node_id, 0, 2, configuration_kind()));
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
            history.record_fixture(&witness(
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
            history.record_fixture(&witness(node_id, 0, 2, configuration_kind()));
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
