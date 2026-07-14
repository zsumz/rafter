use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    hash::{Hash, Hasher},
};

use rafter::{LogEntryKind, LogIndex, NodeId};

use super::super::observations::{Observation, ObservationSet};
use crate::network::ExecutionInstrumentationError;
use crate::{Cluster, ExecutionWitness};

/// Incremental verifier index over the simulator's immutable execution ledger.
///
/// The cluster remains the sole owner of exact witnesses. This index keeps a
/// cursor, append-only ledger revision, and one canonical identity per logical
/// index so AP-02 agreement checks only process appended witnesses.
#[derive(Clone, Debug, Default)]
pub(super) struct ApplicationHistory {
    cursor: usize,
    ledger_rewrite_revision: u64,
    canonical_by_index: BTreeMap<LogIndex, ExecutionWitness>,
    application_nodes_by_index: BTreeMap<LogIndex, BTreeSet<NodeId>>,
    configuration_nodes_by_index: BTreeMap<LogIndex, BTreeSet<NodeId>>,
    epochs_by_node_index: BTreeMap<(NodeId, LogIndex), BTreeSet<u64>>,
    violations: BTreeSet<ExecutionHistoryViolation>,
    instrumentation_errors: BTreeSet<ExecutionHistoryInstrumentationError>,
    coverage: ObservationSet,
}

impl Hash for ApplicationHistory {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.cursor.hash(state);
        self.ledger_rewrite_revision.hash(state);
        self.canonical_by_index.hash(state);
        self.application_nodes_by_index.hash(state);
        self.configuration_nodes_by_index.hash(state);
        self.epochs_by_node_index.hash(state);
        self.violations.hash(state);
        self.instrumentation_errors.hash(state);
        self.coverage.hash(state);
    }
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
        if cluster.execution_history_rewrite_revision() != self.ledger_rewrite_revision {
            self.instrumentation_errors.insert(
                ExecutionHistoryInstrumentationError::LedgerPrefixChanged {
                    observed: self.cursor,
                },
            );
            return self.coverage;
        }
        for witness in &actual[self.cursor..] {
            self.record_witness(witness);
            self.cursor = self.cursor.saturating_add(1);
        }
        self.ledger_rewrite_revision = cluster.execution_history_rewrite_revision();
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
        if let LogEntryKind::Application(expected) = &witness.entry.kind {
            if witness.emitted_application_payload.as_ref() != Some(expected) {
                self.violations.insert(ExecutionHistoryViolation {
                    kind: ExecutionHistoryViolationKind::ApplyOutputMismatch,
                    message: format!(
                        "{} epoch {} emitted {:?} for application log index {} with payload {:?}",
                        witness.node_id,
                        witness.application_epoch,
                        witness.emitted_application_payload,
                        witness.entry.index,
                        expected
                    ),
                });
            }
        } else if witness.emitted_application_payload.is_some() {
            self.violations.insert(ExecutionHistoryViolation {
                kind: ExecutionHistoryViolationKind::ApplyOutputMismatch,
                message: format!(
                    "{} epoch {} attached an application output to non-application log index {}",
                    witness.node_id, witness.application_epoch, witness.entry.index
                ),
            });
        }
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

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::model_check) struct ExecutionHistoryViolation {
    kind: ExecutionHistoryViolationKind,
    pub(in crate::model_check) message: String,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum ExecutionHistoryViolationKind {
    ApplyOutputMismatch,
    InvalidReferenceResult,
    EntryIdentityMismatch,
    StateIdentityMismatch,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::model_check) enum ExecutionHistoryInstrumentationError {
    Simulator(ExecutionInstrumentationError),
    LedgerTruncated { observed: usize, actual: usize },
    LedgerPrefixChanged { observed: usize },
}

impl fmt::Display for ExecutionHistoryInstrumentationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Simulator(error) => error.fmt(formatter),
            Self::LedgerTruncated { observed, actual } => write!(
                formatter,
                "immutable execution ledger shrank from {observed} to {actual} witnesses"
            ),
            Self::LedgerPrefixChanged { observed } => write!(
                formatter,
                "immutable execution ledger changed within its {observed}-witness consumed prefix"
            ),
        }
    }
}

fn independently_derive_reference_result(witness: &ExecutionWitness) -> crate::ReferenceState {
    let mut result = witness.prior_state.clone();
    match &witness.entry.kind {
        LogEntryKind::Application(_) => {
            if let Some(payload) = &witness.emitted_application_payload {
                result.application_value.clone_from(payload);
            }
        }
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
mod tests;
