#[path = "application/cluster.rs"]
mod cluster;
#[path = "application/entry.rs"]
mod entry;
#[path = "application/instrumented.rs"]
mod instrumented;
#[path = "application/operation.rs"]
mod operation;
#[path = "application/restart.rs"]
mod restart;
#[path = "application/soak.rs"]
mod soak;
#[path = "application/transition.rs"]
mod transition;

use std::ops::Deref;

#[cfg(test)]
use rafter::LogEntryKind;

use crate::Cluster;

use super::super::ExplorationState;
use transition::{
    observe_restart_transition, observe_seeded_transition,
    restart_node_losing_application_state_inner, Transition, TransitionError, TransitionOutcome,
};

pub(in crate::model_check) use entry::{
    apply_pending_application_replay_seed, apply_scheduled_operation,
    apply_snapshot_bootstrap_seeds, apply_soak_action, apply_to_restart_snapshot_state,
    apply_to_state, restart_node, restart_node_losing_application_state, scheduler_index,
    try_apply_soak_action,
};
#[cfg(test)]
pub(in crate::model_check) use entry::{
    record_execution_corruption, rewind_execution_cursor_for_fixture,
};

/// Read-only cluster capability held by model-check states.
///
/// The wrapped `Cluster` is deliberately private to this module and its
/// transition-engine children. Model-check drivers can inspect protocol state
/// through `Deref`, but cannot invoke a raw mutating `Cluster` operation and
/// accidentally bypass invariant observation.
#[derive(Clone, Debug, Hash)]
pub(super) struct InstrumentedCluster(Cluster);

pub(in crate::model_check) struct SnapshotBootstrapSeed {
    pub(in crate::model_check) node_id: rafter::NodeId,
    pub(in crate::model_check) snapshot: rafter::RaftSnapshot,
    pub(in crate::model_check) payload: Vec<u8>,
    pub(in crate::model_check) bootstrap: rafter::BootstrapState,
}

pub(in crate::model_check) struct PendingApplicationReplaySeed {
    pub(in crate::model_check) node_id: rafter::NodeId,
    pub(in crate::model_check) bootstrap: rafter::BootstrapState,
}

#[cfg(test)]
pub(in crate::model_check) enum ExecutionRecorderCorruption {
    EntryKind(LogEntryKind),
    ResultingState(crate::ReferenceState),
}

fn apply_transition(
    state: &mut ExplorationState,
    transition: Transition<'_>,
) -> Result<TransitionOutcome, TransitionError> {
    let outcome = match transition {
        Transition::Operation(operation) => {
            operation::apply_to_state_inner(state, &operation);
            TransitionOutcome::Applied
        }
        Transition::Restart { node_id, trace } => {
            let before = state.cluster.transition_observation_snapshot();
            restart::restart_node_inner(state, node_id, trace)
                .map_err(TransitionError::Invariant)?;
            observe_restart_transition(state, &before, node_id);
            TransitionOutcome::Applied
        }
        Transition::ApplicationLossRestart { node_id, trace } => {
            let before = state.cluster.transition_observation_snapshot();
            restart_node_losing_application_state_inner(state, node_id, trace)
                .map_err(TransitionError::Invariant)?;
            observe_restart_transition(state, &before, node_id);
            TransitionOutcome::Applied
        }
        Transition::Soak { operation, trace } => {
            soak::apply_soak_action_inner(state, operation, trace)
                .map_err(TransitionError::Invariant)?;
            TransitionOutcome::Applied
        }
        Transition::SnapshotBootstrapSeeds(seeds) => {
            operation::apply_snapshot_bootstrap_seeds_inner(state, seeds)
                .map_err(TransitionError::Bootstrap)?;
            observe_seeded_transition(state);
            TransitionOutcome::Applied
        }
        Transition::PendingApplicationReplaySeed(seed) => {
            let seed = *seed;
            state
                .cluster
                .0
                .seed_pending_application_replay(seed.node_id, seed.bootstrap)
                .map_err(TransitionError::Bootstrap)?;
            observe_seeded_transition(state);
            TransitionOutcome::Applied
        }
        #[cfg(test)]
        Transition::ExecutionRecorderCorruption {
            mut source,
            corruption,
        } => {
            match corruption {
                ExecutionRecorderCorruption::EntryKind(kind) => {
                    source.entry.kind = kind;
                    source.resulting_state = source.prior_state.clone();
                    match &source.entry.kind {
                        LogEntryKind::Application(payload) => {
                            source.resulting_state.application_value.clone_from(payload);
                        }
                        LogEntryKind::Configuration(configuration) => {
                            source.resulting_state.committed_membership =
                                configuration.membership_config();
                            source.resulting_state.committed_configuration =
                                Some(rafter::CommittedConfiguration {
                                    index: source.entry.index,
                                    config_id: configuration.config_id(),
                                });
                        }
                        LogEntryKind::Noop => {}
                    }
                }
                ExecutionRecorderCorruption::ResultingState(result) => {
                    source.resulting_state = result;
                }
            }
            state.cluster.0.execution_history.push(*source);
            TransitionOutcome::Applied
        }
        #[cfg(test)]
        Transition::ExecutionCursorRewind(node_id) => {
            state.cluster.0.rewind_execution_cursor_for_fixture(node_id);
            TransitionOutcome::Applied
        }
        Transition::SchedulerIndex(len) => {
            TransitionOutcome::SchedulerIndex(state.cluster.0.rng.index(len))
        }
    };
    if matches!(outcome, TransitionOutcome::Applied) {
        state.refresh_application_history();
        state.refresh_snapshot_history();
    }
    Ok(outcome)
}

impl Deref for InstrumentedCluster {
    type Target = Cluster;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use rafter::{BootstrapLogEntry, BootstrapState, LogIndex, NodeConfig, NodeId, Term};

    use crate::{Cluster, SimTick};

    use super::{
        apply_pending_application_replay_seed, apply_snapshot_bootstrap_seeds,
        PendingApplicationReplaySeed, SnapshotBootstrapSeed,
    };
    use crate::model_check::{
        helpers::{bootstrap_with_snapshot, test_snapshot},
        invariants::check_election_history,
        state::ExplorationState,
    };

    #[test]
    fn pending_replay_seed_observes_conflicting_durable_vote_immediately() {
        let mut state = new_state();
        apply_pending_application_replay_seed(
            &mut state,
            PendingApplicationReplaySeed {
                node_id: NodeId(1),
                bootstrap: pending_replay_bootstrap(Term(4), NodeId(2)),
            },
        )
        .expect("first pending replay seed is valid");

        apply_pending_application_replay_seed(
            &mut state,
            PendingApplicationReplaySeed {
                node_id: NodeId(1),
                bootstrap: pending_replay_bootstrap(Term(4), NodeId(3)),
            },
        )
        .expect("second pending replay seed is valid");

        let failure = check_election_history(&state, &[])
            .expect_err("the seed transition must record a same-term vote conflict");
        assert_eq!(failure.invariant(), "EL-02 one durable vote per term");
    }

    #[test]
    fn snapshot_seed_observes_term_regression_immediately() {
        let mut state = new_state();
        for term in [Term(4), Term(3)] {
            let (snapshot, payload) = test_snapshot(1, 1, 1, term.0, b"seeded snapshot");
            apply_snapshot_bootstrap_seeds(
                &mut state,
                vec![SnapshotBootstrapSeed {
                    node_id: NodeId(1),
                    snapshot: snapshot.clone(),
                    payload,
                    bootstrap: bootstrap_with_snapshot(term, snapshot, &[]),
                }],
            )
            .expect("snapshot seed is valid");
        }

        let failure = check_election_history(&state, &[])
            .expect_err("the seed transition must record a term regression");
        assert_eq!(failure.invariant(), "EL-01 term monotonicity");
    }

    fn new_state() -> ExplorationState {
        let config = NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3)
            .expect("test config is valid");
        let state = ExplorationState::new(Cluster::new(vec![config]));
        assert_eq!(state.cluster().clock().now(), SimTick(0));
        state
    }

    fn pending_replay_bootstrap(term: Term, voted_for: NodeId) -> BootstrapState {
        BootstrapState {
            current_term: term,
            voted_for: Some(voted_for),
            commit_index: LogIndex(1),
            committed_configuration: None,
            snapshot: None,
            log: vec![BootstrapLogEntry::application(
                LogIndex(1),
                term,
                b"pending replay".to_vec(),
            )],
        }
    }
}
