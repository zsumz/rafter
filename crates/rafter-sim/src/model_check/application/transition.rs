//! Transition shapes and the observation each one owes.
//!
//! Naming a transition is how a caller declares which observers must run, so
//! the engine can never apply a state change whose evidence was not gathered
//! from the matching pre-transition snapshot.

use rafter::{BootstrapValidationError, NodeId};

use crate::Cluster;

use super::super::super::scheduling::SoakOperation;
use super::super::super::{scheduling::Operation, Action, ExplorationState, Failure};
#[cfg(test)]
use super::ExecutionRecorderCorruption;
use super::{PendingApplicationReplaySeed, SnapshotBootstrapSeed};

pub(super) enum Transition<'a> {
    Operation(Operation),
    Restart {
        node_id: NodeId,
        trace: &'a [Action],
    },
    ApplicationLossRestart {
        node_id: NodeId,
        trace: &'a [Action],
    },
    Soak {
        operation: SoakOperation,
        trace: &'a [Action],
    },
    SnapshotBootstrapSeeds(Vec<SnapshotBootstrapSeed>),
    PendingApplicationReplaySeed(Box<PendingApplicationReplaySeed>),
    #[cfg(test)]
    ExecutionRecorderCorruption {
        source: Box<crate::ExecutionWitness>,
        corruption: ExecutionRecorderCorruption,
    },
    #[cfg(test)]
    ExecutionCursorRewind(NodeId),
    SchedulerIndex(usize),
}

pub(super) enum TransitionError {
    Invariant(Failure),
    Bootstrap(BootstrapValidationError),
}

pub(super) enum TransitionOutcome {
    Applied,
    SchedulerIndex(usize),
}

pub(super) fn observe_seeded_transition(state: &mut ExplorationState) {
    state.election_history.record_seeded_leaders(&state.cluster);
    state.observe_election_authority();
    state.refresh_log_history();
    state.refresh_seeded_commit_history();
    state.refresh_committed_prefixes();
    state.refresh_commit_floors();
    state.refresh_client_history();
    state.record_leader_completeness_observation();
    state.observe_state_coverage();
}

pub(super) fn observe_restart_transition(
    state: &mut ExplorationState,
    before: &Cluster,
    node_id: NodeId,
) {
    state.restarts_issued += 1;
    state.record_purpose_restart(before, node_id);
    state.observe_election_authority();
    state.record_election_observation(before, None, &[]);
    state.refresh_log_history();
    state.refresh_committed_prefixes();
    state.refresh_commit_floors();
    state.refresh_client_history();
    state.record_leader_completeness_observation();
    state.observe_state_coverage();
}

pub(super) fn restart_node_losing_application_state_inner(
    state: &mut ExplorationState,
    node_id: NodeId,
    trace: &[Action],
) -> Result<(), Failure> {
    let before_epoch = state.cluster.application_epoch(node_id);
    let bootstrap = state.cluster.bootstrap_state(node_id);
    state
        .cluster
        .0
        .restart_node_from_bootstrap_losing_application_state(node_id, bootstrap)
        .map_err(|error| Failure {
                kind: super::super::super::FailureKind::HarnessError,
                invariant: super::super::super::catalog::AP_02_STATE_MACHINE_SAFETY,
                message: format!(
                    "{node_id} failed application-state-loss restart from captured bootstrap: {error:?}"
                ),
                trace: trace.to_vec(),
                state: super::super::super::helpers::summarize(state.cluster()),
        })?;
    let after_epoch = state.cluster.application_epoch(node_id);
    if after_epoch <= before_epoch {
        return Err(Failure {
            kind: super::super::super::FailureKind::HarnessError,
            invariant: super::super::super::catalog::AP_02_STATE_MACHINE_SAFETY,
            message: format!(
                "{node_id} application-state-loss restart did not advance epoch {before_epoch}"
            ),
            trace: trace.to_vec(),
            state: super::super::super::helpers::summarize(state.cluster()),
        });
    }
    Ok(())
}
