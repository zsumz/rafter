//! Public transition entry points used by explorers, soaks, and replay.
//!
//! Each entry names one transition shape, hands it to the engine, and maps the
//! engine's outcome back to that shape's contract; an outcome the shape cannot
//! produce is a harness bug rather than a recoverable error.

use rafter::{BootstrapValidationError, NodeId};

use super::super::super::scheduling::SoakOperation;
use super::super::super::{
    scheduling::Operation, Action, ExplorationState, Failure, RestartSnapshotState,
};
use super::apply_transition;
use super::transition::{Transition, TransitionError, TransitionOutcome};
#[cfg(test)]
use super::ExecutionRecorderCorruption;
use super::{operation, PendingApplicationReplaySeed, SnapshotBootstrapSeed};

pub(in crate::model_check) fn apply_to_state(state: &mut ExplorationState, operation: Operation) {
    match apply_transition(state, Transition::Operation(operation)) {
        Ok(TransitionOutcome::Applied) => {}
        Ok(TransitionOutcome::SchedulerIndex(_)) => {
            unreachable!("ordinary model operations return an applied outcome")
        }
        Err(TransitionError::Invariant(_) | TransitionError::Bootstrap(_)) => {
            unreachable!("ordinary model operations are infallible")
        }
    }
}

pub(in crate::model_check) fn restart_node(
    state: &mut ExplorationState,
    node_id: NodeId,
    trace: &[Action],
) -> Result<(), Failure> {
    match apply_transition(state, Transition::Restart { node_id, trace }) {
        Ok(TransitionOutcome::Applied) => Ok(()),
        Ok(TransitionOutcome::SchedulerIndex(_)) => {
            unreachable!("restart transitions return an applied outcome")
        }
        Err(TransitionError::Invariant(failure)) => Err(failure),
        Err(TransitionError::Bootstrap(_)) => {
            unreachable!("restart transitions do not return bootstrap errors")
        }
    }
}

pub(in crate::model_check) fn restart_node_losing_application_state(
    state: &mut ExplorationState,
    node_id: NodeId,
    trace: &[Action],
) -> Result<(), Failure> {
    match apply_transition(state, Transition::ApplicationLossRestart { node_id, trace }) {
        Ok(TransitionOutcome::Applied) => Ok(()),
        Ok(TransitionOutcome::SchedulerIndex(_)) => {
            unreachable!("application-loss restart transitions return an applied outcome")
        }
        Err(TransitionError::Invariant(failure)) => Err(failure),
        Err(TransitionError::Bootstrap(_)) => {
            unreachable!("application-loss restart transitions do not return seed errors")
        }
    }
}

pub(in crate::model_check) fn apply_scheduled_operation(
    state: &mut ExplorationState,
    operation: Operation,
    trace: &[Action],
) -> Result<(), Failure> {
    match operation {
        Operation::Restart(node_id) => restart_node(state, node_id, trace),
        Operation::ApplicationLossRestart(node_id) => {
            restart_node_losing_application_state(state, node_id, trace)
        }
        operation => {
            apply_to_state(state, operation);
            Ok(())
        }
    }
}

pub(in crate::model_check) fn try_apply_soak_action(
    state: &mut ExplorationState,
    operation: SoakOperation,
) -> Result<(), Failure> {
    match apply_transition(
        state,
        Transition::Soak {
            operation,
            trace: &[],
        },
    ) {
        Ok(TransitionOutcome::Applied) => Ok(()),
        Ok(TransitionOutcome::SchedulerIndex(_)) => {
            unreachable!("soak transitions return an applied outcome")
        }
        Err(TransitionError::Invariant(failure)) => Err(failure),
        Err(TransitionError::Bootstrap(_)) => {
            unreachable!("soak transitions do not return seed errors")
        }
    }
}

pub(in crate::model_check) fn apply_soak_action(
    state: &mut ExplorationState,
    operation: SoakOperation,
) {
    if let Err(failure) = try_apply_soak_action(state, operation) {
        state.record_transition_instrumentation_error(&failure);
    }
}

pub(in crate::model_check) fn apply_snapshot_bootstrap_seeds(
    state: &mut ExplorationState,
    seeds: Vec<SnapshotBootstrapSeed>,
) -> Result<(), BootstrapValidationError> {
    match apply_transition(state, Transition::SnapshotBootstrapSeeds(seeds)) {
        Ok(TransitionOutcome::Applied) => Ok(()),
        Ok(TransitionOutcome::SchedulerIndex(_)) => {
            unreachable!("snapshot bootstrap seeding returns an applied outcome")
        }
        Err(TransitionError::Bootstrap(error)) => Err(error),
        Err(TransitionError::Invariant(_)) => {
            unreachable!("snapshot bootstrap seeding does not run invariant checks")
        }
    }
}

pub(in crate::model_check) fn apply_pending_application_replay_seed(
    state: &mut ExplorationState,
    seed: PendingApplicationReplaySeed,
) -> Result<(), BootstrapValidationError> {
    match apply_transition(
        state,
        Transition::PendingApplicationReplaySeed(Box::new(seed)),
    ) {
        Ok(TransitionOutcome::Applied) => Ok(()),
        Ok(TransitionOutcome::SchedulerIndex(_)) => {
            unreachable!("pending-replay seeding returns an applied outcome")
        }
        Err(TransitionError::Bootstrap(error)) => Err(error),
        Err(TransitionError::Invariant(_)) => {
            unreachable!("pending-replay seeding does not run invariant checks")
        }
    }
}

#[cfg(test)]
pub(in crate::model_check) fn record_execution_corruption(
    state: &mut ExplorationState,
    corruption: ExecutionRecorderCorruption,
) -> Result<(), &'static str> {
    let source = state
        .cluster
        .execution_history()
        .last()
        .cloned()
        .ok_or("execution recorder corruption requires a real source witness")?;
    match apply_transition(
        state,
        Transition::ExecutionRecorderCorruption {
            source: Box::new(source),
            corruption,
        },
    ) {
        Ok(TransitionOutcome::Applied) => Ok(()),
        Ok(TransitionOutcome::SchedulerIndex(_)) => {
            Err("execution recorder corruption returned a scheduler outcome")
        }
        Err(TransitionError::Invariant(_) | TransitionError::Bootstrap(_)) => {
            Err("execution recorder corruption returned an unrelated transition error")
        }
    }
}

#[cfg(test)]
pub(in crate::model_check) fn rewind_execution_cursor_for_fixture(
    state: &mut ExplorationState,
    node_id: NodeId,
) {
    match apply_transition(state, Transition::ExecutionCursorRewind(node_id)) {
        Ok(TransitionOutcome::Applied) => {}
        Ok(TransitionOutcome::SchedulerIndex(_)) => {
            unreachable!("execution cursor rewind returned a scheduler outcome")
        }
        Err(TransitionError::Invariant(_) | TransitionError::Bootstrap(_)) => {
            unreachable!("execution cursor rewind returned an unrelated transition error")
        }
    }
}

pub(in crate::model_check) fn scheduler_index(state: &mut ExplorationState, len: usize) -> usize {
    match apply_transition(state, Transition::SchedulerIndex(len)) {
        Ok(TransitionOutcome::SchedulerIndex(index)) => index,
        Ok(TransitionOutcome::Applied)
        | Err(TransitionError::Invariant(_) | TransitionError::Bootstrap(_)) => {
            unreachable!("scheduler choice returns an index")
        }
    }
}

pub(in crate::model_check) fn apply_to_restart_snapshot_state(
    state: &mut RestartSnapshotState,
    operation: Operation,
    trace: &[Action],
) -> Result<(), Failure> {
    operation::apply_to_restart_snapshot_state(state, operation, trace)
}
