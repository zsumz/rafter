#[path = "application/cluster.rs"]
mod cluster;
#[path = "application/operation.rs"]
mod operation;
#[path = "application/restart.rs"]
mod restart;
#[path = "application/soak.rs"]
mod soak;

use std::ops::Deref;

use rafter::{BootstrapValidationError, NodeId};

use crate::Cluster;

use super::super::scheduling::SoakOperation;
use super::super::{
    scheduling::Operation, Action, ExplorationState, Failure, RestartSnapshotState,
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

enum Transition<'a> {
    Operation(Operation),
    Restart {
        node_id: NodeId,
        trace: &'a [Action],
    },
    Soak(SoakOperation),
    SnapshotBootstrapSeeds(Vec<SnapshotBootstrapSeed>),
    SchedulerIndex(usize),
    RandomReadyPosition,
}

enum TransitionError {
    Invariant(Failure),
    Bootstrap(BootstrapValidationError),
}

enum TransitionOutcome {
    Applied,
    SchedulerIndex(usize),
    RandomReadyPosition(Option<usize>),
}

fn apply_transition(
    state: &mut ExplorationState,
    transition: Transition<'_>,
) -> Result<TransitionOutcome, TransitionError> {
    let outcome = match transition {
        Transition::Operation(operation) => {
            operation::apply_to_state_inner(state, operation);
            TransitionOutcome::Applied
        }
        Transition::Restart { node_id, trace } => {
            restart::restart_node_inner(state, node_id, trace)
                .map_err(TransitionError::Invariant)?;
            state.restarts_issued += 1;
            state.reset_commit_floor(node_id);
            state.observe_election_authority();
            state.refresh_log_history();
            state.refresh_committed_prefixes();
            state.refresh_commit_floors();
            state.refresh_client_history();
            state.record_leader_completeness_observation();
            state.observe_state_coverage();
            TransitionOutcome::Applied
        }
        Transition::Soak(operation) => {
            soak::apply_soak_action_inner(state, operation);
            TransitionOutcome::Applied
        }
        Transition::SnapshotBootstrapSeeds(seeds) => {
            operation::apply_snapshot_bootstrap_seeds_inner(state, seeds)
                .map_err(TransitionError::Bootstrap)?;
            TransitionOutcome::Applied
        }
        Transition::SchedulerIndex(len) => {
            TransitionOutcome::SchedulerIndex(state.cluster.0.rng.index(len))
        }
        Transition::RandomReadyPosition => {
            TransitionOutcome::RandomReadyPosition(state.cluster.0.random_ready_position())
        }
    };
    Ok(outcome)
}

pub(in crate::model_check) fn apply_to_state(state: &mut ExplorationState, operation: Operation) {
    match apply_transition(state, Transition::Operation(operation)) {
        Ok(TransitionOutcome::Applied) => {}
        Ok(TransitionOutcome::SchedulerIndex(_) | TransitionOutcome::RandomReadyPosition(_)) => {
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
        Ok(TransitionOutcome::SchedulerIndex(_) | TransitionOutcome::RandomReadyPosition(_)) => {
            unreachable!("restart transitions return an applied outcome")
        }
        Err(TransitionError::Invariant(failure)) => Err(failure),
        Err(TransitionError::Bootstrap(_)) => {
            unreachable!("restart transitions do not return bootstrap errors")
        }
    }
}

pub(in crate::model_check) fn apply_soak_action(
    state: &mut ExplorationState,
    operation: SoakOperation,
) {
    match apply_transition(state, Transition::Soak(operation)) {
        Ok(TransitionOutcome::Applied) => {}
        Ok(TransitionOutcome::SchedulerIndex(_) | TransitionOutcome::RandomReadyPosition(_)) => {
            unreachable!("soak transitions return an applied outcome")
        }
        Err(TransitionError::Invariant(_) | TransitionError::Bootstrap(_)) => {
            unreachable!("soak transitions handle restart failures internally")
        }
    }
}

pub(in crate::model_check) fn apply_snapshot_bootstrap_seeds(
    state: &mut ExplorationState,
    seeds: Vec<SnapshotBootstrapSeed>,
) -> Result<(), BootstrapValidationError> {
    match apply_transition(state, Transition::SnapshotBootstrapSeeds(seeds)) {
        Ok(TransitionOutcome::Applied) => Ok(()),
        Ok(TransitionOutcome::SchedulerIndex(_) | TransitionOutcome::RandomReadyPosition(_)) => {
            unreachable!("snapshot bootstrap seeding returns an applied outcome")
        }
        Err(TransitionError::Bootstrap(error)) => Err(error),
        Err(TransitionError::Invariant(_)) => {
            unreachable!("snapshot bootstrap seeding does not run invariant checks")
        }
    }
}

pub(in crate::model_check) fn scheduler_index(state: &mut ExplorationState, len: usize) -> usize {
    match apply_transition(state, Transition::SchedulerIndex(len)) {
        Ok(TransitionOutcome::SchedulerIndex(index)) => index,
        Ok(TransitionOutcome::Applied | TransitionOutcome::RandomReadyPosition(_))
        | Err(TransitionError::Invariant(_) | TransitionError::Bootstrap(_)) => {
            unreachable!("scheduler choice returns an index")
        }
    }
}

pub(in crate::model_check) fn random_ready_position(state: &mut ExplorationState) -> Option<usize> {
    match apply_transition(state, Transition::RandomReadyPosition) {
        Ok(TransitionOutcome::RandomReadyPosition(position)) => position,
        Ok(TransitionOutcome::Applied | TransitionOutcome::SchedulerIndex(_))
        | Err(TransitionError::Invariant(_) | TransitionError::Bootstrap(_)) => {
            unreachable!("ready-message choice returns an optional position")
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

impl InstrumentedCluster {
    pub(super) const fn new(cluster: Cluster) -> Self {
        Self(cluster)
    }

    #[cfg(test)]
    pub(super) fn inject_applied_record(&mut self, applied: crate::Applied) {
        self.0.applied.push(applied);
    }

    #[cfg(test)]
    pub(super) fn inject_read_grant(&mut self, grant: crate::ReadGranted) {
        self.0.read_grants.push(grant);
    }

    #[cfg(test)]
    pub(super) fn inject_blocked_pair(&mut self, from: rafter::NodeId, to: rafter::NodeId) {
        self.0.blocked_pairs.insert((from, to));
    }

    #[cfg(test)]
    pub(super) fn restart_node_from_bootstrap(
        &mut self,
        node_id: rafter::NodeId,
        bootstrap: rafter::BootstrapState,
    ) -> Result<(), rafter::BootstrapValidationError> {
        self.0.restart_node_from_bootstrap(node_id, bootstrap)
    }

    #[cfg(test)]
    pub(super) fn seed_snapshot_payload(
        &mut self,
        node_id: rafter::NodeId,
        snapshot: &rafter::RaftSnapshot,
        payload: Vec<u8>,
    ) {
        self.0.seed_snapshot_payload(node_id, snapshot, payload);
    }

    #[cfg(test)]
    pub(super) fn queue_message(
        &mut self,
        from: rafter::NodeId,
        to: rafter::NodeId,
        message: rafter::Message,
    ) {
        self.0.queue_message(from, to, message);
    }

    #[cfg(test)]
    pub(super) fn drop_matching(
        &mut self,
        predicate: impl FnMut(&crate::Envelope) -> bool,
    ) -> usize {
        self.0.drop_matching(predicate)
    }
}

impl Deref for InstrumentedCluster {
    type Target = Cluster;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
