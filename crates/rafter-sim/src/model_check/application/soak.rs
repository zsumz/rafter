use super::super::super::{
    scheduling::{Operation, SoakOperation},
    Action, ExplorationState, Failure,
};

pub(super) fn apply_soak_action_inner(
    state: &mut ExplorationState,
    operation: SoakOperation,
    trace: &[Action],
) -> Result<(), Failure> {
    let operation = match classify_operation(operation) {
        Ok(operation) => {
            super::operation::apply_to_state_inner(state, operation);
            return Ok(());
        }
        Err(operation) => operation,
    };

    let before = state.cluster.transition_observation_snapshot();
    match operation {
        SoakOperation::DelayAt(position, ticks) => {
            let ready_at = state.cluster.clock.now().after(ticks);
            if let Some(queued) = state.cluster.0.network.get_mut(position) {
                queued.ready_at = std::cmp::max(queued.ready_at, ready_at);
            }
        }
        SoakOperation::DropAt(position) => {
            let _ = state.cluster.0.network.remove(position);
        }
        SoakOperation::DuplicateAt(position) => {
            if let Some(queued) = state.cluster.network.get(position).cloned() {
                state.cluster.0.network.push_back(queued);
            }
        }
        SoakOperation::Restart(node_id) => {
            super::restart::restart_node_inner(state, node_id, trace)?;
            super::observe_restart_transition(state, &before);
            return Ok(());
        }
        SoakOperation::ApplicationLossRestart(node_id) => {
            super::restart_node_losing_application_state_inner(state, node_id, trace)?;
            super::observe_restart_transition(state, &before);
            return Ok(());
        }
        SoakOperation::Partition { a, b } => {
            let _ = state.cluster.0.partition_between(a, b);
            state.partitions_issued += 1;
        }
        SoakOperation::Heal => state.cluster.0.heal_partitions(),
        SoakOperation::LossyRestart(node_id) => {
            state.cluster.0.restart_node_lossy(node_id);
            state.lossy_restarts_issued += 1;
        }
        SoakOperation::Tick(_)
        | SoakOperation::Propose { .. }
        | SoakOperation::DeliverReadyAt(_)
        | SoakOperation::ReadIndex { .. }
        | SoakOperation::AddLearner { .. }
        | SoakOperation::RemoveLearner { .. }
        | SoakOperation::PromoteLearner { .. }
        | SoakOperation::RemoveVoter { .. }
        | SoakOperation::EnterJoint { .. }
        | SoakOperation::LeaveJoint { .. }
        | SoakOperation::Transfer { .. } => unreachable!("model operations return above"),
    }
    state.observe_election_authority();
    state.record_election_observation(&before, None, &[]);
    state.refresh_commit_floors();
    state.refresh_client_history();
    state.refresh_log_history();
    state.refresh_committed_prefixes();
    state.record_leader_completeness_observation();
    state.observe_state_coverage();
    Ok(())
}

fn classify_operation(operation: SoakOperation) -> Result<Operation, SoakOperation> {
    match operation {
        SoakOperation::Tick(node_id) => Ok(Operation::Tick(node_id)),
        SoakOperation::Propose {
            to,
            proposal_id,
            stale_leader,
        } => Ok(Operation::Propose {
            to,
            proposal_id,
            stale_leader,
        }),
        SoakOperation::DeliverReadyAt(position) => Ok(Operation::DeliverReadyAt(position)),
        SoakOperation::ReadIndex { to, request_id } => Ok(Operation::ReadIndex { to, request_id }),
        SoakOperation::AddLearner { to, learner_id } => {
            Ok(Operation::AddLearner { to, learner_id })
        }
        SoakOperation::RemoveLearner { to, learner_id } => {
            Ok(Operation::RemoveLearner { to, learner_id })
        }
        SoakOperation::PromoteLearner {
            to,
            learner_id,
            promotion_barrier,
        } => Ok(Operation::PromoteLearner {
            to,
            learner_id,
            promotion_barrier,
        }),
        SoakOperation::RemoveVoter { to, voter_id } => Ok(Operation::RemoveVoter { to, voter_id }),
        SoakOperation::EnterJoint {
            to,
            target,
            promotion_barriers,
        } => Ok(Operation::EnterJoint {
            to,
            target,
            promotion_barriers,
        }),
        SoakOperation::LeaveJoint { to } => Ok(Operation::LeaveJoint { to }),
        SoakOperation::Transfer { from, target } => Ok(Operation::Transfer { from, target }),
        operation @ (SoakOperation::DelayAt(_, _)
        | SoakOperation::DropAt(_)
        | SoakOperation::DuplicateAt(_)
        | SoakOperation::Restart(_)
        | SoakOperation::ApplicationLossRestart(_)
        | SoakOperation::Partition { .. }
        | SoakOperation::Heal
        | SoakOperation::LossyRestart(_)) => Err(operation),
    }
}
