use super::super::super::{
    scheduling::{Operation, SoakOperation},
    ExplorationState,
};
use super::{apply_to_state, restart_node};

pub(super) fn apply_soak_action_inner(state: &mut ExplorationState, operation: SoakOperation) {
    match operation {
        SoakOperation::Tick(node_id) => apply_to_state(state, Operation::Tick(node_id)),
        SoakOperation::Propose {
            to,
            proposal_id,
            stale_leader,
        } => apply_to_state(
            state,
            Operation::Propose {
                to,
                proposal_id,
                stale_leader,
            },
        ),
        SoakOperation::DeliverReadyAt(position) => {
            apply_to_state(state, Operation::DeliverReadyAt(position));
        }
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
            restart_node(state, node_id, &[])
                .expect("soak restart from captured durable state must be valid");
        }
        SoakOperation::ReadIndex { to, request_id } => {
            apply_to_state(state, Operation::ReadIndex { to, request_id });
        }
        SoakOperation::AddLearner { to, learner_id } => {
            apply_to_state(state, Operation::AddLearner { to, learner_id });
        }
        SoakOperation::RemoveLearner { to, learner_id } => {
            apply_to_state(state, Operation::RemoveLearner { to, learner_id });
        }
        SoakOperation::PromoteLearner {
            to,
            learner_id,
            promotion_barrier,
        } => {
            apply_to_state(
                state,
                Operation::PromoteLearner {
                    to,
                    learner_id,
                    promotion_barrier,
                },
            );
        }
        SoakOperation::RemoveVoter { to, voter_id } => {
            apply_to_state(state, Operation::RemoveVoter { to, voter_id });
        }
        SoakOperation::EnterJoint {
            to,
            target,
            promotion_barriers,
        } => {
            apply_to_state(
                state,
                Operation::EnterJoint {
                    to,
                    target,
                    promotion_barriers,
                },
            );
        }
        SoakOperation::LeaveJoint { to } => {
            apply_to_state(state, Operation::LeaveJoint { to });
        }
        SoakOperation::Transfer { from, target } => {
            apply_to_state(state, Operation::Transfer { from, target });
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
    }
    state.observe_election_authority();
    state.refresh_commit_floors();
    state.refresh_client_history();
    state.refresh_log_history();
    state.refresh_committed_prefixes();
    state.record_leader_completeness_observation();
}
