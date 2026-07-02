use rafter::{MembershipConfig, MembershipSet, Node, NodeId};

use crate::Cluster;

use super::helpers::{proposal_payload, summarize};
use super::{
    Action, ExplorationState, Failure, Operation, RestartSafetyExplorer, RestartSnapshotState,
    SoakOperation,
};

pub(super) fn apply_to_state(state: &mut ExplorationState, operation: Operation) {
    if let Operation::Propose {
        to,
        proposal_id,
        stale_leader,
    } = &operation
    {
        state.record_client_proposal(*to, *proposal_id, *stale_leader);
        state.proposals_issued += 1;
        if *stale_leader {
            state
                .forbidden_applied_payloads
                .insert(proposal_payload(*proposal_id).into());
        }
    }
    if let Operation::ReadIndex { to, request_id } = &operation {
        state.record_client_read(*to, *request_id, state.cluster.committed_floor());
        state.read_indexes_issued += 1;
    }
    if matches!(
        operation,
        Operation::AddLearner { .. }
            | Operation::RemoveLearner { .. }
            | Operation::PromoteLearner { .. }
            | Operation::RemoveVoter { .. }
            | Operation::EnterJoint { .. }
            | Operation::LeaveJoint { .. }
    ) {
        state.membership_changes_issued += 1;
    }
    apply_to_cluster(&mut state.cluster, operation);
    state.refresh_commit_floors();
    state.refresh_client_history();
}

pub(super) fn apply_to_cluster(cluster: &mut Cluster, operation: Operation) {
    match operation {
        Operation::Tick(node_id) => cluster.tick(node_id),
        Operation::Restart(_) => unreachable!("restart operations need invariant context"),
        Operation::Propose {
            to, proposal_id, ..
        } => cluster.propose(to, proposal_payload(proposal_id)),
        Operation::ReadIndex { to, request_id } => cluster.read_index(to, request_id),
        Operation::AddLearner { to, learner_id } => cluster.add_learner(to, learner_id),
        Operation::RemoveLearner { to, learner_id } => {
            if let Some(target) =
                remove_learner_target(cluster.effective_membership(to), learner_id)
            {
                cluster.change_membership(to, target, Vec::new());
            }
        }
        Operation::PromoteLearner {
            to,
            learner_id,
            promotion_barrier,
        } => cluster.promote_learner(to, learner_id, promotion_barrier),
        Operation::RemoveVoter { to, voter_id } => cluster.remove_voter(to, voter_id),
        Operation::EnterJoint {
            to,
            target,
            promotion_barriers,
        } => cluster.enter_joint(to, target, promotion_barriers),
        Operation::LeaveJoint { to } => cluster.leave_joint(to),
        Operation::DeliverReadyAt(position) => {
            if let Some(queued) = cluster.network.remove(position) {
                cluster.deliver(queued.envelope);
            }
        }
    }
}

pub(super) fn apply_to_restart_snapshot_state(
    state: &mut RestartSnapshotState,
    operation: Operation,
    trace: &[Action],
) -> Result<(), Failure> {
    match operation {
        Operation::Restart(node_id) => {
            restart_node(&mut state.state, node_id, trace)?;
            state.state.restarts_issued += 1;
            state.state.reset_commit_floor(node_id);
        }
        operation => {
            apply_to_state(&mut state.state, operation);
        }
    }
    Ok(())
}

pub(super) fn apply_soak_action(state: &mut ExplorationState, operation: SoakOperation) {
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
            if let Some(queued) = state.cluster.network.get_mut(position) {
                queued.ready_at =
                    std::cmp::max(queued.ready_at, state.cluster.clock.now().after(ticks));
            }
        }
        SoakOperation::DropAt(position) => {
            let _ = state.cluster.network.remove(position);
        }
        SoakOperation::DuplicateAt(position) => {
            if let Some(queued) = state.cluster.network.get(position).cloned() {
                state.cluster.network.push_back(queued);
            }
        }
        SoakOperation::Restart(node_id) => {
            let bootstrap = state.cluster.bootstrap_state(node_id);
            state
                .cluster
                .restart_node_from_bootstrap(node_id, bootstrap)
                .expect("soak restart from captured bootstrap state must be valid");
            state.restarts_issued += 1;
            state.reset_commit_floor(node_id);
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
            state.cluster.transfer_leadership(from, target);
            state.transfers_issued += 1;
        }
        SoakOperation::Partition { a, b } => {
            let _ = state.cluster.partition_between(a, b);
            state.partitions_issued += 1;
        }
        SoakOperation::Heal => state.cluster.heal_partitions(),
        SoakOperation::LossyRestart(node_id) => {
            state.cluster.restart_node_lossy(node_id);
            state.lossy_restarts_issued += 1;
            state.reset_commit_floor(node_id);
        }
    }
    state.refresh_commit_floors();
}

fn remove_learner_target(current: MembershipConfig, learner_id: NodeId) -> Option<MembershipSet> {
    let MembershipConfig::Stable(current) = current else {
        return None;
    };
    if !current.learners().contains(&learner_id) {
        return None;
    }
    let learners = current
        .learners()
        .iter()
        .copied()
        .filter(|node_id| *node_id != learner_id)
        .collect();
    MembershipSet::new(current.voters().to_vec(), learners).ok()
}

pub(super) fn restart_node(
    state: &mut ExplorationState,
    node_id: NodeId,
    trace: &[Action],
) -> Result<(), Failure> {
    let before = state.cluster.bootstrap_state(node_id);
    let before_pending = state
        .cluster
        .nodes
        .get(&node_id)
        .and_then(Node::pending_snapshot_transfer);
    let before_staged = state.cluster.snapshot_staging.get(&node_id).cloned();

    state
        .cluster
        .restart_node_from_bootstrap(node_id, before.clone())
        .map_err(|error| Failure {
            invariant: RestartSafetyExplorer::INVARIANT,
            message: format!("{node_id} failed to restart from bootstrap state: {error:?}"),
            trace: trace.to_vec(),
            state: summarize(&state.cluster),
        })?;

    if let Some(pending) = before_pending.clone() {
        let Some(node) = state.cluster.nodes.get_mut(&node_id) else {
            return Err(Failure {
                invariant: RestartSafetyExplorer::INVARIANT,
                message: format!("{node_id} restart lost the node record"),
                trace: trace.to_vec(),
                state: summarize(&state.cluster),
            });
        };
        let resume_result = node.resume_pending_snapshot_transfer(pending);
        if let Err(error) = resume_result {
            return Err(Failure {
                invariant: RestartSafetyExplorer::INVARIANT,
                message: format!("{node_id} failed to resume pending snapshot transfer: {error:?}"),
                trace: trace.to_vec(),
                state: summarize(&state.cluster),
            });
        }
        // The kernel record resumes only alongside its durably staged byte
        // prefix; a plain restart would have dropped both together.
        if let Some(staged) = before_staged {
            state.cluster.snapshot_staging.insert(node_id, staged);
        }
    }

    let after = state.cluster.bootstrap_state(node_id);
    if after != before {
        return Err(Failure {
            invariant: RestartSafetyExplorer::INVARIANT,
            message: format!("{node_id} restart changed bootstrap state"),
            trace: trace.to_vec(),
            state: summarize(&state.cluster),
        });
    }

    let after_pending = state
        .cluster
        .nodes
        .get(&node_id)
        .and_then(Node::pending_snapshot_transfer);
    if after_pending != before_pending {
        return Err(Failure {
            invariant: RestartSafetyExplorer::INVARIANT,
            message: format!("{node_id} restart changed pending snapshot transfer"),
            trace: trace.to_vec(),
            state: summarize(&state.cluster),
        });
    }

    state.reset_commit_floor(node_id);

    Ok(())
}
