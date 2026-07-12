use rafter::{MembershipConfig, MembershipSet, NodeId, Role};

use super::super::{Action, Bounds, ExplorationState, SoakAction};
use super::operation::{EnabledAction, Operation, SoakOperation};

pub(super) fn enabled_membership_actions(
    state: &ExplorationState,
    bounds: Bounds,
) -> Vec<EnabledAction> {
    if state.membership_changes_issued() >= bounds.membership_change_count as u64 {
        return Vec::new();
    }

    enabled_membership_operations(state)
        .into_iter()
        .map(|operation| EnabledAction {
            trace: membership_trace(&operation),
            operation,
        })
        .collect()
}

pub(super) fn enabled_membership_operations(state: &ExplorationState) -> Vec<Operation> {
    let mut operations = Vec::new();
    let cluster_node_ids = state.cluster().nodes.keys().copied().collect::<Vec<_>>();
    for (leader_id, node) in &state.cluster().nodes {
        if node.role() != Role::Leader {
            continue;
        }
        match node.effective_membership() {
            MembershipConfig::Stable(current) => {
                for node_id in &cluster_node_ids {
                    if !current.voters().contains(node_id) && !current.learners().contains(node_id)
                    {
                        operations.push(Operation::AddLearner {
                            to: *leader_id,
                            learner_id: *node_id,
                        });
                    }
                }
                for learner_id in current.learners() {
                    operations.push(Operation::RemoveLearner {
                        to: *leader_id,
                        learner_id: *learner_id,
                    });
                    if let Some(promotion_barrier) =
                        state.cluster().promotion_barrier(*leader_id, *learner_id)
                    {
                        operations.push(Operation::PromoteLearner {
                            to: *leader_id,
                            learner_id: *learner_id,
                            promotion_barrier,
                        });
                    }
                }
                if current.voters().len() > 1 {
                    for voter_id in current.voters() {
                        operations.push(Operation::RemoveVoter {
                            to: *leader_id,
                            voter_id: *voter_id,
                        });
                        operations.push(Operation::EnterJoint {
                            to: *leader_id,
                            target: remove_voter_target(&current, *voter_id),
                            promotion_barriers: Vec::new(),
                        });
                    }
                }
            }
            MembershipConfig::Joint(_) => {
                operations.push(Operation::LeaveJoint { to: *leader_id });
            }
        }
    }
    operations
}

fn remove_voter_target(current: &MembershipSet, voter_id: NodeId) -> MembershipSet {
    let voters = current
        .voters()
        .iter()
        .copied()
        .filter(|node_id| *node_id != voter_id)
        .collect();
    MembershipSet::new(voters, current.learners().to_vec())
        .expect("enabled membership action keeps at least one voter")
}

fn membership_trace(operation: &Operation) -> Action {
    match operation {
        Operation::AddLearner { to, learner_id } => Action::AddLearner {
            to: *to,
            learner_id: *learner_id,
        },
        Operation::RemoveLearner { to, learner_id } => Action::RemoveLearner {
            to: *to,
            learner_id: *learner_id,
        },
        Operation::PromoteLearner { to, learner_id, .. } => Action::PromoteLearner {
            to: *to,
            learner_id: *learner_id,
        },
        Operation::RemoveVoter { to, voter_id } => Action::RemoveVoter {
            to: *to,
            voter_id: *voter_id,
        },
        Operation::EnterJoint { to, target, .. } => Action::EnterJoint {
            to: *to,
            target: target.clone(),
        },
        Operation::LeaveJoint { to } => Action::LeaveJoint { to: *to },
        Operation::Tick(_)
        | Operation::Restart(_)
        | Operation::Propose { .. }
        | Operation::ReadIndex { .. }
        | Operation::Transfer { .. }
        | Operation::DeliverReadyAt(_) => unreachable!("operation is not a membership action"),
    }
}

pub(super) fn soak_membership_trace(operation: &Operation) -> SoakAction {
    match membership_trace(operation) {
        Action::AddLearner { to, learner_id } => SoakAction::AddLearner { to, learner_id },
        Action::RemoveLearner { to, learner_id } => SoakAction::RemoveLearner { to, learner_id },
        Action::PromoteLearner { to, learner_id } => SoakAction::PromoteLearner { to, learner_id },
        Action::RemoveVoter { to, voter_id } => SoakAction::RemoveVoter { to, voter_id },
        Action::EnterJoint { to, target } => SoakAction::EnterJoint { to, target },
        Action::LeaveJoint { to } => SoakAction::LeaveJoint { to },
        _ => unreachable!("operation is not a membership action"),
    }
}

pub(super) fn soak_membership_operation(operation: Operation) -> SoakOperation {
    match operation {
        Operation::AddLearner { to, learner_id } => SoakOperation::AddLearner { to, learner_id },
        Operation::RemoveLearner { to, learner_id } => {
            SoakOperation::RemoveLearner { to, learner_id }
        }
        Operation::PromoteLearner {
            to,
            learner_id,
            promotion_barrier,
        } => SoakOperation::PromoteLearner {
            to,
            learner_id,
            promotion_barrier,
        },
        Operation::RemoveVoter { to, voter_id } => SoakOperation::RemoveVoter { to, voter_id },
        Operation::EnterJoint {
            to,
            target,
            promotion_barriers,
        } => SoakOperation::EnterJoint {
            to,
            target,
            promotion_barriers,
        },
        Operation::LeaveJoint { to } => SoakOperation::LeaveJoint { to },
        Operation::Tick(_)
        | Operation::Restart(_)
        | Operation::Propose { .. }
        | Operation::ReadIndex { .. }
        | Operation::Transfer { .. }
        | Operation::DeliverReadyAt(_) => unreachable!("operation is not a membership action"),
    }
}
