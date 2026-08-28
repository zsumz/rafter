//! The safe membership flows the managed controller plans, step by step.
//!
//! Adding a learner must wait for catch-up, promotion must go through the
//! barrier, removal must represent the transport fencing the caller performs,
//! and a voter change must both enter and leave joint consensus — with every
//! report produced from the app-layer plan types themselves.

use std::collections::BTreeMap;

use super::*;

#[test]
fn add_learner_plan_waits_for_catchup() {
    let controller = MembershipController::new("group-a");
    let info = node_info("zone", "a");

    let planned = controller.add_learner(NodeId(4), info.clone());

    assert_eq!(
        planned.change,
        MembershipChange::AddLearner {
            node_id: NodeId(4),
            info,
        }
    );
    assert_eq!(
        planned.plan.steps,
        vec![
            MembershipStep::AddLearner(NodeId(4)),
            MembershipStep::WaitForCatchUp(NodeId(4)),
        ]
    );
}

#[test]
fn promote_learner_plan_uses_barrier() {
    let controller = MembershipController::new("group-a");
    let barrier = PromotionBarrier {
        learner_id: NodeId(4),
        required_match_index: LogIndex(99),
    };

    let planned = controller.promote_learner(barrier);

    assert_eq!(
        planned.change,
        MembershipChange::PromoteLearner {
            node_id: NodeId(4),
            barrier,
        }
    );
    assert_eq!(
        planned.plan.steps,
        vec![
            MembershipStep::WaitForCatchUp(NodeId(4)),
            MembershipStep::PromoteLearner(NodeId(4)),
        ]
    );
}

#[test]
fn remove_node_plan_represents_transport_fencing() {
    let controller = MembershipController::new("group-a");

    let planned = controller.remove_node(NodeId(2));

    assert_eq!(
        planned.change,
        MembershipChange::RemoveNode { node_id: NodeId(2) }
    );
    assert_eq!(
        planned.plan.steps,
        vec![
            MembershipStep::RemoveNode(NodeId(2)),
            MembershipStep::FencePeer(NodeId(2)),
        ]
    );
}

#[test]
fn change_voters_plan_enters_and_leaves_joint_consensus() {
    let controller = MembershipController::new("group-a");
    let target = membership(&[1, 3, 4]);

    let planned = controller.change_voters(target.clone());

    assert_eq!(
        planned.change,
        MembershipChange::ChangeVoters {
            target: target.clone(),
        }
    );
    assert_eq!(
        planned.plan.steps,
        vec![
            MembershipStep::EnterJoint(target),
            MembershipStep::LeaveJoint,
        ]
    );
}

#[test]
fn reports_are_produced_from_app_layer_plan_types() {
    let controller = MembershipController::new("group-a");
    let planned = controller.remove_node(NodeId(2));

    let pending = controller.pending_report(LogIndex(10), &planned.plan);
    assert_eq!(pending.group_id, "group-a");
    assert_eq!(pending.started_at, LogIndex(10));
    assert_eq!(pending.completed_at, None);
    assert!(pending
        .steps
        .iter()
        .all(|step| step.status == MembershipStepStatus::Pending));

    let completed = controller.completed_report(LogIndex(10), LogIndex(12), &planned.plan);
    assert_eq!(completed.completed_at, Some(LogIndex(12)));
    assert!(completed.steps.iter().all(|step| {
        step.status
            == MembershipStepStatus::Completed {
                at: Some(LogIndex(12)),
            }
    }));
}

fn node_info(key: &str, value: &str) -> NodeInfo {
    NodeInfo {
        metadata: BTreeMap::from([(key.to_owned(), value.to_owned())]),
    }
}

fn membership(voters: &[u64]) -> MembershipSet {
    MembershipSet::new(voters.iter().copied().map(NodeId).collect(), Vec::new())
        .expect("valid membership")
}
