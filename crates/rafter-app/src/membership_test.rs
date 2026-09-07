//! Application-layer membership requests, plans, and progress reports.
//!
//! The change types must cover every supported request, a plan must express
//! both the safe flow and the transport fencing it leaves to the caller, and a
//! report must track a change step by step.

use super::*;

fn membership(voters: &[u64], learners: &[u64]) -> MembershipSet {
    MembershipSet::new(
        voters.iter().copied().map(NodeId).collect(),
        learners.iter().copied().map(NodeId).collect(),
    )
    .expect("test membership is valid")
}

#[test]
fn membership_change_types_represent_supported_requests() {
    let mut info = NodeInfo::default();
    info.metadata
        .insert("zone".to_owned(), "test-zone".to_owned());

    let changes = [
        MembershipChange::AddLearner {
            node_id: NodeId(4),
            info,
        },
        MembershipChange::PromoteLearner {
            node_id: NodeId(4),
            barrier: PromotionBarrier {
                learner_id: NodeId(4),
                required_match_index: LogIndex(9),
            },
        },
        MembershipChange::RemoveNode { node_id: NodeId(2) },
        MembershipChange::EnterJoint {
            target: membership(&[1, 3, 4], &[]),
            promotion_barriers: Vec::new(),
        },
        MembershipChange::LeaveJoint,
        MembershipChange::ChangeVoters {
            target: membership(&[1, 3, 4], &[]),
        },
    ];

    assert_eq!(changes.len(), 6);
}

#[test]
fn membership_plan_can_represent_safe_flow_and_fencing() {
    let target = membership(&[1, 3, 4], &[]);
    let plan = MembershipPlan {
        group_id: "group-a",
        steps: vec![
            MembershipStep::AddLearner(NodeId(4)),
            MembershipStep::WaitForCatchUp(NodeId(4)),
            MembershipStep::PromoteLearner(NodeId(4)),
            MembershipStep::EnterJoint(target.clone()),
            MembershipStep::LeaveJoint,
            MembershipStep::RemoveNode(NodeId(2)),
            MembershipStep::FencePeer(NodeId(2)),
        ],
    };

    assert!(matches!(
        plan.steps[0],
        MembershipStep::AddLearner(NodeId(4))
    ));
    assert!(matches!(
        plan.steps[1],
        MembershipStep::WaitForCatchUp(NodeId(4))
    ));
    assert!(matches!(
        plan.steps[2],
        MembershipStep::PromoteLearner(NodeId(4))
    ));
    assert_eq!(plan.steps[3], MembershipStep::EnterJoint(target));
    assert!(matches!(plan.steps[4], MembershipStep::LeaveJoint));
    assert!(matches!(
        plan.steps[5],
        MembershipStep::RemoveNode(NodeId(2))
    ));
    assert!(matches!(
        plan.steps[6],
        MembershipStep::FencePeer(NodeId(2))
    ));
}

#[test]
fn membership_change_report_tracks_step_progress() {
    let report = MembershipChangeReport {
        group_id: 7_u64,
        started_at: LogIndex(10),
        completed_at: Some(LogIndex(15)),
        steps: vec![
            MembershipStepReport {
                step: MembershipStep::AddLearner(NodeId(4)),
                status: MembershipStepStatus::Completed {
                    at: Some(LogIndex(11)),
                },
            },
            MembershipStepReport {
                step: MembershipStep::FencePeer(NodeId(2)),
                status: MembershipStepStatus::Pending,
            },
        ],
    };

    assert_eq!(report.group_id, 7);
    assert_eq!(report.started_at, LogIndex(10));
    assert_eq!(report.completed_at, Some(LogIndex(15)));
    assert_eq!(report.steps.len(), 2);
}
