//! Membership plans finish in stable membership before further changes or fencing.

use rafter::{
    ConfigurationPhase, ConfigurationProposalRejection, Input, MembershipConfig, MembershipSet,
    Node, NodeConfig, NodeId, Output, Role,
};
use rafter_app::membership::{MembershipChange, MembershipStep, NodeInfo};
use rafter_service::MembershipController;
use std::collections::VecDeque;

mod support;

fn drive(nodes: &mut [Node; 2], index: usize, input: Input) {
    let mut pending = VecDeque::from([(index, input)]);
    for _ in 0..1024 {
        let Some((index, input)) = pending.pop_front() else {
            return;
        };
        let from = nodes[index].id();
        for output in nodes[index].step(input) {
            match output {
                Output::RejectProposal { reason, .. } => panic!("plan step rejected: {reason:?}"),
                Output::Send { to, message } => {
                    if let Some(next) = nodes.iter().position(|node| node.id() == to) {
                        pending.push_back((next, Input::Message { from, message }));
                    }
                }
                _ => {}
            }
        }
    }
    panic!("message exchange did not quiesce");
}

#[test]
fn promotion_plan_finishes_in_committed_stable_membership() {
    let mut nodes = [
        Node::new(NodeConfig::new(NodeId(1), vec![], 1).unwrap()),
        Node::new(NodeConfig::new_non_voter(NodeId(2), vec![NodeId(1)], 9).unwrap()),
    ];
    drive(&mut nodes, 0, Input::Tick);
    assert_eq!(nodes[0].role(), Role::Leader);
    drive(
        &mut nodes,
        0,
        Input::AddLearner {
            learner_id: NodeId(2),
        },
    );
    let barrier = nodes[0].promotion_barrier(NodeId(2)).unwrap();
    let planned = MembershipController::new(7).promote_learner(barrier);
    for step in planned.plan.steps {
        match step {
            MembershipStep::WaitForCatchUp(id) => {
                assert!(nodes[0]
                    .leader_replication_progress()
                    .iter()
                    .any(|progress| progress.follower_id == id
                        && progress.match_index >= barrier.required_match_index));
            }
            MembershipStep::PromoteLearner(id) => {
                drive(
                    &mut nodes,
                    0,
                    Input::PromoteLearner {
                        learner_id: id,
                        promotion_barrier: barrier,
                    },
                );
                assert!(matches!(
                    nodes[0].committed_membership(),
                    MembershipConfig::Joint(_)
                ));
            }
            MembershipStep::LeaveJoint => drive(&mut nodes, 0, Input::LeaveJoint),
            other => panic!("unexpected promotion obligation: {other:?}"),
        }
        assert_eq!(nodes[0].commit_index(), nodes[0].last_log_index());
    }
    drive(&mut nodes, 0, Input::Tick);
    let expected =
        MembershipConfig::Stable(MembershipSet::new(vec![NodeId(1), NodeId(2)], vec![]).unwrap());
    for node in &nodes {
        assert_eq!(
            node.committed_membership(),
            expected,
            "promotion plan leaves a stable configuration"
        );
    }
    drive(
        &mut nodes,
        0,
        Input::AddLearner {
            learner_id: NodeId(3),
        },
    );
    assert!(nodes[0].committed_membership().contains_learner(NodeId(3)));
}

#[test]
fn explicit_voter_removal_preserves_learners_and_finishes_before_fencing() {
    let mut nodes = [
        Node::new(NodeConfig::new(NodeId(1), vec![NodeId(2)], 1).unwrap()),
        Node::new(NodeConfig::new(NodeId(2), vec![NodeId(1)], 9).unwrap()),
    ];
    drive(&mut nodes, 0, Input::Tick);
    assert_eq!(nodes[0].role(), Role::Leader);
    drive(
        &mut nodes,
        0,
        Input::AddLearner {
            learner_id: NodeId(3),
        },
    );
    let plan = MembershipController::new(7)
        .remove_node_from(NodeId(2), &nodes[0].committed_membership())
        .unwrap();
    assert_eq!(plan.plan.steps.len(), 3);
    for step in plan.plan.steps {
        match step {
            MembershipStep::EnterJoint(target) => {
                drive(
                    &mut nodes,
                    0,
                    Input::EnterJoint {
                        target,
                        promotion_barriers: vec![],
                    },
                );
                assert!(matches!(
                    nodes[0].committed_membership(),
                    MembershipConfig::Joint(_)
                ));
                assert!(nodes[0].committed_membership().contains_voter(NodeId(2)));
            }
            MembershipStep::LeaveJoint => drive(&mut nodes, 0, Input::LeaveJoint),
            MembershipStep::FencePeer(id) => {
                assert!(matches!(
                    nodes[0].committed_membership(),
                    MembershipConfig::Stable(_)
                ));
                assert!(!nodes[0].committed_membership().contains_replica(id));
            }
            other => panic!("unexpected removal obligation: {other:?}"),
        }
        assert_eq!(nodes[0].commit_index(), nodes[0].last_log_index());
    }
    assert_eq!(
        nodes[0].committed_membership(),
        MembershipConfig::Stable(MembershipSet::new(vec![NodeId(1)], vec![NodeId(3)]).unwrap())
    );
    drive(
        &mut nodes,
        0,
        Input::AddLearner {
            learner_id: NodeId(4),
        },
    );
    assert!(nodes[0].committed_membership().contains_learner(NodeId(4)));
}

#[test]
fn explicit_learner_removal_needs_no_joint_phase() {
    use support::transport::{driver_over_bootstrap, elect_single_voter};
    let (driver, transport) = driver_over_bootstrap(1, &[], &[2, 3]);
    elect_single_voter(&driver);
    for id in [2, 3] {
        driver
            .change_membership(MembershipChange::AddLearner {
                node_id: NodeId(id),
                info: NodeInfo::default(),
            })
            .unwrap();
    }
    let current = driver
        .with_group(|group| group.runtime().committed_membership())
        .unwrap();
    let plan = MembershipController::new(7)
        .remove_node_from(NodeId(2), &current)
        .unwrap();
    assert_eq!(plan.plan.steps.len(), 2);
    for step in plan.plan.steps {
        match step {
            MembershipStep::RemoveNode(node_id) => driver
                .change_membership(MembershipChange::RemoveNode { node_id })
                .unwrap(),
            MembershipStep::FencePeer(node_id) => {
                let committed = driver
                    .with_group(|group| group.runtime().committed_membership())
                    .unwrap();
                assert!(matches!(committed, MembershipConfig::Stable(_)));
                assert!(!committed.contains_replica(node_id));
                assert!(transport.retires(node_id));
                assert!(committed.contains_learner(NodeId(3)));
            }
            other => panic!("unexpected learner obligation: {other:?}"),
        }
    }
}

#[test]
fn removal_planning_refuses_joint_unknown_and_last_voter_targets() {
    let one = MembershipSet::new(vec![NodeId(1)], vec![NodeId(2)]).unwrap();
    let joint = MembershipConfig::joint(
        MembershipSet::new(vec![NodeId(1), NodeId(2)], vec![]).unwrap(),
        MembershipSet::new(vec![NodeId(1)], vec![]).unwrap(),
    );
    let cases = [
        (
            joint,
            NodeId(2),
            ConfigurationProposalRejection::StableConfigurationRequired {
                phase: ConfigurationPhase::Joint,
            },
        ),
        (
            MembershipConfig::Stable(one.clone()),
            NodeId(99),
            ConfigurationProposalRejection::VoterNotFound {
                node_id: NodeId(99),
            },
        ),
        (
            MembershipConfig::Stable(one),
            NodeId(1),
            ConfigurationProposalRejection::CannotRemoveLastVoter { node_id: NodeId(1) },
        ),
    ];
    for (current, node_id, expected) in cases {
        assert_eq!(
            MembershipController::new(7).remove_node_from(node_id, &current),
            Err(expected)
        );
    }
}
