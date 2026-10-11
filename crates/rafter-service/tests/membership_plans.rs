//! Returned promotion plans finish in committed stable membership.

use rafter::{Input, MembershipConfig, MembershipSet, Node, NodeConfig, NodeId, Output, Role};
use rafter_app::membership::MembershipStep;
use rafter_service::MembershipController;
use std::collections::VecDeque;

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
