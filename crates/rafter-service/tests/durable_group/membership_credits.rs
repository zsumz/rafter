//! Configuration payloads and both control kinds obey one retained credit.
use super::{membership_admission::*, support::*};
use rafter::{MembershipSet, PromotionBarrier};
use rafter_app::membership::{MembershipChange, NodeInfo};

fn assert_full(node: &Replica) {
    assert!(matches!(
        poll(&mut propose(node, add(9))),
        Poll::Ready(Err(DurableMembershipError::Backpressure))
    ));
    assert!(matches!(
        poll(&mut node.driver.sender().transfer_leadership(GROUP, NodeId(1))),
        Poll::Ready(Err(TransferLeadershipError::Backpressure))
    ));
}

#[test]
fn canceled_queued_membership_keeps_credit_until_discard_without_protocol_effects() {
    let mut node = single(false, 64);
    let before = node.driver.ready_node().unwrap().last_log_index();
    drop(propose(&node, add(2)));
    assert_full(&node);
    node.pump();
    assert_eq!(node.driver.ready_node().unwrap().last_log_index(), before);
    let mut next = propose(&node, add(3));
    finish(&mut node, &mut next).unwrap();
    assert!(!node
        .driver
        .ready_node()
        .unwrap()
        .committed_membership()
        .contains_replica(NodeId(2)));
}

#[test]
fn submitted_cancellation_keeps_credit_until_its_effects_are_drained() {
    let mut node = single(false, 64);
    let request = propose(&node, add(2));
    node.pump();
    drop(request);
    assert_full(&node);
    node.drain();
    assert!(node
        .driver
        .ready_node()
        .unwrap()
        .committed_membership()
        .contains_learner(NodeId(2)));
    let mut next = propose(&node, add(3));
    finish(&mut node, &mut next).unwrap();
}

#[test]
fn uncollected_membership_and_transfer_results_keep_the_shared_slot() {
    let mut node = single(false, 64);
    let mut membership = propose(&node, add(2));
    node.drain();
    assert_full(&node);
    assert!(matches!(poll(&mut membership), Poll::Ready(Ok(_))));
    let mut transfer = node.driver.sender().transfer_leadership(GROUP, NodeId(1));
    assert_full(&node);
    node.drain();
    assert_full(&node);
    assert!(matches!(
        poll(&mut transfer),
        Poll::Ready(Err(TransferLeadershipError::Rejected { .. }))
    ));
    let mut next = propose(&node, add(3));
    finish(&mut node, &mut next).unwrap();
}

#[test]
fn oversized_targets_and_barriers_are_refused_before_the_log_changes() {
    let mut node = single(false, 16);
    let before = node.driver.ready_node().unwrap().last_log_index();
    let large_target = MembershipSet::new(vec![NodeId(1)], vec![NodeId(2), NodeId(3)]).unwrap();
    let target = MembershipSet::new(vec![NodeId(1)], vec![NodeId(2)]).unwrap();
    for change in [
        MembershipChange::ChangeVoters {
            target: large_target,
        },
        MembershipChange::EnterJoint {
            target,
            promotion_barriers: vec![PromotionBarrier {
                learner_id: NodeId(2),
                required_match_index: before,
            }],
        },
    ] {
        assert!(matches!(
            poll(&mut propose(&node, change)),
            Poll::Ready(Err(DurableMembershipError::Backpressure))
        ));
    }
    assert_eq!(node.driver.ready_node().unwrap().last_log_index(), before);
    let mut info = NodeInfo::default();
    info.metadata.insert("opaque".into(), "unused".repeat(4096));
    let mut scalar = propose(
        &node,
        MembershipChange::AddLearner {
            node_id: NodeId(2),
            info,
        },
    );
    finish(&mut node, &mut scalar).unwrap();
    assert!(node
        .driver
        .ready_node()
        .unwrap()
        .committed_membership()
        .contains_learner(NodeId(2)));
}

#[test]
fn over_reserved_vectors_are_normalized_before_retained_payload_accounting() {
    let mut node = single(false, 32);
    let mut voters = Vec::with_capacity(4096);
    voters.push(NodeId(1));
    let mut learners = Vec::with_capacity(4096);
    learners.push(NodeId(2));
    let target = MembershipSet::new(voters, learners).unwrap();
    let mut request = propose(&node, MembershipChange::ChangeVoters { target });
    assert!(poll(&mut request).is_pending());
    finish(&mut node, &mut request).unwrap();
    let core = node.driver.ready_node().unwrap();
    let before = core.last_log_index();
    let barrier = core.promotion_barrier(NodeId(2)).unwrap();
    let mut promotion_barriers = Vec::with_capacity(4096);
    promotion_barriers.push(barrier);
    let mut request = propose(
        &node,
        MembershipChange::EnterJoint {
            target: MembershipSet::new(vec![NodeId(1), NodeId(2)], vec![]).unwrap(),
            promotion_barriers,
        },
    );
    assert!(poll(&mut request).is_pending());
    // There is no replica 2 to satisfy catch-up. The normalized request reaches
    // the kernel's protocol check instead of spending its old spare capacity.
    assert!(matches!(
        finish(&mut node, &mut request),
        Err(DurableMembershipError::Rejected { .. })
    ));
    assert_eq!(node.driver.ready_node().unwrap().last_log_index(), before);
}
