//! Pending quorum rounds cannot silently release admission or invent a proof.
use super::support::*;

#[test]
fn canceled_quorum_round_retains_capacity_until_real_kernel_completion() {
    let mut cluster = Cluster::new(1);
    let read = cluster.nodes[0].read(ReadConsistency::Linearizable);
    while cluster.pump(0) {}
    assert!(!cluster.network.is_empty());
    assert_eq!(cluster.nodes[0].queries.load(Ordering::Acquire), 0);
    drop(read);
    while cluster.pump(0) {}
    assert_eq!(cluster.nodes[0].sender.retained().reads, 1);
    assert!(matches!(
        poll(&mut cluster.nodes[0].read(ReadConsistency::Local)),
        Poll::Ready(Err(ReadError::Transport { .. }))
    ));
    cluster.until(|c| c.nodes[0].sender.retained().reads == 0);
    assert_eq!(cluster.nodes[0].queries.load(Ordering::Acquire), 0);
    let mut next = cluster.nodes[0].read(ReadConsistency::Linearizable);
    cluster.until(|c| c.nodes[0].queries.load(Ordering::Acquire) == 1);
    assert!(matches!(
        poll(&mut next),
        Poll::Ready(Ok(QueryReceipt { proof: Some(_), .. }))
    ));
}

#[test]
fn follower_refusal_and_raw_read_id_collision_do_not_invoke_queries() {
    let mut cluster = Cluster::new(1);
    let mut read = cluster.nodes[1].read(ReadConsistency::Linearizable);
    cluster.settle();
    assert!(matches!(
        poll(&mut read),
        Poll::Ready(Err(ReadError::Rejected { .. }))
    ));
    assert_eq!(cluster.nodes[1].queries.load(Ordering::Acquire), 0);
    let input = Input::ReadIndex { read_id: ReadId(1) };
    assert!(matches!(cluster.nodes[0].owner.try_step(input.clone()),
        Err(rafter_service::durable::DurableClientStepError::UnmanagedRead(returned)) if *returned == input));
}

#[test]
fn shutdown_releases_an_unanswered_quorum_round_before_the_future_is_terminal() {
    let mut cluster = Cluster::new(1);
    let mut read = cluster.nodes[0].read(ReadConsistency::Linearizable);
    while cluster.pump(0) {}
    assert!(poll(&mut read).is_pending());
    let owner = &mut cluster.nodes[0].owner;
    owner.begin_shutdown();
    drain(owner);
    assert!(poll(&mut read).is_pending());
    let _application = owner.finish_shutdown().unwrap();
    assert_eq!(owner.state(), ServiceState::Stopped);
    assert!(owner.ready_node().is_none());
    assert!(matches!(
        poll(&mut read),
        Poll::Ready(Err(ReadError::ShuttingDown))
    ));
}

#[test]
fn a_granted_read_keeps_its_original_term_when_query_execution_follows_stepdown() {
    let mut cluster = Cluster::new(1);
    let original_term = cluster.nodes[0].owner.ready_node().unwrap().current_term();
    let mut read = cluster.nodes[0].read(ReadConsistency::Linearizable);
    cluster.until(|c| !c.grants.is_empty());
    assert_eq!(cluster.nodes[0].queries.load(Ordering::Acquire), 0);
    advance_term(&mut cluster);
    cluster.until(|c| c.nodes[0].queries.load(Ordering::Acquire) == 1);
    let Poll::Ready(Ok(receipt)) = poll(&mut read) else {
        panic!("missing granted query");
    };
    let proof = receipt.proof.unwrap();
    assert_eq!(proof.term, original_term);
    assert_eq!(proof.required_applied_index, LogIndex::ZERO);
    assert!(cluster.nodes[0].owner.ready_node().unwrap().current_term() > proof.term);
}

#[test]
fn kernel_stepdown_cancellation_completes_the_read_without_invoking_its_query() {
    let mut cluster = Cluster::new(1);
    let mut read = cluster.nodes[0].read(ReadConsistency::Linearizable);
    while cluster.pump(0) {}
    assert!(poll(&mut read).is_pending());
    advance_term(&mut cluster);
    while cluster.pump(0) {}
    assert_eq!(cluster.nodes[0].queries.load(Ordering::Acquire), 0);
    assert_eq!(cluster.nodes[0].sender.retained().reads, 1);
    assert!(matches!(
        poll(&mut read),
        Poll::Ready(Err(ReadError::Canceled { .. }))
    ));
    assert_eq!(cluster.nodes[0].sender.retained().reads, 0);
}

#[test]
fn committed_self_removal_refuses_local_queries_from_the_retired_replica() {
    let mut cluster = Cluster::new(1);
    cluster.nodes[0]
        .owner
        .try_step(Input::RemoveVoter {
            voter_id: NodeId(1),
        })
        .unwrap();
    cluster.until(|c| {
        c.nodes[0].owner.state() == ServiceState::Ready
            && c.nodes[0]
                .owner
                .ready_node()
                .is_some_and(|n| n.commit_index() >= LogIndex(2))
    });
    cluster.nodes[0].owner.try_step(Input::LeaveJoint).unwrap();
    cluster.until(|c| {
        c.nodes[0]
            .owner
            .ready_node()
            .is_some_and(|node| !node.committed_membership().contains_replica(NodeId(1)))
    });
    let mut read = cluster.nodes[0].read(ReadConsistency::Local);
    while cluster.pump(0) {}
    assert!(matches!(
        poll(&mut read),
        Poll::Ready(Err(ReadError::Unavailable {
            reason: rafter_service::DriverUnavailableReason::NotMember,
        }))
    ));
    assert_eq!(cluster.nodes[0].queries.load(Ordering::Acquire), 0);
}

fn advance_term(cluster: &mut Cluster) {
    let old_term = cluster.nodes[0].owner.ready_node().unwrap().current_term();
    // Voter 2 generates a genuine newer-term election request. Deliver only
    // that request, so no synthetic protocol message or forged quorum is used.
    for _ in 0..16 {
        cluster.nodes[1].owner.try_step(Input::Tick).unwrap();
        while cluster.pump(1) {}
        if let Some(position) = cluster.network.iter().position(|(from, to, message)| {
            *from == NodeId(2)
                && *to == NodeId(1)
                && matches!(message, Message::RequestVote(vote) if vote.term > old_term)
        }) {
            let (from, _, message) = cluster.network.remove(position).unwrap();
            cluster.nodes[0]
                .owner
                .try_step(Input::Message { from, message })
                .unwrap();
            assert!(cluster.nodes[0].owner.ready_node().unwrap().current_term() > old_term);
            return;
        }
    }
    panic!("voter 2 did not start a newer election");
}
