//! Control credits and outcomes follow actual protocol ownership.
use super::support::*;
use rafter::LeadershipTransferRejection;

#[test]
fn queued_transfer_cancellation_releases_its_slot_without_protocol_effects() {
    let mut cluster = Cluster::new();
    let tx = cluster.nodes[0].driver.sender();
    let first = tx.transfer_leadership(GROUP, NodeId(2));
    drop(first);
    assert!(matches!(
        poll(&mut tx.transfer_leadership(GROUP, NodeId(3))),
        Poll::Ready(Err(TransferLeadershipError::Backpressure))
    ));
    cluster.nodes[0].drain();
    assert_eq!(cluster.link.sends.load(Ordering::Acquire), 0);
    let mut second = tx.transfer_leadership(GROUP, NodeId(3));
    assert!(poll(&mut second).is_pending());
    cluster.nodes[0].drain();
    assert!(matches!(poll(&mut second), Poll::Ready(Ok(()))));
}

#[test]
fn submitted_cancellation_keeps_credit_until_immediate_effects_are_routed() {
    let mut cluster = Cluster::new();
    let tx = cluster.nodes[0].driver.sender();
    let first = tx.transfer_leadership(GROUP, NodeId(2));
    cluster.nodes[0].pump();
    drop(first);
    assert!(matches!(
        poll(&mut tx.transfer_leadership(GROUP, NodeId(3))),
        Poll::Ready(Err(TransferLeadershipError::Backpressure))
    ));
    assert_eq!(cluster.link.sends.load(Ordering::Acquire), 0);
    cluster.nodes[0].drain();
    assert!(cluster.link.sends.load(Ordering::Acquire) > 0);
    let mut second = tx.transfer_leadership(GROUP, NodeId(3));
    assert!(poll(&mut second).is_pending());
    cluster.nodes[0].drain();
    assert!(matches!(
        poll(&mut second),
        Poll::Ready(Err(TransferLeadershipError::Rejected {
            reason: LeadershipTransferRejection::TransferAlreadyInProgress,
            ..
        }))
    ));
}

#[test]
fn completed_transfer_result_retains_its_slot_until_collection() {
    let mut cluster = Cluster::new();
    let tx = cluster.nodes[0].driver.sender();
    let mut first = tx.transfer_leadership(GROUP, NodeId(2));
    cluster.nodes[0].drain();
    assert!(matches!(
        poll(&mut tx.transfer_leadership(GROUP, NodeId(3))),
        Poll::Ready(Err(TransferLeadershipError::Backpressure))
    ));
    assert!(matches!(poll(&mut first), Poll::Ready(Ok(()))));
    let mut second = tx.transfer_leadership(GROUP, NodeId(3));
    assert!(poll(&mut second).is_pending());
}

#[test]
fn protocol_rejection_is_reported_and_raw_transfer_cannot_cross_correlation() {
    let mut node = Replica::single(false);
    let tx = node.driver.sender();
    let mut self_transfer = tx.transfer_leadership(GROUP, NodeId(1));
    assert!(matches!(
        node.driver
            .try_step(Input::TransferLeadership { target: NodeId(2) }),
        Err(rafter_service::durable::DurableClientStepError::UnmanagedTransfer(_))
    ));
    node.drain();
    assert!(matches!(
        poll(&mut self_transfer),
        Poll::Ready(Err(TransferLeadershipError::Rejected {
            reason: LeadershipTransferRejection::TargetIsSelf,
            ..
        }))
    ));
    let mut nonvoter = tx.transfer_leadership(GROUP, NodeId(9));
    node.drain();
    assert!(matches!(
        poll(&mut nonvoter),
        Poll::Ready(Err(TransferLeadershipError::Rejected {
            reason: LeadershipTransferRejection::TargetNotVoter,
            ..
        }))
    ));
}

#[test]
fn transport_refusal_is_counted_and_later_writes_can_still_become_durable() {
    let mut cluster = Cluster::new();
    cluster.link.refusing.store(true, Ordering::Release);
    let tx = cluster.nodes[0].driver.sender();
    let mut transfer = tx.transfer_leadership(GROUP, NodeId(2));
    cluster.nodes[0].drain();
    assert!(matches!(poll(&mut transfer), Poll::Ready(Ok(()))));
    assert!(cluster.nodes[0].driver.refused_sends() > 0);
    assert!(cluster.link.network.lock().unwrap().is_empty());
    cluster.link.refusing.store(false, Ordering::Release);
    for _ in 0..3 {
        cluster.nodes[0].driver.try_step(Input::Tick).unwrap();
        cluster.until(|c| {
            c.nodes
                .iter()
                .all(|n| n.driver.state() == ServiceState::Ready)
        });
        for _ in 0..32 {
            cluster.turn();
        }
        if cluster.nodes[1]
            .driver
            .ready_node()
            .is_some_and(|n| n.role() == Role::Leader)
        {
            break;
        }
    }
    // A lost transfer may expire; it never made the original acknowledgement
    // a guarantee of target leadership. Subsequent protocol progress remains live.
    cluster.until(|c| {
        c.nodes
            .iter()
            .all(|n| n.driver.state() == ServiceState::Ready)
    });
    let leader = cluster
        .nodes
        .iter()
        .position(|n| {
            n.driver
                .ready_node()
                .is_some_and(|core| core.role() == Role::Leader)
        })
        .unwrap();
    let mut write =
        cluster.nodes[leader]
            .driver
            .sender()
            .write(GROUP, vec![9], WriteOptions::default());
    let mut receipt = None;
    cluster.until(|_| {
        if let Poll::Ready(result) = poll(&mut write) {
            receipt = Some(result.unwrap());
            true
        } else {
            false
        }
    });
    assert_eq!(receipt.unwrap().result.value, 9);
}

#[test]
fn backend_release_distinguishes_queued_refusal_from_unconfirmed_transfer() {
    let mut cluster = Cluster::new();
    let tx = cluster.nodes[0].driver.sender();
    let mut transfer = tx.transfer_leadership(GROUP, NodeId(2));
    cluster.nodes[0].pump();
    drop(cluster);
    assert!(matches!(
        poll(&mut transfer),
        Poll::Ready(Err(TransferLeadershipError::Unconfirmed))
    ));
    let node = Replica::single(false);
    let mut queued = node.driver.sender().transfer_leadership(GROUP, NodeId(1));
    drop(node);
    assert!(matches!(
        poll(&mut queued),
        Poll::Ready(Err(TransferLeadershipError::ShuttingDown))
    ));
}
