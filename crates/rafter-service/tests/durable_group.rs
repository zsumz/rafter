#![cfg(feature = "durable")]
//! Public handle behavior uses real protocol and worker ownership.
#[allow(
    dead_code,
    unused_imports,
    reason = "shared real-worker and quorum fixtures"
)]
#[path = "durable_reads/support.rs"]
mod reads_support;
#[path = "durable_group/support.rs"]
mod support;
#[allow(
    dead_code,
    unused_imports,
    reason = "shared real-worker and quorum fixtures"
)]
#[path = "durable_client/support.rs"]
mod write_support;
use support::*;

#[test]
fn grouped_handle_preserves_durability_and_read_proof_fields() {
    fn accepts_send_only_results<
        S: DriverCommandSender<u64, Vec<u8>, Vec<u8>, std::cell::Cell<u8>, std::cell::Cell<u8>>,
    >() {
    }
    accepts_send_only_results::<
        rafter_service::durable::DurableCommandSender<
            u64,
            std::cell::Cell<u8>,
            std::cell::Cell<u8>,
        >,
    >();
    let mut node = Replica::single(true);
    let handle = node.driver.handle();
    let watch = handle.metrics().unwrap();
    assert_eq!(watch.current().group_id, GROUP);
    let mut write = Box::pin(handle.write(vec![7]));
    assert!(poll(&mut write).is_pending());
    node.until(|n| n.entered.try_recv().is_ok());
    assert!(poll(&mut write).is_pending());
    assert_eq!(watch.current().applied_index, LogIndex::ZERO);
    node.release.now();
    let mut result = None;
    node.until(|_| {
        if let Poll::Ready(receipt) = poll(&mut write) {
            result = Some(receipt.unwrap());
            true
        } else {
            false
        }
    });
    let receipt = result.unwrap();
    assert_eq!(receipt.result.value, 7);
    assert_eq!(receipt.index, LogIndex(2));
    let mut read = Box::pin(handle.read_with_options(
        vec![0],
        ReadConsistency::Linearizable,
        ReadOptions::default().with_min_applied_index(receipt.index),
    ));
    assert!(poll(&mut read).is_pending());
    node.until(|n| n.queries.load(Ordering::Acquire) == 1);
    let Poll::Ready(Ok(queried)) = poll(&mut read) else {
        panic!("query did not complete");
    };
    assert_eq!(queried.result, 7);
    let proof = queried.proof.unwrap();
    assert_eq!(proof.group_id, GROUP);
    assert_eq!(proof.issued_by, NodeId(1));
    assert_eq!(proof.term, receipt.term);
    assert_eq!(proof.required_applied_index, receipt.index);
    assert_eq!(proof.local_applied_index, receipt.index);
    node.drain();
    assert_eq!(watch.current().completed_query_reads, 0);
    assert_eq!(node.reads.retained().reads, 0);
}

#[test]
fn every_sender_operation_rejects_a_foreign_group_before_admission() {
    let node = Replica::single(false);
    let tx = node.driver.sender();
    assert!(matches!(
        poll(&mut tx.write(GROUP + 1, vec![1], WriteOptions::default())),
        Poll::Ready(Err(WriteError::WrongGroup))
    ));
    assert!(matches!(
        poll(&mut tx.read(
            GROUP + 1,
            vec![0],
            ReadConsistency::Local,
            ReadOptions::default()
        )),
        Poll::Ready(Err(ReadError::WrongGroup))
    ));
    assert!(matches!(
        poll(&mut tx.transfer_leadership(GROUP + 1, NodeId(2))),
        Poll::Ready(Err(TransferLeadershipError::WrongGroup))
    ));
    assert!(tx.metrics(GROUP + 1).is_err());
    assert!(matches!(
        poll(&mut tx.shutdown(GROUP + 1)),
        Poll::Ready(Err(ShutdownError::WrongGroup))
    ));
    assert_eq!(node.writes.retained().writes, 0);
    assert_eq!(node.reads.retained().reads, 0);
    assert_eq!(node.driver.state(), ServiceState::Ready);
    assert_eq!(node.queries.load(Ordering::Acquire), 0);
}

#[test]
fn duplicate_shutdown_and_metrics_wait_for_application_release() {
    let mut node = Replica::single(true);
    let tx = node.driver.sender();
    let mut write = tx.write(GROUP, vec![7], WriteOptions::default());
    node.until(|n| n.entered.try_recv().is_ok());
    let mut first = tx.shutdown(GROUP);
    let mut second = tx.shutdown(GROUP);
    assert!(poll(&mut first).is_pending());
    assert!(poll(&mut second).is_pending());
    node.drain();
    assert!(!node.dropped.load(Ordering::Acquire));
    assert!(poll(&mut first).is_pending());
    assert!(poll(&mut second).is_pending());
    assert!(matches!(
        poll(&mut tx.write(GROUP, vec![8], WriteOptions::default())),
        Poll::Ready(Err(WriteError::ShuttingDown))
    ));
    node.release.now();
    node.until(|n| n.driver.state() == ServiceState::Stopped);
    assert!(node.dropped.load(Ordering::Acquire));
    assert!(matches!(poll(&mut write), Poll::Ready(Ok(_))));
    assert!(matches!(poll(&mut first), Poll::Ready(Ok(()))));
    assert!(matches!(
        poll(&mut second),
        Poll::Ready(Err(ShutdownError::AlreadyShutDown))
    ));
    assert!(matches!(
        poll(&mut tx.shutdown(GROUP)),
        Poll::Ready(Err(ShutdownError::AlreadyShutDown))
    ));
}

#[test]
fn submitted_transfer_drains_real_peer_effects_before_request_success() {
    let mut cluster = Cluster::new();
    let tx = cluster.nodes[0].driver.sender();
    let mut transfer = tx.transfer_leadership(GROUP, NodeId(2));
    assert!(poll(&mut transfer).is_pending());
    assert!(cluster.nodes[0].pump());
    assert!(poll(&mut transfer).is_pending());
    assert_eq!(cluster.link.sends.load(Ordering::Acquire), 0);
    cluster.nodes[0].drain();
    assert!(cluster.link.sends.load(Ordering::Acquire) > 0);
    assert!(matches!(poll(&mut transfer), Poll::Ready(Ok(()))));
    // No follower has processed those messages yet; acceptance is not election.
    assert_eq!(
        cluster.nodes[1].driver.ready_node().unwrap().role(),
        Role::Follower
    );
}

#[path = "durable_group/control.rs"]
mod control;
#[path = "durable_group/control_load.rs"]
mod control_load;
#[path = "durable_group/lifecycle.rs"]
mod lifecycle;
#[path = "durable_group/membership.rs"]
mod membership;
#[path = "durable_group/membership_admission.rs"]
mod membership_admission;
#[path = "durable_group/membership_credits.rs"]
mod membership_credits;
#[path = "durable_group/membership_lifecycle.rs"]
mod membership_lifecycle;
#[path = "durable_group/membership_load.rs"]
mod membership_load;
#[path = "durable_group/mixed_load.rs"]
mod mixed_load;
#[path = "durable_group/retention.rs"]
mod retention;

#[test]
fn attachment_refusal_returns_busy_backend_and_transport_without_losing_clients() {
    let mut base = reads_support::Cluster::new(2);
    let mut write = base.nodes[0]
        .owner
        .sender()
        .write(vec![8], WriteOptions::default());
    base.pump(0);
    let mut replica = base.nodes.remove(0);
    assert!(replica.owner.ready_node().is_none());
    let link = Link::default();
    let retained = Arc::clone(&link.network);
    let failure = Driver::attach(GROUP, replica.owner, link).unwrap_err();
    assert_eq!(failure.state(), ServiceState::Busy);
    let (owner, link) = failure.into_parts();
    assert!(Arc::ptr_eq(&retained, &link.network));
    replica.owner = owner;
    base.nodes.insert(0, replica);
    let mut receipt = None;
    base.until(|_| {
        if let Poll::Ready(result) = poll(&mut write) {
            receipt = Some(result.unwrap());
            true
        } else {
            false
        }
    });
    assert_eq!(receipt.unwrap().result.value, 8);
}
#[path = "durable_group/snapshot.rs"]
mod snapshot;
