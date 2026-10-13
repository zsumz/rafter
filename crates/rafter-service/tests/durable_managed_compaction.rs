#![cfg(feature = "durable")]
//! Managed snapshot maintenance preserves client ownership and failure semantics.
#[allow(dead_code, unused_imports, reason = "shared grouped-owner fixtures")]
#[path = "durable_group/support.rs"]
mod group_support;
#[allow(
    dead_code,
    unused_imports,
    reason = "shared real-worker and quorum fixtures"
)]
#[path = "durable_reads/support.rs"]
mod reads_support;
#[path = "durable_managed_compaction/support.rs"]
mod support;
#[allow(
    dead_code,
    unused_imports,
    reason = "shared real-worker and quorum fixtures"
)]
#[path = "durable_client/support.rs"]
mod write_support;
use support::*;
#[path = "durable_managed_compaction/failure.rs"]
mod failure;

#[test]
fn managed_compaction_preserves_queued_clients_uncollected_results_and_metrics() {
    let mut node = ready();
    let tx = node.driver.sender();
    let watch = tx.metrics(GROUP).unwrap();
    let mut completed = tx.read(
        GROUP,
        vec![0],
        ReadConsistency::Local,
        ReadOptions::default(),
    );
    node.until(|n| n.queries.load(Ordering::Acquire) == 1);
    node.drain();
    let mut queued = tx.read(
        GROUP,
        vec![0],
        ReadConsistency::Local,
        ReadOptions::default(),
    );
    let mut write = tx.write(GROUP, vec![9], WriteOptions::default());
    assert_eq!(node.reads.retained().reads, 2);
    assert_eq!(node.writes.retained().writes, 1);
    let retired = node.driver.compact_snapshot_with(snapshot).unwrap();
    assert!(!retired.is_empty());
    assert!(retired.payload_bytes() > 0);
    assert_eq!(
        node.driver.ready_node().unwrap().snapshot_index(),
        LogIndex(2)
    );
    assert_eq!(watch.current().snapshot_index, LogIndex(2));
    assert_eq!(watch.current().applied_index, LogIndex(2));
    assert_eq!(node.queries.load(Ordering::Acquire), 1);
    assert_eq!(node.reads.retained().reads, 2);
    assert_eq!(node.writes.retained().writes, 1);
    assert!(matches!(poll(&mut completed), Poll::Ready(Ok(receipt)) if receipt.result == 7));
    let mut receipt = None;
    node.until(|_| {
        if let Poll::Ready(result) = poll(&mut write) {
            receipt = Some(result.unwrap());
            true
        } else {
            false
        }
    });
    node.until(|n| n.queries.load(Ordering::Acquire) == 2);
    assert!(matches!(poll(&mut queued), Poll::Ready(Ok(_))));
    let receipt = receipt.unwrap();
    assert_eq!(receipt.result.value, 9);
    let mut fresh = tx.read(
        GROUP,
        vec![0],
        ReadConsistency::Linearizable,
        ReadOptions::default().with_min_applied_index(receipt.index),
    );
    node.until(|n| n.queries.load(Ordering::Acquire) == 3);
    assert!(
        matches!(poll(&mut fresh), Poll::Ready(Ok(r)) if r.result == 9 && r.proof.as_ref().unwrap().local_applied_index >= receipt.index)
    );
}

#[test]
fn ordinary_snapshot_floor_refusal_preserves_owner_clients() {
    let mut node = ready_owner();
    let writes = node.owner.sender();
    let mut write = writes.write(vec![8], WriteOptions::default());
    let mut read = node.read(ReadConsistency::Local);
    let result = node.owner.compact_snapshot_with(|app, core| {
        let (mut descriptor, source) = snapshot(app, core)?;
        descriptor.metadata.last_included_index = LogIndex(1);
        Ok((descriptor, source))
    });
    assert!(matches!(
        result,
        Err(ServiceError::SnapshotFloor {
            expected: LogIndex(2),
            actual: LogIndex(1)
        })
    ));
    assert_eq!(node.owner.state(), ServiceState::Ready);
    assert_eq!(
        node.owner.ready_node().unwrap().snapshot_index(),
        LogIndex::ZERO
    );
    assert!(poll(&mut read).is_pending());
    assert!(poll(&mut write).is_pending());
    crate::write_support::until(&mut node.owner, |_| {
        matches!(poll(&mut write), Poll::Ready(Ok(_)))
    });
    crate::write_support::until(&mut node.owner, |_| {
        matches!(poll(&mut read), Poll::Ready(Ok(_)))
    });
}

#[test]
fn busy_application_refuses_maintenance_without_calling_the_builder() {
    let mut node = Replica::single(true);
    let mut write = node.writes.write(vec![7], WriteOptions::default());
    node.until(|n| n.entered.try_recv().is_ok());
    assert!(matches!(
        node.driver
            .compact_snapshot_with::<Source>(|_, _| panic!("busy builder invoked")),
        Err(ServiceError::NotQuiescent)
    ));
    assert_ne!(node.driver.state(), ServiceState::Failed);
    node.release.now();
    node.until(|_| matches!(poll(&mut write), Poll::Ready(Ok(_))));
    node.drain();
    assert!(!node
        .driver
        .compact_snapshot_with(snapshot)
        .unwrap()
        .is_empty());
}

#[test]
fn handle_shutdown_refuses_compaction_before_another_poll_and_stays_stopped() {
    let mut node = ready();
    let mut shutdown = node.driver.sender().shutdown(GROUP);
    assert!(matches!(
        node.driver
            .compact_snapshot_with::<Source>(|_, _| panic!("shutdown builder invoked")),
        Err(ServiceError::NotReady(ServiceState::Draining))
    ));
    assert!(poll(&mut shutdown).is_pending());
    node.until(|n| n.driver.state() == ServiceState::Stopped);
    assert!(matches!(poll(&mut shutdown), Poll::Ready(Ok(()))));
    assert!(matches!(
        node.driver
            .compact_snapshot_with::<Source>(|_, _| panic!("stopped builder invoked")),
        Err(ServiceError::NotReady(ServiceState::Stopped))
    ));
    assert_eq!(node.driver.state(), ServiceState::Stopped);
}
