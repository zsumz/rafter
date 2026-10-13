#![cfg(feature = "durable")]
//! Observations follow real protocol, application, and future ownership.

#[allow(
    dead_code,
    unused_imports,
    reason = "shared real-worker and quorum fixtures"
)]
#[path = "durable_reads/support.rs"]
mod support;
#[allow(
    dead_code,
    unused_imports,
    reason = "shared real-worker and quorum fixtures"
)]
#[path = "durable_client/support.rs"]
mod write_support;
use support::*;

#[path = "durable_metrics/notifications.rs"]
mod notifications;

fn counts(owner: &Owner<u8>) -> [usize; 4] {
    let metrics = owner.metrics().unwrap();
    [
        metrics.pending_read_barriers,
        metrics.pending_query_reads,
        metrics.completed_query_reads,
        metrics.reserved_reads,
    ]
}

#[test]
fn metrics_report_durable_application_floor_and_unfinished_proposals() {
    let mut replica = Replica::single(2, true);
    let initial = replica.owner.metrics().unwrap();
    assert_eq!(initial.commit_index, LogIndex(1));
    assert_eq!(initial.applied_index, LogIndex::ZERO);
    let writes = replica.owner.sender();
    let mut write = writes.write(vec![7], WriteOptions::default());
    assert_eq!(replica.owner.metrics().unwrap().pending_proposals, 1);
    until(&mut replica.owner, |_| replica.entered.try_recv().is_ok());
    let stalled = replica.owner.metrics().unwrap();
    assert_eq!(stalled.commit_index, LogIndex(2));
    assert_eq!(
        replica.owner.ready_node().unwrap().dispatched_index(),
        LogIndex(2)
    );
    assert_eq!(stalled.applied_index, LogIndex::ZERO);
    assert_eq!(stalled.pending_proposals, 1);
    replica.release.now();
    write_support::applied(&mut replica.owner);
    let done = replica.owner.metrics().unwrap();
    assert_eq!(done.applied_index, LogIndex(2));
    assert_eq!(done.pending_proposals, 0);
    assert_eq!(writes.retained().writes, 1);
    assert!(matches!(poll(&mut write), Poll::Ready(Ok(_))));
    assert_eq!(writes.retained().writes, 0);
}

#[test]
fn proof_counts_survive_completion_until_collection_and_exclude_local_reads() {
    let mut replica = Replica::single(2, false);
    let mut read = replica.read(ReadConsistency::Linearizable);
    assert_eq!(counts(&replica.owner), [0, 1, 0, 1]);
    until(&mut replica.owner, |_| {
        replica.queries.load(Ordering::Acquire) == 1
    });
    assert_eq!(counts(&replica.owner), [0, 0, 1, 1]);
    let before = replica.owner.metrics().unwrap();
    for _ in 0..16 {
        assert_eq!(replica.owner.metrics().unwrap(), before);
    }
    assert_eq!(replica.queries.load(Ordering::Acquire), 1);
    assert!(matches!(poll(&mut read), Poll::Ready(Ok(receipt)) if receipt.proof.is_some()));
    assert_eq!(counts(&replica.owner), [0, 0, 0, 0]);
    let mut local = replica.read(ReadConsistency::Local);
    assert_eq!(counts(&replica.owner), [0, 0, 0, 0]);
    until(&mut replica.owner, |_| {
        replica.queries.load(Ordering::Acquire) == 2
    });
    assert_eq!(replica.sender.retained().reads, 1);
    assert_eq!(counts(&replica.owner), [0, 0, 0, 0]);
    assert!(matches!(poll(&mut local), Poll::Ready(Ok(receipt)) if receipt.proof.is_none()));
}

#[test]
fn abandoned_completed_proofs_and_completed_errors_are_not_reserved() {
    let mut replica = Replica::single(2, false);
    let read = replica.read(ReadConsistency::Linearizable);
    until(&mut replica.owner, |_| {
        replica.queries.load(Ordering::Acquire) == 1
    });
    assert_eq!(counts(&replica.owner), [0, 0, 1, 1]);
    drop(read);
    // No owner poll is needed to release a completed proof's observation.
    assert_eq!(counts(&replica.owner), [0, 0, 0, 0]);
    let mut refused = replica.sender.read(
        vec![0],
        ReadConsistency::Linearizable,
        ReadOptions::default().with_min_applied_index(LogIndex(99)),
    );
    drain(&mut replica.owner);
    assert_eq!(replica.sender.retained().reads, 1);
    assert_eq!(counts(&replica.owner), [0, 0, 0, 0]);
    assert!(matches!(
        poll(&mut refused),
        Poll::Ready(Err(ReadError::FreshnessUnavailable { .. }))
    ));
    assert_eq!(replica.queries.load(Ordering::Acquire), 1);
}

#[test]
fn canceled_quorum_round_stays_visible_until_the_kernel_releases_it() {
    let mut cluster = Cluster::new(2);
    let read = cluster.nodes[0].read(ReadConsistency::Linearizable);
    assert!(cluster.pump(0));
    assert_eq!(counts(&cluster.nodes[0].owner), [1, 1, 0, 1]);
    drop(read);
    assert!(cluster.pump(0));
    assert_eq!(counts(&cluster.nodes[0].owner), [1, 1, 0, 1]);
    cluster.until(|c| c.nodes[0].sender.retained().reads == 0);
    assert_eq!(counts(&cluster.nodes[0].owner), [0, 0, 0, 0]);
    assert_eq!(cluster.nodes[0].queries.load(Ordering::Acquire), 0);
}

#[test]
fn metrics_are_unavailable_while_persistence_owns_the_node() {
    let mut cluster = Cluster::new(2);
    let mut write = cluster.nodes[0]
        .owner
        .sender()
        .write(vec![7], WriteOptions::default());
    assert!(cluster.nodes[0].owner.metrics().is_some());
    // The eligible append moves the actual node to its persistence worker.
    // Even a finished worker cannot return ownership until the next owner poll.
    assert!(cluster.pump(0));
    assert!(cluster.nodes[0].owner.ready_node().is_none());
    assert!(cluster.nodes[0].owner.metrics().is_none());
    cluster.until(|c| {
        c.nodes[0]
            .owner
            .metrics()
            .is_some_and(|m| m.applied_index == LogIndex(2) && m.pending_proposals == 0)
    });
    assert!(matches!(poll(&mut write), Poll::Ready(Ok(_))));
}

#[test]
fn granted_query_remains_pending_until_evaluated_then_counts_its_proof() {
    let mut cluster = Cluster::new(2);
    let mut read = cluster.nodes[0].read(ReadConsistency::Linearizable);
    cluster.until(|c| !c.grants.is_empty());
    assert_eq!(counts(&cluster.nodes[0].owner), [0, 1, 0, 1]);
    assert_eq!(cluster.nodes[0].queries.load(Ordering::Acquire), 0);
    cluster.until(|c| c.nodes[0].queries.load(Ordering::Acquire) == 1);
    assert_eq!(counts(&cluster.nodes[0].owner), [0, 0, 1, 1]);
    assert!(matches!(poll(&mut read), Poll::Ready(Ok(_))));
    assert_eq!(counts(&cluster.nodes[0].owner), [0, 0, 0, 0]);
}

#[test]
fn metrics_distinguish_failed_incarnation_from_released_backend() {
    use rafter_app::group::GroupFatalState;
    let mut failed = Replica::single(1, false);
    let read = failed
        .sender
        .read(vec![255], ReadConsistency::Local, ReadOptions::default());
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drain(&mut failed.owner)))
            .is_err()
    );
    assert!(matches!(
        failed.owner.metrics().unwrap().fatal_state,
        GroupFatalState::Poisoned { .. }
    ));
    assert_eq!(
        failed.owner.metrics().unwrap().applied_index,
        LogIndex::ZERO
    );
    drop(read);
    let mut stopped = Replica::single(1, false);
    assert_eq!(
        stopped.owner.metrics().unwrap().fatal_state,
        GroupFatalState::Healthy
    );
    stopped.owner.begin_shutdown();
    drain(&mut stopped.owner);
    let application = stopped.owner.finish_shutdown().unwrap();
    assert_eq!(application.floor, LogIndex::ZERO);
    assert!(stopped.owner.metrics().is_none());
}
