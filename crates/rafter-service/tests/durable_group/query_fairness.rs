//! A ready query must get a view after owned application work drains.
use super::support::*;
use std::time::{Duration, Instant};

fn paused() -> Replica {
    let mut options = rafter_runtime::service::ServiceOptions::new();
    options.max_proposals = 1;
    let mut base = crate::reads_support::Replica::with_options(1, vec![], 4, true, options, || {});
    for _ in 0..3 {
        base.owner.try_step(Input::Tick).unwrap();
        crate::write_support::drain(&mut base.owner);
    }
    Replica::attach(base, Link::default())
}

#[test]
fn busy_local_query_keeps_its_turn_before_more_queued_writes() {
    let mut node = paused();
    let sender = node.driver.sender();
    let mut first = sender.write(GROUP, vec![7], WriteOptions::default());
    node.until(|n| n.entered.try_recv().is_ok());
    assert!(poll(&mut first).is_pending());
    let first_index = node.driver.ready_node().unwrap().last_log_index();
    let mut read = sender.read(
        GROUP,
        vec![0],
        ReadConsistency::Local,
        ReadOptions::default(),
    );
    let mut writes: Vec<_> = [8, 9, 10]
        .into_iter()
        .map(|value| sender.write(GROUP, vec![value], WriteOptions::default()))
        .collect();
    // The current apply owns the view. An alternate write turn may precede the
    // read, but once that ready read finds the view busy, it must retain its turn.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let polled = node.driver.poll_next().unwrap();
        if matches!(polled, ServicePoll::Pending) && node.driver.state() == ServiceState::Ready {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "owner did not reach the blocked query"
        );
        std::thread::yield_now();
    }
    assert!(poll(&mut read).is_pending());
    let appended = node.driver.ready_node().unwrap().last_log_index();
    assert!(appended.0 <= first_index.0 + 1,
        "ready read yielded its turn to queued writes: first={first_index:?}, appended={appended:?}");
    // Repeated wakeups must not hand the next admission turn back to writes
    // while the application view remains occupied.
    for _ in 0..16 {
        node.drain();
        assert_eq!(node.driver.ready_node().unwrap().last_log_index(), appended);
        assert!(poll(&mut read).is_pending());
    }
    node.release.now();
    let mut observed = None;
    node.until(|_| {
        if let Poll::Ready(result) = poll(&mut read) {
            observed = Some(result.unwrap());
            true
        } else {
            false
        }
    });
    let observed = observed.unwrap();
    assert!(observed.proof.is_none());
    assert!((7..=8).contains(&observed.result));
    node.until(|_| {
        writes.retain_mut(|future| match poll(future) {
            Poll::Ready(result) => {
                result.unwrap();
                false
            }
            Poll::Pending => true,
        });
        writes.is_empty()
    });
    assert!(matches!(poll(&mut first), Poll::Ready(Ok(_))));
    node.drain();
    assert_eq!(sender.retained().writes.writes, 0);
    assert_eq!(sender.retained().reads.unwrap().reads, 0);
}

#[test]
fn canceling_a_busy_query_releases_its_turn_to_queued_writes() {
    let mut node = paused();
    let sender = node.driver.sender();
    let mut first = sender.write(GROUP, vec![7], WriteOptions::default());
    node.until(|n| n.entered.try_recv().is_ok());
    let read = sender.read(
        GROUP,
        vec![0],
        ReadConsistency::Local,
        ReadOptions::default(),
    );
    node.drain();
    assert_eq!(node.queries.load(Ordering::Acquire), 0);
    let before = node.driver.ready_node().unwrap().last_log_index();
    drop(read);
    let mut next = sender.write(GROUP, vec![8], WriteOptions::default());
    node.until(|n| {
        n.driver
            .ready_node()
            .is_some_and(|ready| ready.last_log_index() > before)
    });
    assert!(poll(&mut first).is_pending());
    assert!(poll(&mut next).is_pending());
    assert_eq!(sender.retained().reads.unwrap().reads, 0);
    node.release.now();
    node.until(|_| matches!(poll(&mut first), Poll::Ready(Ok(_))));
    node.until(|_| matches!(poll(&mut next), Poll::Ready(Ok(_))));
    node.drain();
    assert_eq!(sender.retained().writes.writes, 0);
}

#[test]
fn an_unconfirmed_quorum_read_does_not_hold_the_write_turn() {
    let mut cluster = Cluster::new();
    let sender = cluster.nodes[0].driver.sender();
    let watch = cluster.nodes[0].driver.handle().metrics().unwrap();
    let mut read = sender.read(
        GROUP,
        vec![0],
        ReadConsistency::Linearizable,
        ReadOptions::default(),
    );
    // Leave the ReadIndex messages in the link, so no follower can confirm it.
    cluster.nodes[0].until(|_| watch.current().pending_read_barriers == 1);
    cluster.nodes[0].drain();
    let before = cluster.nodes[0]
        .driver
        .ready_node()
        .unwrap()
        .last_log_index();
    let mut write = sender.write(GROUP, vec![8], WriteOptions::default());
    cluster.nodes[0].until(|n| {
        n.driver
            .ready_node()
            .is_some_and(|ready| ready.last_log_index() > before)
    });
    assert_eq!(watch.current().pending_read_barriers, 1);
    assert!(poll(&mut read).is_pending());
    assert!(poll(&mut write).is_pending());
    cluster.until(|_| match poll(&mut read) {
        Poll::Ready(result) => {
            let receipt = result.unwrap();
            let proof = receipt.proof.unwrap();
            assert_eq!(proof.group_id, GROUP);
            assert!(proof.local_applied_index >= proof.required_applied_index);
            assert!([0, 8].contains(&receipt.result));
            true
        }
        Poll::Pending => false,
    });
    cluster.until(|_| matches!(poll(&mut write), Poll::Ready(Ok(_))));
    assert_eq!(sender.retained().reads.unwrap().reads, 0);
    assert_eq!(sender.retained().writes.writes, 0);
}
