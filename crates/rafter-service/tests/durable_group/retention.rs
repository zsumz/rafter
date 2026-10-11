//! Passive observations follow credit ownership, including terminal results.
use super::{membership_admission, support::*};
use rafter::MembershipSet;
use rafter_app::membership::MembershipChange;
use rafter_service::durable::{DurableCommandStats, DurableRaftDriver};

fn buffer(value: u8, capacity: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(capacity);
    bytes.push(value);
    bytes
}

fn counts(stats: &DurableCommandStats) -> [usize; 6] {
    [
        stats.writes.writes,
        stats.writes.payload_bytes,
        stats.reads.map_or(0, |r| r.reads),
        stats.reads.map_or(0, |r| r.payload_bytes),
        stats.controls,
        stats.control_payload_bytes,
    ]
}

fn learner_target() -> MembershipChange {
    MembershipChange::ChangeVoters {
        target: MembershipSet::new(vec![NodeId(1)], vec![NodeId(2)]).unwrap(),
    }
}

#[test]
fn retained_observation_does_not_drive_queued_or_busy_work() {
    let mut node = membership_admission::single(true, 64);
    let tx = node.driver.sender();
    assert!(tx.retained().reads.is_some());
    assert_eq!(counts(&tx.retained()), [0; 6]);
    let watch = node.driver.handle().metrics().unwrap();
    let before = watch.current();
    let write = tx.write(GROUP, buffer(7, 16), WriteOptions::default());
    let read = tx.read(
        GROUP,
        buffer(0, 32),
        ReadConsistency::Local,
        ReadOptions::default(),
    );
    let change = tx.propose_membership(&GROUP, learner_target());
    let charged = [1, 16, 1, 32, 1, 2 * std::mem::size_of::<NodeId>()];
    for _ in 0..16 {
        assert_eq!(counts(&tx.retained()), charged);
        assert_eq!(watch.current(), before);
        assert_eq!(node.driver.state(), ServiceState::Ready);
        assert_eq!(node.queries.load(Ordering::Acquire), 0);
    }
    drop((write, read, change));
    assert_eq!(counts(&tx.retained()), charged);
    node.drain();
    assert_eq!(counts(&tx.retained()), [0; 6]);
    assert_eq!(
        node.driver.ready_node().unwrap().last_log_index(),
        before.last_log_index
    );
    let write = tx.write(GROUP, buffer(7, 16), WriteOptions::default());
    node.until(|n| n.entered.try_recv().is_ok());
    for _ in 0..16 {
        assert_eq!(counts(&tx.retained()), [1, 16, 0, 0, 0, 0]);
        assert_eq!(node.driver.durable_through(), Some(LogIndex::ZERO));
    }
    node.release.now();
    node.until(|_| {
        let completed = watch.current();
        // The worker's floor may advance before its completion is delivered.
        completed.applied_index > before.last_log_index && completed.pending_proposals == 0
    });
    node.drain();
    assert_eq!(counts(&tx.retained()), [1, 16, 0, 0, 0, 0]);
    drop(write);
    assert_eq!(counts(&tx.retained()), [0; 6]);
}

#[test]
fn uncollected_client_and_membership_results_remain_visible_after_shutdown() {
    let mut node = membership_admission::single(false, 64);
    let tx = node.driver.sender();
    let mut write = tx.write(GROUP, buffer(7, 16), WriteOptions::default());
    let mut read = tx.read(
        GROUP,
        buffer(0, 32),
        ReadConsistency::Local,
        ReadOptions::default(),
    );
    let mut change = tx.propose_membership(&GROUP, learner_target());
    let watch = node.driver.handle().metrics().unwrap();
    let before = watch.current().last_log_index;
    node.until(|n| {
        let completed = watch.current();
        n.queries.load(Ordering::Acquire) == 1
            && completed.applied_index > before
            && completed.pending_proposals == 0
    });
    node.drain();
    let charged = [1, 16, 1, 32, 1, 2 * std::mem::size_of::<NodeId>()];
    assert_eq!(counts(&tx.retained()), charged);
    let mut shutdown = tx.shutdown(GROUP);
    node.until(|n| n.driver.state() == ServiceState::Stopped);
    assert!(node.dropped.load(Ordering::Acquire));
    assert!(matches!(poll(&mut shutdown), Poll::Ready(Ok(()))));
    assert_eq!(counts(&tx.retained()), charged);
    assert!(matches!(poll(&mut change), Poll::Ready(Ok(_))));
    assert_eq!(counts(&tx.retained()), [1, 16, 1, 32, 0, 0]);
    assert!(matches!(poll(&mut read), Poll::Ready(Ok(_))));
    assert_eq!(counts(&tx.retained()), [1, 16, 0, 0, 0, 0]);
    assert!(matches!(poll(&mut write), Poll::Ready(Ok(_))));
    assert_eq!(counts(&tx.retained()), [0; 6]);
}

#[test]
fn disabled_reads_stay_distinct_and_observation_does_not_retain_the_backend() {
    let (owner, _writes, _entered, _release, dropped) = crate::write_support::setup(2, false);
    let driver = DurableRaftDriver::attach(GROUP, owner, Link::default()).unwrap();
    let tx = driver.sender();
    assert!(tx.retained().reads.is_none());
    let mut transfer = tx.transfer_leadership(GROUP, NodeId(1));
    assert_eq!(counts(&tx.retained()), [0, 0, 0, 0, 1, 0]);
    drop(driver);
    assert!(dropped.load(Ordering::Acquire));
    assert!(tx.retained().reads.is_none());
    assert_eq!(counts(&tx.retained()), [0, 0, 0, 0, 1, 0]);
    assert!(matches!(
        poll(&mut transfer),
        Poll::Ready(Err(TransferLeadershipError::ShuttingDown))
    ));
    assert_eq!(counts(&tx.retained()), [0; 6]);
}
