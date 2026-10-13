//! Snapshot handoff must drain an earlier committed command's real completion.
use super::{support::Partition, *};

#[test]
fn committed_write_keeps_its_receipt_before_snapshot_replaces_blocked_application() {
    let mut cluster = Partition::with_paused_leader();
    let writes = cluster.cluster.nodes[0].owner.sender();
    let original_term = cluster.cluster.nodes[0]
        .owner
        .ready_node()
        .unwrap()
        .current_term();
    let mut original = writes.write(vec![7], WriteOptions::default());
    cluster.until(|c| c.cluster.nodes[0].entered.try_recv().is_ok());
    for _ in 0..3 {
        cluster.tick(0);
    }
    cluster.until(|c| {
        c.cluster.nodes[1..]
            .iter()
            .all(|n| n.owner.durable_through() == Some(LogIndex(2)))
    });
    assert_eq!(
        cluster.cluster.nodes[0]
            .owner
            .ready_node()
            .unwrap()
            .commit_index(),
        LogIndex(2)
    );
    assert_eq!(
        cluster.cluster.nodes[0].owner.durable_through(),
        Some(LogIndex::ZERO)
    );
    assert!(poll(&mut original).is_pending());
    cluster.partitioned = true;
    cluster.elect_replacement();
    let mut later = cluster.cluster.nodes[1]
        .owner
        .sender()
        .write(vec![4], WriteOptions::default());
    cluster.until(|_| match poll(&mut later) {
        Poll::Ready(value) => {
            let receipt = value.unwrap();
            assert_eq!(receipt.index, LogIndex(4));
            assert!(receipt.term > original_term);
            true
        }
        Poll::Pending => false,
    });
    cluster.until(|c| c.cluster.nodes[1].owner.state() == ServiceState::Ready);
    cluster.compact();
    cluster.heal();
    for _ in 0..3 {
        cluster.send_tick(1);
    }
    cluster.until(|c| {
        c.cluster.nodes[0]
            .owner
            .ready_node()
            .is_some_and(|n| n.snapshot_index() == LogIndex(4))
    });
    // The Raft snapshot is durable, but the application is still executing the
    // committed old command. Repeated owner turns may not hand that store off.
    for _ in 0..16 {
        cluster.turn();
        assert_eq!(cluster.installs, 0);
        assert!(poll(&mut original).is_pending());
        assert_eq!(
            cluster.cluster.nodes[0].owner.durable_through(),
            Some(LogIndex::ZERO)
        );
        assert_eq!(writes.retained().writes, 1);
    }
    cluster.cluster.nodes[0].release.now();
    cluster.until(|_| match poll(&mut original) {
        Poll::Ready(value) => {
            let receipt = value.unwrap();
            assert_eq!(receipt.index, LogIndex(2));
            assert_eq!(receipt.term, original_term);
            assert_eq!(receipt.result.value, 7);
            true
        }
        Poll::Pending => false,
    });
    assert_eq!(
        cluster.installs, 0,
        "committed completion must precede handoff"
    );
    assert_eq!(writes.retained().writes, 0);
    cluster.until(|c| c.installs == 1);
    assert_eq!(
        cluster.cluster.nodes[0].owner.durable_through(),
        Some(LogIndex(4))
    );
    let mut read = cluster.cluster.nodes[0].read(ReadConsistency::Local);
    cluster.until(|_| match poll(&mut read) {
        Poll::Ready(value) => {
            assert_eq!(value.unwrap().result, 4);
            true
        }
        Poll::Pending => false,
    });
    assert_eq!(writes.retained().payload_bytes, 0);
}
