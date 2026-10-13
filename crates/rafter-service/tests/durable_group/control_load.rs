//! Client backlog must not postpone completion of already-drained control work.
use super::support::*;
use rafter::LeadershipTransferRejection;
use rafter_service::{DriverFuture, QueryReceipt};

type Read = DriverFuture<Result<QueryReceipt<u64, u8>, ReadError>>;

fn drive_with_local_read_backlog(
    node: &mut Replica,
    transfer: &mut DriverFuture<Result<(), TransferLeadershipError>>,
) -> (Result<(), TransferLeadershipError>, Vec<Read>) {
    let sender = node.driver.sender();
    let mut reads = Vec::new();
    let mut completed = 0;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        reads.retain_mut(|read: &mut Read| {
            if let Poll::Ready(result) = poll(read) {
                assert!(result.unwrap().proof.is_none());
                completed += 1;
                false
            } else {
                true
            }
        });
        while reads.len() < 4 {
            reads.push(sender.read(
                GROUP,
                vec![0],
                ReadConsistency::Local,
                ReadOptions::default(),
            ));
        }
        assert!(node.reads.retained().reads <= 4);
        node.pump();
        if let Poll::Ready(result) = poll(transfer) {
            return (result, reads);
        }
        assert!(completed < 64, "client reads starved the transfer reply");
        assert!(std::time::Instant::now() < deadline, "control work stalled");
        std::thread::yield_now();
    }
}

fn finish_reads(node: &mut Replica, mut reads: Vec<Read>) {
    assert!(!reads.is_empty());
    node.until(|_| {
        reads.retain_mut(|read| {
            if let Poll::Ready(result) = poll(read) {
                assert!(result.unwrap().proof.is_none());
                false
            } else {
                true
            }
        });
        reads.is_empty()
    });
    node.drain();
    assert_eq!(node.reads.retained().reads, 0);
}

#[test]
fn transfer_rejection_finishes_with_a_continuously_backlogged_read_queue() {
    let mut node = Replica::single(false);
    let mut transfer = node.driver.sender().transfer_leadership(GROUP, NodeId(1));
    let (result, reads) = drive_with_local_read_backlog(&mut node, &mut transfer);
    assert!(matches!(
        result,
        Err(TransferLeadershipError::Rejected {
            reason: LeadershipTransferRejection::TargetIsSelf,
            ..
        })
    ));
    finish_reads(&mut node, reads);
}

#[test]
fn transfer_acceptance_finishes_after_sends_despite_continuous_read_admission() {
    let mut cluster = Cluster::new();
    let mut transfer = cluster.nodes[0]
        .driver
        .sender()
        .transfer_leadership(GROUP, NodeId(2));
    let (result, reads) = drive_with_local_read_backlog(&mut cluster.nodes[0], &mut transfer);
    result.unwrap();
    assert!(cluster.link.sends.load(Ordering::Acquire) > 0);
    // Only the leader was pumped. Completion confirms immediate effects have
    // been routed; no follower has received them or elected a successor yet.
    assert_eq!(
        cluster.nodes[1].driver.ready_node().unwrap().role(),
        Role::Follower
    );
    finish_reads(&mut cluster.nodes[0], reads);
}

#[test]
fn continuously_queued_transfers_do_not_starve_existing_client_work() {
    let mut node = Replica::single(false);
    let sender = node.driver.sender();
    let mut read = sender.read(
        GROUP,
        vec![0],
        ReadConsistency::Local,
        ReadOptions::default(),
    );
    for _ in 0..64 {
        let mut transfer = sender.transfer_leadership(GROUP, NodeId(1));
        node.until(|_| match poll(&mut transfer) {
            Poll::Ready(Err(TransferLeadershipError::Rejected {
                reason: LeadershipTransferRejection::TargetIsSelf,
                ..
            })) => true,
            Poll::Pending => false,
            other @ Poll::Ready(_) => panic!("unexpected transfer result: {other:?}"),
        });
        if let Poll::Ready(result) = poll(&mut read) {
            assert!(result.unwrap().proof.is_none());
            return;
        }
    }
    panic!("control requests starved the queued client read");
}

#[test]
fn repeated_queued_transfer_cancellations_do_not_starve_existing_clients() {
    let mut node = Replica::single(false);
    let sender = node.driver.sender();
    let mut read = sender.read(
        GROUP,
        vec![0],
        ReadConsistency::Local,
        ReadOptions::default(),
    );
    for _ in 0..64 {
        let mut transfer = sender.transfer_leadership(GROUP, NodeId(1));
        assert!(poll(&mut transfer).is_pending());
        drop(transfer);
        node.pump();
        if let Poll::Ready(result) = poll(&mut read) {
            assert!(result.unwrap().proof.is_none());
            return;
        }
    }
    panic!("canceled control requests starved the queued client read");
}
