//! New membership controls preserve progress in both directions under backlog.
use super::{membership_admission::*, support::*};
use rafter_app::membership::MembershipChange;
use rafter_service::{DriverFuture, QueryReceipt};

type Read = DriverFuture<Result<QueryReceipt<u64, u8>, ReadError>>;

#[test]
fn repeated_membership_refusals_give_queued_clients_an_admission_turn() {
    let mut node = single(false, 64);
    let mut read = node.driver.sender().read(
        GROUP,
        vec![0],
        ReadConsistency::Local,
        ReadOptions::default(),
    );
    for _ in 0..64 {
        let mut request = propose(
            &node,
            MembershipChange::RemoveNode {
                node_id: NodeId(99),
            },
        );
        assert!(matches!(
            finish(&mut node, &mut request),
            Err(DurableMembershipError::Rejected { .. })
        ));
        if let Poll::Ready(result) = poll(&mut read) {
            assert!(result.unwrap().proof.is_none());
            return;
        }
    }
    panic!("membership controls starved the queued read");
}

#[test]
fn repeated_queued_membership_cancellations_do_not_starve_clients() {
    let mut node = single(false, 64);
    let mut read = node.driver.sender().read(
        GROUP,
        vec![0],
        ReadConsistency::Local,
        ReadOptions::default(),
    );
    for _ in 0..64 {
        drop(propose(&node, add(2)));
        node.pump();
        if let Poll::Ready(result) = poll(&mut read) {
            assert!(result.unwrap().proof.is_none());
            assert!(!node
                .driver
                .ready_node()
                .unwrap()
                .effective_membership()
                .contains_replica(NodeId(2)));
            return;
        }
    }
    panic!("canceled memberships starved the queued read");
}

#[test]
fn continuously_replenished_reads_do_not_postpone_membership_results() {
    for accepted in [false, true] {
        let mut node = single(false, 64);
        let sender = node.driver.sender();
        let change = if accepted {
            add(2)
        } else {
            MembershipChange::RemoveNode {
                node_id: NodeId(99),
            }
        };
        let mut request = propose(&node, change);
        let mut reads: Vec<Read> = Vec::new();
        let mut completed = 0;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            reads.retain_mut(|read| {
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
            node.pump();
            if let Poll::Ready(result) = poll(&mut request) {
                if accepted {
                    result.unwrap();
                } else {
                    assert!(matches!(
                        result,
                        Err(DurableMembershipError::Rejected { .. })
                    ));
                }
                break;
            }
            assert!(
                completed < 64,
                "client backlog starved membership completion"
            );
            assert!(std::time::Instant::now() < deadline);
        }
        node.until(|_| {
            reads.retain_mut(|read| match poll(read) {
                Poll::Ready(result) => {
                    result.unwrap();
                    false
                }
                Poll::Pending => true,
            });
            reads.is_empty()
        });
        assert_eq!(node.reads.retained().reads, 0);
    }
}
