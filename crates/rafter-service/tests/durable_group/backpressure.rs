//! Count real admission refusals without confusing other errors or driving work.
use super::{membership_admission::*, support::*};
use rafter::{MembershipSet, PromotionBarrier};
use rafter_app::membership::MembershipChange;
use rafter_service::durable::DurableCommandSender;

type Sender = DurableCommandSender<u64, crate::write_support::Outcome, u8>;

fn refuse_clients(tx: &Sender) {
    assert!(matches!(
        poll(&mut tx.write(GROUP, vec![1], WriteOptions::default())),
        Poll::Ready(Err(WriteError::Backpressure))
    ));
    assert!(matches!(
        poll(&mut tx.read(
            GROUP,
            vec![0],
            ReadConsistency::Local,
            ReadOptions::default()
        )),
        Poll::Ready(Err(ReadError::Backpressure))
    ));
}

fn refuse_controls(tx: &Sender) {
    assert!(matches!(
        poll(&mut tx.propose_membership(&GROUP, add(9))),
        Poll::Ready(Err(DurableMembershipError::Backpressure))
    ));
    assert!(matches!(
        poll(&mut tx.transfer_leadership(GROUP, NodeId(1))),
        Poll::Ready(Err(TransferLeadershipError::Backpressure))
    ));
}

#[test]
fn cloned_senders_count_each_budget_refusal_without_driving_the_owner() {
    let mut node = single(false, 64);
    let tx = node.driver.sender();
    let before = node.driver.ready_node().unwrap().last_log_index();
    let writes: Vec<_> = (0..4)
        .map(|_| tx.write(GROUP, vec![1], WriteOptions::default()))
        .collect();
    let reads: Vec<_> = (0..4)
        .map(|_| {
            tx.read(
                GROUP,
                vec![0],
                ReadConsistency::Local,
                ReadOptions::default(),
            )
        })
        .collect();
    let change = tx.propose_membership(&GROUP, add(2));
    let retained = tx.retained();
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let tx = tx.clone();
            scope.spawn(move || {
                for _ in 0..32 {
                    refuse_clients(&tx);
                    refuse_controls(&tx);
                }
            });
        }
    });
    let refused = tx.backpressure_refusals();
    assert_eq!(refused.writes, 128);
    assert_eq!(refused.reads, Some(128));
    assert_eq!(refused.controls, 256);
    assert_eq!(node.writes.backpressure_refusals(), refused.writes);
    assert_eq!(node.reads.backpressure_refusals(), refused.reads.unwrap());
    assert_eq!(tx.retained(), retained);
    assert_eq!(node.driver.ready_node().unwrap().last_log_index(), before);
    assert_eq!(node.queries.load(Ordering::Acquire), 0);
    drop((writes, reads, change));
    node.drain();
    assert_eq!(tx.retained().writes.writes, 0);
    assert_eq!(tx.retained().reads.unwrap().reads, 0);
    assert_eq!(tx.retained().controls, 0);
    assert_eq!(tx.backpressure_refusals(), refused);
}

fn oversized_capacity() -> Vec<u8> {
    let mut bytes = Vec::with_capacity(65);
    bytes.push(0);
    bytes
}

#[test]
fn capacity_and_normalization_refusals_count_once_without_counting_protocol_errors() {
    let mut node = single(false, 16);
    let tx = node.driver.sender();
    assert!(matches!(
        poll(&mut tx.write(GROUP, oversized_capacity(), WriteOptions::default())),
        Poll::Ready(Err(WriteError::Backpressure))
    ));
    assert!(matches!(
        poll(&mut tx.read(
            GROUP,
            oversized_capacity(),
            ReadConsistency::Local,
            ReadOptions::default()
        )),
        Poll::Ready(Err(ReadError::Backpressure))
    ));
    for change in [
        MembershipChange::ChangeVoters {
            target: MembershipSet::new(vec![NodeId(1)], vec![NodeId(2), NodeId(3)]).unwrap(),
        },
        MembershipChange::EnterJoint {
            target: MembershipSet::new(vec![NodeId(1)], vec![NodeId(2)]).unwrap(),
            promotion_barriers: vec![PromotionBarrier {
                learner_id: NodeId(2),
                required_match_index: LogIndex::ZERO,
            }],
        },
    ] {
        assert!(matches!(
            poll(&mut tx.propose_membership(&GROUP, change)),
            Poll::Ready(Err(DurableMembershipError::Backpressure))
        ));
    }
    let refused = tx.backpressure_refusals();
    assert_eq!(
        (refused.writes, refused.reads, refused.controls),
        (1, Some(1), 2)
    );
    let max = rafter_runtime::service::ServiceOptions::new().max_proposal_bytes;
    assert!(matches!(
        poll(&mut tx.write(GROUP, vec![0; max + 1], WriteOptions::default())),
        Poll::Ready(Err(WriteError::PayloadTooLarge { .. }))
    ));
    assert!(matches!(
        poll(&mut tx.read(
            GROUP,
            vec![0],
            ReadConsistency::LeaseRead,
            ReadOptions::default()
        )),
        Poll::Ready(Err(ReadError::UnsupportedConsistency { .. }))
    ));
    let mut invalid =
        tx.propose_membership(&GROUP, MembershipChange::RemoveNode { node_id: NodeId(1) });
    assert!(matches!(
        finish(&mut node, &mut invalid),
        Err(DurableMembershipError::Rejected { .. })
    ));
    let mut transfer = tx.transfer_leadership(GROUP, NodeId(1));
    node.drain();
    assert!(matches!(
        poll(&mut transfer),
        Poll::Ready(Err(TransferLeadershipError::Rejected { .. }))
    ));
    assert_eq!(tx.backpressure_refusals(), refused);
}

#[test]
fn refusal_lifetime_follows_the_shared_budget_and_ignores_closed_or_foreign_requests() {
    let (owner, writes, _entered, _release, dropped) = crate::write_support::setup(1, false);
    let accepted = writes.write(vec![1], WriteOptions::default());
    let mut refused = writes.write(vec![1], WriteOptions::default());
    assert_eq!(writes.backpressure_refusals(), 1);
    assert!(matches!(
        poll(&mut refused),
        Poll::Ready(Err(WriteError::Backpressure))
    ));
    let driver =
        rafter_service::durable::DurableRaftDriver::attach(GROUP, owner, Link::default()).unwrap();
    let tx = driver.sender();
    assert_eq!(tx.backpressure_refusals().writes, 1);
    assert_eq!(tx.backpressure_refusals().reads, None);
    assert!(matches!(
        poll(&mut tx.propose_membership(&GROUP, add(2))),
        Poll::Ready(Err(DurableMembershipError::NotEnabled))
    ));
    assert!(matches!(
        poll(&mut tx.write(GROUP + 1, vec![1], WriteOptions::default())),
        Poll::Ready(Err(WriteError::WrongGroup))
    ));
    assert!(matches!(
        poll(&mut tx.propose_membership(&(GROUP + 1), add(2))),
        Poll::Ready(Err(DurableMembershipError::WrongGroup))
    ));
    assert_eq!(tx.backpressure_refusals().controls, 0);
    let refused = tx.backpressure_refusals();
    drop(driver);
    assert!(dropped.load(Ordering::Acquire));
    assert!(matches!(
        poll(&mut tx.write(GROUP, vec![1], WriteOptions::default())),
        Poll::Ready(Err(WriteError::ShuttingDown))
    ));
    assert!(matches!(
        poll(&mut tx.propose_membership(&GROUP, add(2))),
        Poll::Ready(Err(DurableMembershipError::ShuttingDown))
    ));
    assert!(matches!(
        poll(&mut tx.transfer_leadership(GROUP, NodeId(1))),
        Poll::Ready(Err(TransferLeadershipError::ShuttingDown))
    ));
    drop(accepted);
    assert_eq!(tx.backpressure_refusals(), refused);
    assert_eq!(writes.backpressure_refusals(), refused.writes);
    assert_eq!(tx.retained().writes.writes, 0);
}
