//! Quorum cuts and caller floors are distinct from application callbacks.
use super::support::*;

#[test]
fn election_noop_needs_no_phantom_application_update_and_result_retains_credit() {
    let mut replica = Replica::single(1, false);
    let mut read = replica.read(ReadConsistency::Linearizable);
    drain(&mut replica.owner);
    assert_eq!(replica.sender.retained().reads, 1);
    assert!(matches!(
        poll(&mut replica.read(ReadConsistency::Local)),
        Poll::Ready(Err(ReadError::Backpressure))
    ));
    let Poll::Ready(Ok(receipt)) = poll(&mut read) else {
        panic!("missing read");
    };
    let proof = receipt.proof.unwrap();
    assert_eq!(proof.read_index, LogIndex(1));
    assert_eq!(proof.required_applied_index, LogIndex::ZERO);
    assert_eq!(proof.local_applied_index, LogIndex::ZERO);
    assert_eq!(receipt.result, 0);
    assert_eq!(replica.sender.retained().reads, 0);
    assert_eq!(replica.sender.retained().payload_bytes, 0);
}

#[test]
fn caller_floors_are_never_lowered_and_local_reads_have_no_quorum_proof() {
    let mut replica = Replica::single(2, false);
    let mut local = replica.read(ReadConsistency::Local);
    drain(&mut replica.owner);
    assert!(matches!(
        poll(&mut local),
        Poll::Ready(Ok(QueryReceipt {
            proof: None,
            result: 0
        }))
    ));
    for consistency in [ReadConsistency::Local, ReadConsistency::Linearizable] {
        let mut read = replica.sender.read(
            vec![0],
            consistency,
            ReadOptions::default().with_min_applied_index(LogIndex(50)),
        );
        drain(&mut replica.owner);
        assert!(matches!(
            poll(&mut read),
            Poll::Ready(Err(ReadError::FreshnessUnavailable {
                required_applied_index: LogIndex(50),
                local_applied_index: LogIndex::ZERO,
                ..
            }))
        ));
    }
    assert_eq!(replica.queries.load(Ordering::Acquire), 1);
    let mut padded = Vec::with_capacity(65);
    padded.push(0);
    assert!(matches!(
        poll(
            &mut replica
                .sender
                .read(padded, ReadConsistency::Local, ReadOptions::default())
        ),
        Poll::Ready(Err(ReadError::Backpressure))
    ));
    assert!(matches!(
        poll(&mut replica.read(ReadConsistency::LeaseRead)),
        Poll::Ready(Err(ReadError::UnsupportedConsistency { .. }))
    ));
    assert_eq!(replica.sender.retained().reads, 0);
}

#[test]
fn local_and_linearizable_queries_wait_for_a_paused_application_transaction() {
    let mut replica = Replica::single(2, true);
    let mut write = replica
        .owner
        .sender()
        .write(vec![7], WriteOptions::default());
    until(&mut replica.owner, |_| replica.entered.try_recv().is_ok());
    let mut local = replica.read(ReadConsistency::Local);
    let mut linear = replica.read(ReadConsistency::Linearizable);
    drain(&mut replica.owner);
    assert!(poll(&mut local).is_pending());
    assert!(poll(&mut linear).is_pending());
    assert_eq!(replica.queries.load(Ordering::Acquire), 0);
    replica.release.now();
    until(&mut replica.owner, |_| {
        replica.queries.load(Ordering::Acquire) == 2
    });
    assert!(matches!(
        poll(&mut local),
        Poll::Ready(Ok(QueryReceipt {
            result: 7,
            proof: None
        }))
    ));
    let Poll::Ready(Ok(receipt)) = poll(&mut linear) else {
        panic!("missing durable query");
    };
    assert_eq!(receipt.result, 7);
    let proof = receipt.proof.unwrap();
    assert_eq!(proof.required_applied_index, LogIndex(2));
    assert_eq!(proof.local_applied_index, LogIndex(2));
    until(&mut replica.owner, |_| {
        matches!(poll(&mut write), Poll::Ready(Ok(_)))
    });
}
