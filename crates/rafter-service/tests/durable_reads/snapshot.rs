//! Snapshot failure closes admission without an extra owner turn.
use super::support::*;
use rafter_runtime::service::ServiceError;
use rafter_service::WriteError;

mod support;
use support::{handoff, SnapshotApp, FLOOR};

#[test]
fn invalid_snapshot_resumption_closes_clients_before_another_owner_poll() {
    let mut replica = Replica::single(2, false);
    let writes = replica.owner.sender();
    let mut write = writes.write(vec![7], WriteOptions::default());
    let mut read = replica.read(ReadConsistency::Local);
    assert!(poll(&mut write).is_pending());
    assert!(poll(&mut read).is_pending());
    let replacement = App {
        floor: LogIndex::ZERO,
        value: 0,
        gate: None,
        dropped: Arc::new(AtomicBool::new(false)),
    };
    assert!(matches!(
        replica.owner.resume_snapshot(replacement),
        Err(ServiceError::NotReady(ServiceState::Ready))
    ));
    assert_eq!(replica.owner.state(), ServiceState::Failed);
    // Do not poll the owner: failure itself must close both cloned endpoints.
    assert!(matches!(
        poll(&mut writes.write(vec![8], WriteOptions::default())),
        Poll::Ready(Err(WriteError::ShuttingDown))
    ));
    assert!(matches!(
        poll(&mut replica.read(ReadConsistency::Local)),
        Poll::Ready(Err(ReadError::ShuttingDown))
    ));
    assert_eq!(writes.retained().writes, 1);
    assert_eq!(replica.sender.retained().reads, 1);
    assert!(poll(&mut write).is_pending());
    assert!(poll(&mut read).is_pending());
    assert!(!replica.dropped.load(Ordering::Acquire));
    drop(replica.owner);
    assert!(replica.dropped.load(Ordering::Acquire));
    assert!(matches!(
        poll(&mut write),
        Poll::Ready(Err(WriteError::ShuttingDown))
    ));
    assert!(matches!(
        poll(&mut read),
        Poll::Ready(Err(ReadError::ShuttingDown))
    ));
}

#[test]
fn snapshot_floor_mismatch_closes_both_endpoints_without_releasing_owned_waiters() {
    let (mut owner, application) = handoff();
    let reads = owner.read_sender().unwrap();
    let writes = owner.sender();
    let mut read = reads.read(vec![0], ReadConsistency::Local, ReadOptions::default());
    let mut write = writes.write(vec![7], WriteOptions::default());
    assert!(matches!(
        owner.resume_snapshot(application),
        Err(ServiceError::ApplicationFloor {
            expected: FLOOR,
            actual: LogIndex::ZERO,
        })
    ));
    assert_eq!(owner.state(), ServiceState::Failed);
    assert!(matches!(
        poll(&mut reads.read(vec![0], ReadConsistency::Local, ReadOptions::default())),
        Poll::Ready(Err(ReadError::ShuttingDown))
    ));
    assert!(matches!(
        poll(&mut writes.write(vec![8], WriteOptions::default())),
        Poll::Ready(Err(WriteError::ShuttingDown))
    ));
    assert!(poll(&mut read).is_pending());
    assert!(poll(&mut write).is_pending());
    drop(owner);
    assert!(matches!(
        poll(&mut read),
        Poll::Ready(Err(ReadError::ShuttingDown))
    ));
    assert!(matches!(
        poll(&mut write),
        Poll::Ready(Err(WriteError::ShuttingDown))
    ));
}

#[test]
fn caught_snapshot_floor_panic_fences_owner_and_cannot_resume_again() {
    let (mut owner, mut application) = handoff();
    let reads = owner.read_sender().unwrap();
    let writes = owner.sender();
    let mut read = reads.read(vec![0], ReadConsistency::Local, ReadOptions::default());
    application.panic_floor = true;
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
        || owner.resume_snapshot(application)
    ))
    .is_err());
    assert_eq!(owner.state(), ServiceState::Failed);
    assert!(matches!(
        poll(&mut reads.read(vec![0], ReadConsistency::Local, ReadOptions::default())),
        Poll::Ready(Err(ReadError::ShuttingDown))
    ));
    assert!(matches!(
        poll(&mut writes.write(vec![8], WriteOptions::default())),
        Poll::Ready(Err(WriteError::ShuttingDown))
    ));
    assert!(owner.poll_next().is_err());
    assert!(owner.try_step(Input::Tick).is_err());
    assert!(matches!(
        owner.resume_snapshot(SnapshotApp {
            floor: FLOOR,
            value: 9,
            panic_floor: false
        }),
        Err(ServiceError::Failed)
    ));
    assert!(poll(&mut read).is_pending());
    drop(owner);
    assert!(matches!(
        poll(&mut read),
        Poll::Ready(Err(ReadError::ShuttingDown))
    ));
}

#[test]
fn valid_snapshot_resumption_preserves_clients_and_queries_installed_state() {
    let (mut owner, mut application) = handoff();
    assert!(owner.metrics().is_none());
    let reads = owner.read_sender().unwrap();
    let read = reads.read(vec![0], ReadConsistency::Local, ReadOptions::default());
    application.floor = FLOOR;
    application.value = 9;
    owner.resume_snapshot(application).unwrap();
    let later = reads.read(vec![0], ReadConsistency::Local, ReadOptions::default());
    let mut write = owner.sender().write(vec![7], WriteOptions::default());
    assert!(
        poll(&mut write).is_pending(),
        "successful resume closed writes"
    );
    drop(write);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    for mut read in [read, later] {
        loop {
            let _ = owner.poll_next().unwrap();
            if let Poll::Ready(result) = poll(&mut read) {
                let receipt = result.unwrap();
                assert_eq!(receipt.result, 9);
                assert!(receipt.proof.is_none());
                break;
            }
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
        }
    }
    assert_eq!(owner.state(), ServiceState::Ready);
    assert_eq!(owner.durable_through(), Some(FLOOR));
    assert_eq!(owner.metrics().unwrap().applied_index, FLOOR);
}
