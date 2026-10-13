#![cfg(feature = "durable")]
//! Caught backend panics close admission before another owner turn.

#[path = "durable_panics/support.rs"]
mod support;
#[allow(
    dead_code,
    unused_imports,
    reason = "shared real-worker application fixture"
)]
#[path = "durable_client/support.rs"]
mod write_support;
use support::*;

#[test]
fn caught_consensus_storage_panic_closes_both_clients_before_another_poll() {
    let mut fixture = Fixture::new(false);
    let writes = fixture.owner.sender();
    let reads = fixture.owner.read_sender().unwrap();
    let mut write = writes.write(vec![7], WriteOptions::default());
    let mut read = reads.read(vec![0], ReadConsistency::Local, ReadOptions::default());
    fixture.storage_fault.arm();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = fixture.owner.try_step(Input::Tick);
    }));
    assert!(panic.is_err());
    assert_eq!(fixture.storage_fault.hits(), 1);
    fixture.assert_fenced();
    assert!(poll(&mut write).is_pending());
    assert!(poll(&mut read).is_pending());
    let checker = fixture.released_waker();
    assert!(write
        .as_mut()
        .poll(&mut Context::from_waker(&checker))
        .is_pending());
    assert!(read
        .as_mut()
        .poll(&mut Context::from_waker(&checker))
        .is_pending());
    drop(fixture.owner);
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
fn caught_proposal_persistence_panic_preserves_uncertain_write_and_closes_clients() {
    let mut fixture = Fixture::new(true);
    let mut write = fixture.identified_write();
    fixture.storage_fault.arm();
    fixture.catch_poll_panic();
    assert_eq!(fixture.storage_fault.hits(), 1);
    assert_eq!(fixture.adapter_fault.hits(), 0);
    fixture.assert_fenced();
    assert!(poll(&mut write).is_pending());
    let checker = fixture.released_waker();
    assert!(write
        .as_mut()
        .poll(&mut Context::from_waker(&checker))
        .is_pending());
    drop(fixture.owner);
    assert_unknown(&mut write);
}

#[test]
fn caught_command_adapter_panic_preserves_uncertain_write_and_closes_clients() {
    let mut fixture = Fixture::new(true);
    let mut write = fixture.identified_write();
    fixture.adapter_fault.arm();
    fixture.catch_poll_panic();
    assert_eq!(fixture.storage_fault.hits(), 0);
    assert_eq!(fixture.adapter_fault.hits(), 1);
    fixture.assert_fenced();
    assert!(poll(&mut write).is_pending());
    let checker = fixture.released_waker();
    assert!(write
        .as_mut()
        .poll(&mut Context::from_waker(&checker))
        .is_pending());
    drop(fixture.owner);
    assert_unknown(&mut write);
}

#[test]
fn busy_consensus_refusal_keeps_healthy_clients_open() {
    let mut fixture = Fixture::new(true);
    // An invalid transfer emits a real protocol rejection; its undelivered
    // output must temporarily close consensus admission without poisoning it.
    fixture
        .owner
        .try_step(Input::TransferLeadership { target: NodeId(2) })
        .unwrap();
    assert_eq!(fixture.owner.state(), ServiceState::Busy);
    assert!(matches!(
        fixture.owner.try_step(Input::Tick),
        Err(DurableClientStepError::Service(
            ServiceStepError::Unavailable {
                state: ServiceState::Busy,
                ..
            }
        ))
    ));
    assert_eq!(fixture.owner.state(), ServiceState::Busy);
    let mut write = fixture.identified_write();
    let mut read = fixture.owner.read_sender().unwrap().read(
        vec![0],
        ReadConsistency::Local,
        ReadOptions::default(),
    );
    assert!(poll(&mut write).is_pending());
    assert!(poll(&mut read).is_pending());
    let deadline = deadline();
    let mut acknowledged = false;
    let mut queried = false;
    while !acknowledged || !queried {
        let _ = fixture.owner.poll_next().unwrap();
        if !acknowledged {
            if let Poll::Ready(result) = poll(&mut write) {
                assert_eq!(result.unwrap().result.value, 7);
                acknowledged = true;
            }
        }
        if !queried {
            if let Poll::Ready(result) = poll(&mut read) {
                assert!(result.unwrap().proof.is_none());
                queried = true;
            }
        }
        assert!(std::time::Instant::now() < deadline);
        std::thread::yield_now();
    }
    assert_ne!(fixture.owner.state(), ServiceState::Failed);
}
