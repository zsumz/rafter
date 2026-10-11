#![cfg(feature = "durable")]
//! Real service workers and deterministic barriers at client lifetime edges.

#[path = "durable_client/support.rs"]
mod support;
use support::*;

#[test]
fn write_waits_for_application_durability_and_retains_uncollected_credit() {
    let (mut owner, sender, entered, mut release, _) = setup(1, true);
    let mut write = sender.write(vec![7], WriteOptions::default());
    until(&mut owner, |_| entered.try_recv().is_ok());
    assert!(poll(&mut write).is_pending());
    assert_eq!(sender.retained().writes, 1);
    assert!(
        matches!(poll(&mut sender.write(vec![8], WriteOptions::default())), Poll::Ready(Err(ref error)) if error.fate() == WriteFate::NotAppended)
    );
    release.now();
    until(&mut owner, |owner| {
        owner.durable_through() == Some(LogIndex(2))
    });
    // Consume the completion event, but deliberately leave the future unpolled.
    drain(&mut owner);
    assert_eq!(sender.retained().writes, 1);
    let Poll::Ready(Ok(receipt)) = poll(&mut write) else {
        panic!("missing durable receipt")
    };
    assert_eq!(receipt.index, LogIndex(2));
    assert_eq!(receipt.result.value, 7);
    assert_eq!(sender.retained().writes, 0);
}

#[test]
fn canceled_accepted_write_keeps_credit_until_the_owner_consumes_completion() {
    let (mut owner, sender, entered, mut release, _) = setup(1, true);
    let write = sender.write(vec![9], WriteOptions::default());
    until(&mut owner, |_| entered.try_recv().is_ok());
    drop(write);
    assert_eq!(sender.retained().writes, 1);
    assert!(
        matches!(poll(&mut sender.write(vec![10], WriteOptions::default())), Poll::Ready(Err(ref error)) if error.fate() == WriteFate::NotAppended)
    );
    release.now();
    until(&mut owner, |_| sender.retained().writes == 0);
    assert_eq!(owner.durable_through(), Some(LogIndex(2)));
}

#[test]
fn dropping_owner_releases_backend_before_unknown_outcome_and_cannot_be_prevented_by_sender() {
    let (mut owner, sender, entered, mut release, dropped) = setup(1, true);
    let mut write = sender.write(vec![11], WriteOptions::default());
    until(&mut owner, |_| entered.try_recv().is_ok());
    let waker = Waker::from(Arc::new(CheckReleased(Arc::clone(&dropped))));
    assert!(write
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    let (done, finished) = mpsc::sync_channel(1);
    let thread = std::thread::spawn(move || {
        drop(owner);
        done.send(()).unwrap();
    });
    assert!(finished.recv_timeout(Duration::from_millis(25)).is_err());
    assert!(write
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    release.now();
    finished.recv_timeout(Duration::from_secs(5)).unwrap();
    thread.join().unwrap();
    assert!(dropped.load(Ordering::Acquire));
    assert!(matches!(
        poll(&mut write),
        Poll::Ready(Err(WriteError::UnknownOutcome {
            local_proposal_id: LocalProposalId(1),
            ..
        }))
    ));
    assert!(matches!(
        poll(&mut sender.write(vec![12], WriteOptions::default())),
        Poll::Ready(Err(WriteError::ShuttingDown))
    ));
}

#[test]
fn queued_cancellation_shutdown_and_capacity_refusal_do_not_append() {
    let (mut owner, sender, _, _release, _) = setup(2, false);
    let canceled = sender.write(vec![1], WriteOptions::default());
    drop(canceled);
    drain(&mut owner);
    assert_eq!(sender.retained().writes, 0);
    let mut padded = Vec::with_capacity(1024);
    padded.push(2);
    assert!(
        matches!(poll(&mut sender.write(padded, WriteOptions::default())), Poll::Ready(Err(ref error)) if error.fate() == WriteFate::NotAppended)
    );
    let mut queued = sender.write(vec![3], WriteOptions::default());
    owner.begin_shutdown();
    drain(&mut owner);
    let application = owner.finish_shutdown().unwrap();
    assert_eq!(application.floor, LogIndex::ZERO);
    assert!(matches!(
        poll(&mut queued),
        Poll::Ready(Err(WriteError::ShuttingDown))
    ));
}

#[test]
fn raw_proposals_cannot_collide_with_managed_local_ids() {
    let (mut owner, _, _, _release, _) = setup(2, false);
    let input = Input::TrackedClientProposal {
        proposal_id: LocalProposalId(1),
        payload: vec![1],
    };
    assert!(
        matches!(owner.try_step(input.clone()), Err(DurableClientStepError::UnmanagedProposal(returned)) if *returned == input)
    );
}

#[test]
fn queued_cancellation_releases_its_credit_while_another_application_write_is_stalled() {
    let (mut owner, sender, entered, _release, _) = setup(2, true);
    let first = sender.write(vec![1], WriteOptions::default());
    until(&mut owner, |_| entered.try_recv().is_ok());
    let second = sender.write(vec![2], WriteOptions::default());
    assert_eq!(sender.retained().writes, 2);
    drop(second);
    let _ = owner.poll_next().unwrap();
    assert_eq!(sender.retained().writes, 1);
    drop(first);
}

#[test]
fn caught_outcome_clone_panic_fences_owner_until_backend_is_discarded() {
    let (mut owner, sender, _, _release, dropped) = setup(2, false);
    let mut write = sender.write(vec![255], WriteOptions::default());
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        until(&mut owner, |owner| {
            owner.durable_through() == Some(LogIndex(2))
        });
        drain(&mut owner);
    }));
    assert!(panic.is_err());
    assert_eq!(owner.state(), rafter_runtime::service::ServiceState::Failed);
    assert!(owner.poll_next().is_err());
    assert!(owner.try_step(Input::Tick).is_err());
    assert!(matches!(
        poll(&mut sender.write(vec![1], WriteOptions::default())),
        Poll::Ready(Err(WriteError::ShuttingDown))
    ));
    assert!(poll(&mut write).is_pending());
    drop(owner);
    assert!(dropped.load(Ordering::Acquire));
    assert!(matches!(
        poll(&mut write),
        Poll::Ready(Err(WriteError::UnknownOutcome { .. }))
    ));
}

#[test]
fn application_failure_closes_admission_and_waits_for_backend_disposal() {
    let (mut owner, sender, _, _release, dropped) = setup(1, false);
    let mut write = sender.write(vec![254], WriteOptions::default());
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while owner.poll_next().is_ok() {
        assert!(std::time::Instant::now() < deadline);
        std::thread::yield_now();
    }
    assert!(matches!(
        poll(&mut sender.write(vec![1], WriteOptions::default())),
        Poll::Ready(Err(WriteError::ShuttingDown))
    ));
    assert!(poll(&mut write).is_pending());
    drop(owner);
    assert!(dropped.load(Ordering::Acquire));
    assert!(matches!(
        poll(&mut write),
        Poll::Ready(Err(WriteError::UnknownOutcome { .. }))
    ));
}

#[test]
fn panicking_terminal_waker_does_not_strand_other_clients() {
    struct PanicWake;
    impl std::task::Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            panic!("injected wake panic");
        }
    }
    let (owner, sender, _, _release, dropped) = setup(2, false);
    let mut first = sender.write(vec![1], WriteOptions::default());
    let mut second = sender.write(vec![2], WriteOptions::default());
    assert!(first
        .as_mut()
        .poll(&mut Context::from_waker(&Waker::from(Arc::new(PanicWake))))
        .is_pending());
    let checker = Waker::from(Arc::new(CheckReleased(Arc::clone(&dropped))));
    assert!(second
        .as_mut()
        .poll(&mut Context::from_waker(&checker))
        .is_pending());
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(owner)));
    assert!(panic.is_err());
    assert!(dropped.load(Ordering::Acquire));
    assert!(matches!(
        poll(&mut first),
        Poll::Ready(Err(WriteError::ShuttingDown))
    ));
    assert!(matches!(
        poll(&mut second),
        Poll::Ready(Err(WriteError::ShuttingDown))
    ));
    assert_eq!(sender.retained().writes, 0);
}
