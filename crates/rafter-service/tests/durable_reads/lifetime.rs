//! Canceled and terminal reads obey the backend's actual lifetime.
use super::support::*;

#[test]
fn queued_cancellation_releases_credit_without_borrowing_a_busy_application() {
    let mut replica = Replica::single(1, true);
    let write = replica
        .owner
        .sender()
        .write(vec![1], WriteOptions::default());
    until(&mut replica.owner, |_| replica.entered.try_recv().is_ok());
    let read = replica.read(ReadConsistency::Local);
    drop(read);
    let _ = replica.owner.poll_next().unwrap();
    assert_eq!(replica.sender.retained().reads, 0);
    assert_eq!(replica.queries.load(Ordering::Acquire), 0);
    drop(write);
}

#[test]
fn caught_query_panic_fences_both_client_queues_until_backend_release() {
    let mut replica = Replica::single(2, false);
    let mut read = replica
        .sender
        .read(vec![255], ReadConsistency::Local, ReadOptions::default());
    let panic =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drain(&mut replica.owner)));
    assert!(panic.is_err());
    assert_eq!(replica.owner.state(), ServiceState::Failed);
    assert!(replica.owner.poll_next().is_err());
    assert!(replica.owner.try_step(Input::Tick).is_err());
    assert!(poll(&mut read).is_pending());
    assert!(matches!(
        poll(&mut replica.read(ReadConsistency::Local)),
        Poll::Ready(Err(ReadError::ShuttingDown))
    ));
    assert!(matches!(
        poll(
            &mut replica
                .owner
                .sender()
                .write(vec![2], WriteOptions::default())
        ),
        Poll::Ready(Err(rafter_service::WriteError::ShuttingDown))
    ));
    drop(replica.owner);
    assert!(replica.dropped.load(Ordering::Acquire));
    assert!(matches!(
        poll(&mut read),
        Poll::Ready(Err(ReadError::ShuttingDown))
    ));
}

#[test]
fn releasing_owner_joins_busy_application_before_publishing_read_failure() {
    let mut replica = Replica::single(1, true);
    let write = replica
        .owner
        .sender()
        .write(vec![3], WriteOptions::default());
    until(&mut replica.owner, |_| replica.entered.try_recv().is_ok());
    let mut read = replica.read(ReadConsistency::Local);
    let waker = Waker::from(Arc::new(CheckReleased(Arc::clone(&replica.dropped))));
    assert!(read
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    let (done, finished) = mpsc::sync_channel(1);
    let thread = std::thread::spawn(move || {
        drop(replica.owner);
        done.send(()).unwrap();
    });
    assert!(finished.recv_timeout(Duration::from_millis(25)).is_err());
    assert!(poll(&mut read).is_pending());
    replica.release.now();
    finished.recv_timeout(Duration::from_secs(5)).unwrap();
    thread.join().unwrap();
    assert!(replica.dropped.load(Ordering::Acquire));
    assert!(matches!(
        poll(&mut read),
        Poll::Ready(Err(ReadError::ShuttingDown))
    ));
    assert!(matches!(
        poll(
            &mut replica
                .sender
                .read(vec![0], ReadConsistency::Local, ReadOptions::default())
        ),
        Poll::Ready(Err(ReadError::ShuttingDown))
    ));
    drop(write);
}

#[test]
fn one_panicking_terminal_waker_cannot_strand_other_reads_or_writes() {
    let replica = Replica::single(2, false);
    let mut first = replica.read(ReadConsistency::Local);
    let mut second = replica.read(ReadConsistency::Local);
    let mut write = replica
        .owner
        .sender()
        .write(vec![4], WriteOptions::default());
    let waker = Waker::from(Arc::new(PanicWake));
    assert!(first
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    assert!(poll(&mut second).is_pending());
    assert!(poll(&mut write).is_pending());
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(replica.owner))).is_err()
    );
    assert!(replica.dropped.load(Ordering::Acquire));
    assert!(matches!(
        poll(&mut first),
        Poll::Ready(Err(ReadError::ShuttingDown))
    ));
    assert!(matches!(
        poll(&mut second),
        Poll::Ready(Err(ReadError::ShuttingDown))
    ));
    assert!(matches!(
        poll(&mut write),
        Poll::Ready(Err(rafter_service::WriteError::ShuttingDown))
    ));
}
struct PanicWake;
impl std::task::Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        panic!("injected terminal waker panic");
    }
}

#[test]
fn a_panicking_completion_waker_fences_the_owner_without_stranding_other_queries() {
    let mut replica = Replica::single(2, false);
    let mut first = replica.read(ReadConsistency::Local);
    let mut second = replica.read(ReadConsistency::Local);
    let waker = Waker::from(Arc::new(PanicWake));
    assert!(first
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drain(&mut replica.owner)))
            .is_err()
    );
    assert_eq!(replica.owner.state(), ServiceState::Failed);
    assert!(poll(&mut second).is_pending());
    assert!(matches!(poll(&mut first), Poll::Ready(Ok(_))));
    assert!(matches!(
        poll(&mut replica.read(ReadConsistency::Local)),
        Poll::Ready(Err(ReadError::ShuttingDown))
    ));
    drop(replica.owner);
    assert!(matches!(
        poll(&mut second),
        Poll::Ready(Err(ReadError::ShuttingDown))
    ));
}
