//! The group lifetime outlives canceled requests but not backend disposal.
use super::support::*;
use std::task::Wake;

#[test]
fn canceling_the_first_shutdown_does_not_reopen_or_strand_the_second() {
    let mut node = Replica::single(true);
    let tx = node.driver.sender();
    let write = tx.write(GROUP, vec![7], WriteOptions::default());
    node.until(|n| n.entered.try_recv().is_ok());
    let first = tx.shutdown(GROUP);
    drop(first);
    let mut second = tx.shutdown(GROUP);
    node.drain();
    assert!(poll(&mut second).is_pending());
    node.release.now();
    node.until(|n| n.driver.state() == ServiceState::Stopped);
    assert!(matches!(
        poll(&mut second),
        Poll::Ready(Err(ShutdownError::AlreadyShutDown))
    ));
    drop(write);
}

struct Terminal {
    dropped: Arc<AtomicBool>,
    calls: Arc<AtomicUsize>,
    panic: bool,
}
impl Wake for Terminal {
    fn wake(self: Arc<Self>) {
        assert!(
            self.dropped.load(Ordering::Acquire),
            "backend must be released before terminal wake"
        );
        self.calls.fetch_add(1, Ordering::AcqRel);
        assert!(!self.panic, "injected terminal callback panic");
    }
}

#[test]
fn panicking_write_and_metrics_wakeups_do_not_strand_shutdown_or_transfer() {
    let node = Replica::single(false);
    let tx = node.driver.sender();
    let mut write = tx.write(GROUP, vec![7], WriteOptions::default());
    let mut transfer = tx.transfer_leadership(GROUP, NodeId(1));
    let mut metrics = tx.metrics(GROUP).unwrap();
    let mut changed = Box::pin(metrics.changed());
    let mut shutdown = tx.shutdown(GROUP);
    let calls = Arc::new(AtomicUsize::new(0));
    let panic = Waker::from(Arc::new(Terminal {
        dropped: Arc::clone(&node.dropped),
        calls: Arc::clone(&calls),
        panic: true,
    }));
    let ordinary = Waker::from(Arc::new(Terminal {
        dropped: Arc::clone(&node.dropped),
        calls: Arc::clone(&calls),
        panic: false,
    }));
    assert!(write
        .as_mut()
        .poll(&mut Context::from_waker(&panic))
        .is_pending());
    assert!(changed
        .as_mut()
        .poll(&mut Context::from_waker(&panic))
        .is_pending());
    assert!(transfer
        .as_mut()
        .poll(&mut Context::from_waker(&ordinary))
        .is_pending());
    assert!(shutdown
        .as_mut()
        .poll(&mut Context::from_waker(&ordinary))
        .is_pending());
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(node))).is_err());
    assert_eq!(calls.load(Ordering::Acquire), 4);
    assert!(matches!(
        poll(&mut write),
        Poll::Ready(Err(WriteError::ShuttingDown))
    ));
    assert!(matches!(
        poll(&mut transfer),
        Poll::Ready(Err(TransferLeadershipError::ShuttingDown))
    ));
    assert!(matches!(poll(&mut shutdown), Poll::Ready(Ok(()))));
    assert!(matches!(poll(&mut changed), Poll::Ready(None)));
}

#[test]
fn query_panic_fences_all_group_operations_until_backend_release() {
    let mut node = Replica::single(false);
    let tx = node.driver.sender();
    let mut read = tx.read(
        GROUP,
        vec![255],
        ReadConsistency::Local,
        ReadOptions::default(),
    );
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| node.drain())).is_err());
    assert_eq!(node.driver.state(), ServiceState::Failed);
    assert!(node.driver.poll_next().is_err());
    assert!(matches!(
        tx.metrics(GROUP).unwrap().current().fatal_state,
        rafter_app::group::GroupFatalState::Poisoned { .. }
    ));
    assert!(matches!(
        poll(&mut tx.transfer_leadership(GROUP, NodeId(1))),
        Poll::Ready(Err(TransferLeadershipError::ShuttingDown))
    ));
    assert!(poll(&mut read).is_pending());
    let mut shutdown = tx.shutdown(GROUP);
    assert!(poll(&mut shutdown).is_pending());
    drop(node);
    assert!(matches!(
        poll(&mut read),
        Poll::Ready(Err(ReadError::ShuttingDown))
    ));
    assert!(matches!(poll(&mut shutdown), Poll::Ready(Ok(()))));
}

struct PanicUpdate;
impl Wake for PanicUpdate {
    fn wake(self: Arc<Self>) {
        panic!("injected metrics update panic");
    }
}

#[test]
fn metrics_update_panic_fences_the_wrapper_even_when_the_inner_owner_succeeded() {
    let mut node = Replica::single(false);
    let tx = node.driver.sender();
    let mut watch = tx.metrics(GROUP).unwrap();
    let mut changed = Box::pin(watch.changed());
    let waker = Waker::from(Arc::new(PanicUpdate));
    assert!(changed
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    let mut read = tx.read(
        GROUP,
        vec![0],
        ReadConsistency::Linearizable,
        ReadOptions::default(),
    );
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| node.drain())).is_err());
    assert_eq!(node.driver.state(), ServiceState::Failed);
    assert!(matches!(
        poll(&mut tx.write(GROUP, vec![8], WriteOptions::default())),
        Poll::Ready(Err(WriteError::ShuttingDown))
    ));
    assert!(poll(&mut read).is_pending());
    drop(changed);
    drop(node);
    assert!(matches!(
        poll(&mut read),
        Poll::Ready(Err(ReadError::ShuttingDown))
    ));
}

#[test]
fn transport_callback_panic_fences_admission_and_preserves_transfer_uncertainty() {
    let mut cluster = Cluster::new();
    let tx = cluster.nodes[0].driver.sender();
    let mut transfer = tx.transfer_leadership(GROUP, NodeId(2));
    cluster.nodes[0].pump();
    cluster.link.panicking.store(true, Ordering::Release);
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| cluster.nodes[0].drain()))
            .is_err()
    );
    assert_eq!(cluster.nodes[0].driver.state(), ServiceState::Failed);
    assert!(matches!(
        poll(&mut tx.write(GROUP, vec![8], WriteOptions::default())),
        Poll::Ready(Err(WriteError::ShuttingDown))
    ));
    assert!(poll(&mut transfer).is_pending());
    drop(cluster);
    assert!(matches!(
        poll(&mut transfer),
        Poll::Ready(Err(TransferLeadershipError::Unconfirmed))
    ));
}
