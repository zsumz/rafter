//! Terminal membership results follow backend release, including callback failure.
use super::{membership_admission::*, support::*};
use std::task::Wake;

#[test]
fn backend_release_distinguishes_queued_from_submitted_membership() {
    let node = single(false, 64);
    let dropped = Arc::clone(&node.dropped);
    let mut queued = propose(&node, add(2));
    drop(node);
    assert!(dropped.load(Ordering::Acquire));
    assert!(matches!(
        poll(&mut queued),
        Poll::Ready(Err(DurableMembershipError::ShuttingDown))
    ));
    let mut node = single(false, 64);
    let dropped = Arc::clone(&node.dropped);
    let mut submitted = propose(&node, add(2));
    node.pump();
    assert!(poll(&mut submitted).is_pending());
    assert!(!dropped.load(Ordering::Acquire));
    drop(node);
    assert!(dropped.load(Ordering::Acquire));
    assert!(matches!(
        poll(&mut submitted),
        Poll::Ready(Err(DurableMembershipError::Unconfirmed))
    ));
}

#[test]
fn shutdown_keeps_queued_membership_pending_until_the_busy_backend_is_released() {
    let mut node = single(true, 64);
    let sender = node.driver.sender();
    let mut write = sender.write(GROUP, vec![7], WriteOptions::default());
    node.until(|n| n.entered.try_recv().is_ok());
    let mut request = propose(&node, add(2));
    let mut shutdown = sender.shutdown(GROUP);
    node.drain();
    assert!(poll(&mut request).is_pending());
    assert!(poll(&mut shutdown).is_pending());
    assert!(!node.dropped.load(Ordering::Acquire));
    assert!(matches!(
        poll(&mut propose(&node, add(3))),
        Poll::Ready(Err(DurableMembershipError::ShuttingDown))
    ));
    node.release.now();
    node.until(|n| n.driver.state() == ServiceState::Stopped);
    assert!(node.dropped.load(Ordering::Acquire));
    assert!(matches!(poll(&mut write), Poll::Ready(Ok(_))));
    assert!(matches!(
        poll(&mut request),
        Poll::Ready(Err(DurableMembershipError::ShuttingDown))
    ));
    assert!(matches!(poll(&mut shutdown), Poll::Ready(Ok(()))));
}

#[test]
fn transport_panic_fences_admission_but_keeps_submitted_membership_uncertain() {
    let mut cluster = cluster(64);
    let dropped = Arc::clone(&cluster.nodes[0].dropped);
    let mut request = propose(&cluster.nodes[0], add(4));
    cluster.nodes[0].pump();
    cluster.link.panicking.store(true, Ordering::Release);
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| cluster.nodes[0].drain()))
            .is_err()
    );
    assert_eq!(cluster.nodes[0].driver.state(), ServiceState::Failed);
    assert!(matches!(
        poll(&mut propose(&cluster.nodes[0], add(5))),
        Poll::Ready(Err(DurableMembershipError::ShuttingDown))
    ));
    assert!(poll(&mut request).is_pending());
    assert!(!dropped.load(Ordering::Acquire));
    drop(cluster);
    assert!(dropped.load(Ordering::Acquire));
    assert!(matches!(
        poll(&mut request),
        Poll::Ready(Err(DurableMembershipError::Unconfirmed))
    ));
}

struct TerminalWake {
    dropped: Arc<AtomicBool>,
    calls: Arc<AtomicUsize>,
    panicking: bool,
}
impl Wake for TerminalWake {
    fn wake(self: Arc<Self>) {
        assert!(
            self.dropped.load(Ordering::Acquire),
            "backend still owned at terminal notification"
        );
        self.calls.fetch_add(1, Ordering::AcqRel);
        assert!(!self.panicking, "injected metrics close panic");
    }
}

#[test]
fn panicking_metrics_close_still_notifies_membership_after_backend_release() {
    let node = single(false, 64);
    let mut request = propose(&node, add(2));
    let mut metrics = node.driver.sender().metrics(GROUP).unwrap();
    let mut changed = Box::pin(metrics.changed());
    let calls = Arc::new(AtomicUsize::new(0));
    let panicking = Waker::from(Arc::new(TerminalWake {
        dropped: Arc::clone(&node.dropped),
        calls: Arc::clone(&calls),
        panicking: true,
    }));
    let ordinary = Waker::from(Arc::new(TerminalWake {
        dropped: Arc::clone(&node.dropped),
        calls: Arc::clone(&calls),
        panicking: false,
    }));
    assert!(changed
        .as_mut()
        .poll(&mut Context::from_waker(&panicking))
        .is_pending());
    assert!(request
        .as_mut()
        .poll(&mut Context::from_waker(&ordinary))
        .is_pending());
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(node))).is_err());
    assert_eq!(calls.load(Ordering::Acquire), 2);
    assert!(matches!(poll(&mut changed), Poll::Ready(None)));
    assert!(matches!(
        poll(&mut request),
        Poll::Ready(Err(DurableMembershipError::ShuttingDown))
    ));
}
