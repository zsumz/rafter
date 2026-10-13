//! External-consumer cancellation contract, also run against extracted archives.
use rafter::{LogIndex, NodeConfig, NodeId};
use rafter_app::group::RaftGroup;
use rafter_reference_ledger::{LedgerConfig, LedgerStateMachine};
use rafter_runtime::DurableRaftNode;
use rafter_service::MetricsPublisher;
use std::{
    future::Future,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};

struct Task;
impl Wake for Task {
    fn wake(self: Arc<Self>) {}
}

#[test]
fn metrics_waits_release_replaced_and_canceled_executor_tasks_without_publication() {
    let node = DurableRaftNode::new(
        NodeConfig::new(NodeId(1), vec![], 1).unwrap(),
        rafter_storage::InMemoryRaftHardStateStore::new(),
    )
    .unwrap();
    let app = LedgerStateMachine::new(LedgerConfig::new(1, 2).unwrap());
    let group = RaftGroup::new(7_u64, NodeId(1), node, app);
    let publisher = MetricsPublisher::new(group.metrics());
    let mut watch = publisher.watch();
    for _ in 0..128 {
        let mut changed = Box::pin(watch.changed());
        let mut previous = None;
        for _ in 0..2 {
            let task = Arc::new(Task);
            let retained = Arc::downgrade(&task);
            let waker = Waker::from(task);
            assert!(changed
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending());
            drop(waker);
            if let Some(old) = previous.replace(retained) {
                assert!(old.upgrade().is_none());
            }
        }
        drop(changed);
        assert!(previous.unwrap().upgrade().is_none());
    }
    assert_eq!(watch.current().applied_index, LogIndex::ZERO);
    let update = group.metrics();
    assert!(publisher.publish(update.clone()));
    assert_eq!(
        Box::pin(watch.changed())
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Some(update))
    );
}

struct Notify {
    calls: Arc<std::sync::atomic::AtomicUsize>,
    panic: bool,
}
impl Wake for Notify {
    fn wake(self: Arc<Self>) {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        assert!(!self.panic, "subscriber wake failed");
    }
}

#[test]
fn closing_metrics_notifies_later_subscribers_even_when_an_earlier_wakeup_panics() {
    let node = DurableRaftNode::new(
        NodeConfig::new(NodeId(1), vec![], 1).unwrap(),
        rafter_storage::InMemoryRaftHardStateStore::new(),
    )
    .unwrap();
    let app = LedgerStateMachine::new(LedgerConfig::new(1, 2).unwrap());
    let group = RaftGroup::new(7_u64, NodeId(1), node, app);
    let publisher = MetricsPublisher::new(group.metrics());
    let mut first = publisher.watch();
    let mut second = publisher.watch();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut waits = [Box::pin(first.changed()), Box::pin(second.changed())];
    for (id, wait) in waits.iter_mut().enumerate() {
        let waker = Waker::from(Arc::new(Notify {
            calls: Arc::clone(&calls),
            panic: id == 0,
        }));
        assert!(wait
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
    }
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| publisher.close())).is_err());
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    for wait in &mut waits {
        assert_eq!(
            wait.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(None)
        );
    }
    publisher.close();
}
