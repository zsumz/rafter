//! Canceled metrics waits must release executor tasks without another publish.
use super::{tests::metrics, MetricsPublisher};
use rafter::{LogIndex, Role, Term};
use std::{
    future::Future,
    sync::Arc,
    task::{Context, Wake, Waker},
};

struct Task;
impl Wake for Task {
    fn wake(self: Arc<Self>) {}
}

#[test]
fn canceling_a_metrics_wait_releases_its_task_while_the_watch_stays_open() {
    let publisher = MetricsPublisher::new(metrics(Role::Leader, Term(1), LogIndex(2)));
    let mut watch = publisher.watch();
    let task = Arc::new(Task);
    let retained = Arc::downgrade(&task);
    let waker = Waker::from(task);
    let mut changed = Box::pin(watch.changed());
    assert!(changed
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    drop(waker);
    assert!(
        retained.upgrade().is_some(),
        "a pending wait owns its task wakeup"
    );
    drop(changed);
    assert!(
        retained.upgrade().is_none(),
        "cancellation must release the executor task without publishing"
    );
    assert_eq!(watch.current().applied_index, LogIndex(2));
}

#[test]
fn repolling_one_metrics_wait_replaces_its_previous_task_waker() {
    let publisher = MetricsPublisher::new(metrics(Role::Leader, Term(1), LogIndex(2)));
    let mut watch = publisher.watch();
    let mut changed = Box::pin(watch.changed());
    let mut previous = None;
    for _ in 0..64 {
        let task = Arc::new(Task);
        let retained = Arc::downgrade(&task);
        let waker = Waker::from(task);
        assert!(changed
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
        drop(waker);
        if let Some(old) = previous.replace(retained) {
            assert!(
                old.upgrade().is_none(),
                "one wait must not accumulate executor tasks"
            );
        }
    }
    drop(changed);
    assert!(previous.unwrap().upgrade().is_none());
}

#[test]
fn repeated_cancellation_releases_registration_records_without_a_publish() {
    let publisher = MetricsPublisher::new(metrics(Role::Leader, Term(1), LogIndex(2)));
    let mut watch = publisher.watch();
    for _ in 0..128 {
        let mut changed = Box::pin(watch.changed());
        assert!(changed
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        let registration = {
            let state = super::lock_state(&publisher.shared);
            assert_eq!(state.waiters.len(), 1);
            Arc::downgrade(&state.waiters[0])
        };
        drop(changed);
        assert!(
            registration.upgrade().is_none(),
            "empty wake records must not accumulate either"
        );
    }
}

#[derive(Default)]
struct Counter(std::sync::atomic::AtomicUsize);
impl Wake for Counter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[test]
fn canceling_one_clone_keeps_the_other_live_and_does_not_consume_an_update() {
    let publisher = MetricsPublisher::new(metrics(Role::Leader, Term(1), LogIndex(2)));
    let mut first = publisher.watch();
    let mut second = first.clone();
    let task = Arc::new(Counter::default());
    let waker = Waker::from(Arc::clone(&task));
    let mut canceled = Box::pin(first.changed());
    let mut live = Box::pin(second.changed());
    assert!(canceled
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    assert!(live
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    drop(canceled);
    let update = metrics(Role::Leader, Term(1), LogIndex(3));
    assert!(publisher.publish(update.clone()));
    assert_eq!(task.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        live.as_mut().poll(&mut Context::from_waker(&waker)),
        std::task::Poll::Ready(Some(update.clone()))
    );
    drop(live);
    assert_eq!(
        Box::pin(first.changed())
            .as_mut()
            .poll(&mut Context::from_waker(&waker)),
        std::task::Poll::Ready(Some(update))
    );
    publisher.close();
    assert_eq!(
        Box::pin(first.changed())
            .as_mut()
            .poll(&mut Context::from_waker(&waker)),
        std::task::Poll::Ready(None)
    );
}

struct InspectOnDrop {
    publisher: MetricsPublisher<u64>,
    count: Arc<std::sync::atomic::AtomicUsize>,
}
impl Wake for InspectOnDrop {
    fn wake(self: Arc<Self>) {}
}
impl Drop for InspectOnDrop {
    fn drop(&mut self) {
        assert_eq!(self.publisher.current().applied_index, LogIndex(2));
        self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

fn bounded_thread(run: impl FnOnce() + Send + 'static) {
    let (done, finished) = std::sync::mpsc::sync_channel(1);
    let worker = std::thread::spawn(move || {
        run();
        done.send(()).unwrap();
    });
    finished
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("callback must not deadlock a watch lock");
    worker.join().unwrap();
}

#[test]
fn replaced_and_canceled_task_destructors_can_reenter_the_publisher() {
    bounded_thread(|| {
        let publisher = MetricsPublisher::new(metrics(Role::Leader, Term(1), LogIndex(2)));
        let mut watch = publisher.watch();
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut changed = Box::pin(watch.changed());
        for expected in 0..16 {
            let waker = Waker::from(Arc::new(InspectOnDrop {
                publisher: publisher.clone(),
                count: Arc::clone(&count),
            }));
            assert!(changed
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending());
            drop(waker);
            assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), expected);
        }
        drop(changed);
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 16);
    });
}

type Change = std::pin::Pin<
    Box<dyn Future<Output = Option<rafter_app::metrics::RaftGroupMetrics<u64>>> + Send>,
>;
struct CancelOnWake {
    future: Arc<std::sync::Mutex<Option<Change>>>,
}
impl Wake for CancelOnWake {
    fn wake(self: Arc<Self>) {
        let future = self.future.lock().unwrap().take();
        drop(future);
    }
}

#[test]
fn publishing_can_wake_a_task_that_cancels_its_own_pending_future() {
    bounded_thread(|| {
        let publisher = MetricsPublisher::new(metrics(Role::Leader, Term(1), LogIndex(2)));
        let mut watch = publisher.watch();
        let future: Change = Box::pin(async move { watch.changed().await });
        let slot = Arc::new(std::sync::Mutex::new(Some(future)));
        let task = Arc::new(CancelOnWake {
            future: Arc::clone(&slot),
        });
        let retained = Arc::downgrade(&task);
        let waker = Waker::from(task);
        assert!(slot
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
        drop(waker);
        assert!(publisher.publish(metrics(Role::Leader, Term(1), LogIndex(3))));
        assert!(slot.lock().unwrap().is_none());
        assert!(retained.upgrade().is_none());
        assert!(super::lock_state(&publisher.shared).waiters.is_empty());
    });
}

#[test]
fn close_wakes_only_the_latest_task_and_releases_it_before_the_future_is_polled() {
    let publisher = MetricsPublisher::new(metrics(Role::Leader, Term(1), LogIndex(2)));
    let mut watch = publisher.watch();
    let old = Arc::new(Counter::default());
    let latest = Arc::new(Counter::default());
    let mut changed = Box::pin(watch.changed());
    for task in [&old, &latest] {
        let waker = Waker::from(Arc::clone(task));
        assert!(changed
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
    }
    assert_eq!(Arc::strong_count(&old), 1);
    assert_eq!(Arc::strong_count(&latest), 2);
    publisher.close();
    assert_eq!(old.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(latest.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(Arc::strong_count(&latest), 1);
    assert_eq!(
        changed
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        std::task::Poll::Ready(None)
    );
}

struct PublishOnDrop(MetricsPublisher<u64>);
impl Wake for PublishOnDrop {
    fn wake(self: Arc<Self>) {}
}
impl Drop for PublishOnDrop {
    fn drop(&mut self) {
        assert!(self.0.publish(metrics(Role::Leader, Term(1), LogIndex(3))));
    }
}

#[test]
fn a_displaced_task_can_publish_without_losing_the_replacement_wakeup() {
    bounded_thread(|| {
        let publisher = MetricsPublisher::new(metrics(Role::Leader, Term(1), LogIndex(2)));
        let mut watch = publisher.watch();
        let mut changed = Box::pin(watch.changed());
        let old = Waker::from(Arc::new(PublishOnDrop(publisher)));
        assert!(changed
            .as_mut()
            .poll(&mut Context::from_waker(&old))
            .is_pending());
        drop(old);
        let task = Arc::new(Counter::default());
        let latest = Waker::from(Arc::clone(&task));
        // Replacing the old waker publishes after the new registration is
        // installed, while this poll is still about to return Pending.
        assert!(changed
            .as_mut()
            .poll(&mut Context::from_waker(&latest))
            .is_pending());
        assert_eq!(task.0.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(
            matches!(changed.as_mut().poll(&mut Context::from_waker(&latest)), std::task::Poll::Ready(Some(update)) if update.applied_index == LogIndex(3))
        );
    });
}
