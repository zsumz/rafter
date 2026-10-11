//! One executor callback must not strand the remaining metrics subscribers.
use super::{tests::metrics, MetricsPublisher};
use rafter::{LogIndex, Role, Term};
use std::{
    future::Future,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{Arc, Mutex},
    task::{Context, Poll, Wake, Waker},
};

struct Task {
    id: usize,
    panic: bool,
    publisher: MetricsPublisher<u64>,
    notified: Arc<Mutex<Vec<usize>>>,
}
impl Wake for Task {
    fn wake(self: Arc<Self>) {
        // Reentry also proves that neither publisher nor waiter lock is held.
        assert_eq!(self.publisher.current().group_id, 7);
        self.notified.lock().unwrap().push(self.id);
        if self.panic {
            std::panic::panic_any(self.id);
        }
    }
}

fn check_notify_all(close: bool) {
    let publisher = MetricsPublisher::new(metrics(Role::Leader, Term(1), LogIndex(2)));
    let notified = Arc::new(Mutex::new(Vec::new()));
    let mut watches: Vec<_> = (0..3).map(|_| publisher.watch()).collect();
    let mut futures: Vec<_> = watches.iter_mut().map(|w| Box::pin(w.changed())).collect();
    for (id, future) in futures.iter_mut().enumerate() {
        let waker = Waker::from(Arc::new(Task {
            id,
            panic: id < 2,
            publisher: publisher.clone(),
            notified: Arc::clone(&notified),
        }));
        assert!(future
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
    }
    let next = metrics(Role::Follower, Term(2), LogIndex(3));
    let panic = catch_unwind(AssertUnwindSafe(|| {
        if close {
            publisher.close();
        } else {
            assert!(publisher.publish(next.clone()));
        }
    }))
    .unwrap_err();
    assert_eq!(
        *panic.downcast::<usize>().unwrap(),
        0,
        "first panic survives"
    );
    assert_eq!(*notified.lock().unwrap(), [0, 1, 2]);
    for future in &mut futures {
        assert_eq!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready((!close).then(|| next.clone()))
        );
    }
    drop(futures);
    assert!(super::lock_state(&publisher.shared).waiters.is_empty());
    if close {
        publisher.close();
        assert!(!publisher.publish(next));
    } else {
        // Catching the callback panic does not poison the publisher's locks.
        let later = metrics(Role::Leader, Term(3), LogIndex(4));
        assert!(publisher.publish(later.clone()));
        assert_eq!(
            Box::pin(watches[2].changed())
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Some(later))
        );
    }
}

#[test]
fn publication_notifies_every_subscriber_before_resuming_a_callback_panic() {
    check_notify_all(false);
}

#[test]
fn closure_notifies_every_subscriber_before_resuming_a_callback_panic() {
    check_notify_all(true);
}
