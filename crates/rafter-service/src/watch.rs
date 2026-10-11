//! Observable service state and metrics watches.

use std::{
    future::poll_fn,
    sync::{Arc, Mutex, MutexGuard},
    task::{Context, Poll},
};

use rafter_app::metrics::RaftGroupMetrics;

mod registration;
use registration::{wake_all, Registration, Waiter};

/// Publisher side for managed-service metrics.
#[derive(Clone, Debug)]
pub struct MetricsPublisher<G> {
    shared: Arc<Mutex<MetricsState<G>>>,
}

/// Watch side for managed-service metrics.
#[derive(Clone, Debug)]
pub struct MetricsWatch<G> {
    shared: Arc<Mutex<MetricsState<G>>>,
    seen_version: u64,
}

#[derive(Debug)]
struct MetricsState<G> {
    current: RaftGroupMetrics<G>,
    version: u64,
    closed: bool,
    waiters: Vec<Arc<Waiter>>,
}

impl<G> MetricsPublisher<G> {
    /// Creates a publisher initialized with the current metrics snapshot.
    #[must_use]
    pub fn new(current: RaftGroupMetrics<G>) -> Self {
        Self {
            shared: Arc::new(Mutex::new(MetricsState {
                current,
                version: 0,
                closed: false,
                waiters: Vec::new(),
            })),
        }
    }

    /// Publishes a new metrics snapshot and wakes watchers.
    ///
    /// Returns `false` when the publisher has already been closed, which means
    /// the snapshot was *dropped*: no watcher will see it, and none ever will.
    ///
    /// `#[must_use]` rather than a `()` return, and the asymmetry with
    /// [`MetricsPublisher::close`] is the argument. `close` returning early
    /// leaves the publisher closed, so its caller's intent is satisfied either
    /// way and there is nothing to report. This one returning early leaves the
    /// caller's intent unmet, and a method that can silently fail to do what it
    /// was asked should say so where the compiler can see it. The type is
    /// `Clone`, so "did I close it?" is not always answerable locally: one
    /// clone can close while another publishes.
    ///
    /// Discarding it with `let _ =` is correct wherever the caller closed the
    /// publisher itself and `false` therefore only means "already stopped" —
    /// which is what both of this crate's own call sites do, and both say so.
    #[must_use = "a closed publisher drops the snapshot instead of publishing it"]
    pub fn publish(&self, metrics: RaftGroupMetrics<G>) -> bool {
        let waiters = {
            let mut state = lock_state(&self.shared);
            if state.closed {
                return false;
            }
            state.current = metrics;
            state.version = state.version.saturating_add(1);
            std::mem::take(&mut state.waiters)
        };
        wake_all(waiters);
        true
    }

    /// Closes the stream and wakes watchers that are waiting for changes.
    pub fn close(&self) {
        let waiters = {
            let mut state = lock_state(&self.shared);
            if state.closed {
                return;
            }
            state.closed = true;
            std::mem::take(&mut state.waiters)
        };
        wake_all(waiters);
    }

    /// Creates a watch that observes future metrics changes from this publisher.
    #[must_use]
    pub fn watch(&self) -> MetricsWatch<G> {
        let version = lock_state(&self.shared).version;
        MetricsWatch {
            shared: self.shared.clone(),
            seen_version: version,
        }
    }
}

impl<G: Clone> MetricsPublisher<G> {
    /// Returns the latest metrics snapshot held by the publisher.
    #[must_use]
    pub fn current(&self) -> RaftGroupMetrics<G> {
        lock_state(&self.shared).current.clone()
    }
}

impl<G> MetricsWatch<G> {
    /// Creates a standalone watch initialized with a single metrics snapshot.
    #[must_use]
    pub fn new(current: RaftGroupMetrics<G>) -> Self {
        MetricsPublisher::new(current).watch()
    }

    /// Waits for the next metrics change.
    ///
    /// Returns `None` after the publisher closes and no newer snapshot is
    /// pending for this watcher. Each pending call retains at most its latest
    /// task waker. Dropping the future unregisters it immediately, even if this
    /// watch remains alive and no further metrics are published. Cancellation
    /// does not consume a later update; another call can observe it.
    pub async fn changed(&mut self) -> Option<RaftGroupMetrics<G>>
    where
        G: Clone,
    {
        let registration = Registration::new(&self.shared);
        let result = poll_fn(|context| self.poll_changed(context, &registration.waiter)).await;
        drop(registration);
        result
    }

    fn poll_changed(
        &mut self,
        context: &mut Context<'_>,
        waiter: &Arc<Waiter>,
    ) -> Poll<Option<RaftGroupMetrics<G>>>
    where
        G: Clone,
    {
        // Cloning or dropping a custom waker can call user code too.
        let next = context.waker().clone();
        let mut state = lock_state(&self.shared);
        if state.version != self.seen_version {
            self.seen_version = state.version;
            return Poll::Ready(Some(state.current.clone()));
        }
        if state.closed {
            return Poll::Ready(None);
        }
        let previous = lock_state(&waiter.waker).replace(next);
        if !state
            .waiters
            .iter()
            .any(|registered| Arc::ptr_eq(registered, waiter))
        {
            state.waiters.push(Arc::clone(waiter));
        }
        drop(state);
        drop(previous);
        Poll::Pending
    }
}

impl<G: Clone> MetricsWatch<G> {
    /// Returns the latest metrics snapshot visible to this watch.
    #[must_use]
    pub fn current(&self) -> RaftGroupMetrics<G> {
        lock_state(&self.shared).current.clone()
    }
}

fn lock_state<T>(shared: &Mutex<T>) -> MutexGuard<'_, T> {
    shared
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
#[path = "watch_test.rs"]
mod tests;

#[cfg(test)]
#[path = "watch_cancellation_test.rs"]
mod cancellation_tests;
