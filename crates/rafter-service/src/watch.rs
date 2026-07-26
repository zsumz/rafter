//! Observable service state and metrics watches.

use std::{
    future::poll_fn,
    sync::{Arc, Mutex, MutexGuard},
    task::{Context, Poll, Waker},
};

use rafter_app::metrics::RaftGroupMetrics;

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
    wakers: Vec<Waker>,
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
                wakers: Vec::new(),
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
        let wakers = {
            let mut state = lock_state(&self.shared);
            if state.closed {
                return false;
            }
            state.current = metrics;
            state.version = state.version.saturating_add(1);
            std::mem::take(&mut state.wakers)
        };
        wake_all(wakers);
        true
    }

    /// Closes the stream and wakes watchers that are waiting for changes.
    pub fn close(&self) {
        let wakers = {
            let mut state = lock_state(&self.shared);
            if state.closed {
                return;
            }
            state.closed = true;
            std::mem::take(&mut state.wakers)
        };
        wake_all(wakers);
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
    /// pending for this watcher.
    pub async fn changed(&mut self) -> Option<RaftGroupMetrics<G>>
    where
        G: Clone,
    {
        poll_fn(|context| self.poll_changed(context)).await
    }

    fn poll_changed(&mut self, context: &mut Context<'_>) -> Poll<Option<RaftGroupMetrics<G>>>
    where
        G: Clone,
    {
        let mut state = lock_state(&self.shared);
        if state.version != self.seen_version {
            self.seen_version = state.version;
            return Poll::Ready(Some(state.current.clone()));
        }
        if state.closed {
            return Poll::Ready(None);
        }
        if !state
            .wakers
            .iter()
            .any(|waker| waker.will_wake(context.waker()))
        {
            state.wakers.push(context.waker().clone());
        }
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

fn wake_all(wakers: Vec<Waker>) {
    for waker in wakers {
        waker.wake();
    }
}

fn lock_state<G>(shared: &Mutex<MetricsState<G>>) -> MutexGuard<'_, MetricsState<G>> {
    shared
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        pin::Pin,
        task::{Context, Poll, Waker},
    };

    use rafter::{LogIndex, MembershipConfig, MembershipSet, NodeId, Role, Term};
    use rafter_app::{group::GroupFatalState, metrics::RaftGroupMetrics};

    use super::*;

    #[test]
    fn current_metrics_are_always_available() {
        let publisher = MetricsPublisher::new(metrics(Role::Leader, Term(1), LogIndex(2)));
        let watch = publisher.watch();

        assert_eq!(watch.current().role, Role::Leader);
        assert_eq!(watch.current().term, Term(1));

        publisher.close();

        assert_eq!(watch.current().applied_index, LogIndex(2));
    }

    #[test]
    fn watches_emit_role_term_and_index_changes() {
        let publisher = MetricsPublisher::new(metrics(Role::Follower, Term(1), LogIndex(1)));
        let mut watch = publisher.watch();

        let role_change = metrics(Role::Leader, Term(1), LogIndex(1));
        assert!(publisher.publish(role_change.clone()));
        assert_eq!(block_on(watch.changed()), Some(role_change));

        let term_change = metrics(Role::Leader, Term(2), LogIndex(1));
        assert!(publisher.publish(term_change.clone()));
        assert_eq!(block_on(watch.changed()), Some(term_change));

        let index_change = metrics(Role::Leader, Term(2), LogIndex(8));
        assert!(publisher.publish(index_change.clone()));
        assert_eq!(block_on(watch.changed()), Some(index_change));
    }

    #[test]
    fn pending_watch_is_woken_by_publish() {
        let publisher = MetricsPublisher::new(metrics(Role::Follower, Term(1), LogIndex(1)));
        let mut watch = publisher.watch();
        let mut changed = Box::pin(watch.changed());

        assert!(poll_once(changed.as_mut()).is_pending());

        let update = metrics(Role::Leader, Term(3), LogIndex(5));
        assert!(publisher.publish(update.clone()));

        assert_eq!(poll_once(changed.as_mut()), Poll::Ready(Some(update)));
    }

    #[test]
    fn poison_state_is_visible_in_current_and_changed_metrics() {
        let publisher = MetricsPublisher::new(metrics(Role::Leader, Term(1), LogIndex(1)));
        let mut watch = publisher.watch();

        let mut poisoned = metrics(Role::Leader, Term(1), LogIndex(1));
        poisoned.fatal_state = GroupFatalState::Poisoned {
            reason: "apply failed".to_owned(),
        };
        assert!(publisher.publish(poisoned.clone()));

        assert_eq!(watch.current().fatal_state, poisoned.fatal_state);
        assert_eq!(block_on(watch.changed()), Some(poisoned));
    }

    #[test]
    fn changed_returns_none_after_close_without_pending_update() {
        let publisher = MetricsPublisher::new(metrics(Role::Leader, Term(1), LogIndex(1)));
        let mut watch = publisher.watch();

        publisher.close();

        assert_eq!(block_on(watch.changed()), None);
        assert!(!publisher.publish(metrics(Role::Follower, Term(2), LogIndex(2))));
    }

    fn metrics(role: Role, term: Term, index: LogIndex) -> RaftGroupMetrics<u64> {
        RaftGroupMetrics {
            group_id: 7,
            node_id: NodeId(1),
            role,
            term,
            leader_hint: Some(NodeId(1)),
            commit_index: index,
            applied_index: index,
            last_log_index: index,
            snapshot_index: LogIndex::ZERO,
            membership: MembershipConfig::Stable(
                MembershipSet::new(vec![NodeId(1)], Vec::new()).expect("valid membership"),
            ),
            replication: Vec::new(),
            pending_proposals: 0,
            pending_reads: 0,
            pending_read_barriers: 0,
            pending_query_reads: 0,
            completed_query_reads: 0,
            reserved_reads: 0,
            fatal_state: GroupFatalState::Healthy,
        }
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        let mut future = Box::pin(future);
        loop {
            match poll_once(future.as_mut()) {
                Poll::Ready(output) => return output,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        future.poll(&mut context)
    }
}
