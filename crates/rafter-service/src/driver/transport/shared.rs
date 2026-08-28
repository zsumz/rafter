#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

//! The lock every clone and every client future shares.
//!
//! Two locks rather than one, because the second exists to be takeable when the
//! first is not: a client future's `Drop` can run on a thread that already holds
//! the state, so reclamation asks rather than waits. What lives here is that
//! arrangement and nothing else — the state it guards is [`super::state`]'s.

use std::sync::TryLockError;

use crate::transport::{AuthenticatedPeerValidator, RaftTransport};

use super::state::TransportDriverState;
use super::*;

/// The driver's state, shared by every clone and every client future.
pub(super) type SharedState<G, A, R, T, V> = Arc<DriverShared<G, A, R, T, V>>;

/// One held driver lock.
pub(super) type StateGuard<'a, G, A, R, T, V> = MutexGuard<'a, TransportDriverState<G, A, R, T, V>>;

/// One driver's state, and the reclamations that could not run yet.
///
/// Two locks rather than one, because the second exists to be takeable when the
/// first is not. A client future's `Drop` reclaims its own waiter, and it may
/// run on a thread that already holds `state`: inside a
/// [`TransportRaftDriver::with_group`] closure, inside a
/// [`RaftTransport`] call this driver makes while routing a report, inside a
/// [`ReplicatedStateMachine`] apply, or inside a `Waker` this driver woke while
/// resolving a waiter. `std::sync::Mutex` is not reentrant, so a `Drop` that
/// waited for `state` in any of those would stop that thread forever.
///
/// So reclamation never waits. It asks for the lock, and a guard that cannot
/// have it leaves its waiter here for the next acquisition to take.
pub(super) struct DriverShared<G, A, R, T, V>
where
    A: ReplicatedStateMachine,
{
    state: Mutex<TransportDriverState<G, A, R, T, V>>,
    /// Waiters whose client futures were dropped while `state` was held.
    ///
    /// Locked only across a push and a take. No group method, no transport
    /// call, and no embedder code of any kind runs under it, so it is a leaf
    /// and cannot become the next version of the defect it exists to fix.
    deferred: Mutex<Vec<WaiterId>>,
}

impl<G, A, R, T, V> DriverShared<G, A, R, T, V>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine + Send + 'static,
    A::Command: Send + 'static,
    A::CommandResult: Clone + Send + 'static,
    A::Query: Clone + Send + 'static,
    A::QueryResult: Send + 'static,
    R: PersistedRaftRuntime + Send + 'static,
    T: RaftTransport<G>,
    V: AuthenticatedPeerValidator<G, T::PeerPrincipal> + Send + Sync + 'static,
{
    pub(super) fn new(state: TransportDriverState<G, A, R, T, V>) -> Self {
        Self {
            state: Mutex::new(state),
            deferred: Mutex::new(Vec::new()),
        }
    }

    /// Locks the driver, reclaiming anything a drop had to defer first.
    ///
    /// This is the only way into the state, so "the next acquisition" in
    /// [`DriverShared::reclaim`]'s contract is every public method of the
    /// driver, every poll of a client future, and every guard drop that finds
    /// the lock free.
    pub(super) fn lock(&self) -> StateGuard<'_, G, A, R, T, V> {
        let mut state = lock_state(&self.state);
        self.reclaim_deferred(&mut state);
        state
    }

    /// Reclaims one dropped future's waiter now, or leaves it for the next
    /// acquisition.
    ///
    /// `try_lock` decides which, and it cannot decide wrongly. It hands out a
    /// `MutexGuard`, so it cannot succeed on a thread that already holds one —
    /// the two guards would alias the same `&mut`. A re-entrant drop therefore
    /// always takes the deferred path by the type system rather than by a
    /// platform detail, and the converse mistake costs nothing: deferring
    /// because another thread happened to hold the lock reclaims at that
    /// thread's next acquisition, which is at most one call away.
    pub(super) fn reclaim(&self, waiter: WaiterId) {
        let Some(mut state) = self.try_lock() else {
            lock_state(&self.deferred).push(waiter);
            return;
        };
        state.discard(waiter);
        self.reclaim_deferred(&mut state);
    }

    fn try_lock(&self) -> Option<StateGuard<'_, G, A, R, T, V>> {
        match self.state.try_lock() {
            Ok(state) => Some(state),
            // A poisoned lock is still this driver's state, and the driver's
            // own `lock` recovers from one rather than propagating it.
            Err(TryLockError::Poisoned(poisoned)) => Some(poisoned.into_inner()),
            Err(TryLockError::WouldBlock) => None,
        }
    }

    /// Takes deferred reclamations until there are none left.
    ///
    /// It loops rather than draining once because reclaiming a read publishes
    /// metrics, publishing wakes watchers under this lock, and a woken task may
    /// drop another client future — which defers onto the queue being drained.
    /// It terminates for the reason the tables are bounded at all: each entry
    /// comes from one guard, and a guard reclaims once.
    fn reclaim_deferred(&self, state: &mut TransportDriverState<G, A, R, T, V>) {
        loop {
            let batch = std::mem::take(&mut *lock_state(&self.deferred));
            if batch.is_empty() {
                return;
            }
            for waiter in batch {
                state.discard(waiter);
            }
        }
    }
}
