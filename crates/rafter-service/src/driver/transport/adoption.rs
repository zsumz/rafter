#![allow(clippy::wildcard_imports)]

//! What a client future owns, and what a group must prove to be adopted.
//!
//! A third file under the driver rather than a third of `transport.rs`, split
//! the way the other two are: that file is the driver's public contract, and
//! these are the two mechanisms an embedder never names. They share a file
//! because they are the same subject from either end — the guard is what a
//! waiter's lifetime is made of, and the watermarks are what a driver checks so
//! that every waiter it will ever hold is one it created.

use super::super::*;
use super::state::{SharedState, WaiterId};
use crate::transport::{AuthenticatedPeerValidator, RaftTransport};

/// Reclaims one waiter when its client future is dropped.
///
/// Every client future owns one of these. A future polled to completion
/// releases it, because `poll_write` and `poll_read` already removed the entry
/// they answered from; a future dropped before that reclaims the entry itself,
/// which is what keeps the tables bounded for a driver whose clients time out.
///
/// The guard is the only remover other than a completing poll. Abandonment
/// deliberately resolves without removing, so an abandoned waiter still answers
/// a late poll from a future its caller kept.
///
/// Reclamation goes through [`DriverShared::reclaim`] rather than taking the
/// driver's lock here, and that indirection is the whole point of it: a future
/// may be dropped by code this driver is running under its own lock — an
/// embedder's transport, state machine, or waker, or a
/// [`TransportRaftDriver::with_group`] closure — and a `Drop` that waited for
/// that lock would stop the thread it ran on.
pub(super) struct WaiterGuard<G, A, R, T, V>
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
    inner: SharedState<G, A, R, T, V>,
    waiter: Option<WaiterId>,
}

impl<G, A, R, T, V> WaiterGuard<G, A, R, T, V>
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
    pub(super) fn new(inner: SharedState<G, A, R, T, V>, waiter: WaiterId) -> Self {
        Self {
            inner,
            waiter: Some(waiter),
        }
    }

    pub(super) fn state(&self) -> &SharedState<G, A, R, T, V> {
        &self.inner
    }

    /// Marks the waiter as already consumed by a completed poll.
    pub(super) fn release(&mut self) {
        self.waiter = None;
    }
}

impl<G, A, R, T, V> Drop for WaiterGuard<G, A, R, T, V>
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
    fn drop(&mut self) {
        let Some(waiter) = self.waiter.take() else {
            return;
        };
        self.inner.reclaim(waiter);
    }
}

pub(super) fn highest(current: Option<u64>, adopted: Option<u64>) -> Option<u64> {
    match (current, adopted) {
        (Some(current), Some(adopted)) => Some(current.max(adopted)),
        (value, None) | (None, value) => value,
    }
}

/// Whether appended proposals may travel into a driver with the group.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PendingProposals {
    /// A group from outside: a waiter this driver did not create can never be
    /// resolved, so pending proposals are refused.
    Refuse,
    /// A group this driver released: it already resolved those waiters as
    /// unknown outcomes, and the entries themselves are durable.
    Carry,
}

/// Validates one group for adoption and returns the ID floors above it.
pub(super) fn adopted_watermarks<G, A, R>(
    group: &RaftGroup<G, A, R>,
    pending_proposals: PendingProposals,
) -> Result<(Option<u64>, Option<u64>), ManagedDriverError>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine,
    A::CommandResult: Clone,
    R: PersistedRaftRuntime,
{
    let node_id = group.node_id();
    match group.fatal_state() {
        GroupFatalState::Poisoned { reason } => {
            return Err(ManagedDriverError::PoisonedGroup {
                node_id,
                reason: reason.clone(),
            });
        }
        GroupFatalState::Healthy if !group.poisoned_waiters().is_empty() => {
            return Err(ManagedDriverError::PoisonedGroup {
                node_id,
                reason: "group has undrained poisoned waiters".to_owned(),
            });
        }
        GroupFatalState::Healthy => {}
    }
    let metrics = group.metrics();
    let refused_proposals =
        pending_proposals == PendingProposals::Refuse && metrics.pending_proposals != 0;
    if refused_proposals || metrics.reserved_reads != 0 {
        return Err(ManagedDriverError::NonQuiescentGroup {
            node_id,
            pending_proposals: metrics.pending_proposals,
            reserved_reads: metrics.reserved_reads,
        });
    }
    let next_proposal_id = match group.local_proposal_id_watermark() {
        Some(last_seen_local_proposal_id) => {
            Some(last_seen_local_proposal_id.0.checked_add(1).ok_or(
                ManagedDriverError::LocalProposalIdExhausted {
                    node_id,
                    last_seen_local_proposal_id,
                },
            )?)
        }
        None => Some(1),
    };
    let next_read_id = match group.read_id_watermark() {
        Some(last_seen_read_id) => Some(last_seen_read_id.0.checked_add(1).ok_or(
            ManagedDriverError::ReadIdExhausted {
                node_id,
                last_seen_read_id,
            },
        )?),
        None => Some(1),
    };
    Ok((next_proposal_id, next_read_id))
}
