//! The client-waiter tables behind one transport driver.
//!
//! A second impl block on [`TransportDriverState`] rather than a second type:
//! the waiter tables and the step loop share one lock and one group, and the
//! split is by what a reader came for. `state.rs` answers "what does a driver
//! hold"; this answers "what happens to the client". Its three neighbours split
//! the same question again by direction: [`super::writes`] and [`super::reads`]
//! admit work, and [`super::resolution`] ends it.

#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

use std::task::{Context, Poll, Waker};

use crate::transport::{AuthenticatedPeerValidator, RaftTransport};

use super::super::*;
use super::state::{TransportDriverState, WaiterId};

pub(super) struct WriteWaiter<R> {
    pub(super) options: WriteOptions,
    pub(super) outcome: Option<Result<WriteReceipt<R>, WriteError>>,
    pub(super) waker: Option<Waker>,
}

pub(super) struct ReadWaiter<G, Q, QR> {
    pub(super) request: ReadRequest<G, Q>,
    /// Whether a routed [`ReadEvent::Granted`] said this barrier's proof is
    /// cached and waiting for a read call to consume it.
    ///
    /// This is the whole retry policy. A read against a barrier the group
    /// already tracks returns an unstepped report, so a second attempt without
    /// a grant in between cannot see a different answer.
    pub(super) proof_ready: bool,
    pub(super) outcome: Option<Result<QueryReceipt<G, QR>, ReadError>>,
    pub(super) waker: Option<Waker>,
}

impl<G, A, R, T, V> TransportDriverState<G, A, R, T, V>
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
    pub(super) fn resolve_write(
        &mut self,
        local_proposal_id: LocalProposalId,
        outcome: Result<WriteReceipt<A::CommandResult>, WriteError>,
    ) {
        let Some(waiter) = self.write_waiters.get_mut(&local_proposal_id) else {
            return;
        };
        if waiter.outcome.is_some() {
            return;
        }
        waiter.outcome = Some(outcome);
        if let Some(waker) = waiter.waker.take() {
            waker.wake();
        }
    }

    pub(super) fn resolve_read(
        &mut self,
        read_id: ReadId,
        outcome: Result<QueryReceipt<G, A::QueryResult>, ReadError>,
    ) {
        let Some(waiter) = self.read_waiters.get_mut(&read_id) else {
            return;
        };
        if waiter.outcome.is_some() {
            return;
        }
        waiter.outcome = Some(outcome);
        if let Some(waker) = waiter.waker.take() {
            waker.wake();
        }
    }

    /// Removes one dropped client future's waiter, whichever kind it is.
    ///
    /// The entry point the reclamation path uses, so that a deferred waiter and
    /// an immediately reclaimed one take exactly the same route.
    pub(super) fn discard(&mut self, waiter: WaiterId) {
        match waiter {
            WaiterId::Write(local_proposal_id) => self.discard_write(local_proposal_id),
            WaiterId::Read(read_id) => self.discard_read(read_id),
        }
    }

    /// Removes a waiter whose client future was dropped.
    ///
    /// The future is the only thing that can consume a resolved outcome, so a
    /// dropped future is the moment a waiter provably has no reader. Until this
    /// existed, a client that timed out and dropped its future left its entry —
    /// and, for a read, its cloned request — in the table for the life of the
    /// driver. The bound was never the leak: `max_pending_waiters` counts
    /// unresolved waiters, so the slot came back and the entry did not.
    ///
    /// This does not replace abandonment. `abandon_write` and `abandon_read`
    /// resolve rather than remove, so a caller that abandons and still holds its
    /// future gets its answer on the next poll; only the future's own drop
    /// removes the entry, and a future cannot be dropped and polled.
    pub(super) fn discard_write(&mut self, local_proposal_id: LocalProposalId) {
        self.write_waiters.remove(&local_proposal_id);
    }

    /// Removes a dropped read's waiter, cancelling its barrier first.
    ///
    /// A client that stopped listening must not leave a barrier reserved in the
    /// group any more than it leaves a waiter in the driver, so an unresolved
    /// read gives its `reserved_reads` slot back on the way out. A resolved one
    /// has no barrier left to cancel.
    pub(super) fn discard_read(&mut self, read_id: ReadId) {
        let Some(waiter) = self.read_waiters.remove(&read_id) else {
            return;
        };
        if waiter.outcome.is_some() {
            return;
        }
        if let Some(group) = self.group.as_mut() {
            group.cancel_read(read_id);
        }
        self.publish_metrics();
    }

    pub(super) fn pending_writes(&self) -> Vec<PendingWrite> {
        self.write_waiters
            .iter()
            .filter(|(_, waiter)| waiter.outcome.is_none())
            .map(|(local_proposal_id, waiter)| PendingWrite {
                local_proposal_id: *local_proposal_id,
                client_request_id: waiter.options.client_request_id,
            })
            .collect()
    }

    pub(super) fn pending_reads(&self) -> Vec<ReadId> {
        self.read_waiters
            .iter()
            .filter(|(_, waiter)| waiter.outcome.is_none())
            .map(|(read_id, _)| *read_id)
            .collect()
    }

    pub(super) fn poll_write(
        &mut self,
        local_proposal_id: LocalProposalId,
        context: &Context<'_>,
    ) -> Poll<Result<WriteReceipt<A::CommandResult>, WriteError>> {
        let Some(waiter) = self.write_waiters.get_mut(&local_proposal_id) else {
            return Poll::Ready(Err(WriteError::ManagedInvariantViolation {
                fate: WriteFate::Unresolved,
                message: format!("no write waiter remains for {local_proposal_id}"),
            }));
        };
        if waiter.outcome.is_some() {
            let outcome = self
                .write_waiters
                .remove(&local_proposal_id)
                .and_then(|waiter| waiter.outcome);
            return Poll::Ready(outcome.unwrap_or_else(|| {
                Err(WriteError::ManagedInvariantViolation {
                    fate: WriteFate::Unresolved,
                    message: format!("write {local_proposal_id} finished without an outcome"),
                })
            }));
        }
        waiter.waker = Some(context.waker().clone());
        Poll::Pending
    }

    pub(super) fn poll_read(
        &mut self,
        read_id: ReadId,
        context: &Context<'_>,
    ) -> Poll<Result<QueryReceipt<G, A::QueryResult>, ReadError>> {
        let Some(waiter) = self.read_waiters.get_mut(&read_id) else {
            return Poll::Ready(Err(ReadError::ManagedInvariantViolation {
                message: format!("no read waiter remains for {read_id}"),
            }));
        };
        if waiter.outcome.is_some() {
            let outcome = self
                .read_waiters
                .remove(&read_id)
                .and_then(|waiter| waiter.outcome);
            return Poll::Ready(outcome.unwrap_or_else(|| {
                Err(ReadError::ManagedInvariantViolation {
                    message: format!("read {read_id} finished without an outcome"),
                })
            }));
        }
        waiter.waker = Some(context.waker().clone());
        Poll::Pending
    }
}
