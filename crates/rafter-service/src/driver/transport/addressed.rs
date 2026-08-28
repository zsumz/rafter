//! Work a caller names before it awaits it.
//!
//! A write or a read this driver admitted, returned beside the identifier it
//! allocated, so the caller holds a name for the waiter before the future
//! resolves. Everything here is the naming half: what a caller may abandon, what
//! it may still be holding, and the two shapes those answers travel in.

#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

use crate::transport::{AuthenticatedPeerValidator, RaftTransport};

use super::*;

/// One unresolved write a driver is still holding.
///
/// Named both ways a caller can name it, because the two IDs answer different
/// questions: the driver's own ID is what
/// [`TransportRaftDriver::abandon_write`] takes, and the caller's is how a
/// caller with several writes in flight tells them apart.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct PendingWrite {
    /// The ID this driver allocated for the proposal.
    pub local_proposal_id: LocalProposalId,
    /// The ID the caller supplied in [`WriteOptions`], if any.
    pub client_request_id: Option<ClientRequestId>,
}

/// A write this driver admitted, named and awaited separately.
///
/// Returned by [`TransportRaftDriver::begin_write`]. The ID is what
/// [`TransportRaftDriver::abandon_write`] takes; the future is what
/// [`DriverCommandSender::write`] would have returned alone.
pub type AddressedWrite<R> = (
    LocalProposalId,
    DriverFuture<Result<WriteReceipt<R>, WriteError>>,
);

/// A read barrier this driver reserved, named and awaited separately.
///
/// The read counterpart of [`AddressedWrite`], returned by
/// [`TransportRaftDriver::begin_read`].
pub type AddressedRead<G, QR> = (ReadId, DriverFuture<Result<QueryReceipt<G, QR>, ReadError>>);

impl<G, A, R, T, V> TransportRaftDriver<G, A, R, T, V>
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
    /// Stops waiting for one write and resolves its client.
    ///
    /// This is the caller's own decision, not the cluster's, so the client
    /// resolves as [`WriteError::UnknownOutcome`] with
    /// [`UnknownOutcomeReason::DriveBoundReached`]: the proposal may already be
    /// in the durable log and may still commit, and there is no
    /// `cancel_proposal` that could make it otherwise. The waiter stops counting
    /// against [`TransportDriverOptions::max_pending_waiters`] immediately.
    ///
    /// Abandonment is terminal for the client. A later `Applied`, `Rejected`, or
    /// `UnknownOutcome` event for this proposal resolves nothing and changes
    /// nothing, which is the correct direction: the client already holds a
    /// terminal answer, and on this side that answer is *unknown*, which is
    /// exactly the statement that the proposal may still commit. A caller that
    /// wants the eventual fact keeps its future and does not call this.
    ///
    /// Returns whether a waiter was retired. Abandoning a write this driver no
    /// longer holds, one that has already resolved, or one whose client future
    /// was dropped — which reclaims the waiter — is a no-op rather than an
    /// error: a caller racing its own completion is not a fault, and abandonment
    /// resolves a client, so there is nothing to do for one that left.
    #[must_use]
    pub fn abandon_write(&self, local_proposal_id: LocalProposalId) -> bool {
        self.inner.lock().abandon_write(local_proposal_id)
    }

    /// Stops waiting for one read and resolves its client.
    ///
    /// The barrier is cancelled through
    /// [`rafter_app::group::RaftGroup::cancel_read`] first, so the group's
    /// `reserved_reads` returns to its previous value, and the client resolves
    /// as [`ReadError::Abandoned`] with
    /// [`ReadAbandonReason::DriveBoundReached`]. The `ReadId` is spent: a retry
    /// issues a new read.
    ///
    /// Returns whether a waiter was retired. A read whose client future was
    /// already dropped has none: dropping the future cancels the barrier and
    /// reclaims the waiter on its own.
    #[must_use]
    pub fn abandon_read(&self, read_id: ReadId) -> bool {
        self.inner.lock().abandon_read(read_id)
    }

    /// Returns every write this driver has not resolved.
    ///
    /// This answers "what is this driver still holding", which is the question a
    /// supervisor draining one asks. It is not how a caller finds its *own*
    /// write: use [`TransportRaftDriver::begin_write`], which returns the ID it
    /// allocated. A caller that answered the second question with this one was
    /// taking the highest unresolved ID and relying on no other write being
    /// admitted in between, which holds only while nothing else uses a driver
    /// that is [`Sync`].
    #[must_use]
    pub fn pending_writes(&self) -> Vec<PendingWrite> {
        self.inner.lock().pending_writes()
    }

    /// Returns the read IDs of every barrier this driver has not resolved.
    ///
    /// The read counterpart of [`TransportRaftDriver::pending_writes`], and the
    /// same distinction applies: a caller looking for the barrier it just
    /// started wants [`TransportRaftDriver::begin_read`].
    #[must_use]
    pub fn pending_reads(&self) -> Vec<ReadId> {
        self.inner.lock().pending_reads()
    }

    /// Proposes `command` and returns the ID this driver allocated for it
    /// beside the future that resolves it.
    ///
    /// [`DriverCommandSender::write`] returns the future alone, so the only name
    /// for the waiter it created arrives when that future resolves — which is
    /// after the point a caller would have used it, because the one thing the
    /// name is for is [`TransportRaftDriver::abandon_write`].
    ///
    /// No group ID: this driver names one group for its whole life, so it
    /// supplies its own and cannot be handed the wrong one. The future is the
    /// one `write` returns, built by the same call, so the two cannot answer
    /// differently.
    ///
    /// # Errors
    ///
    /// As [`DriverCommandSender::write`], except that a refusal which allocated
    /// no ID is returned here rather than delivered through the future: there is
    /// no waiter to name, so there is no pair to return.
    pub fn begin_write(
        &self,
        command: A::Command,
        options: WriteOptions,
    ) -> Result<AddressedWrite<A::CommandResult>, WriteError> {
        // One acquisition: the shared body takes the group ID this driver was
        // built with, and reading it under the same lock that registers the
        // waiter leaves nothing to argue about. `write_future` takes no lock —
        // the guard is a handle and the poll closure is lazy — so the lock is
        // released before the pair is built.
        let local_proposal_id = {
            let mut state = self.inner.lock();
            let group_id = state.group_id.clone();
            state.begin_write(&group_id, command, options)?
        };
        Ok((local_proposal_id, self.write_future(local_proposal_id)))
    }

    /// Begins a linearizable read and returns the ID of the barrier it reserved
    /// beside the future that resolves it.
    ///
    /// The read counterpart of [`TransportRaftDriver::begin_write`], and
    /// linearizable-only for a reason a consistency parameter would hide: this
    /// exists to name a waiter so that [`TransportRaftDriver::abandon_read`] can
    /// retire it, and a [`ReadConsistency::Local`] read reserves no barrier,
    /// registers no waiter, and is answered inside the call that starts it.
    /// There would be no ID to return and nothing to abandon. Run one through
    /// [`DriverCommandSender::read`], which serves both levels.
    ///
    /// # Errors
    ///
    /// As [`DriverCommandSender::read`], except that a refusal which reserved no
    /// barrier is returned here rather than delivered through the future.
    pub fn begin_read(
        &self,
        query: A::Query,
        options: ReadOptions,
    ) -> Result<AddressedRead<G, A::QueryResult>, ReadError> {
        // One acquisition, for the reason [`TransportRaftDriver::begin_write`]
        // gives.
        let read_id = {
            let mut state = self.inner.lock();
            let group_id = state.group_id.clone();
            state.begin_linearizable_read(&group_id, query, options)?
        };
        Ok((read_id, self.barrier_future(read_id)))
    }
}
