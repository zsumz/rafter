#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

//! Managed driver for one local Raft group over an attached transport.
//!
//! This file is the driver type itself: what it holds, how a clone shares it,
//! how an embedder looks at the group it holds, and the two builders every
//! client future is made by. Its neighbours carry what a caller *does* with it.
//! [`construction`], [`handover`], and [`takeover`] hold the lifecycle;
//! [`entry`] holds the four calls that advance the protocol; [`addressed`] holds
//! the work a caller names before awaiting it; [`sender`] holds the
//! [`DriverCommandSender`] surface a handle reaches through, which
//! `InMemoryRaftDriver` implements too; [`bounds`] holds what the driver refuses
//! to accumulate; and [`health`] holds what an operator reads off a running one.

use std::future::poll_fn;

use crate::transport::{AuthenticatedPeerValidator, RaftTransport};

use super::*;

mod addressed;
mod adopted;
mod adoption;
mod barrier;
mod bounds;
mod candidate;
mod checkpoint;
mod condition;
mod construction;
mod control_plane;
mod dispatch;
mod durable;
mod entry;
mod error;
mod handover;
mod health;
mod install;
mod merge;
mod observation;
mod policy;
mod reads;
mod reconciliation;
mod resolution;
mod sender;
mod shared;
mod standing;
mod state;
mod step;
mod takeover;
mod transaction;
mod waiters;
mod writes;

pub use addressed::{AddressedRead, AddressedWrite, PendingWrite};
pub use bounds::TransportDriverOptions;
pub use checkpoint::{CurrentCommittedState, PeerControlPlaneCheckpoint};
pub use condition::DriverServiceState;
pub use error::InboundEnvelopeError;

use adoption::WaiterGuard;
use state::{SharedState, WaiterId};

/// Managed driver for one local Raft group over an attached transport.
///
/// This is the driver an embedder writes when frames leave the process.
/// [`InMemoryRaftDriver`] owns every replica of a group and moves frames
/// between them itself, which makes it a complete cluster and an unusable
/// node; this driver owns exactly one replica, hands its outbound frames to a
/// [`RaftTransport`], and receives inbound frames from whatever loop the
/// embedder runs. Rafter opens no sockets and spawns no tasks: the embedder
/// calls [`TransportRaftDriver::tick`] and [`TransportRaftDriver::deliver`],
/// and this type owns everything between those calls and a resolved client
/// future.
///
/// Cloning shares the driver. Handles obtained from
/// [`TransportRaftDriver::handle`] stay valid across a group release and
/// re-adoption, because a handle names a service rather than a node
/// incarnation.
///
/// Reads are [`ReadConsistency::Linearizable`] and [`ReadConsistency::Local`],
/// which are the two levels [`rafter_app::group::RaftGroup::read`] implements.
/// Any other level is refused with [`ReadError::UnsupportedConsistency`] rather
/// than served at a weaker one — including [`ReadConsistency::LeaseRead`], which
/// is refused because the app layer refuses it and not because this driver chose
/// to.
///
/// A local read answers from this replica's own applied state. It submits no
/// read-index round, reserves no barrier, allocates no [`ReadId`], and its
/// [`QueryReceipt::proof`] is `None`, which is the honest report that it proved
/// nothing about any other replica. What bounds it is
/// [`ReadOptions::min_applied_index`], honored verbatim: a floor this replica
/// has not reached is reported as [`ReadError::FreshnessUnavailable`] carrying
/// both the required and the local applied index, rather than answered from
/// behind it. A caller that states no floor is asking for this replica's applied
/// state and gets it.
pub struct TransportRaftDriver<G, A, R, T, V>
where
    A: ReplicatedStateMachine,
{
    inner: SharedState<G, A, R, T, V>,
}

impl<G, A, R, T, V> Clone for TransportRaftDriver<G, A, R, T, V>
where
    A: ReplicatedStateMachine,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<G, A, R, T, V> Debug for TransportRaftDriver<G, A, R, T, V>
where
    A: ReplicatedStateMachine,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TransportRaftDriver")
            .finish_non_exhaustive()
    }
}

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
    /// Reads the adopted group under this driver's own lock.
    ///
    /// The closure receives a shared borrow for its own duration and nothing
    /// outlives the call: no guard, no owned escape, no way to keep the group
    /// after the lock is released. `&RaftGroup` rather than `&mut` is the whole
    /// policy — this driver correlates outcomes to waiters it created, and a
    /// group stepped, read, or cancelled from outside would break that
    /// correspondence silently.
    ///
    /// This is how an embedder observes a *running* replica.
    /// [`TransportRaftDriver::release_group`] also hands the group back, but it
    /// resolves every outstanding waiter to do so, which makes it a way to
    /// retire a replica rather than a way to look at one.
    ///
    /// The closure runs with the driver locked, so it must not *call* back into
    /// this driver: the lock is not reentrant, and a second acquisition on the
    /// same thread stops it. That includes polling a client future, which is a
    /// call like any other.
    ///
    /// **Dropping a value is not calling in.** A client future of either kind,
    /// resolved or not, may be dropped inside the closure. Dropping one
    /// reclaims its waiter, and reclamation never waits for this lock — it
    /// leaves the waiter for the next acquisition instead. The same guarantee
    /// holds wherever else this driver runs an embedder's code under its lock: a
    /// [`crate::RaftTransport`] call, a
    /// [`rafter_app::state_machine::ReplicatedStateMachine`] apply or read, and
    /// a `Waker` woken while a waiter resolves.
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError::NoGroup`] when the driver has released its
    /// group.
    pub fn with_group<U>(
        &self,
        read: impl FnOnce(&RaftGroup<G, A, R>) -> U,
    ) -> Result<U, ManagedDriverError> {
        let state = self.inner.lock();
        let group = state.group.as_ref().ok_or(ManagedDriverError::NoGroup)?;
        Ok(read(group))
    }

    /// Returns the index this replica's state machine must reach to have
    /// applied every application command it knows to be committed.
    ///
    /// A direct forwarder because this one is the readiness gate the
    /// decomposition recipe is written around, and because it takes no argument
    /// and returns a scalar, so [`TransportRaftDriver::with_group`] would be
    /// pure ceremony around it. Reads that project out of the state machine keep
    /// using the closure, which is what they need anyway.
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError::NoGroup`] when the driver has released its
    /// group.
    pub fn committed_application_index(&self) -> Result<LogIndex, ManagedDriverError> {
        self.with_group(RaftGroup::committed_application_index)
    }

    /// Builds the client future for one registered write waiter.
    ///
    /// Shared by [`TransportRaftDriver::begin_write`] and
    /// [`DriverCommandSender::write`] so there is one polling path rather than
    /// two that could drift.
    fn write_future(
        &self,
        local_proposal_id: LocalProposalId,
    ) -> DriverFuture<Result<WriteReceipt<A::CommandResult>, WriteError>> {
        let mut guard = WaiterGuard::new(self.inner.clone(), WaiterId::Write(local_proposal_id));
        Box::pin(poll_fn(move |context| {
            let polled = guard.state().lock().poll_write(local_proposal_id, context);
            if polled.is_ready() {
                guard.release();
            }
            polled
        }))
    }

    /// Builds the client future for one reserved barrier, shared the same way
    /// [`TransportRaftDriver::write_future`] is.
    fn barrier_future(
        &self,
        read_id: ReadId,
    ) -> DriverFuture<Result<QueryReceipt<G, A::QueryResult>, ReadError>> {
        let mut guard = WaiterGuard::new(self.inner.clone(), WaiterId::Read(read_id));
        Box::pin(poll_fn(move |context| {
            let polled = guard.state().lock().poll_read(read_id, context);
            if polled.is_ready() {
                guard.release();
            }
            polled
        }))
    }

    /// Returns a cloneable handle connected to this driver.
    #[must_use]
    pub fn handle(
        &self,
    ) -> RaftHandle<G, A::Command, A::Query, A::CommandResult, A::QueryResult, Self> {
        let group_id = self.inner.lock().group_id.clone();
        RaftHandle::new(group_id, self.clone())
    }
}
