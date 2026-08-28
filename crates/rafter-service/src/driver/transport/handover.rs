#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

//! Retiring one incarnation and installing the next.
//!
//! The driver-level half of decomposition: a released group leaves with every
//! waiter resolved, and an adopted one arrives with its recovery outputs still
//! to route. The slot between them is what an embedder would otherwise have to
//! build, and it refuses every operation while it is empty.

use crate::transport::{AuthenticatedPeerValidator, RaftTransport};

use super::*;

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
    /// Retires the running incarnation and returns its group.
    ///
    /// This is the driver-level half of decomposition.
    /// [`rafter_app::group::RaftGroup::into_parts`] consumes the group it
    /// retires, and a driver's group lives behind the lock its cloned handles
    /// share, which nothing can move out of. The driver owns the movable slot
    /// so an embedder does not have to build one.
    ///
    /// Every outstanding waiter resolves before this returns. Writes resolve as
    /// [`WriteError::UnknownOutcome`] with
    /// [`UnknownOutcomeReason::DriverReleased`], because a proposal already
    /// appended may still commit and apply under the next incarnation. Reads
    /// resolve as [`ReadError::Abandoned`] with
    /// [`ReadAbandonReason::DriverReleased`], and their barriers are cancelled
    /// through the group first so the retired group is quiescent *in reads*.
    /// It is deliberately not quiescent in proposals — the appended entry stays
    /// in the group's table, which is what
    /// [`TransportRaftDriver::adopt_group`] accepts and
    /// [`TransportRaftDriver::new`] does not.
    ///
    /// The driver refuses every operation until
    /// [`TransportRaftDriver::adopt_group`] installs a new incarnation. It does
    /// not close the transport: the same link serves the next incarnation, and
    /// closing it is the embedder's decision.
    ///
    /// The metrics watch stays open across the gap, because a handle names a
    /// service rather than an incarnation — but its last snapshot describes the
    /// retired one and nothing refreshes it until `adopt_group` publishes the
    /// next. Closing the watch would break re-adoption, and there is no metrics
    /// snapshot to publish for a group that does not exist, so the surface that
    /// tells a released driver from an idle one is
    /// [`TransportRaftDriver::with_group`] and
    /// [`TransportRaftDriver::committed_application_index`], which answer
    /// [`ManagedDriverError::NoGroup`].
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError::NoGroup`] when the driver has already
    /// released its group.
    pub fn release_group(&self) -> Result<RaftGroup<G, A, R>, ManagedDriverError> {
        let mut state = self.inner.lock();
        if state.group.is_none() {
            return Err(ManagedDriverError::NoGroup);
        }
        state.release_waiters();
        state.group.take().ok_or(ManagedDriverError::NoGroup)
    }

    /// Installs a new incarnation and routes its recovery outputs.
    ///
    /// `recovery_outputs` are the outputs the recovered runtime released, and
    /// the driver applies them itself rather than accepting an already-applied
    /// group. That is deliberate: the recovery report carries peer messages and
    /// snapshot directives that must be routed, and a caller that applied them
    /// outside the driver would drop exactly the effects a restart depends on.
    ///
    /// The new group must serve the group ID this driver was built with. A
    /// driver names one group for its whole life — its handles and metrics
    /// watch were issued against that ID and adoption does not reissue them —
    /// so a group with a different ID is refused rather than adopted under the
    /// wrong name. The node ID may change, because a replacement incarnation is
    /// still a replica of the same group.
    ///
    /// The new group must hold no reserved reads, and its local ID watermarks
    /// must be at or above the retired incarnation's when the two share a
    /// runtime; see [`rafter_app::group::RaftGroupParts`]. A driver that
    /// rebuilt its runtime from durable storage may restart its IDs at zero.
    ///
    /// Unlike [`TransportRaftDriver::new`], this accepts a group that still
    /// tracks appended proposals, because that is precisely what a released
    /// group carries: the entry is in the durable log and may commit under this
    /// incarnation. Its client already received
    /// [`UnknownOutcomeReason::DriverReleased`], so the later `Applied` event
    /// resolves no waiter — which is the correct outcome and not a lost one.
    /// A group whose waiters were never resolved must not be adopted here; use
    /// [`TransportRaftDriver::new`], which refuses it.
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError::ShuttingDown`] when the driver has shut
    /// down — that is terminal, and a supervisor that wants to serve again
    /// builds a driver — [`ManagedDriverError::GroupAlreadyAdopted`] when the
    /// driver still holds a group, [`ManagedDriverError::MixedGroups`] when the
    /// group serves a different group ID than this driver, the same validation
    /// errors as [`TransportRaftDriver::new`], and a group error when the
    /// recovery outputs fail to apply.
    pub fn adopt_group(
        &self,
        group: RaftGroup<G, A, R>,
        recovery_outputs: Vec<RaftOutput>,
    ) -> Result<(), ManagedDriverError> {
        let checkpoint = PeerControlPlaneCheckpoint::empty(group.group_id().clone());
        self.adopt_group_with_checkpoint(group, recovery_outputs, checkpoint)
    }
}
