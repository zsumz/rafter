#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

//! The calls that advance one replica.
//!
//! Four entry points and no schedule behind them: Rafter spawns no tasks, so a
//! tick, a delivery, a membership change, and a barrier collection all happen
//! because the embedder called. Each routes everything its step produced before
//! it returns, and each flushes the peer policy first so a refused publication
//! is retried on the embedder's own timer.

use crate::transport::{
    validate_inbound_peer_envelope, AuthenticatedPeerEnvelope, AuthenticatedPeerValidator,
    RaftTransport,
};

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
    /// Steps the group with a tick and routes everything the step produced.
    ///
    /// This is one of the two entry points that advance the protocol. Call it
    /// on the embedder's own timer; the app layer's election and heartbeat
    /// timing is measured in ticks, not in wall time, so the tick interval is
    /// the embedder's policy and Rafter does not choose it.
    ///
    /// The step's report is routed before this returns: peer messages go to
    /// the transport, proposal and read events resolve waiters, and the metrics
    /// snapshot is published. A terminal event resolves its waiter whichever
    /// step observed it, which is why a client future can complete inside a
    /// tick it has no other relationship to.
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError`] when the driver has released its group,
    /// is shutting down, or the group step fails.
    pub fn tick(&self) -> Result<(), ManagedDriverError> {
        let mut state = self.inner.lock();
        state.reject_if_shutting_down()?;
        // Before the step, so a policy the link layer refused earlier is retried
        // on the embedder's own timer rather than waiting for the cluster's next
        // configuration change — which may never come.
        state.flush_peer_policy();
        state.step(GroupInput::Tick)
    }

    /// Starts one caller-planned membership change and routes every immediate
    /// side effect.
    ///
    /// This is the execution counterpart to
    /// [`crate::MembershipController`]'s plans. The driver does not choose a
    /// target membership, decide when a learner is caught up, or turn an
    /// accepted step into a completion claim; those remain deployment policy.
    /// `Ok(())` means the local group accepted and processed this input. The
    /// caller must observe committed membership before retiring an identity or
    /// advancing its durable allocation record.
    ///
    /// The control-plane checkpoint is reconciled before this returns, just as
    /// it is after ticks and peer deliveries. An embedder that persists on
    /// [`TransportRaftDriver::control_plane_checkpoint_epoch`] therefore sees a
    /// membership change through the same fail-closed path as every other
    /// protocol input.
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError`] when the driver has released its group,
    /// is shutting down, or the local group refuses the membership input.
    pub fn change_membership(&self, change: MembershipChange) -> Result<(), ManagedDriverError> {
        let mut state = self.inner.lock();
        state.reject_if_shutting_down()?;
        state.flush_peer_policy();
        state.step(GroupInput::Membership { change })
    }

    /// Validates one inbound authenticated envelope and steps the group with
    /// it.
    ///
    /// Validation happens in two stages, and they answer different questions.
    /// [`validate_inbound_peer_envelope`] asks the *validator* whether the link
    /// layer authenticated this principal as this replica, and whether the policy
    /// that deployment currently holds authorizes it rather than retiring it.
    /// Then this driver asks itself whether its own group's membership names the
    /// sender at all. Both run before the group is touched, exactly where a
    /// production embedder refuses a frame, and the group never sees one that
    /// fails either.
    ///
    /// The second stage is the fail-closed half, and it exists because the first
    /// one can be out of date. [`crate::RaftTransport::update_peers`] is how a
    /// removed replica stops being authorized, and it is allowed to fail — so
    /// between the moment the cluster commits a removal and the moment the
    /// transport accepts the policy that retires it, the validator still
    /// authorizes a replica the cluster has retired. The driver knows better
    /// than its own link layer in that window: its admission reads the
    /// effective and raw committed memberships and the positioned committed
    /// register, less every spent identity, so it can refuse the frame itself
    /// rather than let a transient control-plane failure become an
    /// authorization.
    ///
    /// It cannot refuse a legitimate joiner. The membership it checks includes
    /// the effective configuration, so a replica added by a change that has
    /// appended and not committed is present and its frames are accepted — which
    /// it must be, or it can never catch up and the change can never commit.
    ///
    /// Rejection is not a driver failure: an unauthorized or retired peer
    /// sending frames is an expected condition, and the caller decides
    /// whether to log it, count it, or drop the connection.
    ///
    /// # Errors
    ///
    /// Returns [`InboundEnvelopeError::Rejected`] when the validator refuses the
    /// frame, [`InboundEnvelopeError::NotInMembership`] when this driver's own
    /// membership does not name the sender — both leaving the group untouched —
    /// and [`InboundEnvelopeError::Driver`] when the group step itself fails.
    pub fn deliver(
        &self,
        envelope: AuthenticatedPeerEnvelope<G, T::PeerPrincipal>,
    ) -> Result<(), InboundEnvelopeError> {
        let mut state = self.inner.lock();
        state
            .reject_if_shutting_down()
            .map_err(|source| InboundEnvelopeError::Driver { source })?;
        // A delivery is an entry point like a tick, and a frame from a replica
        // the current policy retires is the likeliest moment for the retry to
        // matter.
        state.flush_peer_policy();
        let node_id = state.node_id;
        let envelope = validate_inbound_peer_envelope(envelope, node_id, &state.validator)
            .map_err(|source| InboundEnvelopeError::Rejected { source })?;
        if !state.is_admitted(envelope.from) {
            state.refused_non_member_frames = state.refused_non_member_frames.saturating_add(1);
            return Err(InboundEnvelopeError::NotInMembership {
                node_id: envelope.from,
            });
        }
        state
            .step(GroupInput::PeerMessage { envelope })
            .map_err(|source| InboundEnvelopeError::Driver { source })
    }

    /// Collects every barrier whose proof this driver has been told is ready.
    ///
    /// A grant is announced — `tick` and `deliver` route the group's read
    /// events — but the proof it announces is *consumed* by a read call, and
    /// that call runs the state machine, which this driver will not do inside a
    /// tick the embedder asked for on its own timer. So the third entry point
    /// stays: call it after each batch of deliveries and after each tick.
    ///
    /// It attempts exactly the barriers a routed `ReadEvent::Granted` named and
    /// leaves the rest alone. A barrier still waiting on its quorum round, or
    /// granted at an index this replica has not applied through, cannot answer
    /// differently until the group says so, and a read against a barrier the
    /// group already tracks returns an unstepped report — so attempting one
    /// anyway would spin. With nothing granted this is a no-op, and it is safe
    /// to call at any time.
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError::NoGroup`] when the driver has released its
    /// group, and nothing else. Every group error a read call can raise names
    /// the one barrier that call was for, so it resolves that barrier's client
    /// and the pass continues: one barrier's fault does not deny service to the
    /// rest.
    pub fn drive_pending_reads(&self) -> Result<(), ManagedDriverError> {
        let mut state = self.inner.lock();
        // The third entry point flushes like the other two. This is the one an
        // embedder calls after each batch of deliveries, so it is the supervisory
        // surface a driver whose link layer just recovered is most likely to
        // reach first.
        state.flush_peer_policy();
        state.drive_pending_reads()
    }
}
