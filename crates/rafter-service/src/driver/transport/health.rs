#![allow(clippy::wildcard_imports)]

//! What an operator reads off a running driver.
//!
//! Every accessor here is an observation and none of them is an operation. They
//! are split from the driver's public operations because they are read for a
//! different reason and at a different time: an embedder calls `tick`, `deliver`,
//! and `begin_write` on its own schedule, and reads these when something looks
//! wrong.
//!
//! Two shapes, and the difference between them is load-bearing. The `refused_*`
//! counters are cumulative *history* — they rise on every retry, so a driver that
//! failed nine times and succeeded on the tenth reads the same as one still
//! failing. Everything else is current *state*, and falls back to zero or
//! `Serving` when the condition ends. Alert on the second; read the first to
//! tell a link that has always worked from one that recovered.

use crate::transport::{AuthenticatedPeerValidator, RaftTransport};

use super::super::*;
use super::{DriverServiceState, PeerControlPlaneCheckpoint, TransportRaftDriver};

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
    /// Returns how many outbound sends the attached transport refused.
    ///
    /// Two producers, counted together: a peer frame [`crate::RaftTransport`]
    /// would not take, and a leader snapshot chunk directive it could not
    /// resolve and send. Neither is a failure. Raft tolerates drops and the
    /// protocol re-sends, and the kernel says the same of a chunk directive its
    /// source cannot serve — the transfer resumes from the follower's
    /// acknowledged offset — so the driver counts refusals rather than
    /// propagating them: a write must not fail because one heartbeat could not
    /// be delivered.
    ///
    /// That shared property is why they share a counter, and it is what
    /// separates both from [`TransportRaftDriver::refused_peer_updates`], which
    /// counts the one link-layer statement that does *not* repair itself.
    ///
    /// The count is how an operator tells a cut link from an idle cluster, and a
    /// driver that discarded it would leave nothing to tell them apart.
    #[must_use]
    pub fn refused_sends(&self) -> u64 {
        self.inner.lock().refused_sends
    }

    /// Returns how many peer-control-plane statements this driver could not
    /// install, cumulatively.
    ///
    /// Counted rather than propagated, for the reason a refused send is: a peer
    /// set that could not be updated is a link-layer condition, and a client's
    /// write must not fail for one. It is a separate count from
    /// [`TransportRaftDriver::refused_sends`] because the two do not repair the
    /// same way — Raft re-sends a dropped frame on its own, and a peer set or a
    /// fence is re-published only because this driver retries it.
    ///
    /// A non-zero value means either the transport refused a policy publication,
    /// or this driver's validator could not name a principal for some replica. It
    /// counts *attempts* and therefore rises on every retry, which makes it a
    /// history rather than a health check: a driver that has refused nine times
    /// and succeeded on the tenth reads the same as one still failing. Read
    /// [`TransportRaftDriver::peer_policy_is_stale`] for the current state; read
    /// this to tell a link that has always worked from one that recovered.
    #[must_use]
    pub fn refused_peer_updates(&self) -> u64 {
        self.inner.lock().refused_peer_updates
    }

    /// Returns the peer-control-plane state an embedder must make durable.
    ///
    /// **Read it, persist it, and hand it back at the next open.** This is the
    /// part of the control plane a restarted process cannot rebuild from Raft:
    /// retirement is derived from the difference between two committed
    /// configurations, a new process sees only the latest, and compaction erases
    /// the rest. [`PeerControlPlaneCheckpoint`] states the whole contract,
    /// including what a stale one costs.
    ///
    /// Cheap but not free — it clones two `NodeId` sets bounded by the cluster
    /// size — so poll
    /// [`TransportRaftDriver::control_plane_checkpoint_epoch`] and take this
    /// when that has moved.
    #[must_use]
    pub fn control_plane_checkpoint(&self) -> PeerControlPlaneCheckpoint<G> {
        self.inner.lock().control_plane_checkpoint()
    }

    /// Returns how many times this driver's checkpointable state has changed.
    ///
    /// The persist trigger. It advances on every committed configuration that
    /// moves the retirement record, on every restored checkpoint, and on nothing
    /// else — so an embedder that persists whenever this differs from the epoch
    /// it last persisted writes exactly the changes it must not lose, and writes
    /// nothing on a tick that changed nothing.
    ///
    /// **Instance-local and monotone within the instance.** A driver built from
    /// a recovered checkpoint starts at zero and counts from there, so the value
    /// is meaningful only against an epoch recorded for *this* driver. Comparing
    /// it with one persisted by a previous process is meaningless; the durable
    /// artifact is the checkpoint, never the epoch.
    #[must_use]
    pub fn control_plane_checkpoint_epoch(&self) -> u64 {
        self.inner.lock().checkpoint_epoch
    }

    /// Returns whether the transport's authorization policy is behind the one
    /// the group requires.
    ///
    /// True when the policy this driver would publish — the authorized peers
    /// *and* the retirement floor — differs from the last one the transport
    /// accepted, because a publication was refused, or because the validator
    /// could not name every replica in it, and no retry has succeeded since. It
    /// is `true` before the first accepted publication for the same reason:
    /// nothing has been accepted, so nothing is level.
    ///
    /// **This is the number to alert on**, and it is one number because the two
    /// halves are one statement. It used to be two surfaces — a stale peer set
    /// beside a count of fences still owed — and they reported different
    /// severities for the same underlying fact, which is that this driver and its
    /// link layer disagree about who may speak.
    ///
    /// What that costs is bounded rather than open-ended. A stale policy retires
    /// *fewer* identities than the current one, never more, because the floor is
    /// monotone; and inbound frames from a replica the cluster removed are
    /// refused locally in the meantime — see
    /// [`InboundEnvelopeError::NotInMembership`]. So the window is degraded
    /// rather than unsafe, and it is a window that does not close by itself if
    /// the link layer stays refusing.
    #[must_use]
    pub fn peer_policy_is_stale(&self) -> bool {
        self.inner.lock().peer_policy_is_stale()
    }

    /// Returns how many inbound frames this driver refused because its group's
    /// membership does not name the sender.
    ///
    /// The observable half of the fail-closed inbound check
    /// ([`InboundEnvelopeError::NotInMembership`]). A non-zero value means the
    /// link layer and the group disagree about who may speak: the validator
    /// authorized a replica this driver's membership has retired. Read beside
    /// [`TransportRaftDriver::peer_policy_is_stale`], it separates the two causes
    /// — a policy this driver has not managed to publish, or a validator that
    /// authorizes a replica no committed configuration ever named.
    #[must_use]
    pub fn refused_non_member_frames(&self) -> u64 {
        self.inner.lock().refused_non_member_frames
    }

    /// Returns how many retired replica identities this group's membership names
    /// again.
    ///
    /// Zero on every cluster that keeps the single-use contract [`NodeId`]
    /// states: a `(group_id, NodeId)` pair is spent by a committed removal, and
    /// a replica that returns returns under a fresh ID. A non-zero value means
    /// that contract was broken — some ID this driver watched a committed
    /// removal for has been committed back into the membership — and it is the
    /// only surface that says so, because the kernel keeps no removed-node
    /// tombstones and cannot refuse the re-addition once compaction has erased
    /// the history it would need.
    ///
    /// The driver's response is to stay refusing. The identity is left out of
    /// the published peer set — and therefore stays beneath the retirement floor
    /// the same policy states — and its inbound frames are refused as
    /// [`InboundEnvelopeError::NotInMembership`]. That leaves the forbidden
    /// membership change wedged, which is the correct outcome and not a defect to
    /// work around: the `(group_id, NodeId)` pair was consumed by a committed
    /// removal, so a driver that admitted the replica would be re-authorizing an
    /// identity the cluster spent. Alert on this and fix the identity allocator;
    /// nothing here recovers on its own, and nothing here attempts to.
    ///
    /// Restart is not removal, and does not reach this. A replica killed and
    /// restarted under the same ID keeps its ID and its principal, and no
    /// committed removal happened, so nothing was retired.
    ///
    /// A deployment that allocates a "fresh" ID *below* the highest this group
    /// has ever committed also lands here, and that is the same finding wearing
    /// different clothes: single-use is enforced by a high-water mark, so fresh
    /// means greater than anything ever committed, and an ID from a gap under
    /// the mark is indistinguishable from one a removal spent. Allocate
    /// monotonically per group.
    #[must_use]
    pub fn readmitted_retired_peers(&self) -> usize {
        self.inner.lock().readmitted_retired_peers()
    }

    /// Returns why this driver is refusing new client work, if it is.
    ///
    /// The one accessor a supervisor polls to decide whether this replica is
    /// still worth routing to. Every non-serving state leaves the protocol
    /// running — the driver still ticks, delivers, and applies what commits — so
    /// this is about *client* service and never about whether the replica is
    /// participating.
    ///
    /// [`DriverServiceState::NotMember`] ends on its own when a configuration
    /// names this replica again. [`DriverServiceState::Decommissioned`] does not:
    /// the cluster spent this replica's identity, and the supervisor's move is
    /// [`TransportRaftDriver::release_group`] followed by an adoption under a
    /// *fresh* node ID. Adopting the removed one back is refused with
    /// [`ManagedDriverError::RetiredNodeId`], which is the same fact reported at
    /// the boundary that can still do something about it.
    ///
    /// [`DriverServiceState::ContradictoryCurrentState`] does not end either, and
    /// it is the one state in which this driver deliberately stops publishing:
    /// the facts licensing its permanent statement about who is retired disagree,
    /// so there is no policy it could issue that is not a guess. Release and
    /// rebuild from durable state.
    #[must_use]
    pub fn service_state(&self) -> DriverServiceState {
        self.inner.lock().service_state()
    }
}
