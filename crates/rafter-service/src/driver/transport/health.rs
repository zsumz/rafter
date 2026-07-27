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
use super::{DriverServiceState, TransportRaftDriver};

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
    /// A non-zero value means either the transport refused a publication or a
    /// fence, or this driver's validator could not name a principal for some
    /// replica. It counts *attempts* and therefore rises on every retry, which
    /// makes it a history rather than a health check: a driver that has refused
    /// nine times and succeeded on the tenth reads the same as one still
    /// failing. Read [`TransportRaftDriver::pending_peer_fences`] and
    /// [`TransportRaftDriver::peer_set_is_stale`] for the current state; read
    /// this to tell a link that has always worked from one that recovered.
    #[must_use]
    pub fn refused_peer_updates(&self) -> u64 {
        self.inner.lock().refused_peer_updates
    }

    /// Returns how many committed removals this driver has not managed to fence
    /// yet.
    ///
    /// Current state rather than history, and the distinction is the point.
    /// [`crate::RaftTransport::fence_peer`] is what stops later frames from a
    /// replica the cluster has removed, and it is allowed to fail — so a
    /// non-zero value here means that, right now, this driver owes the link
    /// layer an admission control it has not accepted. It falls to zero when
    /// every outstanding fence has been accepted, and it is retried at every
    /// entry point of the driver.
    ///
    /// This is the number to alert on. Inbound frames from an unfenced removed
    /// replica are refused locally in the meantime — see
    /// [`InboundEnvelopeError::NotInMembership`] — so the window is degraded
    /// rather than unsafe, but it is a window that does not close by itself if
    /// the link layer stays refusing.
    #[must_use]
    pub fn pending_peer_fences(&self) -> usize {
        self.inner.lock().pending_fences.len()
    }

    /// Returns whether the transport's peer set is behind the group's
    /// membership.
    ///
    /// True when the set this driver would publish differs from the last one the
    /// transport accepted — because a publication was refused, or because the
    /// validator could not name every replica in it, and no retry has succeeded
    /// since. It is `true` before the first accepted publication for the same
    /// reason: nothing has been accepted, so nothing is level.
    ///
    /// The peer-set counterpart of
    /// [`TransportRaftDriver::pending_peer_fences`], and the milder of the two.
    /// A stale peer set means the link layer authorizes a set the cluster has
    /// moved on from, which is a liveness and least-privilege concern; an
    /// unfenced removal is an authorization the cluster explicitly retracted.
    #[must_use]
    pub fn peer_set_is_stale(&self) -> bool {
        self.inner.lock().peer_set_is_stale()
    }

    /// Returns how many inbound frames this driver refused because its group's
    /// membership does not name the sender.
    ///
    /// The observable half of the fail-closed inbound check
    /// ([`InboundEnvelopeError::NotInMembership`]). A non-zero value means the
    /// link layer and the group disagree about who may speak: the validator
    /// authorized a replica this driver's membership has retired. Read beside
    /// [`TransportRaftDriver::pending_peer_fences`], it separates the two causes
    /// — a fence this driver still owes, or a validator that authorizes a
    /// replica no fence was ever licensed for.
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
    /// the published peer set, its inbound frames are refused as
    /// [`InboundEnvelopeError::NotInMembership`], and any fence still owed for
    /// it stays owed. That leaves the forbidden membership change wedged, which
    /// is the correct outcome and not a defect to work around: fencing is
    /// permanent for a principal ([`crate::RaftTransport::fence_peer`] has no
    /// inverse, by design), so a driver that admitted the replica would be
    /// promising an authorization no transport can give back. Alert on this and
    /// fix the identity allocator; nothing here recovers on its own, and nothing
    /// here attempts to.
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
    /// still worth routing to. Both non-serving states leave the protocol
    /// running — the driver still ticks, delivers, flushes its peer control
    /// plane, and applies what commits — so this is about *client* service and
    /// never about whether the replica is participating.
    ///
    /// [`DriverServiceState::FenceBacklog`] ends on its own when the link layer
    /// catches up. [`DriverServiceState::Decommissioned`] does not: the cluster
    /// spent this replica's identity, and the supervisor's move is
    /// [`TransportRaftDriver::release_group`] followed by an adoption under a
    /// *fresh* node ID. Adopting the removed one back is refused with
    /// [`ManagedDriverError::RetiredNodeId`], which is the same fact reported at
    /// the boundary that can still do something about it.
    #[must_use]
    pub fn service_state(&self) -> DriverServiceState {
        self.inner.lock().service_state()
    }
}
