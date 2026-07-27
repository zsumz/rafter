//! The managed service transport trait and its validation glue.
//!
//! Production transports must authenticate peers before constructing an
//! authenticated envelope, keep group peer sets current, fence removed peers,
//! and use the current `rafter-codec` peer wire format. This crate
//! intentionally provides a trait only, so that no transport ships as the
//! default by being the one that is here.
//!
//! One trait, and it is synchronous. [`RaftTransport::send`] means "accepted or
//! enqueued", which is the seam an async link layer already needs: the embedder
//! owns the queue and the task that drains it, and this crate hands frames to
//! the queue. An async twin of this trait would describe how that task is
//! spawned and awaited, which is a design this crate has never run and cannot
//! validate — see the fourth revision of the Transport-Attached Group Driver
//! entry in `docs/api-promotions.md`.
//!
//! An unauthenticated transport must say so in its own name, not only in its
//! documentation: `rafter-transport-tcp-insecure` is the shipped example of the
//! rule, and it is a demo rather than a deployment target.

use std::error::Error;

use rafter::{NodeId, SnapshotChunkSend};

pub use rafter_app::transport::{
    message_sender, AuthenticatedPeerEnvelope, AuthenticatedPeerEnvelopeError,
    AuthenticatedPeerValidator, PeerEnvelope,
};

/// One leader snapshot chunk directive addressed to a peer.
///
/// A directive rather than a message, because the kernel never holds an
/// application snapshot payload: `chunk` names the bytes by transfer, offset,
/// and length, and the transport reads them from the snapshot store with
/// [`SnapshotChunkSend::resolve`] before framing them. This is the shape the
/// kernel already documents for the boundary — payload bytes flow from the
/// application's snapshot store to the network without entering kernel state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotChunkEnvelope<G> {
    /// The group whose snapshot is being transferred.
    pub group_id: G,
    /// The leader sending the chunk.
    pub from: NodeId,
    /// The follower being caught up.
    pub to: NodeId,
    /// Which bytes to send, named by transfer, offset, and length.
    ///
    /// Resolve it against the local snapshot store with
    /// [`SnapshotChunkSend::resolve`]; the bytes themselves never pass through
    /// kernel state.
    pub chunk: SnapshotChunkSend,
}

/// Current transport principals authorized for a group.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PeerSet<P> {
    peers: Vec<P>,
}

impl<P> PeerSet<P> {
    /// Creates a peer set from the currently authorized principals.
    #[must_use]
    pub fn new(peers: Vec<P>) -> Self {
        Self { peers }
    }

    /// Returns the current authorized principals.
    #[must_use]
    pub fn peers(&self) -> &[P] {
        &self.peers
    }

    /// Consumes the peer set and returns the stored principals.
    #[must_use]
    pub fn into_peers(self) -> Vec<P> {
        self.peers
    }
}

/// Synchronous service transport boundary.
///
/// # Delivery semantics
///
/// Raft safety tolerates dropped, duplicated, reordered, reconnected, and
/// non-FIFO peer messages. A production transport may redial, retry, or deliver
/// messages after later messages without violating safety; the protocol
/// validates terms, indices, and snapshot metadata before accepting effects.
///
/// Liveness and performance still depend on transport discipline. Use bounded
/// queues or another explicit backpressure policy rather than unbounded memory
/// growth, and provide eventual delivery between healthy authorized peers.
/// Per-peer FIFO is not required for safety, but it reduces wasted work and is
/// usually beneficial. A successful [`RaftTransport::send`] means the message
/// was accepted or enqueued by the transport, not that it was delivered,
/// processed, committed, or applied.
///
/// Message sizing follows `rafter-codec`: append-entries frames fit within the
/// configured append budget plus headers, while snapshot transfers use
/// `InstallSnapshotChunk` and `InstallSnapshotResponse` frames. The current
/// peer wire format does not encode whole `InstallSnapshot` payloads. This
/// pre-release crate supports only the current peer wire format; future public
/// wire-format bumps need an explicit compatibility or migration plan.
pub trait RaftTransport<G>: Send + Sync + 'static {
    /// The transport's own authenticated identity for a peer.
    ///
    /// This is what the link layer proved, not what Raft believes: a
    /// certificate subject, a mutual-TLS identity, a signed token. Rafter never
    /// derives one from a [`NodeId`], because a node ID travels inside frames
    /// and proves nothing. An [`AuthenticatedPeerValidator`] maps between the
    /// two in both directions, and the mapping is the trust boundary — a
    /// principal that maps to the wrong node authorizes that node's votes.
    type PeerPrincipal;
    /// Error returned when the transport cannot send or update peer metadata.
    ///
    /// Transport errors are part of the public app/service error stack, so
    /// implementations expose typed errors rather than debug-only strings —
    /// the same contract [`rafter_runtime_api::PersistedRaftRuntime::Error`]
    /// and [`rafter_app::state_machine::ReplicatedStateMachine::Error`] state
    /// for the other halves of it. Without the bound a driver cannot preserve
    /// a send failure as a [`crate::error::ErrorCause`], and would have to
    /// render it.
    type Error: Error + Send + Sync + 'static;

    /// Sends one validated outbound Raft peer envelope.
    ///
    /// # Errors
    ///
    /// Returns the transport implementation's error when the frame cannot be
    /// sent.
    fn send(&self, envelope: PeerEnvelope<G>) -> Result<(), Self::Error>;

    /// Resolves one leader snapshot chunk directive and sends it.
    ///
    /// This is the only path by which a follower below the leader's snapshot
    /// boundary is caught up. Implementations resolve `envelope.chunk` against
    /// their own [`rafter::SnapshotChunkSource`] with
    /// [`SnapshotChunkSend::resolve`] and send the resulting
    /// [`rafter::Message::InstallSnapshotChunk`] frame. A directive the source
    /// cannot serve is dropped like a lost message, exactly as `resolve`
    /// documents: the transfer resumes from the follower's acknowledged offset
    /// once the source and the kernel agree on the current snapshot again.
    ///
    /// There is no provided body on purpose. The only one expressible returns
    /// `Ok(())` and drops the chunk, which would let a transport disable
    /// snapshot transfer by omission and report success for it.
    ///
    /// A runtime that resolves directives itself — `DurableRaftNode` does,
    /// because it owns its snapshot store — never produces one of these, so an
    /// embedder over the shipped runtime may implement this as a refusal.
    ///
    /// # Errors
    ///
    /// Returns the transport implementation's error when the chunk cannot be
    /// sent. Like [`RaftTransport::send`], a refusal is counted by the driver
    /// rather than propagated to a client.
    fn send_snapshot_chunk(&self, envelope: SnapshotChunkEnvelope<G>) -> Result<(), Self::Error>;

    /// Replaces the authorized transport principals for `group_id`.
    ///
    /// Idempotent, and may be called with a set the transport already holds. A
    /// driver retries a refused publication at its next entry point rather than
    /// waiting for the cluster's next configuration change, so an
    /// implementation must treat a repeat of the current set as a no-op rather
    /// than as a reconfiguration.
    ///
    /// **A set published here never re-authorizes a fenced principal, and an
    /// implementation must not read one as an unfence.**
    /// [`RaftTransport::fence_peer`] is permanent, so the two operations do not
    /// contradict each other and never have to be ordered against each other: a
    /// driver excludes a fenced replica from every later set it publishes.
    /// Fencing wins over authorization on any implementation that checks both.
    ///
    /// # Errors
    ///
    /// Returns the transport implementation's error when peer metadata cannot
    /// be updated. A refusal is not final: the driver holds the set it could
    /// not publish and tries again.
    fn update_peers(
        &self,
        group_id: &G,
        peers: PeerSet<Self::PeerPrincipal>,
    ) -> Result<(), Self::Error>;

    /// Fences `peer` for `group_id` so later frames from it are rejected.
    ///
    /// This is the operation that retires a replica the cluster has committed
    /// the removal of, and it is allowed to fail — which makes retrying it the
    /// caller's obligation. A driver that observes a refusal keeps the fence
    /// outstanding and re-issues it at every later entry point until it is
    /// accepted, because no later membership event re-derives it: the removal is
    /// already behind the driver's record of the membership.
    ///
    /// Idempotent, for the same reason [`RaftTransport::update_peers`] is:
    /// fencing an already-fenced peer must succeed rather than report a fault,
    /// or a retried fence could never converge.
    ///
    /// # A fence is permanent, and there is no unfence
    ///
    /// Deliberately. This trait has no inverse of this method and will not
    /// acquire one, because there is no fact that could license calling it: a
    /// replica is fenced by a *committed* removal, and a committed removal
    /// retires the `(group_id, node_id)` pair for good — see [`NodeId`], which
    /// states the single-use contract. A replica that returns to a group
    /// returns under a fresh ID with its own principal, and that principal was
    /// never fenced, so nothing needs unfencing.
    ///
    /// So an implementation may treat its fenced set as append-only for the
    /// lifetime of the group, and callers must assume it does. A membership
    /// change that names a retired ID again is a violation of the contract above
    /// rather than a request this boundary can serve; a driver refuses the
    /// replica locally and reports the violation instead of asking for an
    /// unfence that does not exist.
    ///
    /// **Restarting a replica is not removing it.** A replica killed and
    /// restarted under the same node ID was never removed, is never fenced, and
    /// keeps its principal across the restart. Nothing on this boundary changes
    /// for it.
    ///
    /// # Errors
    ///
    /// Returns the transport implementation's error when the peer cannot be
    /// fenced. A refusal leaves the driver's own inbound check as the only
    /// admission control for that replica until a retry is accepted, so an
    /// implementation that cannot fence should report it rather than swallow
    /// it.
    fn fence_peer(&self, group_id: &G, peer: Self::PeerPrincipal) -> Result<(), Self::Error>;
}

/// Validates and converts one authenticated inbound envelope before it enters
/// a managed [`rafter_app::group::RaftGroup`].
///
/// # Errors
///
/// Returns [`AuthenticatedPeerEnvelopeError`] if the authenticated principal
/// is not mapped to the Raft sender, the target is not the local node, the
/// group is unknown, the peer is unauthorized or fenced, or the embedded Raft
/// message sender disagrees with the envelope sender.
pub fn validate_inbound_peer_envelope<G, P, V>(
    envelope: AuthenticatedPeerEnvelope<G, P>,
    local_node_id: NodeId,
    validator: &V,
) -> Result<PeerEnvelope<G>, AuthenticatedPeerEnvelopeError>
where
    V: AuthenticatedPeerValidator<G, P>,
{
    envelope.try_into_peer_envelope(local_node_id, validator)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use rafter::{LogIndex, Message, RequestVote, Term};

    use super::*;

    #[derive(Default)]
    struct Validator {
        known_groups: BTreeSet<u64>,
        principal_map: BTreeMap<&'static str, NodeId>,
        authorized: BTreeSet<NodeId>,
        fenced: BTreeSet<NodeId>,
    }

    impl AuthenticatedPeerValidator<u64, &'static str> for Validator {
        fn is_known_group(&self, group_id: &u64) -> bool {
            self.known_groups.contains(group_id)
        }

        fn node_for_authenticated_peer(
            &self,
            _group_id: &u64,
            peer: &&'static str,
        ) -> Option<NodeId> {
            self.principal_map.get(peer).copied()
        }

        fn principal_for_node(&self, _group_id: &u64, node_id: NodeId) -> Option<&'static str> {
            self.principal_map
                .iter()
                .find_map(|(principal, mapped)| (*mapped == node_id).then_some(*principal))
        }

        fn is_authorized_peer(&self, _group_id: &u64, node_id: NodeId) -> bool {
            self.authorized.contains(&node_id)
        }

        fn is_fenced_peer(&self, _group_id: &u64, node_id: NodeId) -> bool {
            self.fenced.contains(&node_id)
        }
    }

    #[test]
    fn peer_set_round_trips_principals() {
        let peers = PeerSet::new(vec!["node-1", "node-2"]);

        assert_eq!(peers.peers(), ["node-1", "node-2"]);
        assert_eq!(peers.into_peers(), vec!["node-1", "node-2"]);
    }

    #[test]
    fn inbound_envelope_validates_before_group_delivery() {
        let validator = validator();

        let envelope = validate_inbound_peer_envelope(envelope(), NodeId(1), &validator)
            .expect("valid envelope");

        assert_eq!(envelope.group_id, 7);
        assert_eq!(envelope.from, NodeId(2));
        assert_eq!(envelope.to, NodeId(1));
        assert_eq!(message_sender(&envelope.message), NodeId(2));
    }

    #[test]
    fn inbound_validation_rejects_unknown_group() {
        let mut envelope = envelope();
        envelope.group_id = 99;

        assert_eq!(
            validate_inbound_peer_envelope(envelope, NodeId(1), &validator()),
            Err(AuthenticatedPeerEnvelopeError::UnknownGroup)
        );
    }

    #[test]
    fn inbound_validation_rejects_unmapped_principal() {
        let mut envelope = envelope();
        envelope.authenticated_peer = "node-9";

        assert_eq!(
            validate_inbound_peer_envelope(envelope, NodeId(1), &validator()),
            Err(AuthenticatedPeerEnvelopeError::AuthenticatedPeerNotMapped)
        );
    }

    #[test]
    fn inbound_validation_rejects_principal_sender_mismatch() {
        let mut envelope = envelope();
        envelope.raft_from = NodeId(3);

        assert_eq!(
            validate_inbound_peer_envelope(envelope, NodeId(1), &validator()),
            Err(AuthenticatedPeerEnvelopeError::AuthenticatedPeerMismatch {
                expected: NodeId(2),
                actual: NodeId(3),
            })
        );
    }

    #[test]
    fn inbound_validation_rejects_wrong_recipient() {
        let mut envelope = envelope();
        envelope.raft_to = NodeId(3);

        assert_eq!(
            validate_inbound_peer_envelope(envelope, NodeId(1), &validator()),
            Err(AuthenticatedPeerEnvelopeError::WrongRecipient {
                expected: NodeId(1),
                actual: NodeId(3),
            })
        );
    }

    #[test]
    fn inbound_validation_rejects_unauthorized_peer() {
        let mut validator = validator();
        validator.authorized.clear();

        assert_eq!(
            validate_inbound_peer_envelope(envelope(), NodeId(1), &validator),
            Err(AuthenticatedPeerEnvelopeError::UnauthorizedPeer { node_id: NodeId(2) })
        );
    }

    #[test]
    fn inbound_validation_rejects_fenced_peer() {
        let mut validator = validator();
        validator.fenced.insert(NodeId(2));

        assert_eq!(
            validate_inbound_peer_envelope(envelope(), NodeId(1), &validator),
            Err(AuthenticatedPeerEnvelopeError::FencedPeer { node_id: NodeId(2) })
        );
    }

    #[test]
    fn inbound_validation_rejects_embedded_sender_mismatch() {
        let mut envelope = envelope();
        envelope.message = vote_from(NodeId(3));

        assert_eq!(
            validate_inbound_peer_envelope(envelope, NodeId(1), &validator()),
            Err(AuthenticatedPeerEnvelopeError::SenderMismatch {
                envelope_from: NodeId(2),
                message_from: NodeId(3),
            })
        );
    }

    fn validator() -> Validator {
        let mut validator = Validator::default();
        validator.known_groups.insert(7);
        validator.principal_map.insert("node-2", NodeId(2));
        validator.authorized.insert(NodeId(2));
        validator
    }

    fn envelope() -> AuthenticatedPeerEnvelope<u64, &'static str> {
        AuthenticatedPeerEnvelope {
            group_id: 7,
            authenticated_peer: "node-2",
            raft_from: NodeId(2),
            raft_to: NodeId(1),
            message: vote_from(NodeId(2)),
        }
    }

    fn vote_from(node_id: NodeId) -> Message {
        Message::RequestVote(RequestVote {
            term: Term(3),
            candidate_id: node_id,
            last_log_index: LogIndex(9),
            last_log_term: Term(2),
        })
    }
}
