//! The managed service transport trait and its validation glue.
//!
//! Production transports must authenticate peers before constructing an
//! authenticated envelope, keep each group's authorization policy current —
//! which is who may speak *and* which identities the cluster has retired, in one
//! value — and use the current `rafter-codec` peer wire format. This crate
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

/// Who may speak for a group, and which identities are retired.
///
/// **One statement rather than a set plus a stream of per-principal fences**, and
/// that is the whole design. Retiring a replica used to be an operation —
/// `fence_peer`, once per removal, permanent, and owed until the link layer took
/// it — which meant a driver had to *remember which removals it had already
/// acted on*. That memory was a bounded set with an unbounded question attached:
/// "has this identity's fence been made" is not the same question as "may this
/// identity be admitted again", and one bit answered both. An exact committed
/// removal arriving beneath a later observation was suppressed as
/// already-handled, and the join that merged two records was not order-free in
/// the fences it derived.
///
/// A floor answers the second question and deletes the first. Under the
/// single-use, monotonically-allocated identity contract [`NodeId`] states,
/// every identity a group has ever committed is at or below the greatest one it
/// has committed — so "authorized, plus the greatest identity ever committed" is
/// a complete statement of who may speak and who is retired, with no per-removal
/// history behind it.
///
/// # The rule
///
/// A principal in `peers` is authorized. An identity at or below
/// `retirement_floor` whose principal is **not** in `peers` is **retired**: the
/// cluster committed its removal, and it must never be admitted under that
/// identity again. An identity *above* the floor is simply not authorized yet —
/// a replica whose addition has not committed here, or one this deployment has
/// provisioned and the cluster has not admitted.
///
/// The distinction is worth carrying because the two are not equally repairable.
/// An unauthorized principal becomes authorized by the next publication; a
/// retired one does not, because the floor never falls.
///
/// # What an implementation may assume, and what it may not
///
/// **The floor is monotone.** A driver's floor is the greatest identity it has
/// ever seen in a committed configuration, so a later publication never carries a
/// lower one. An implementation may therefore keep the highest floor it has ever
/// accepted, and doing so is what makes a *stale* policy unable to widen
/// admission: a publication the link layer missed leaves fewer identities
/// retired, never more.
///
/// **The set is authoritative and is re-read on every publication.** This value
/// replaces the previous one whole. An implementation must not latch a denial
/// past the policy that produced it — if a later policy authorizes a principal,
/// that principal is authorized, and a link torn down for a retired peer must be
/// re-establishable.
///
/// That is weaker than the append-only fenced set `fence_peer` allowed, and the
/// weakening is exactly the unbounded per-removal record this design deletes.
/// What replaces it is a property of the *driver*: it never authorizes an
/// identity its own spent test refuses, and that test is monotone for the life of
/// an incarnation. See `PeerControlPlaneCheckpoint` for what carries it across
/// one.
///
/// **The local replica is not a peer of itself**, so it is never in `peers` and
/// this policy says nothing about it. A node does not authorize its own frames,
/// and a driver adopted under a fresh identity retires its previous one here like
/// any other: the old identity is at or below the floor and absent from the set.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PeerPolicy<P> {
    peers: Vec<P>,
    retirement_floor: Option<NodeId>,
}

impl<P> PeerPolicy<P> {
    /// Creates a policy from the authorized principals and the retirement floor.
    ///
    /// `retirement_floor` is `None` before this group has committed any
    /// configuration, which retires nothing. It is not zero: [`NodeId`] `0` is a
    /// legal identity and cannot double as "no floor".
    #[must_use]
    pub fn new(peers: Vec<P>, retirement_floor: Option<NodeId>) -> Self {
        Self {
            peers,
            retirement_floor,
        }
    }

    /// Returns the currently authorized principals.
    #[must_use]
    pub fn peers(&self) -> &[P] {
        &self.peers
    }

    /// Consumes the policy and returns the authorized principals.
    #[must_use]
    pub fn into_peers(self) -> Vec<P> {
        self.peers
    }

    /// Returns the greatest identity this group has ever committed, if any.
    ///
    /// Half of the denial rule; [`PeerPolicy::peers`] is the other half. An
    /// implementation that maps principals to replicas — which every
    /// [`AuthenticatedPeerValidator`] does — refuses an identity at or below this
    /// whose principal the set does not name.
    #[must_use]
    pub fn retirement_floor(&self) -> Option<NodeId> {
        self.retirement_floor
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

    /// Replaces the authorization policy for `group_id`: who may speak, and
    /// which identities are retired.
    ///
    /// **The only admission statement this boundary carries**, and it used to be
    /// two. A driver published a peer set here and retired replicas through a
    /// separate per-principal `fence_peer` call that was permanent, fallible, and
    /// therefore owed until accepted. Both statements are now one value, which is
    /// what removes the driver's obligation ledger and everything that could go
    /// wrong inside it — see [`PeerPolicy`] for what the floor buys and what it
    /// gives up.
    ///
    /// Idempotent, and may be called with a policy the transport already holds.
    /// A driver retries a refused publication at its next entry point rather than
    /// waiting for the cluster's next configuration change, so an implementation
    /// must treat a repeat of the current policy as a no-op rather than as a
    /// reconfiguration.
    ///
    /// # What a refusal costs
    ///
    /// A refused publication leaves the link layer holding the *previous* policy,
    /// which authorizes a set the cluster has moved on from and retires fewer
    /// identities than it should. That is stale rather than wrong in the
    /// dangerous direction — the floor is monotone, so an older policy never
    /// retires an identity the current one does not — and the driver's own
    /// inbound membership check refuses the retired replica in the meantime. Two
    /// layers, and this is the outer one.
    ///
    /// **Restarting a replica is not removing it.** A replica killed and
    /// restarted under the same node ID was never removed, stays in the peer set,
    /// and keeps its principal across the restart. Nothing on this boundary
    /// changes for it.
    ///
    /// # Errors
    ///
    /// Returns the transport implementation's error when the policy cannot be
    /// installed. A refusal is not final: the driver holds the policy it could
    /// not publish and tries again at every later entry point.
    fn update_peers(
        &self,
        group_id: &G,
        policy: PeerPolicy<Self::PeerPrincipal>,
    ) -> Result<(), Self::Error>;
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
    fn peer_policy_round_trips_principals_and_floor() {
        let policy = PeerPolicy::new(vec!["node-1", "node-2"], Some(NodeId(6)));

        assert_eq!(policy.peers(), ["node-1", "node-2"]);
        assert_eq!(policy.retirement_floor(), Some(NodeId(6)));
        assert_eq!(policy.into_peers(), vec!["node-1", "node-2"]);
    }

    /// A group that has committed nothing retires nothing.
    ///
    /// `None` rather than zero, because `NodeId(0)` is a legal identity: a floor
    /// of zero retires it, and "no committed configuration observed" must not.
    #[test]
    fn a_policy_with_no_floor_retires_nothing() {
        let policy = PeerPolicy::<&str>::new(Vec::new(), None);

        assert_eq!(policy.retirement_floor(), None);
        assert!(policy.peers().is_empty());
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
