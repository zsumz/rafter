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
//!
//! The boundary itself lives in `raft` and the value it carries in `policy`,
//! both private and re-exported here. What stays in this file is the inbound
//! direction: the one function an embedder calls before a frame reaches a group.

use rafter::NodeId;

pub use rafter_app::transport::{
    message_sender, AuthenticatedPeerEnvelope, AuthenticatedPeerEnvelopeError,
    AuthenticatedPeerValidator, PeerEnvelope,
};

mod policy;
mod raft;

pub use policy::PeerPolicy;
pub use raft::{RaftTransport, SnapshotChunkEnvelope};

/// Validates and converts one authenticated inbound envelope before it enters
/// a managed [`rafter_app::group::RaftGroup`].
///
/// # Errors
///
/// Returns [`AuthenticatedPeerEnvelopeError`] if the authenticated principal
/// is not mapped to the Raft sender, the target is not the local node, the
/// group is unknown, the peer is retired or unauthorized, or the embedded Raft
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

    /// A directory holding one published [`PeerPolicy`], the way the trait asks
    /// for one: the authorized replicas and the floor beneath which an
    /// unauthorized identity is retired.
    #[derive(Default)]
    struct Validator {
        known_groups: BTreeSet<u64>,
        principal_map: BTreeMap<&'static str, NodeId>,
        authorized: BTreeSet<NodeId>,
        retirement_floor: Option<NodeId>,
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

        fn is_retired_peer(&self, _group_id: &u64, node_id: NodeId) -> bool {
            self.retirement_floor.is_some_and(|floor| node_id <= floor)
                && !self.authorized.contains(&node_id)
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

    /// An identity the policy does not authorize and the floor does not cover is
    /// unauthorized: not admitted yet, and admissible by the next publication.
    #[test]
    fn inbound_validation_rejects_unauthorized_peer() {
        let mut validator = validator();
        validator.authorized.clear();

        assert_eq!(
            validate_inbound_peer_envelope(envelope(), NodeId(1), &validator),
            Err(AuthenticatedPeerEnvelopeError::UnauthorizedPeer { node_id: NodeId(2) })
        );
    }

    /// The same identity beneath the floor is retired, and says so.
    ///
    /// The pair is the whole of the boundary's vocabulary and the two halves come
    /// from one value, so the only thing separating this case from the one above
    /// is the floor. Read at this seam rather than only at the app layer's,
    /// because this is the function an embedder calls.
    #[test]
    fn inbound_validation_rejects_a_retired_peer_as_retired() {
        let mut validator = validator();
        validator.authorized.clear();
        validator.retirement_floor = Some(NodeId(2));

        assert_eq!(
            validate_inbound_peer_envelope(envelope(), NodeId(1), &validator),
            Err(AuthenticatedPeerEnvelopeError::RetiredPeer { node_id: NodeId(2) })
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
