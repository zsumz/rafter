//! Peer envelope types for caller-owned transport and routing.
//!
//! This module defines explicit group-aware envelopes. Authentication,
//! authorization, and the refusal of retired peers remain the responsibility of
//! the embedding runtime before messages enter the group driver.

use rafter::{Message, NodeId};

mod envelope;
mod policy;
mod validation;

pub use envelope::{AuthenticatedPeerEnvelope, PeerEnvelope};
pub use policy::{AuthenticatedPeerEnvelopeError, AuthenticatedPeerValidator};

/// Returns the Raft node ID carried as sender by a protocol message.
#[must_use]
pub fn message_sender(message: &Message) -> NodeId {
    match message {
        Message::AppendEntries(message) => message.leader_id,
        Message::AppendEntriesResponse(message) => message.follower_id,
        Message::InstallSnapshot(message) => message.leader_id,
        Message::InstallSnapshotChunk(message) => message.leader_id,
        Message::InstallSnapshotResponse(message) => message.follower_id,
        Message::PreVote(message) => message.candidate_id,
        Message::PreVoteResponse(message) => message.voter_id,
        Message::TimeoutNow(message) => message.leader_id,
        Message::RequestVote(message) => message.candidate_id,
        Message::RequestVoteResponse(message) => message.voter_id,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use rafter::{LogIndex, RequestVote, Term};

    use super::*;

    /// A directory that derives both admission answers from one published
    /// policy, which is what the trait asks of one.
    ///
    /// Two independent sets here would let a fixture state a pair no deployment
    /// can hold — retired and authorized at once — and it did: the retirement
    /// arm was only reachable from such a pair, so it passed its tests while
    /// being dead for every directory that follows the contract.
    #[derive(Default)]
    struct TestValidator {
        known_groups: BTreeSet<u64>,
        principals: BTreeMap<&'static str, NodeId>,
        authorized: BTreeSet<NodeId>,
        retirement_floor: Option<NodeId>,
    }

    impl AuthenticatedPeerValidator<u64, &'static str> for TestValidator {
        fn is_known_group(&self, group_id: &u64) -> bool {
            self.known_groups.contains(group_id)
        }

        fn node_for_authenticated_peer(
            &self,
            _group_id: &u64,
            peer: &&'static str,
        ) -> Option<NodeId> {
            self.principals.get(peer).copied()
        }

        fn principal_for_node(&self, _group_id: &u64, node_id: NodeId) -> Option<&'static str> {
            self.principals
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

    fn validator() -> TestValidator {
        let mut validator = TestValidator::default();
        validator.known_groups.insert(7);
        validator.principals.insert("node-2", NodeId(2));
        validator.authorized.insert(NodeId(2));
        validator
    }

    fn vote_from(node_id: NodeId) -> Message {
        Message::RequestVote(RequestVote {
            term: Term(3),
            candidate_id: node_id,
            last_log_index: LogIndex(9),
            last_log_term: Term(2),
        })
    }

    fn authenticated_envelope() -> AuthenticatedPeerEnvelope<u64, &'static str> {
        AuthenticatedPeerEnvelope {
            group_id: 7,
            authenticated_peer: "node-2",
            raft_from: NodeId(2),
            raft_to: NodeId(1),
            message: vote_from(NodeId(2)),
        }
    }

    #[test]
    fn peer_envelope_carries_route_and_message() {
        let envelope = PeerEnvelope {
            group_id: 7,
            from: NodeId(2),
            to: NodeId(1),
            message: vote_from(NodeId(2)),
        };

        assert_eq!(envelope.group_id, 7);
        assert_eq!(envelope.from, NodeId(2));
        assert_eq!(envelope.to, NodeId(1));
        assert_eq!(message_sender(&envelope.message), NodeId(2));
    }

    #[test]
    fn authenticated_envelope_validates_and_converts() {
        let envelope = authenticated_envelope();
        let peer_envelope = envelope
            .try_into_peer_envelope(NodeId(1), &validator())
            .expect("valid envelope converts");

        assert_eq!(peer_envelope.group_id, 7);
        assert_eq!(peer_envelope.from, NodeId(2));
        assert_eq!(peer_envelope.to, NodeId(1));
        assert_eq!(message_sender(&peer_envelope.message), NodeId(2));
    }

    #[test]
    fn authenticated_envelope_rejects_unknown_group() {
        let mut envelope = authenticated_envelope();
        envelope.group_id = 99;

        assert_eq!(
            envelope.validate(NodeId(1), &validator()),
            Err(AuthenticatedPeerEnvelopeError::UnknownGroup)
        );
    }

    #[test]
    fn authenticated_envelope_rejects_wrong_target() {
        let mut envelope = authenticated_envelope();
        envelope.raft_to = NodeId(3);

        assert_eq!(
            envelope.validate(NodeId(1), &validator()),
            Err(AuthenticatedPeerEnvelopeError::WrongRecipient {
                expected: NodeId(1),
                actual: NodeId(3),
            })
        );
    }

    /// A retired identity is reported as retired rather than as merely
    /// unauthorized.
    ///
    /// **The distinction is the whole reason both predicates exist**, and it was
    /// unreachable. Retirement is derived from the published policy — at or below
    /// the floor, and not in the authorized set — so a retired identity is
    /// *by definition* unauthorized, and an authorization check that ran first
    /// answered every retired peer with the repairable variant. An operator
    /// reading it could not tell "the control plane has not caught up with this
    /// replica yet" from "the cluster consumed this identity and never will
    /// again".
    #[test]
    fn authenticated_envelope_rejects_a_retired_peer_as_retired() {
        let mut validator = validator();
        // The committed removal that retired node 2: out of the authorized set,
        // and beneath a floor that covers it.
        validator.authorized.remove(&NodeId(2));
        validator.retirement_floor = Some(NodeId(2));

        assert_eq!(
            authenticated_envelope().validate(NodeId(1), &validator),
            Err(AuthenticatedPeerEnvelopeError::RetiredPeer { node_id: NodeId(2) })
        );
    }

    /// An identity *above* the floor is unauthorized and not retired.
    ///
    /// The control for the clause above, and the direction the two differ in: a
    /// replica this deployment has provisioned and the cluster has not admitted
    /// becomes authorized at the next publication, so reporting it as retired
    /// would name a permanent condition for a transient one.
    #[test]
    fn authenticated_envelope_rejects_an_unadmitted_peer_as_unauthorized() {
        let mut validator = validator();
        validator.authorized.remove(&NodeId(2));
        validator.retirement_floor = Some(NodeId(1));

        assert_eq!(
            authenticated_envelope().validate(NodeId(1), &validator),
            Err(AuthenticatedPeerEnvelopeError::UnauthorizedPeer { node_id: NodeId(2) })
        );
    }

    #[test]
    fn authenticated_envelope_rejects_sender_mismatch() {
        let mut envelope = authenticated_envelope();
        envelope.message = vote_from(NodeId(3));

        assert_eq!(
            envelope.validate(NodeId(1), &validator()),
            Err(AuthenticatedPeerEnvelopeError::SenderMismatch {
                envelope_from: NodeId(2),
                message_from: NodeId(3),
            })
        );
    }

    #[test]
    fn authenticated_peer_envelope_error_is_a_standard_error() {
        let error = AuthenticatedPeerEnvelopeError::AuthenticatedPeerMismatch {
            expected: NodeId(2),
            actual: NodeId(3),
        };
        let standard_error: &(dyn std::error::Error + 'static) = &error;

        assert_eq!(
            standard_error.to_string(),
            "authenticated peer maps to node-2, but the envelope claims sender node-3"
        );
    }
}
