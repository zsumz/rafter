//! Peer envelope types for caller-owned transport and routing.
//!
//! This module defines explicit group-aware envelopes. Authentication,
//! authorization, and the refusal of retired peers remain the responsibility of
//! the embedding runtime before messages enter the group driver.

use std::{error::Error, fmt};

use rafter::{Message, NodeId};

/// A Raft peer message annotated with caller-defined group identity.
///
/// The app layer returns envelopes to the caller; it does not send them. A
/// multi-group or route-aware runtime can inspect `group_id`, authenticate the
/// sender at its own transport boundary, and dispatch the embedded Raft
/// message under its own routing and admission policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerEnvelope<G> {
    /// Caller-defined Raft group identity.
    pub group_id: G,
    /// Raft sender identity.
    pub from: NodeId,
    /// Raft recipient identity.
    pub to: NodeId,
    /// Protocol message to route without reinterpretation.
    pub message: Message,
}

/// A transport-authenticated inbound peer message before app-layer validation.
///
/// Production runtimes should validate this envelope before converting it to
/// [`PeerEnvelope`]:
///
/// - `group_id` is known locally;
/// - `authenticated_peer` maps to `raft_from`;
/// - `raft_to` is the local node ID;
/// - the peer is not retired;
/// - the peer is authorized for the group;
/// - the sender embedded in the Raft message matches `raft_from`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticatedPeerEnvelope<G, P> {
    /// Caller-defined Raft group identity.
    pub group_id: G,
    /// Principal established by the transport security boundary.
    pub authenticated_peer: P,
    /// Raft sender claimed by the envelope.
    pub raft_from: NodeId,
    /// Raft recipient claimed by the envelope.
    pub raft_to: NodeId,
    /// Protocol message whose embedded sender must agree with the envelope.
    pub message: Message,
}

/// Policy used to validate authenticated transport envelopes.
pub trait AuthenticatedPeerValidator<G, P> {
    /// Returns whether this runtime currently hosts `group_id`.
    fn is_known_group(&self, group_id: &G) -> bool;

    /// Maps an authenticated principal to its stable Raft identity.
    fn node_for_authenticated_peer(&self, group_id: &G, peer: &P) -> Option<NodeId>;

    /// Returns the principal this deployment issues to `node_id`, when it can
    /// name one.
    ///
    /// The inverse of
    /// [`AuthenticatedPeerValidator::node_for_authenticated_peer`], and the
    /// same policy: the object that decides which replica a principal is, is
    /// the object that knows which principal a replica has. A driver needs this
    /// direction to publish a group's membership as a transport peer set.
    ///
    /// `None` means this deployment cannot name a principal for `node_id`. A
    /// caller must not read that as an empty peer set: a peer set missing a
    /// replica the membership contains authorizes fewer replicas than the
    /// cluster has, which is a quorum-splitting configuration change made by
    /// accident.
    ///
    /// # Stability
    ///
    /// **A principal is stable for the lifetime of its [`NodeId`].** The mapping
    /// may be *learned* — a directory that cannot name a replica yet answers
    /// `None` and answers it later — but once a `node_id` resolves to a
    /// principal it must keep resolving to that principal for as long as that ID
    /// exists in the group. A `NodeId` is single-use per group, retired by a
    /// committed removal, so "the lifetime of its ID" is bounded and this is not
    /// a promise about a machine, an address, or a socket.
    ///
    /// A directory that allocates replica identities is also the thing that owes
    /// [`NodeId`]'s monotonic-allocation contract: within a group, every newly
    /// admitted ID must exceed every ID ever committed before it. A driver
    /// derives which identities a removal has spent from that ordering rather
    /// than from a record of every removal, so a directory that reuses an ID
    /// below the mark — or fills a gap under it — has its replica refused as
    /// spent.
    ///
    /// The principal is a subject name, not a credential instance. Certificates,
    /// keys, and tokens rotate beneath one principal and nothing above this
    /// trait observes that they did; what may not change is which subject a
    /// replica *is*. Drivers publish peer sets as sets of replicas and compare
    /// them as such, so a directory that remapped a live ID to a different
    /// principal would leave the link layer authorizing the wrong subject with
    /// every published set still reading as current.
    ///
    /// **A removed replica need not stay resolvable.** This is asked for the
    /// replicas a driver is *authorizing*, and a driver never authorizes an
    /// identity a committed removal has spent. Retirement is published as a floor
    /// beside the authorized set rather than as a call naming each removed
    /// principal, so no lookup for a removed identity is ever made — a directory
    /// may forget the mapping as soon as the removal commits.
    fn principal_for_node(&self, group_id: &G, node_id: NodeId) -> Option<P>;

    /// Whether this deployment's current authorization policy names `node_id`.
    fn is_authorized_peer(&self, group_id: &G, node_id: NodeId) -> bool;

    /// Whether a committed removal has retired `node_id` for this group.
    ///
    /// **Derived from the authorization policy rather than recorded per
    /// principal.** An embedder's transport is handed one statement — the
    /// authorized principals and the greatest identity the group has ever
    /// committed — and this is the half of it that says "not merely unauthorized,
    /// but retired": `node_id` is at or below that floor and the authorized set
    /// does not name it.
    ///
    /// Kept distinct from [`AuthenticatedPeerValidator::is_authorized_peer`]
    /// because the two are not equally repairable: an unauthorized principal
    /// becomes authorized at the next publication, and a retired one does not,
    /// because the floor never falls.
    ///
    /// **This is asked first**, and under the derivation above it has to be. A
    /// retired identity is one the authorized set does not name, so it is
    /// unauthorized by construction — and an authorization check that ran ahead
    /// of this one answered every retired peer with the repairable variant,
    /// leaving [`AuthenticatedPeerEnvelopeError::RetiredPeer`] unreachable for
    /// every directory that follows the contract. The two are still one check
    /// from the frame's point of view; what the order decides is which of them an
    /// operator is told.
    ///
    /// A directory that cannot map principals to replicas may answer `false`
    /// here and rely on the authorization half alone; the driver's own inbound
    /// membership check refuses the retired replica either way, and the only cost
    /// is that the refusal reads as repairable when it is not.
    fn is_retired_peer(&self, group_id: &G, node_id: NodeId) -> bool;
}

/// Validation failure before an authenticated frame may enter a group.
///
/// This enum is exhaustive for the validation checks performed by the app
/// transport helper.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthenticatedPeerEnvelopeError {
    /// The runtime does not host the envelope's group.
    UnknownGroup,
    /// The authenticated principal has no Raft identity in this group.
    AuthenticatedPeerNotMapped,
    /// The principal's mapped identity differs from the envelope sender.
    AuthenticatedPeerMismatch {
        /// Raft identity established by the authenticated principal map.
        expected: NodeId,
        /// Raft identity claimed by the envelope.
        actual: NodeId,
    },
    /// The envelope targets a different local node.
    WrongRecipient {
        /// Local node identity.
        expected: NodeId,
        /// Recipient claimed by the envelope.
        actual: NodeId,
    },
    /// The sender is not in the group's current authorization policy.
    UnauthorizedPeer {
        /// Refused sender identity.
        node_id: NodeId,
    },
    /// A committed removal permanently retired the sender identity.
    RetiredPeer {
        /// Refused retired identity.
        node_id: NodeId,
    },
    /// The message's embedded sender differs from its authenticated envelope.
    SenderMismatch {
        /// Sender identity established by the envelope.
        envelope_from: NodeId,
        /// Sender identity encoded inside the Raft message.
        message_from: NodeId,
    },
}

impl fmt::Display for AuthenticatedPeerEnvelopeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownGroup => formatter.write_str("authenticated envelope targets an unknown group"),
            Self::AuthenticatedPeerNotMapped => {
                formatter.write_str("authenticated peer is not mapped to a Raft node")
            }
            Self::AuthenticatedPeerMismatch { expected, actual } => write!(
                formatter,
                "authenticated peer maps to {expected}, but the envelope claims sender {actual}"
            ),
            Self::WrongRecipient { expected, actual } => write!(
                formatter,
                "authenticated envelope targets {actual}, but this node is {expected}"
            ),
            Self::UnauthorizedPeer { node_id } => {
                write!(formatter, "peer {node_id} is not authorized for this group")
            }
            Self::RetiredPeer { node_id } => write!(
                formatter,
                "peer {node_id} was retired by a committed removal and may never speak for this group again"
            ),
            Self::SenderMismatch {
                envelope_from,
                message_from,
            } => write!(
                formatter,
                "authenticated envelope sender {envelope_from} does not match embedded message sender {message_from}"
            ),
        }
    }
}

impl Error for AuthenticatedPeerEnvelopeError {}

impl<G, P> AuthenticatedPeerEnvelope<G, P> {
    /// Validates this authenticated envelope against the local node and group
    /// authorization policy.
    ///
    /// # Errors
    ///
    /// Returns an envelope error when the group is unknown, the authenticated
    /// peer does not map to `raft_from`, the message targets another local
    /// node, the peer is retired or unauthorized, or the embedded Raft message
    /// sender does not match the envelope sender.
    pub fn validate<V>(
        &self,
        local_node_id: NodeId,
        validator: &V,
    ) -> Result<(), AuthenticatedPeerEnvelopeError>
    where
        V: AuthenticatedPeerValidator<G, P>,
    {
        if !validator.is_known_group(&self.group_id) {
            return Err(AuthenticatedPeerEnvelopeError::UnknownGroup);
        }
        let Some(mapped_node_id) =
            validator.node_for_authenticated_peer(&self.group_id, &self.authenticated_peer)
        else {
            return Err(AuthenticatedPeerEnvelopeError::AuthenticatedPeerNotMapped);
        };
        if mapped_node_id != self.raft_from {
            return Err(AuthenticatedPeerEnvelopeError::AuthenticatedPeerMismatch {
                expected: mapped_node_id,
                actual: self.raft_from,
            });
        }
        if self.raft_to != local_node_id {
            return Err(AuthenticatedPeerEnvelopeError::WrongRecipient {
                expected: local_node_id,
                actual: self.raft_to,
            });
        }
        // **Retirement first, and the order is the whole of what makes the
        // distinction sayable.** Both answers come out of one published policy —
        // authorized means "in the set", retired means "beneath the floor and
        // not in the set" — so retirement *implies* unauthorized for every
        // directory that follows the contract. Asking about authorization first
        // therefore answered every retired peer with the repairable variant, and
        // the permanent one was dead code at the boundary.
        if validator.is_retired_peer(&self.group_id, self.raft_from) {
            return Err(AuthenticatedPeerEnvelopeError::RetiredPeer {
                node_id: self.raft_from,
            });
        }
        if !validator.is_authorized_peer(&self.group_id, self.raft_from) {
            return Err(AuthenticatedPeerEnvelopeError::UnauthorizedPeer {
                node_id: self.raft_from,
            });
        }
        let message_from = message_sender(&self.message);
        if message_from != self.raft_from {
            return Err(AuthenticatedPeerEnvelopeError::SenderMismatch {
                envelope_from: self.raft_from,
                message_from,
            });
        }
        Ok(())
    }

    /// Validates and converts this authenticated envelope into a plain peer
    /// envelope for the group driver.
    ///
    /// # Errors
    ///
    /// Returns the same validation errors as
    /// [`AuthenticatedPeerEnvelope::validate`].
    pub fn try_into_peer_envelope<V>(
        self,
        local_node_id: NodeId,
        validator: &V,
    ) -> Result<PeerEnvelope<G>, AuthenticatedPeerEnvelopeError>
    where
        V: AuthenticatedPeerValidator<G, P>,
    {
        self.validate(local_node_id, validator)?;
        Ok(PeerEnvelope {
            group_id: self.group_id,
            from: self.raft_from,
            to: self.raft_to,
            message: self.message,
        })
    }
}

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
