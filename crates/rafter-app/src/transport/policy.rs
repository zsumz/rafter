//! The admission policy a deployment implements, and its refusals.
//!
//! One trait naming what a directory must answer about a principal, and
//! the closed set of reasons an authenticated frame is refused before it
//! may enter a group.

use std::{error::Error, fmt};

use rafter::NodeId;

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
