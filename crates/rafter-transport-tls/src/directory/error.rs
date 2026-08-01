//! Peer-directory errors.

use std::{error::Error, fmt};

use rafter::NodeId;

use crate::PeerId;

/// Refusal while updating the authenticated peer directory.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DirectoryError {
    /// Adding another group would exceed the configured bound.
    GroupLimit {
        /// Maximum configured groups.
        maximum: usize,
    },
    /// Adding another mapping would exceed one group's configured bound.
    BindingLimit {
        /// Maximum configured bindings per group.
        maximum: usize,
    },
    /// The requested group is not known locally.
    UnknownGroup,
    /// One node ID was already bound to another principal.
    NodeAlreadyBound {
        /// Node whose stable binding would change.
        node_id: NodeId,
        /// Existing transport principal.
        existing: PeerId,
        /// Requested transport principal.
        requested: PeerId,
    },
    /// One principal was already bound to another live node in the same group.
    PeerAlreadyBound {
        /// Principal whose live group identity would change.
        peer_id: PeerId,
        /// Existing node identity.
        existing: NodeId,
        /// Requested node identity.
        requested: NodeId,
    },
    /// A new mapping attempted to use an already-retired node identity.
    RetiredNodeBinding {
        /// Refused node identity.
        node_id: NodeId,
        /// Monotonic retirement floor.
        retirement_floor: NodeId,
    },
    /// A policy attempted to reauthorize a previously retired node.
    RetiredNodeReauthorization {
        /// Refused node identity.
        node_id: NodeId,
        /// Monotonic retirement floor.
        retirement_floor: NodeId,
    },
    /// A policy named a principal with no node binding in that group.
    UnknownPolicyPeer {
        /// Unmapped principal.
        peer_id: PeerId,
    },
    /// A policy named the same principal more than once.
    DuplicatePolicyPeer {
        /// Duplicate principal.
        peer_id: PeerId,
    },
    /// A caller attempted to forget a node still authorized by policy.
    AuthorizedNodeUnbind {
        /// Still-authorized node identity.
        node_id: NodeId,
    },
    /// A panic poisoned shared directory state; operations fail closed.
    Poisoned,
}

impl fmt::Display for DirectoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GroupLimit { maximum } => write!(
                formatter,
                "peer directory already holds its maximum {maximum} groups"
            ),
            Self::BindingLimit { maximum } => write!(
                formatter,
                "group already holds its maximum {maximum} peer bindings"
            ),
            Self::UnknownGroup => formatter.write_str("peer directory does not know this group"),
            Self::NodeAlreadyBound {
                node_id,
                existing,
                requested,
            } => write!(
                formatter,
                "{node_id} is bound to {existing}, not requested principal {requested}"
            ),
            Self::PeerAlreadyBound {
                peer_id,
                existing,
                requested,
            } => write!(
                formatter,
                "peer {peer_id} is bound to {existing}, not requested node {requested}"
            ),
            Self::RetiredNodeBinding {
                node_id,
                retirement_floor,
            } => write!(
                formatter,
                "cannot bind retired {node_id} at or below floor {retirement_floor}"
            ),
            Self::RetiredNodeReauthorization {
                node_id,
                retirement_floor,
            } => write!(
                formatter,
                "cannot reauthorize retired {node_id} at or below floor \
                 {retirement_floor}"
            ),
            Self::UnknownPolicyPeer { peer_id } => write!(
                formatter,
                "peer policy names {peer_id}, which has no node binding in this group"
            ),
            Self::DuplicatePolicyPeer { peer_id } => {
                write!(formatter, "peer policy names {peer_id} more than once")
            }
            Self::AuthorizedNodeUnbind { node_id } => write!(
                formatter,
                "cannot forget {node_id} while the installed policy authorizes it"
            ),
            Self::Poisoned => formatter.write_str("peer directory state is poisoned"),
        }
    }
}

impl Error for DirectoryError {}
