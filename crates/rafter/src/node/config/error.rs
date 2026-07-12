//! Closed validation errors for static node configuration.

use std::{error::Error, fmt};

use crate::NodeId;

/// Error returned while building a [`NodeConfig`](super::NodeConfig).
///
/// This enum is exhaustive because node configuration validation is closed
/// over these structural errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeConfigError {
    EmptyVoters,
    SelfPeer {
        id: NodeId,
    },
    DuplicatePeer {
        peer: NodeId,
    },
    /// A zero election timeout can never fire; it was previously accepted
    /// silently and produced a node that never campaigns.
    ZeroElectionTimeout,
}

impl fmt::Display for NodeConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyVoters => {
                formatter.write_str("Raft node config requires at least one voter")
            }
            Self::SelfPeer { id } => {
                write!(formatter, "Raft node {id} cannot list itself as a peer")
            }
            Self::DuplicatePeer { peer } => {
                write!(formatter, "Raft peer {peer} appears more than once")
            }
            Self::ZeroElectionTimeout => {
                formatter.write_str("election timeout must be at least one tick")
            }
        }
    }
}

impl Error for NodeConfigError {}
