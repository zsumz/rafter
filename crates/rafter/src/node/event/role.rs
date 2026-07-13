//! Raft role vocabulary.
//!
//! Roles describe volatile authority state. Election and lifecycle modules own
//! transitions between them.

use std::fmt;

/// Current role of a Raft node.
///
/// This enum is exhaustive because the Raft role state machine has a closed
/// set of roles.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Role {
    Follower,
    /// Probing electability with a pre-vote round (thesis 9.6): the node has
    /// timed out but has not incremented its term or voted for itself.
    PreCandidate,
    Candidate,
    Leader,
}

impl fmt::Display for Role {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Follower => "follower",
            Self::PreCandidate => "pre-candidate",
            Self::Candidate => "candidate",
            Self::Leader => "leader",
        })
    }
}
