//! Pure session-state transition failures and their reports.
//!
//! This module owns the closed set of reasons a session transition is refused
//! and how each one reads to an operator. It performs no transition itself and
//! holds no state.

use std::{error::Error, fmt};

use crate::PeerId;

/// Pure session-state transition failure.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SessionStateError {
    /// Adding another physical peer would exceed the configured bound.
    PeerLimit {
        /// Maximum retained peer records.
        maximum: usize,
    },
    /// One peer consumed the complete outbound session number space.
    OutboundExhausted {
        /// Peer whose next session cannot be represented.
        peer: PeerId,
    },
    /// One peer consumed the complete inbound session number space.
    InboundExhausted {
        /// Peer from which no newer session can be represented.
        peer: PeerId,
    },
    /// Recovered state contained a record with no high-water in either direction.
    EmptyPeerRecord {
        /// Peer named by the noncanonical empty record.
        peer: PeerId,
    },
}

impl fmt::Display for SessionStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PeerLimit { maximum } => write!(
                formatter,
                "session state already retains its maximum {maximum} peers"
            ),
            Self::OutboundExhausted { peer } => {
                write!(
                    formatter,
                    "outbound connection sessions for {peer} are exhausted"
                )
            }
            Self::InboundExhausted { peer } => {
                write!(
                    formatter,
                    "inbound connection sessions from {peer} are exhausted"
                )
            }
            Self::EmptyPeerRecord { peer } => {
                write!(
                    formatter,
                    "session state contains an empty record for {peer}"
                )
            }
        }
    }
}

impl Error for SessionStateError {}
