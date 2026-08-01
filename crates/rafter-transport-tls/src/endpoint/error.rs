//! Endpoint-book errors.

use std::{error::Error, fmt};

/// Refusal while reading or replacing resolved endpoints.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EndpointBookError {
    /// Endpoint replacement must name at least one endpoint.
    Empty,
    /// One endpoint appeared more than once in the replacement set.
    Duplicate {
        /// Zero-based index of the duplicate occurrence.
        index: usize,
    },
    /// One peer's endpoint set exceeded its configured bound.
    EndpointLimit {
        /// Submitted endpoint count.
        actual: usize,
        /// Maximum accepted count.
        maximum: usize,
    },
    /// Adding another peer would exceed the configured bound.
    PeerLimit {
        /// Maximum configured peers.
        maximum: usize,
    },
    /// The monotonic generation reached `u64::MAX`.
    GenerationExhausted,
    /// A panic poisoned the shared endpoint state; operations fail closed.
    Poisoned,
}

impl fmt::Display for EndpointBookError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("peer endpoint replacement must not be empty"),
            Self::Duplicate { index } => {
                write!(formatter, "peer endpoint at index {index} is a duplicate")
            }
            Self::EndpointLimit { actual, maximum } => write!(
                formatter,
                "peer endpoint replacement has {actual} entries, exceeding maximum {maximum}"
            ),
            Self::PeerLimit { maximum } => write!(
                formatter,
                "endpoint book already holds its maximum {maximum} peers"
            ),
            Self::GenerationExhausted => formatter.write_str("endpoint generation is exhausted"),
            Self::Poisoned => formatter.write_str("endpoint book state is poisoned"),
        }
    }
}

impl Error for EndpointBookError {}
