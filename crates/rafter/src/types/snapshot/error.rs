//! Snapshot metadata validation errors and operator-facing diagnostics.

use std::{error::Error, fmt};

use super::super::{LogIndex, Term};

/// Errors returned while constructing snapshot metadata.
///
/// This enum is exhaustive because snapshot metadata validation is closed over
/// these structural checks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotMetadataError {
    /// Application snapshot version zero is reserved and invalid.
    ZeroApplicationSnapshotVersion,
    /// A snapshot cannot represent the empty log prefix.
    ZeroLastIncludedIndex,
    /// A boundary at the maximum log index has no successor: nothing could
    /// ever be appended after it, and index arithmetic on it overflows.
    LastIncludedIndexAtMaximum,
    /// A non-empty snapshot boundary carried term zero.
    ZeroLastIncludedTerm {
        /// Boundary index whose term was zero.
        last_included_index: LogIndex,
    },
    /// The snapshot boundary term exceeds the writer's visible hard-state term.
    SnapshotTermAheadOfHardState {
        /// Snapshot boundary index.
        last_included_index: LogIndex,
        /// Term stored at the snapshot boundary.
        last_included_term: Term,
        /// Greatest term visible in durable hard state.
        hard_state_term: Term,
    },
}

impl fmt::Display for SnapshotMetadataError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroApplicationSnapshotVersion => {
                formatter.write_str("application snapshot version cannot be zero")
            }
            Self::ZeroLastIncludedIndex => {
                formatter.write_str("Raft snapshot last included index cannot be zero")
            }
            Self::LastIncludedIndexAtMaximum => formatter
                .write_str("Raft snapshot last included index cannot be the maximum log index"),
            Self::ZeroLastIncludedTerm {
                last_included_index,
            } => write!(
                formatter,
                "Raft snapshot last included term at index {last_included_index} cannot be zero"
            ),
            Self::SnapshotTermAheadOfHardState {
                last_included_index,
                last_included_term,
                hard_state_term,
            } => write!(
                formatter,
                concat!(
                    "Raft snapshot term {last_included_term} at index {last_included_index} ",
                    "is ahead of hard-state term {hard_state_term}"
                ),
                last_included_term = last_included_term,
                last_included_index = last_included_index,
                hard_state_term = hard_state_term,
            ),
        }
    }
}

impl Error for SnapshotMetadataError {}
