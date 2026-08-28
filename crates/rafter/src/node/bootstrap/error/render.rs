//! Human-readable rendering of bootstrap validation refusals.
//!
//! Rendering is separate from the refusal taxonomy so the variant contracts
//! in `error.rs` stay adjacent to the preconditions they document.

use std::{error::Error, fmt};

use super::BootstrapValidationError;

impl fmt::Display for BootstrapValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::VoteForNonVoter { voted_for } => write!(
                formatter,
                "Raft bootstrap records a vote for {voted_for} which is not a configured voter"
            ),
            Self::VoteInZeroTerm { voted_for } => write!(
                formatter,
                "Raft bootstrap records a vote for {voted_for} in term zero"
            ),
            Self::AppliedFloorBeyondLog {
                applied_through,
                last_log_index,
            } => write!(
                formatter,
                concat!(
                    "declared applied floor {applied_through} lies beyond the persisted ",
                    "log end {last_log_index}"
                ),
                applied_through = applied_through,
                last_log_index = last_log_index,
            ),
            Self::AppliedFloorBeyondCommit {
                applied_through,
                commit_index,
            } => write!(
                formatter,
                concat!(
                    "declared applied floor {applied_through} lies beyond the recovered ",
                    "commit index {commit_index}"
                ),
                applied_through = applied_through,
                commit_index = commit_index,
            ),
            Self::NonContiguousLog { expected, actual } => write!(
                formatter,
                concat!(
                    "Raft bootstrap log entry at index {actual} is not contiguous with ",
                    "expected index {expected}"
                ),
                actual = actual,
                expected = expected,
            ),
            Self::ZeroTermLogEntry { index } => write!(
                formatter,
                "Raft bootstrap log entry at index {index} has term zero"
            ),
            Self::EntryTermAheadOfCurrentTerm {
                index,
                entry_term,
                current_term,
            } => write!(
                formatter,
                concat!(
                    "Raft bootstrap log entry at index {index} has term {entry_term} ",
                    "ahead of current term {current_term}"
                ),
                index = index,
                entry_term = entry_term,
                current_term = current_term,
            ),
            Self::SnapshotWriterNotReplica { .. }
            | Self::SnapshotHardStateTermAheadOfCurrentTerm { .. }
            | Self::CompactedLogEntry { .. }
            | Self::SnapshotBoundaryTermMismatch { .. } => self.fmt_snapshot_error(formatter),
            Self::LogIndexAtMaximum { index } => write!(
                formatter,
                "Raft bootstrap log entry at index {index} is at the maximum representable index"
            ),
            Self::MultipleUncommittedConfigurationEntries {
                first_index,
                second_index,
            } => write!(
                formatter,
                concat!(
                    "Raft bootstrap log holds uncommitted configuration entries at ",
                    "indexes {first_index} and {second_index}"
                ),
                first_index = first_index,
                second_index = second_index,
            ),
            Self::CommitIndexBeyondLog { .. }
            | Self::CommittedConfigurationAheadOfCommit { .. }
            | Self::CommittedConfigurationMissing { .. }
            | Self::CommittedConfigurationIdMismatch { .. }
            | Self::CommittedConfigurationNotLatest { .. }
            | Self::CompactedCommittedConfigurationWithoutSnapshotMembership { .. } => {
                self.fmt_committed_state_error(formatter)
            }
        }
    }
}

impl Error for BootstrapValidationError {}
