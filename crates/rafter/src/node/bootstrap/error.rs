//! Errors returned when a persisted Raft image cannot be hydrated.

use std::{error::Error, fmt};

use crate::{LogIndex, NodeId, Term};

/// Validation error returned when a bootstrap image cannot form a legal node.
///
/// This enum is exhaustive because bootstrap validation is closed over these
/// persisted-state invariants.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BootstrapValidationError {
    VoteForNonVoter {
        voted_for: NodeId,
    },
    VoteInZeroTerm {
        voted_for: NodeId,
    },
    /// The declared applied floor lies beyond the persisted log.
    AppliedFloorBeyondLog {
        applied_through: LogIndex,
        last_log_index: LogIndex,
    },
    /// The declared applied floor lies beyond the recovered committed prefix.
    AppliedFloorBeyondCommit {
        applied_through: LogIndex,
        commit_index: LogIndex,
    },
    NonContiguousLog {
        expected: LogIndex,
        actual: LogIndex,
    },
    ZeroTermLogEntry {
        index: LogIndex,
    },
    EntryTermAheadOfCurrentTerm {
        index: LogIndex,
        entry_term: Term,
        current_term: Term,
    },
    SnapshotWriterNonVoter {
        writer_id: NodeId,
    },
    SnapshotHardStateTermAheadOfCurrentTerm {
        snapshot_hard_state_term: Term,
        current_term: Term,
    },
    CompactedLogEntry {
        snapshot_index: LogIndex,
        entry_index: LogIndex,
    },
    SnapshotBoundaryTermMismatch {
        index: LogIndex,
        snapshot_term: Term,
        entry_term: Term,
    },
    MultipleUncommittedConfigurationEntries {
        first_index: LogIndex,
        second_index: LogIndex,
    },
    CommitIndexBeyondLog {
        commit_index: LogIndex,
        last_log_index: LogIndex,
    },
    CommittedConfigurationAheadOfCommit {
        committed_configuration_index: LogIndex,
        commit_index: LogIndex,
    },
    CommittedConfigurationMissing {
        committed_configuration_index: LogIndex,
    },
    CommittedConfigurationIdMismatch {
        index: LogIndex,
        expected: crate::ConfigurationId,
        actual: crate::ConfigurationId,
    },
    CommittedConfigurationNotLatest {
        recorded_index: LogIndex,
        latest_index: LogIndex,
    },
    CompactedCommittedConfigurationWithoutSnapshotMembership {
        committed_configuration_index: LogIndex,
    },
    /// The log reaches the maximum representable index: its successor
    /// cannot exist and index arithmetic on it overflows.
    LogIndexAtMaximum {
        index: LogIndex,
    },
}

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
            Self::SnapshotWriterNonVoter { .. }
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

impl BootstrapValidationError {
    fn fmt_snapshot_error(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SnapshotWriterNonVoter { writer_id } => write!(
                formatter,
                "Raft bootstrap snapshot writer {writer_id} is not a voter"
            ),
            Self::SnapshotHardStateTermAheadOfCurrentTerm {
                snapshot_hard_state_term,
                current_term,
            } => write!(
                formatter,
                concat!(
                    "Raft bootstrap snapshot hard-state term {snapshot_hard_state_term} ",
                    "is ahead of current term {current_term}"
                ),
                snapshot_hard_state_term = snapshot_hard_state_term,
                current_term = current_term,
            ),
            Self::CompactedLogEntry {
                snapshot_index,
                entry_index,
            } => write!(
                formatter,
                concat!(
                    "Raft bootstrap log entry at index {entry_index} is already compacted ",
                    "by snapshot index {snapshot_index}"
                ),
                entry_index = entry_index,
                snapshot_index = snapshot_index,
            ),
            Self::SnapshotBoundaryTermMismatch {
                index,
                snapshot_term,
                entry_term,
            } => write!(
                formatter,
                concat!(
                    "Raft bootstrap boundary entry at index {index} has term {entry_term} ",
                    "but the snapshot recorded term {snapshot_term}"
                ),
                index = index,
                entry_term = entry_term,
                snapshot_term = snapshot_term,
            ),
            _ => unreachable!("caller filters snapshot errors"),
        }
    }

    fn fmt_committed_state_error(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CommitIndexBeyondLog {
                commit_index,
                last_log_index,
            } => write!(
                formatter,
                concat!(
                    "Raft bootstrap commit index {commit_index} lies beyond the persisted ",
                    "log end {last_log_index}"
                ),
                commit_index = commit_index,
                last_log_index = last_log_index,
            ),
            Self::CommittedConfigurationAheadOfCommit {
                committed_configuration_index,
                commit_index,
            } => write!(
                formatter,
                concat!(
                    "Raft bootstrap committed configuration index ",
                    "{committed_configuration_index} lies beyond the commit index ",
                    "{commit_index}"
                ),
                committed_configuration_index = committed_configuration_index,
                commit_index = commit_index,
            ),
            Self::CommittedConfigurationMissing {
                committed_configuration_index,
            } => write!(
                formatter,
                concat!(
                    "Raft bootstrap committed configuration index ",
                    "{committed_configuration_index} does not point at a retained ",
                    "configuration entry"
                ),
                committed_configuration_index = committed_configuration_index,
            ),
            Self::CommittedConfigurationIdMismatch {
                index,
                expected,
                actual,
            } => write!(
                formatter,
                concat!(
                    "Raft bootstrap committed configuration at index {index} has id ",
                    "{actual} but hard state recorded {expected}"
                ),
                index = index,
                actual = actual,
                expected = expected,
            ),
            Self::CommittedConfigurationNotLatest {
                recorded_index,
                latest_index,
            } => write!(
                formatter,
                concat!(
                    "Raft bootstrap committed configuration index {recorded_index} is ",
                    "older than latest committed configuration index {latest_index}"
                ),
                recorded_index = recorded_index,
                latest_index = latest_index,
            ),
            Self::CompactedCommittedConfigurationWithoutSnapshotMembership {
                committed_configuration_index,
            } => write!(
                formatter,
                concat!(
                    "Raft bootstrap committed configuration index ",
                    "{committed_configuration_index} is compacted but the snapshot ",
                    "records no committed membership"
                ),
                committed_configuration_index = committed_configuration_index,
            ),
            Self::VoteForNonVoter { .. }
            | Self::VoteInZeroTerm { .. }
            | Self::AppliedFloorBeyondLog { .. }
            | Self::AppliedFloorBeyondCommit { .. }
            | Self::NonContiguousLog { .. }
            | Self::ZeroTermLogEntry { .. }
            | Self::EntryTermAheadOfCurrentTerm { .. }
            | Self::SnapshotWriterNonVoter { .. }
            | Self::SnapshotHardStateTermAheadOfCurrentTerm { .. }
            | Self::CompactedLogEntry { .. }
            | Self::SnapshotBoundaryTermMismatch { .. }
            | Self::MultipleUncommittedConfigurationEntries { .. }
            | Self::LogIndexAtMaximum { .. } => {
                unreachable!("caller filters committed-state errors")
            }
        }
    }
}
