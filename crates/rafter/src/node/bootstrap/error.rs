//! Errors returned when a persisted Raft image cannot be hydrated.

use std::{error::Error, fmt};

use crate::{LogIndex, NodeId, Term};

/// Validation error returned when a bootstrap image cannot form a legal node.
///
/// This enum is exhaustive because bootstrap validation is closed over these
/// persisted-state invariants.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BootstrapValidationError {
    /// Durable vote targets a node outside the recovered voter set.
    VoteForNonVoter {
        /// Invalid durable vote target.
        voted_for: NodeId,
    },
    /// Durable state records a vote in reserved term zero.
    VoteInZeroTerm {
        /// Vote target recorded in term zero.
        voted_for: NodeId,
    },
    /// The declared applied floor lies beyond the persisted log.
    AppliedFloorBeyondLog {
        /// Caller-declared applied floor.
        applied_through: LogIndex,
        /// Greatest index available from snapshot plus retained log.
        last_log_index: LogIndex,
    },
    /// The declared applied floor lies beyond the recovered committed prefix.
    AppliedFloorBeyondCommit {
        /// Caller-declared applied floor.
        applied_through: LogIndex,
        /// Durable committed prefix.
        commit_index: LogIndex,
    },
    /// Recovered log entries do not form one contiguous suffix.
    NonContiguousLog {
        /// Required next retained index.
        expected: LogIndex,
        /// Index carried by the recovered entry.
        actual: LogIndex,
    },
    /// A recovered log entry uses reserved term zero.
    ZeroTermLogEntry {
        /// Index of the invalid entry.
        index: LogIndex,
    },
    /// A recovered entry term exceeds durable current term.
    EntryTermAheadOfCurrentTerm {
        /// Index of the invalid entry.
        index: LogIndex,
        /// Term carried by the entry.
        entry_term: Term,
        /// Durable current term.
        current_term: Term,
    },
    /// Snapshot writer is not a replica — voter or learner — of the membership
    /// captured at the snapshot's boundary.
    ///
    /// A learner replicates the same committed prefix a voter does, so it may
    /// author a snapshot of it; what this rejects is a writer the boundary
    /// membership does not contain at all.
    SnapshotWriterNotReplica {
        /// Unauthorized snapshot writer.
        writer_id: NodeId,
    },
    /// Snapshot-visible hard-state term exceeds recovered current term.
    SnapshotHardStateTermAheadOfCurrentTerm {
        /// Hard-state term captured by the snapshot.
        snapshot_hard_state_term: Term,
        /// Durable current term being recovered.
        current_term: Term,
    },
    /// A retained log entry lies inside the compacted snapshot prefix.
    CompactedLogEntry {
        /// Greatest index covered by the snapshot.
        snapshot_index: LogIndex,
        /// Invalid retained entry index.
        entry_index: LogIndex,
    },
    /// Retained log and snapshot disagree on the boundary term.
    SnapshotBoundaryTermMismatch {
        /// Shared snapshot/log boundary index.
        index: LogIndex,
        /// Boundary term recorded by the snapshot.
        snapshot_term: Term,
        /// Boundary term recorded by the retained log.
        entry_term: Term,
    },
    /// More than one configuration change remains uncommitted.
    MultipleUncommittedConfigurationEntries {
        /// Index of the first uncommitted configuration.
        first_index: LogIndex,
        /// Index of the second uncommitted configuration.
        second_index: LogIndex,
    },
    /// Durable commit index lies beyond recovered snapshot plus log.
    CommitIndexBeyondLog {
        /// Durable committed prefix.
        commit_index: LogIndex,
        /// Greatest index available from snapshot plus retained log.
        last_log_index: LogIndex,
    },
    /// Durable committed-configuration identity lies beyond commit.
    CommittedConfigurationAheadOfCommit {
        /// Recorded committed-configuration position.
        committed_configuration_index: LogIndex,
        /// Durable committed prefix.
        commit_index: LogIndex,
    },
    /// Durable state names a configuration entry absent from snapshot and log.
    CommittedConfigurationMissing {
        /// Missing committed-configuration position.
        committed_configuration_index: LogIndex,
    },
    /// Configuration identity at the durable position disagrees with hard state.
    CommittedConfigurationIdMismatch {
        /// Log position of the configuration.
        index: LogIndex,
        /// Configuration identity recorded in hard state.
        expected: crate::ConfigurationId,
        /// Configuration identity recovered from the entry.
        actual: crate::ConfigurationId,
    },
    /// Hard state does not name the latest configuration at or below commit.
    CommittedConfigurationNotLatest {
        /// Configuration position recorded in hard state.
        recorded_index: LogIndex,
        /// Latest committed configuration position in snapshot plus log.
        latest_index: LogIndex,
    },
    /// A compacted committed configuration cannot be reconstructed from the snapshot.
    CompactedCommittedConfigurationWithoutSnapshotMembership {
        /// Configuration position now below the snapshot boundary.
        committed_configuration_index: LogIndex,
    },
    /// The log reaches the maximum representable index: its successor
    /// cannot exist and index arithmetic on it overflows.
    LogIndexAtMaximum {
        /// Maximum recovered log index.
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

impl BootstrapValidationError {
    fn fmt_snapshot_error(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SnapshotWriterNotReplica { writer_id } => write!(
                formatter,
                "Raft bootstrap snapshot writer {writer_id} is not a replica of the captured membership"
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
            | Self::SnapshotWriterNotReplica { .. }
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
