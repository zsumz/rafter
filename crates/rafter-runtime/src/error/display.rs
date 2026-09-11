//! How a [`RaftRuntimeError`] reads to whoever is handed one.
//!
//! Each variant renders the failed operation and defers detail to the source
//! it wraps. The local-snapshot boundary refusals are one family — the
//! kernel's rules in this crate's vocabulary — so they render together, one
//! arm away from the rest, and cannot drift apart.

use std::fmt;

use super::RaftRuntimeError;

impl fmt::Display for RaftRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bootstrap(error) => {
                write!(formatter, "Raft bootstrap validation failed: {error}")
            }
            Self::HardStateWrite(error) => {
                write!(formatter, "Raft hard state could not be written: {error}")
            }
            Self::PersistenceBatch(error) => {
                write!(formatter, "Raft persistence batch failed: {error}")
            }
            Self::LogAppend(error) => {
                write!(formatter, "Raft log entries could not be appended: {error}")
            }
            Self::LogTruncate(error) => {
                write!(formatter, "Raft log could not be truncated: {error}")
            }
            Self::LogCompact(error) => {
                write!(formatter, "Raft log could not be compacted: {error}")
            }
            Self::SnapshotWrite(error) => {
                write!(formatter, "Raft snapshot could not be written: {error}")
            }
            Self::PendingSnapshotTransferResume(error) => write!(
                formatter,
                "pending Raft snapshot transfer could not be resumed: {error}"
            ),
            Self::SnapshotAheadOfCommit { .. }
            | Self::SnapshotAheadOfApplied { .. }
            | Self::SnapshotBelowInstalledBoundary { .. }
            | Self::SnapshotRefusedByKernel { .. }
            | Self::SnapshotBoundaryTermMismatch { .. }
            | Self::SnapshotMembershipMismatch { .. }
            | Self::SnapshotCommittedConfigurationMismatch { .. } => {
                self.fmt_local_snapshot_boundary(formatter)
            }
            Self::LogPrefixDiverged { index } => write!(
                formatter,
                "persisted Raft log diverges from committed state at index {index}"
            ),
            Self::UnsupportedConfigurationEntry { index } => write!(
                formatter,
                "Raft log entry at index {index} holds an unsupported configuration entry"
            ),
            Self::CompactionAheadOfSnapshot {
                compacted_through,
                snapshot_index,
            } => write!(
                formatter,
                "durable Raft log is compacted through index {compacted_through} but the current snapshot only covers index {snapshot_index}"
            ),
            Self::LogBehindSnapshotBoundary {
                segment_next_index,
                snapshot_index,
            } => write!(
                formatter,
                "durable Raft log can only append at index {segment_next_index}, at or behind the snapshot boundary {snapshot_index}; appending would mislabel entries"
            ),
            Self::Poisoned { cause } => write!(
                formatter,
                "Raft runtime is poisoned by an earlier fatal error: {cause}"
            ),
        }
    }
}

impl RaftRuntimeError {
    /// Renders the local-snapshot boundary refusals, which are one family: the
    /// kernel's `LocalSnapshotInstallError` rules, in this crate's vocabulary.
    fn fmt_local_snapshot_boundary(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SnapshotAheadOfCommit {
                snapshot_index,
                commit_index,
            } => write!(
                formatter,
                "Raft snapshot boundary at index {snapshot_index} is ahead of commit index {commit_index}"
            ),
            Self::SnapshotAheadOfApplied {
                snapshot_index,
                applied_index,
            } => write!(
                formatter,
                "Raft snapshot boundary at index {snapshot_index} is ahead of applied index {applied_index}"
            ),
            Self::SnapshotBelowInstalledBoundary {
                snapshot_index,
                installed_index,
            } => write!(
                formatter,
                "Raft snapshot boundary at index {snapshot_index} lies below the installed boundary {installed_index}"
            ),
            Self::SnapshotRefusedByKernel { reason } => write!(
                formatter,
                "Raft snapshot boundary was refused by the kernel: {reason}"
            ),
            Self::SnapshotBoundaryTermMismatch {
                snapshot_index,
                snapshot_term,
                local_term: Some(local_term),
            } => write!(
                formatter,
                "Raft snapshot boundary at index {snapshot_index} has local term {local_term} but the snapshot recorded term {snapshot_term}"
            ),
            Self::SnapshotBoundaryTermMismatch {
                snapshot_index,
                snapshot_term,
                local_term: None,
            } => write!(
                formatter,
                "Raft snapshot boundary at index {snapshot_index} with term {snapshot_term} has no local entry to prove its term"
            ),
            Self::SnapshotMembershipMismatch {
                snapshot_index,
                expected,
                actual,
            } => write!(
                formatter,
                "Raft snapshot boundary at index {snapshot_index} recorded committed membership {actual:?} but local committed membership is {expected:?}"
            ),
            Self::SnapshotCommittedConfigurationMismatch {
                snapshot_index,
                expected,
                actual,
            } => write!(
                formatter,
                "Raft snapshot boundary at index {snapshot_index} recorded committed configuration {actual:?} but local committed configuration is {expected:?}"
            ),
            // Unreachable: `Display` routes only the boundary family here, and
            // that arm and this match list the same variants.
            _ => Ok(()),
        }
    }
}
