//! The poisoning subset of runtime failures, and the rule that selects it.
//!
//! A failure belongs here exactly when the kernel may already have advanced
//! past what the medium holds. This module names those failures, renders them,
//! and classifies any [`RaftRuntimeError`] against that question without a
//! wildcard arm, so a new variant cannot default itself to harmless.

use std::{error::Error, fmt};

use rafter::LogIndex;
use rafter_storage::{
    RaftHardStateStoreWriteError, RaftLogSegmentAppendError, RaftLogSegmentCompactError,
    RaftLogSegmentTruncateError, RaftSnapshotStoreWriteError,
};

use rafter_storage::durable_batch::RaftPersistenceBatchError;

use super::RaftRuntimeError;

/// Fatal persistence errors that poison an in-memory runtime until restart.
///
/// These are exactly the failures after which the kernel's in-memory state may
/// describe a log the medium does not hold. The kernel advances first and the
/// store is written second, so a write that fails leaves the two disagreeing —
/// and the kernel's view is the one that is wrong. Continuing from it would let
/// a node vote, or acknowledge an entry, on the strength of state a crash would
/// erase.
///
/// So the runtime refuses every later step rather than retrying: there is no
/// in-memory repair for a divergence whose correct value only the medium has.
/// Recovery is [`crate::DurableRaftNode::into_storage`] and a reopen, which
/// rebuilds the kernel from what was actually persisted.
///
/// A validation failure is not here, and that is the distinction: a snapshot
/// boundary ahead of the commit index, or a bootstrap configuration the kernel
/// rejects, is caught before anything is written, so nothing diverged and the
/// runtime keeps serving.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RaftRuntimeFatalError {
    /// Durable hard-state publication failed after the kernel advanced.
    HardStateWrite(RaftHardStateStoreWriteError),
    /// Atomic log and hard-state publication failed after the kernel advanced.
    PersistenceBatch(RaftPersistenceBatchError),
    /// Durable log append failed after the kernel advanced.
    LogAppend(RaftLogSegmentAppendError),
    /// Durable suffix truncation failed after the kernel advanced.
    LogTruncate(RaftLogSegmentTruncateError),
    /// Durable prefix compaction failed after the kernel advanced.
    LogCompact(RaftLogSegmentCompactError),
    /// Durable snapshot publication failed after the kernel advanced.
    SnapshotWrite(RaftSnapshotStoreWriteError),
    /// Persisted log contents disagree with the kernel's recovered prefix.
    LogPrefixDiverged {
        /// First index at which the two prefixes disagree.
        index: LogIndex,
    },
    /// Recovery encountered a configuration entry this runtime cannot apply.
    UnsupportedConfigurationEntry {
        /// Index of the unsupported entry.
        index: LogIndex,
    },
    /// The durable segment would append at or behind the snapshot boundary.
    LogBehindSnapshotBoundary {
        /// Next index the durable segment would assign to an append.
        segment_next_index: LogIndex,
        /// Last index covered by the current durable snapshot.
        snapshot_index: LogIndex,
    },
}

impl fmt::Display for RaftRuntimeFatalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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
            Self::LogPrefixDiverged { index } => write!(
                formatter,
                "persisted Raft log diverges from committed state at index {index}"
            ),
            Self::UnsupportedConfigurationEntry { index } => write!(
                formatter,
                "Raft log entry at index {index} holds an unsupported configuration entry"
            ),
            Self::LogBehindSnapshotBoundary {
                segment_next_index,
                snapshot_index,
            } => write!(
                formatter,
                "durable Raft log can only append at index {segment_next_index}, at or behind the snapshot boundary {snapshot_index}; appending would mislabel entries"
            ),
        }
    }
}

impl Error for RaftRuntimeFatalError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::HardStateWrite(error) => Some(error),
            Self::PersistenceBatch(error) => Some(error),
            Self::LogAppend(error) => Some(error),
            Self::LogTruncate(error) => Some(error),
            Self::LogCompact(error) => Some(error),
            Self::SnapshotWrite(error) => Some(error),
            Self::LogPrefixDiverged { .. }
            | Self::UnsupportedConfigurationEntry { .. }
            | Self::LogBehindSnapshotBoundary { .. } => None,
        }
    }
}

impl RaftRuntimeFatalError {
    /// Decides whether one runtime error poisoned the runtime.
    ///
    /// `Some` for a failure that could leave the kernel ahead of the medium —
    /// every store write, plus the two structural faults a step can discover
    /// about an already-persisted log. `None` for a failure caught before
    /// anything was written, which leaves the runtime usable.
    ///
    /// The match is exhaustive on purpose: a new [`RaftRuntimeError`] variant
    /// must be classified here rather than defaulting to non-fatal, because the
    /// safe default for an unclassified persistence failure is to poison, and a
    /// wildcard arm would silently choose the other one.
    pub(crate) fn from_runtime_error(error: &RaftRuntimeError) -> Option<Self> {
        match error {
            RaftRuntimeError::HardStateWrite(error) => Some(Self::HardStateWrite(error.clone())),
            RaftRuntimeError::PersistenceBatch(error) => {
                Some(Self::PersistenceBatch(error.clone()))
            }
            RaftRuntimeError::LogAppend(error) => Some(Self::LogAppend(error.clone())),
            RaftRuntimeError::LogTruncate(error) => Some(Self::LogTruncate(error.clone())),
            RaftRuntimeError::LogCompact(error) => Some(Self::LogCompact(error.clone())),
            RaftRuntimeError::SnapshotWrite(error) => Some(Self::SnapshotWrite(error.clone())),
            RaftRuntimeError::LogPrefixDiverged { index } => {
                Some(Self::LogPrefixDiverged { index: *index })
            }
            RaftRuntimeError::UnsupportedConfigurationEntry { index } => {
                Some(Self::UnsupportedConfigurationEntry { index: *index })
            }
            RaftRuntimeError::LogBehindSnapshotBoundary {
                segment_next_index,
                snapshot_index,
            } => Some(Self::LogBehindSnapshotBoundary {
                segment_next_index: *segment_next_index,
                snapshot_index: *snapshot_index,
            }),
            RaftRuntimeError::Bootstrap(_)
            | RaftRuntimeError::PendingSnapshotTransferResume(_)
            | RaftRuntimeError::SnapshotAheadOfCommit { .. }
            | RaftRuntimeError::SnapshotAheadOfApplied { .. }
            | RaftRuntimeError::SnapshotBelowInstalledBoundary { .. }
            | RaftRuntimeError::SnapshotRefusedByKernel { .. }
            | RaftRuntimeError::SnapshotBoundaryTermMismatch { .. }
            | RaftRuntimeError::SnapshotMembershipMismatch { .. }
            | RaftRuntimeError::SnapshotCommittedConfigurationMismatch { .. }
            | RaftRuntimeError::CompactionAheadOfSnapshot { .. }
            | RaftRuntimeError::Poisoned { .. } => None,
        }
    }
}
