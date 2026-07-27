//! Errors returned by the durable runtime.
//!
//! Two types, split by one question: did the in-memory kernel run ahead of the
//! durable medium? [`RaftRuntimeError`] is everything a caller can be told;
//! [`RaftRuntimeFatalError`] is the subset that poisons the runtime, and
//! [`RaftRuntimeFatalError::from_runtime_error`] is the rule that decides.

use std::{error::Error, fmt};

use rafter::{
    BootstrapValidationError, CommittedConfiguration, LogIndex, MembershipConfig,
    PendingSnapshotTransferResumeError, Term,
};
use rafter_storage::{
    RaftHardStateStoreWriteError, RaftLogSegmentAppendError, RaftLogSegmentCompactError,
    RaftLogSegmentTruncateError, RaftSnapshotStoreWriteError,
};

/// Errors returned by durable runtime construction, recovery, stepping, and
/// local snapshot compaction.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RaftRuntimeError {
    Bootstrap(BootstrapValidationError),
    HardStateWrite(RaftHardStateStoreWriteError),
    LogAppend(RaftLogSegmentAppendError),
    LogTruncate(RaftLogSegmentTruncateError),
    LogCompact(RaftLogSegmentCompactError),
    SnapshotWrite(RaftSnapshotStoreWriteError),
    PendingSnapshotTransferResume(PendingSnapshotTransferResumeError),
    SnapshotAheadOfCommit {
        snapshot_index: LogIndex,
        commit_index: LogIndex,
    },
    /// A local snapshot boundary lies within the committed prefix but above
    /// what this node has applied, so compacting through it would skip
    /// committed entries the state machine was never handed.
    SnapshotAheadOfApplied {
        snapshot_index: LogIndex,
        applied_index: LogIndex,
    },
    /// A local snapshot boundary lies below the installed snapshot boundary, so
    /// compacting through it would rewind the compacted prefix and replace a
    /// newer descriptor with an older one.
    SnapshotBelowInstalledBoundary {
        snapshot_index: LogIndex,
        installed_index: LogIndex,
    },
    /// The kernel refused a local snapshot install for a reason this crate
    /// predates. [`rafter::LocalSnapshotInstallError`] is `#[non_exhaustive]`;
    /// a rule added there must still refuse here, carrying its own rendering.
    SnapshotRefusedByKernel {
        reason: String,
    },
    SnapshotBoundaryTermMismatch {
        snapshot_index: LogIndex,
        snapshot_term: Term,
        local_term: Option<Term>,
    },
    SnapshotMembershipMismatch {
        snapshot_index: LogIndex,
        expected: Box<MembershipConfig>,
        actual: Box<MembershipConfig>,
    },
    SnapshotCommittedConfigurationMismatch {
        snapshot_index: LogIndex,
        expected: Option<CommittedConfiguration>,
        actual: Option<CommittedConfiguration>,
    },
    LogPrefixDiverged {
        index: LogIndex,
    },
    UnsupportedConfigurationEntry {
        index: LogIndex,
    },
    /// The durable log was compacted through an index the current snapshot
    /// does not cover: a state no reopen repair can make consistent.
    CompactionAheadOfSnapshot {
        compacted_through: LogIndex,
        snapshot_index: LogIndex,
    },
    /// The durable log's next appendable index is at or below the snapshot
    /// boundary, so appending would stamp kernel entries with wrong segment
    /// indexes. The open-time compaction repair makes this unreachable; it
    /// exists so a bypassed repair can never mislabel an acknowledged entry.
    LogBehindSnapshotBoundary {
        segment_next_index: LogIndex,
        snapshot_index: LogIndex,
    },
    Poisoned {
        cause: RaftRuntimeFatalError,
    },
}

impl fmt::Display for RaftRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bootstrap(error) => {
                write!(formatter, "Raft bootstrap validation failed: {error}")
            }
            Self::HardStateWrite(error) => {
                write!(formatter, "Raft hard state could not be written: {error}")
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

impl Error for RaftRuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Bootstrap(error) => Some(error),
            Self::HardStateWrite(error) => Some(error),
            Self::LogAppend(error) => Some(error),
            Self::LogTruncate(error) => Some(error),
            Self::LogCompact(error) => Some(error),
            Self::SnapshotWrite(error) => Some(error),
            Self::PendingSnapshotTransferResume(error) => Some(error),
            Self::Poisoned { cause } => Some(cause),
            Self::SnapshotAheadOfCommit { .. }
            | Self::SnapshotAheadOfApplied { .. }
            | Self::SnapshotBelowInstalledBoundary { .. }
            | Self::SnapshotRefusedByKernel { .. }
            | Self::SnapshotBoundaryTermMismatch { .. }
            | Self::SnapshotMembershipMismatch { .. }
            | Self::SnapshotCommittedConfigurationMismatch { .. }
            | Self::LogPrefixDiverged { .. }
            | Self::UnsupportedConfigurationEntry { .. }
            | Self::CompactionAheadOfSnapshot { .. }
            | Self::LogBehindSnapshotBoundary { .. } => None,
        }
    }
}

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
    HardStateWrite(RaftHardStateStoreWriteError),
    LogAppend(RaftLogSegmentAppendError),
    LogTruncate(RaftLogSegmentTruncateError),
    LogCompact(RaftLogSegmentCompactError),
    SnapshotWrite(RaftSnapshotStoreWriteError),
    LogPrefixDiverged {
        index: LogIndex,
    },
    UnsupportedConfigurationEntry {
        index: LogIndex,
    },
    LogBehindSnapshotBoundary {
        segment_next_index: LogIndex,
        snapshot_index: LogIndex,
    },
}

impl fmt::Display for RaftRuntimeFatalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HardStateWrite(error) => {
                write!(formatter, "Raft hard state could not be written: {error}")
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
