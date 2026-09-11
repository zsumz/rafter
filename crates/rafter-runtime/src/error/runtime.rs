//! The caller-facing runtime error vocabulary and its source chain.
//!
//! One variant per refusal a caller can be handed: recovered state the kernel
//! rejects, a store write that failed, a local snapshot boundary the kernel
//! refused, a durable log that disagrees with the kernel, and the poison an
//! earlier fatal error left behind. Rendering lives beside it, not here.

use std::error::Error;

use rafter::{
    BootstrapValidationError, CommittedConfiguration, LogIndex, MembershipConfig,
    PendingSnapshotTransferResumeError, Term,
};
use rafter_storage::{
    RaftHardStateStoreWriteError, RaftLogSegmentAppendError, RaftLogSegmentCompactError,
    RaftLogSegmentTruncateError, RaftSnapshotStoreWriteError,
};

use rafter_storage::durable_batch::RaftPersistenceBatchError;

use super::RaftRuntimeFatalError;

/// Errors returned by durable runtime construction, recovery, stepping, and
/// local snapshot compaction.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RaftRuntimeError {
    /// Recovered state cannot construct a valid kernel.
    Bootstrap(BootstrapValidationError),
    /// Durable hard-state publication failed.
    HardStateWrite(RaftHardStateStoreWriteError),
    /// Atomic log and hard-state publication failed after the kernel advanced.
    PersistenceBatch(RaftPersistenceBatchError),
    /// Appending entries to the durable log failed.
    LogAppend(RaftLogSegmentAppendError),
    /// Removing a conflicting durable log suffix failed.
    LogTruncate(RaftLogSegmentTruncateError),
    /// Compacting the durable log prefix failed.
    LogCompact(RaftLogSegmentCompactError),
    /// Publishing a durable snapshot failed.
    SnapshotWrite(RaftSnapshotStoreWriteError),
    /// A persisted incoming snapshot transfer cannot be resumed safely.
    PendingSnapshotTransferResume(PendingSnapshotTransferResumeError),
    /// A local snapshot claims committed state beyond the commit index.
    SnapshotAheadOfCommit {
        /// Boundary claimed by the local snapshot.
        snapshot_index: LogIndex,
        /// Highest index known committed by the kernel.
        commit_index: LogIndex,
    },
    /// A local snapshot boundary lies within the committed prefix but above
    /// what this node has applied, so compacting through it would skip
    /// committed entries the state machine was never handed.
    SnapshotAheadOfApplied {
        /// Boundary claimed by the local snapshot.
        snapshot_index: LogIndex,
        /// Highest index durably applied by the application.
        applied_index: LogIndex,
    },
    /// A local snapshot boundary lies below the installed snapshot boundary, so
    /// compacting through it would rewind the compacted prefix and replace a
    /// newer descriptor with an older one.
    SnapshotBelowInstalledBoundary {
        /// Boundary claimed by the local snapshot.
        snapshot_index: LogIndex,
        /// Boundary of the snapshot already installed.
        installed_index: LogIndex,
    },
    /// The kernel refused a local snapshot install for a reason this crate
    /// predates. [`rafter::LocalSnapshotInstallError`] is `#[non_exhaustive]`;
    /// a rule added there must still refuse here, carrying its own rendering.
    SnapshotRefusedByKernel {
        /// Kernel-provided refusal detail retained for diagnostics.
        reason: String,
    },
    /// The local log term disagrees with the snapshot boundary term.
    SnapshotBoundaryTermMismatch {
        /// Boundary claimed by the local snapshot.
        snapshot_index: LogIndex,
        /// Term recorded by the local snapshot.
        snapshot_term: Term,
        /// Term of the local log entry, or `None` when no entry proves it.
        local_term: Option<Term>,
    },
    /// Snapshot membership disagrees with membership committed at its boundary.
    SnapshotMembershipMismatch {
        /// Boundary claimed by the local snapshot.
        snapshot_index: LogIndex,
        /// Membership reconstructed from the committed log.
        expected: Box<MembershipConfig>,
        /// Membership recorded by the local snapshot.
        actual: Box<MembershipConfig>,
    },
    /// Snapshot configuration metadata disagrees with the committed log.
    SnapshotCommittedConfigurationMismatch {
        /// Boundary claimed by the local snapshot.
        snapshot_index: LogIndex,
        /// Configuration reconstructed from the committed log.
        expected: Option<CommittedConfiguration>,
        /// Configuration recorded by the local snapshot.
        actual: Option<CommittedConfiguration>,
    },
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
    /// The durable log was compacted through an index the current snapshot
    /// does not cover: a state no reopen repair can make consistent.
    CompactionAheadOfSnapshot {
        /// Last index already removed from the durable log.
        compacted_through: LogIndex,
        /// Last index covered by the current durable snapshot.
        snapshot_index: LogIndex,
    },
    /// The durable log's next appendable index is at or below the snapshot
    /// boundary, so appending would stamp kernel entries with wrong segment
    /// indexes. The open-time compaction repair makes this unreachable; it
    /// exists so a bypassed repair can never mislabel an acknowledged entry.
    LogBehindSnapshotBoundary {
        /// Next index the durable segment would assign to an append.
        segment_next_index: LogIndex,
        /// Last index covered by the current durable snapshot.
        snapshot_index: LogIndex,
    },
    /// An earlier fatal error left the in-memory runtime unusable.
    Poisoned {
        /// Original persistence or recovered-prefix failure.
        cause: RaftRuntimeFatalError,
    },
}

impl Error for RaftRuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Bootstrap(error) => Some(error),
            Self::HardStateWrite(error) => Some(error),
            Self::PersistenceBatch(error) => Some(error),
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
