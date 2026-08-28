//! Translation of the kernel's local-install refusals into runtime errors.
//!
//! Every local boundary rule is judged by the kernel, so this is the single
//! place its refusal is renamed in this crate's vocabulary. A rule the kernel
//! gains later must still refuse here, carrying the kernel's own rendering
//! rather than being installed by default.

use crate::RaftRuntimeError;

/// Renders the kernel's local-install refusal in this crate's vocabulary.
///
/// The kernel is the only place a local boundary is judged; this runtime asks
/// it first, on a staged clone, and writes nothing until it agrees. So every
/// refusal a caller sees from the compaction APIs arrives through here, and
/// this crate keeps no second copy of the rules that could drift from them.
pub(super) fn local_snapshot_install_error(
    error: rafter::LocalSnapshotInstallError,
) -> RaftRuntimeError {
    match error {
        rafter::LocalSnapshotInstallError::BoundaryBelowInstalledSnapshot {
            snapshot_index,
            installed_index,
        } => RaftRuntimeError::SnapshotBelowInstalledBoundary {
            snapshot_index,
            installed_index,
        },
        rafter::LocalSnapshotInstallError::BoundaryAheadOfCommit {
            snapshot_index,
            commit_index,
        } => RaftRuntimeError::SnapshotAheadOfCommit {
            snapshot_index,
            commit_index,
        },
        rafter::LocalSnapshotInstallError::BoundaryAheadOfApplied {
            snapshot_index,
            applied_index,
        } => RaftRuntimeError::SnapshotAheadOfApplied {
            snapshot_index,
            applied_index,
        },
        rafter::LocalSnapshotInstallError::BoundaryTermMismatch {
            snapshot_index,
            snapshot_term,
            local_term,
        } => RaftRuntimeError::SnapshotBoundaryTermMismatch {
            snapshot_index,
            snapshot_term,
            local_term,
        },
        rafter::LocalSnapshotInstallError::CommittedMembershipMismatch {
            snapshot_index,
            expected,
            actual,
        } => RaftRuntimeError::SnapshotMembershipMismatch {
            snapshot_index,
            expected,
            actual,
        },
        rafter::LocalSnapshotInstallError::CommittedConfigurationMismatch {
            snapshot_index,
            expected,
            actual,
        } => RaftRuntimeError::SnapshotCommittedConfigurationMismatch {
            snapshot_index,
            expected,
            actual,
        },
        // `LocalSnapshotInstallError` is `#[non_exhaustive]`: a kernel that
        // gains a local-install rule must still be refused here rather than
        // installed, and the rendered kernel error carries the reason.
        other => RaftRuntimeError::SnapshotRefusedByKernel {
            reason: other.to_string(),
        },
    }
}
