//! Refusals of the application-driven local snapshot install path.
//!
//! Every variant is a checked precondition from [`Node::install_local_snapshot`]
//! (super): the node is exactly as it was before the call, nothing was
//! compacted, and no output was emitted.

use std::{error::Error, fmt};

use crate::{CommittedConfiguration, LogIndex, MembershipConfig, NodeId, Term};

#[allow(
    unused_imports,
    reason = "named only by the rustdoc links in the variant contracts, never by code"
)]
use super::super::Node;

/// Why a caller-supplied local snapshot descriptor was not installed.
///
/// Every variant is a refusal: the node is exactly as it was before the call,
/// nothing was compacted, and no output was emitted.
///
/// This enum is `#[non_exhaustive]`. It was exhaustive when a local install was
/// closed over one precondition; it is closed over seven, and the set is the
/// list of facts a descriptor asserts that the local node can check for itself
/// — which grows as the descriptor carries more.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum LocalSnapshotInstallError {
    /// The boundary lies strictly below the installed snapshot's boundary.
    ///
    /// Installing it would rewind the compacted prefix and replace a newer
    /// descriptor with an older one. Nothing below the installed boundary is
    /// retained, so such a descriptor cannot even be checked against the local
    /// log — this is the refusal, not a term disagreement.
    BoundaryBelowInstalledSnapshot {
        /// Boundary proposed by the caller.
        snapshot_index: LogIndex,
        /// Boundary of the snapshot already installed.
        installed_index: LogIndex,
    },
    /// The descriptor's boundary lies beyond this node's committed prefix.
    ///
    /// Installing it would compact away entries no quorum has accepted and
    /// raise this node's commit index on the strength of a local call.
    BoundaryAheadOfCommit {
        /// Boundary proposed by the caller.
        snapshot_index: LogIndex,
        /// Greatest locally committed index.
        commit_index: LogIndex,
    },
    /// The boundary is committed, but this node has not applied through it.
    ///
    /// Kept distinct from [`Self::BoundaryAheadOfCommit`] because it is a
    /// different mistake: the entries exist and are committed, but this node
    /// has never handed them to a state machine, so raising the applied index
    /// to the boundary would skip them silently and forever. Reachable on a
    /// node recovered below its committed prefix — see
    /// [`Node::from_bootstrap_applied_through`](crate::Node::from_bootstrap_applied_through)
    /// and [`Node::drain_committed_outputs`](crate::Node::drain_committed_outputs).
    BoundaryAheadOfApplied {
        /// Boundary proposed by the caller.
        snapshot_index: LogIndex,
        /// Greatest index already handed to the application.
        applied_index: LogIndex,
    },
    /// The descriptor's boundary term disagrees with the local log.
    ///
    /// `local_term` is `None` when this node retains nothing at the boundary.
    /// On the leader-sent install path a term disagreement means the local
    /// suffix belongs to another history and is discarded; a *local* descriptor
    /// carries no such authority, so the same disagreement is caller error.
    BoundaryTermMismatch {
        /// Boundary proposed by the caller.
        snapshot_index: LogIndex,
        /// Boundary term declared by the snapshot.
        snapshot_term: Term,
        /// Boundary term retained locally, when the position is available.
        local_term: Option<Term>,
    },
    /// The descriptor records committed membership the local node does not
    /// derive at the boundary.
    ///
    /// The descriptor outlives the entries it compacts and becomes this node's
    /// membership of record below the boundary, so a disagreeing copy would
    /// redefine the voter set out of a local call.
    CommittedMembershipMismatch {
        /// Boundary proposed by the caller.
        snapshot_index: LogIndex,
        /// Membership derived from the local committed history.
        expected: Box<MembershipConfig>,
        /// Membership declared by the snapshot.
        actual: Box<MembershipConfig>,
    },
    /// The descriptor records a committed configuration identity the local node
    /// does not derive at the boundary.
    CommittedConfigurationMismatch {
        /// Boundary proposed by the caller.
        snapshot_index: LogIndex,
        /// Configuration identity derived from local committed history.
        expected: Option<CommittedConfiguration>,
        /// Configuration identity declared by the snapshot.
        actual: Option<CommittedConfiguration>,
    },
    /// The descriptor's author is not a replica — voter or learner — of the
    /// membership committed at its boundary.
    ///
    /// Bootstrap validation refuses to hydrate such a descriptor, so installing
    /// it would trade a refusal now for an unbootable node at the next restart:
    /// the snapshot would be durable, the log below it compacted away, and the
    /// process unable to come up without discarding both. It is also
    /// untransferable — a receiver applies the same author rule. Refusing at the
    /// call that produced it is the only refusal that leaves a caller something
    /// to do.
    WriterNotBoundaryReplica {
        /// Boundary proposed by the caller.
        snapshot_index: LogIndex,
        /// Author named by the descriptor.
        writer_id: NodeId,
        /// Membership this node derives at the boundary.
        membership: Box<MembershipConfig>,
    },
}

impl fmt::Display for LocalSnapshotInstallError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BoundaryBelowInstalledSnapshot {
                snapshot_index,
                installed_index,
            } => write!(
                formatter,
                "local snapshot boundary {snapshot_index} lies below the installed snapshot boundary {installed_index}"
            ),
            Self::BoundaryAheadOfCommit {
                snapshot_index,
                commit_index,
            } => write!(
                formatter,
                "local snapshot boundary {snapshot_index} lies beyond the committed index {commit_index}"
            ),
            Self::BoundaryAheadOfApplied {
                snapshot_index,
                applied_index,
            } => write!(
                formatter,
                "local snapshot boundary {snapshot_index} lies beyond the applied index {applied_index}"
            ),
            Self::BoundaryTermMismatch {
                snapshot_index,
                snapshot_term,
                local_term: Some(local_term),
            } => write!(
                formatter,
                "local snapshot boundary {snapshot_index} records term {snapshot_term} but the local log holds term {local_term}"
            ),
            Self::BoundaryTermMismatch {
                snapshot_index,
                snapshot_term,
                local_term: None,
            } => write!(
                formatter,
                "local snapshot boundary {snapshot_index} records term {snapshot_term} but the local log retains nothing at that index"
            ),
            Self::CommittedMembershipMismatch {
                snapshot_index,
                expected,
                actual,
            } => write!(
                formatter,
                "local snapshot boundary {snapshot_index} records committed membership {actual:?} but the local committed membership is {expected:?}"
            ),
            Self::CommittedConfigurationMismatch {
                snapshot_index,
                expected,
                actual,
            } => write!(
                formatter,
                "local snapshot boundary {snapshot_index} records committed configuration {actual:?} but the local committed configuration is {expected:?}"
            ),
            Self::WriterNotBoundaryReplica {
                snapshot_index,
                writer_id,
                membership,
            } => write!(
                formatter,
                "local snapshot boundary {snapshot_index} names writer {writer_id}, which is not a replica of the boundary membership {membership:?}"
            ),
        }
    }
}

impl Error for LocalSnapshotInstallError {}
