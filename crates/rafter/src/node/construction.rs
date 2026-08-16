//! Construction and restart hydration for [`Node`].
//!
//! This module restores canonical protocol state from a validated bootstrap
//! image. It does not emit recovery outputs; callers explicitly drain the
//! committed suffix after construction when their application requires it.

use crate::LogIndex;

use super::state::{DerivedState, ElectionState, LeaderState, PersistentState, VolatileState};
use super::{BootstrapState, BootstrapValidationError, Node, NodeConfig};

impl Node {
    /// Builds a fresh node with empty durable state.
    #[must_use]
    pub fn new(config: NodeConfig) -> Self {
        Self {
            config,
            persistent: PersistentState::default(),
            volatile: VolatileState::default(),
            election: ElectionState::default(),
            leader: LeaderState::default(),
            derived: DerivedState::default(),
        }
    }

    /// Constructs a deterministic Raft node from persisted protocol state.
    ///
    /// Hydration restores hard state and log entries, then starts the node as a
    /// follower with default volatile election and replication progress.
    ///
    /// # Errors
    ///
    /// Returns [`BootstrapValidationError`] when the bootstrap state is not a
    /// valid persisted Raft state for the supplied static configuration.
    pub fn from_bootstrap(
        config: NodeConfig,
        bootstrap: BootstrapState,
    ) -> Result<Self, BootstrapValidationError> {
        let parts = bootstrap.into_parts(&config)?;
        let applied_index = parts.snapshot.as_ref().map_or(LogIndex::ZERO, |snapshot| {
            snapshot.metadata.last_included_index
        });
        let persistent = PersistentState {
            current_term: parts.current_term,
            voted_for: parts.voted_for,
            committed_configuration: parts.committed_configuration,
            snapshot: parts.snapshot,
            log: parts.log,
        };

        let derived = DerivedState::from_log(&persistent.log);
        let mut volatile = VolatileState::at_applied_index(applied_index);
        volatile.commit_index = parts.commit_index;
        Ok(Self {
            config,
            persistent,
            volatile,
            election: ElectionState::default(),
            leader: LeaderState::default(),
            derived,
        })
    }

    /// Like [`Node::from_bootstrap`], but the application declares it has
    /// already durably applied entries through `applied_through`: committed
    /// entries at or below the floor are not re-emitted as
    /// [`crate::Output::Apply`] after this restart. Call
    /// [`Node::drain_committed_outputs`] after construction to replay
    /// committed entries above the floor immediately, without waiting for a
    /// later commit-index advance. Use this when the state machine persists
    /// its own state.
    ///
    /// # A floor below the snapshot boundary is raised, and the caller owns
    /// what that costs
    ///
    /// The effective floor is `max(snapshot_index, applied_through)`, and the
    /// asymmetry with the two errors below is deliberate rather than an
    /// oversight: a floor that is too high is refused, and a floor that is too
    /// low is raised. The kernel cannot do better in either direction. It
    /// retains no entry at or below its snapshot boundary and holds a snapshot
    /// *descriptor* rather than payload bytes, so it can neither emit the
    /// entries a lower floor asks for nor restore the state machine from the
    /// snapshot that covers them.
    ///
    /// Nor can it refuse, because a correct recovery reaches this state. An
    /// inbound snapshot is promoted durably before the application installs
    /// it, so a crash between those two writes leaves an application short of
    /// a boundary its Raft state already carries — and the repair is for the
    /// composition to restore the application from that snapshot, which is
    /// exactly what a caller holding the snapshot store can do and this
    /// constructor cannot.
    ///
    /// So the obligation is the caller's, and it is total: **before serving
    /// anything, the state machine must hold the effective floor**, either
    /// because it already applied through it or because the caller restored it
    /// from the snapshot the boundary names. The entries between a lower
    /// declaration and the boundary are never emitted, in any form, and
    /// nothing later reports that they were skipped. Compare
    /// [`Node::applied_index`] against [`Node::snapshot_index`] after
    /// construction to see whether a declaration was raised.
    ///
    /// A composition that owns both halves should enforce this rather than
    /// assume it, and enforcing it means two separate things.
    /// `rafter-app`'s `RaftGroup` does both: it refuses to *run* a group whose
    /// state machine is below the boundary — no step, no proposal, no read —
    /// and it refuses to apply a committed entry to one, which is the half
    /// that matters here. The suffix [`Node::drain_committed_outputs`] hands
    /// back starts above the raised floor, so applying it to a state machine
    /// that is still below the boundary writes the gap into durable
    /// application state and leaves every index reporting a replica that has
    /// caught up. `RaftGroup::apply_recovery_outputs` is the composition's
    /// answer: it restores the application from the snapshot the boundary
    /// names and only then applies what this constructor's companion drained.
    ///
    /// # Errors
    ///
    /// Returns [`BootstrapValidationError`] as [`Node::from_bootstrap`] does,
    /// plus [`BootstrapValidationError::AppliedFloorBeyondLog`] when the floor
    /// lies beyond the persisted log and
    /// [`BootstrapValidationError::AppliedFloorBeyondCommit`] when it lies
    /// beyond the recovered committed prefix.
    pub fn from_bootstrap_applied_through(
        config: NodeConfig,
        bootstrap: BootstrapState,
        applied_through: LogIndex,
    ) -> Result<Self, BootstrapValidationError> {
        let mut node = Self::from_bootstrap(config, bootstrap)?;
        if applied_through > node.last_log_index() {
            return Err(BootstrapValidationError::AppliedFloorBeyondLog {
                applied_through,
                last_log_index: node.last_log_index(),
            });
        }
        if applied_through > node.commit_index() {
            return Err(BootstrapValidationError::AppliedFloorBeyondCommit {
                applied_through,
                commit_index: node.commit_index(),
            });
        }
        // `from_bootstrap` starts the applied index at the snapshot boundary,
        // which is the lowest floor the retained log can serve.
        let floor = node.volatile.applied_index.max(applied_through);
        node.volatile.applied_index = floor;
        Ok(node)
    }
}
