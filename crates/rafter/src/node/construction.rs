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
    /// its own state; without a floor, every committed entry above the
    /// snapshot boundary replays and the application must deduplicate.
    ///
    /// # Errors
    ///
    /// Returns [`BootstrapValidationError`] as [`Node::from_bootstrap`]
    /// does, or [`BootstrapValidationError::AppliedFloorBeyondLog`] when the
    /// floor lies beyond the persisted log.
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
        let floor = node.volatile.applied_index.max(applied_through);
        node.volatile.applied_index = floor;
        Ok(node)
    }
}
