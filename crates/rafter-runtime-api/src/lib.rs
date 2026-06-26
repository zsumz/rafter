//! Runtime trait boundary for persisted Rafter nodes.
//!
//! This crate owns only the application-facing runtime contract. It depends on
//! the deterministic `rafter` kernel and deliberately does not depend on
//! storage, durable runtime, app, service, transport, or async crates.

use std::error::Error;

use rafter::{
    Input as RaftInput, LogIndex, MembershipConfig, NodeId as RaftNodeId, Output as RaftOutput,
    ReplicationProgress, Role as RaftRole, Term,
};

/// Persist-before-output runtime contract consumed by application layers.
///
/// Implementations must return Raft outputs only after every durability
/// obligation for those outputs has completed. In particular, outputs that
/// depend on hard-state, log, snapshot, or compaction changes must not be
/// released until those changes are durably reflected according to the
/// implementation's persistence contract. Callers may therefore apply,
/// publish, or transmit returned outputs without adding another Raft-log
/// durability fence.
pub trait PersistedRaftRuntime {
    /// Error returned when the runtime cannot step or query local persisted
    /// state. Runtime errors are part of the public app/service error stack,
    /// so implementations should expose typed errors rather than debug-only
    /// strings.
    type Error: Error + Send + Sync + 'static;

    /// Returns the local Raft node ID.
    fn id(&self) -> RaftNodeId;

    /// Returns the current best-known leader, when the kernel has one.
    fn leader_hint(&self) -> Option<RaftNodeId>;

    /// Returns the local node's current role.
    fn role(&self) -> RaftRole;

    /// Returns the local node's current Raft term.
    fn current_term(&self) -> Term;

    /// Returns the local committed index.
    fn commit_index(&self) -> LogIndex;

    /// Returns the local log tail index.
    fn last_log_index(&self) -> LogIndex;

    /// Returns the installed snapshot boundary index.
    fn snapshot_index(&self) -> LogIndex;

    /// Returns the currently effective Raft membership.
    fn membership(&self) -> MembershipConfig;

    /// Returns the latest committed Raft membership.
    fn committed_membership(&self) -> MembershipConfig {
        self.membership()
    }

    /// Returns per-follower replication progress when this node is leader.
    fn replication(&self) -> Vec<ReplicationProgress>;

    /// Steps the persisted node and releases outputs only after required
    /// persistence has completed.
    ///
    /// # Errors
    ///
    /// Returns an implementation-defined error when the runtime cannot persist
    /// state required by the input step or when the runtime is already in a
    /// fatal error state.
    fn step(&mut self, input: RaftInput) -> Result<Vec<RaftOutput>, Self::Error>;

    /// Looks up the local term at a log index covered by the retained log or
    /// snapshot boundary.
    fn term_at_index(&self, index: LogIndex) -> Option<Term>;
}
