//! Runtime trait boundary for persisted Rafter nodes.
//!
//! This crate owns only the application-facing runtime contract. It depends on
//! the deterministic `rafter` kernel and deliberately does not depend on
//! storage, durable runtime, app, service, transport, or async crates.

use std::error::Error;

use rafter::{
    ClientProposalInput, Input as RaftInput, LogIndex, MembershipConfig, NodeId as RaftNodeId,
    Output as RaftOutput, ReplicationProgress, Role as RaftRole, Term,
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

    /// Returns the index the local state machine must reach to have consumed
    /// every committed application command.
    ///
    /// This is the highest index at or below
    /// [`PersistedRaftRuntime::commit_index`] whose log entry carries an
    /// application payload, or [`PersistedRaftRuntime::snapshot_index`] when the
    /// snapshot boundary is higher — a snapshot subsumes every application entry
    /// it covers. It is `LogIndex::ZERO` when the node has committed no
    /// application entry and holds no snapshot.
    ///
    /// Elections and membership changes commit entries the state machine never
    /// sees, so this is not `commit_index`, and a fully caught-up state machine
    /// may trail the committed index forever.
    ///
    /// The value never decreases within one node incarnation: committed entries
    /// are never truncated, and the snapshot boundary only advances.
    ///
    /// The value is local. It says nothing about what the cluster has committed:
    /// a stale follower and an isolated former leader each report their own view,
    /// and both can report a fully applied state machine while missing entries a
    /// current leader has committed. It is a recovery and readiness signal, not a
    /// freshness proof — a linearizable read still requires a read-index barrier.
    ///
    /// Implementations must report the true value for their own log rather than
    /// an optimistic bound. A runtime that reports zero makes a readiness gate
    /// pass before recovery has replayed anything.
    fn committed_application_index(&self) -> LogIndex;

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

    /// Steps one contiguous client-proposal batch under one runtime
    /// durability boundary.
    ///
    /// This is the hot-path proposal API. It lets application layers carry a
    /// write batch as proposal-shaped data instead of wrapping it in a
    /// generic input stream that the kernel must rediscover.
    ///
    /// # Errors
    ///
    /// Returns an implementation-defined error when the runtime cannot satisfy
    /// its documented proposal-batch persistence and output-release contract.
    fn step_proposal_batch(
        &mut self,
        proposals: Vec<ClientProposalInput>,
    ) -> Result<Vec<RaftOutput>, Self::Error>;

    /// Steps several inputs under one runtime durability boundary.
    ///
    /// Implementations must choose and document their batch semantics
    /// explicitly. A runtime that commits any side effects for earlier inputs
    /// and then returns an error for a later input can otherwise black-hole the
    /// earlier outputs. Durable implementations should therefore release no
    /// outputs until every accepted input in the batch satisfies the
    /// implementation's persistence contract.
    ///
    /// # Errors
    ///
    /// Returns an implementation-defined error when the runtime cannot satisfy
    /// its documented batch persistence and output-release contract.
    fn step_batch(&mut self, inputs: Vec<RaftInput>) -> Result<Vec<RaftOutput>, Self::Error>;

    /// Looks up the local term at a log index covered by the retained log or
    /// snapshot boundary.
    fn term_at_index(&self, index: LogIndex) -> Option<Term>;
}
