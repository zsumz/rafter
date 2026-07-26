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
    /// every committed application command at or below `index`.
    ///
    /// This is the highest index at or below both `index` and
    /// [`PersistedRaftRuntime::commit_index`] whose log entry carries an
    /// application payload; when no such entry is retained it is the snapshot
    /// boundary, which subsumes every application entry it covers, capped at
    /// `index`. It is `LogIndex::ZERO` when the node holds no snapshot and has
    /// committed no application entry at or below `index`.
    ///
    /// The result never exceeds `index`. That is the load-bearing property for a
    /// read barrier: elections and membership changes commit entries the state
    /// machine never sees, so a barrier that required its state machine to reach
    /// the read index itself would require an index the kernel guarantees it will
    /// never report. Requiring more than this is not conservative — it makes a
    /// read wait for a write that is not ordered before it.
    ///
    /// The value is non-decreasing in `index`, and non-decreasing over time for a
    /// fixed `index` within one node incarnation: committed entries are never
    /// truncated, and compaction can only raise the answer to a boundary the
    /// state machine has itself already reached.
    ///
    /// The value is local, and it is not a freshness proof. Pairing it with a
    /// granted read index is what makes a read linearizable; on its own it says
    /// only what this replica knows.
    ///
    /// Implementations must report the true value for their own log rather than
    /// an optimistic bound. A runtime that reports an index below the highest
    /// committed application entry at or below `index` lets a barrier grant
    /// before the state machine has applied an acknowledged write.
    fn committed_application_index_through(&self, index: LogIndex) -> LogIndex;

    /// Returns the index the local state machine must reach to have consumed
    /// every committed application command.
    ///
    /// This is [`PersistedRaftRuntime::committed_application_index_through`] at
    /// the commit index, and it is the readiness predicate: compare it with the
    /// state machine's applied index after recovery. Implementations should not
    /// override it.
    ///
    /// Elections and membership changes commit entries the state machine never
    /// sees, so this is not `commit_index`, and a fully caught-up state machine
    /// may trail the committed index forever.
    fn committed_application_index(&self) -> LogIndex {
        self.committed_application_index_through(self.commit_index())
    }

    /// Returns the currently effective Raft membership.
    ///
    /// Effective, not committed: a configuration takes effect on append, so
    /// this may name a change that is still uncommitted and could yet be
    /// reverted. Widening a transport's peer set to match it is safe and
    /// necessary — a joining replica must be able to speak before the change
    /// commits, or it can never catch up. Narrowing one is not.
    fn membership(&self) -> MembershipConfig;

    /// Returns the latest committed Raft membership.
    ///
    /// A committed configuration cannot be reverted, which makes this the only
    /// membership that licenses narrowing a peer set or fencing a replica that
    /// left it.
    ///
    /// There is no default, for the reason `ReplicatedStateMachine`'s
    /// `SNAPSHOT_SUPPORT` const in `rafter-app` has none: a provided body would
    /// make a claim on the implementor's behalf, and the claim is one only the
    /// implementor can make. Answering with
    /// [`PersistedRaftRuntime::membership`] is correct for a runtime that has
    /// no uncommitted configuration to distinguish — a fixed-membership test
    /// runtime, say — and wrong for every runtime that does, because it reports
    /// an appended-but-uncommitted change as committed, and the layer above
    /// fences on this answer.
    ///
    /// A runtime that genuinely cannot be mid-change says so in one line by
    /// forwarding to [`PersistedRaftRuntime::membership`]. Writing that line is
    /// the point: the implementor asserts it, rather than receiving it.
    fn committed_membership(&self) -> MembershipConfig;

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
