//! Read-only projections of the durable runtime's current state.
//!
//! Identity, role, term, indexes, membership, and the retained log suffix are
//! reported here, plus the committed-application index the embedding needs at
//! its readiness boundaries. Nothing in this module writes to a store or steps
//! the kernel, so a poisoned runtime still answers — from memory, not the medium.

use rafter::{
    CommittedConfiguration, ConfigurationEntry, LogEntry, LogIndex, MembershipConfig,
    NodeId as RaftNodeId, RaftSnapshot, ReplicationProgress, Role as RaftRole, SnapshotChunkSource,
    SnapshotTransferStatus, Term,
};
use rafter_storage::{RaftHardState, RaftHardStateStore, RaftLogSegment, RaftSnapshotStore};

use crate::hard_state::hard_state_for_node;
use crate::DurableRaftNode;
#[allow(
    unused_imports,
    reason = "named only by the rustdoc links on the accessors below, never by their code"
)]
use crate::{PersistedRaftRuntime, RaftRuntimeError};

impl<H: RaftHardStateStore, L: RaftLogSegment, S: RaftSnapshotStore + SnapshotChunkSource>
    DurableRaftNode<H, L, S>
{
    /// Returns the local Raft node ID.
    #[must_use]
    pub fn id(&self) -> RaftNodeId {
        self.node.id()
    }

    /// Returns the best-known leader for client redirection, if one is known.
    #[must_use]
    pub fn leader_hint(&self) -> Option<RaftNodeId> {
        self.node.leader_hint()
    }

    /// Returns the local node's current Raft role.
    #[must_use]
    pub fn role(&self) -> RaftRole {
        self.node.role()
    }

    /// Returns the local node's current term.
    #[must_use]
    pub fn current_term(&self) -> Term {
        self.node.current_term()
    }

    /// Whether a read requested now would use the active leader lease.
    #[must_use]
    pub fn read_lease_active(&self) -> bool {
        self.node.read_lease_active()
    }

    /// Returns the highest log index committed by the local Raft kernel.
    #[must_use]
    pub fn commit_index(&self) -> rafter::LogIndex {
        self.node.commit_index()
    }

    /// Returns the highest log index this runtime has emitted application
    /// outputs through.
    ///
    /// This is the floor a local compaction may not exceed — see
    /// [`RaftRuntimeError::SnapshotAheadOfApplied`]. On a node recovered with
    /// the default applied floor it starts at the snapshot boundary and rises
    /// as committed entries are emitted, so a restart path that intends to
    /// compact must drain first, or declare its floor at construction with
    /// [`DurableRaftNode::with_storage_and_snapshot_store_applied_through`].
    #[must_use]
    pub fn applied_index(&self) -> rafter::LogIndex {
        self.node.applied_index()
    }

    /// Returns the highest log index known to the local Raft kernel.
    #[must_use]
    pub fn last_log_index(&self) -> rafter::LogIndex {
        self.node.last_log_index()
    }

    /// The term recorded at `index`, when the local log or snapshot
    /// boundary still covers it — what an application needs to build
    /// snapshot metadata for [`DurableRaftNode::compact_log_with_snapshot`]
    /// at an arbitrary applied boundary.
    #[must_use]
    pub fn term_at_index(&self, index: rafter::LogIndex) -> Option<Term> {
        self.node.term_at_index(index)
    }

    /// Returns the installed snapshot boundary index.
    #[must_use]
    pub fn snapshot_index(&self) -> rafter::LogIndex {
        self.node.snapshot_index()
    }

    /// Returns the installed snapshot descriptor, if the local kernel has one.
    #[must_use]
    pub fn snapshot(&self) -> Option<&RaftSnapshot> {
        self.node.snapshot()
    }

    /// Returns the durable snapshot store backing this runtime.
    #[must_use]
    pub fn snapshot_store(&self) -> &S {
        &self.snapshot_store
    }

    /// Returns the current inbound snapshot transfer state.
    #[must_use]
    pub fn snapshot_transfer_status(&self) -> SnapshotTransferStatus {
        self.node.snapshot_transfer_status()
    }

    /// Returns per-follower replication progress when this node is leader.
    #[must_use]
    pub fn leader_replication_progress(&self) -> Vec<ReplicationProgress> {
        self.node.leader_replication_progress()
    }

    /// Returns the catch-up barrier for a learner promotion, if one is active.
    #[must_use]
    pub fn promotion_barrier(&self, learner_id: RaftNodeId) -> Option<rafter::PromotionBarrier> {
        self.node.promotion_barrier(learner_id)
    }

    /// Returns the committed membership configuration.
    #[must_use]
    pub fn committed_membership(&self) -> MembershipConfig {
        self.node.committed_membership()
    }

    /// Returns the index the local state machine must reach to have consumed
    /// every committed application command at or below `index`.
    ///
    /// See [`PersistedRaftRuntime::committed_application_index_through`] for the
    /// contract. This implementation scans the committed retained log suffix
    /// backwards for the highest application entry at or below the bound and
    /// falls back to the snapshot boundary, which subsumes every application
    /// entry it covers, capped at the bound.
    ///
    /// The scan is O(1) whenever the entry at the bound is an application entry,
    /// which is the steady state of a busy group. Its worst case is the whole
    /// committed retained suffix, reached when the retained log holds no
    /// committed application entry at all — a group that has only ever elected
    /// and reconfigured. Callers evaluate this at readiness boundaries and once
    /// per read barrier, not per step, so no derived index is kept in the kernel.
    #[must_use]
    pub fn committed_application_index_through(&self, index: LogIndex) -> LogIndex {
        let snapshot_index = self.node.snapshot_index();
        let bound = index.min(self.node.commit_index());
        let first_retained = snapshot_index.next();
        self.node
            .log_entries_slice_from(first_retained)
            .iter()
            .enumerate()
            .rev()
            .map(|(offset, entry)| (LogIndex(first_retained.0 + offset as u64), entry))
            .find(|(entry_index, entry)| {
                *entry_index <= bound && entry.application_payload().is_some()
            })
            .map_or_else(|| snapshot_index.min(index), |(entry_index, _)| entry_index)
    }

    /// Returns the index the local state machine must reach to have consumed
    /// every committed application command.
    ///
    /// See [`PersistedRaftRuntime::committed_application_index`] for the
    /// contract. This is
    /// [`DurableRaftNode::committed_application_index_through`] at the commit
    /// index.
    #[must_use]
    pub fn committed_application_index(&self) -> LogIndex {
        self.committed_application_index_through(self.node.commit_index())
    }

    /// Returns the committed configuration entry, if the log has committed one.
    #[must_use]
    pub fn committed_configuration_entry(&self) -> Option<ConfigurationEntry> {
        self.node.committed_configuration_entry()
    }

    /// Returns the committed configuration state, including joint-consensus
    /// metadata when applicable.
    #[must_use]
    pub fn committed_configuration_state(&self) -> Option<CommittedConfiguration> {
        self.node.committed_configuration_state()
    }

    /// Returns the effective membership used for current quorum decisions.
    #[must_use]
    pub fn effective_membership(&self) -> MembershipConfig {
        self.node.effective_membership()
    }

    /// Returns the effective configuration entry used by the local kernel.
    #[must_use]
    pub fn effective_configuration_entry(&self) -> Option<ConfigurationEntry> {
        self.node.effective_configuration_entry()
    }

    /// Returns the membership recorded at the installed snapshot boundary.
    #[must_use]
    pub fn snapshot_committed_membership(&self) -> Option<MembershipConfig> {
        self.node.snapshot_committed_membership()
    }

    /// Returns local log entries starting at `first_index`.
    #[must_use]
    pub fn log_entries_from(&self, first_index: LogIndex) -> Vec<LogEntry> {
        self.node.log_entries_from(first_index)
    }

    /// Returns the hard-state image corresponding to the current in-memory
    /// kernel state.
    #[must_use]
    pub fn hard_state(&self) -> RaftHardState {
        hard_state_for_node(&self.node)
    }
}
