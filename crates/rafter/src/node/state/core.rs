//! Canonical durable state and process-local follower state.
//!
//! These structures separate restart state from volatile cursors, local
//! correlation, incoming snapshot progress, and diagnostic counters.

use crate::{
    CommittedConfiguration, LogEntry, LogIndex, NodeId, RaftSnapshot,
    SnapshotChunkRejectionCounters, Term,
};

use super::super::Role;
use super::proposal::LocalProposalTracker;
use super::snapshot::IncomingSnapshotTransfer;

/// Canonical protocol state that survives restart.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub(in crate::node) struct PersistentState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub committed_configuration: Option<CommittedConfiguration>,
    pub snapshot: Option<RaftSnapshot>,
    pub log: Vec<LogEntry>,
}

/// Process-local protocol state reconstructed or reset at startup.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(in crate::node) struct VolatileState {
    pub role: Role,
    pub commit_index: LogIndex,
    pub applied_index: LogIndex,
    /// Local-only proposal correlation. This is volatile by design: it is not
    /// replicated, persisted, snapshotted, or restored after restart.
    pub local_proposals: LocalProposalTracker,
    pub incoming_snapshot: Option<IncomingSnapshotTransfer>,
    /// The node this replica believes is the current leader, set on accepted
    /// leader traffic and consulted by the pre-vote leader-stickiness rule
    /// (thesis 4.2.3). Never persisted.
    pub leader_hint: Option<NodeId>,
    /// Diagnostic counters for rejected snapshot chunks. They never influence
    /// protocol decisions and reset on process restart.
    pub snapshot_chunk_rejections: SnapshotChunkRejectionCounters,
}

impl Default for VolatileState {
    fn default() -> Self {
        Self {
            role: Role::Follower,
            commit_index: LogIndex::ZERO,
            applied_index: LogIndex::ZERO,
            local_proposals: LocalProposalTracker::default(),
            incoming_snapshot: None,
            leader_hint: None,
            snapshot_chunk_rejections: SnapshotChunkRejectionCounters::default(),
        }
    }
}

impl VolatileState {
    /// Builds follower volatile state with commit and apply floors at `index`.
    pub(in crate::node) fn at_applied_index(index: LogIndex) -> Self {
        Self {
            role: Role::Follower,
            commit_index: index,
            applied_index: index,
            local_proposals: LocalProposalTracker::default(),
            incoming_snapshot: None,
            leader_hint: None,
            snapshot_chunk_rejections: SnapshotChunkRejectionCounters::default(),
        }
    }
}
