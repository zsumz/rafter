//! Stable group metrics exposed by the embedded driver.

use rafter::{LogIndex, MembershipConfig, NodeId, ReplicationProgress, Role, Term};

use crate::group::GroupFatalState;

/// Metrics snapshot for one embedded Raft group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftGroupMetrics<G> {
    pub group_id: G,
    pub node_id: NodeId,
    pub role: Role,
    pub term: Term,
    pub leader_hint: Option<NodeId>,
    pub commit_index: LogIndex,
    pub applied_index: LogIndex,
    pub last_log_index: LogIndex,
    pub snapshot_index: LogIndex,
    pub membership: MembershipConfig,
    pub replication: Vec<ReplicationProgress>,
    pub pending_proposals: usize,
    /// Compatibility alias for `pending_read_barriers`.
    ///
    /// Prefer the more specific read metrics below in new code.
    pub pending_reads: usize,
    /// Low-level read-index barriers currently waiting for a core read result
    /// or local apply freshness.
    pub pending_read_barriers: usize,
    /// Linearizable query helper reads waiting on a read barrier.
    pub pending_query_reads: usize,
    /// Completed linearizable query helper proofs waiting to be consumed or
    /// dropped by the caller.
    pub completed_query_reads: usize,
    /// Distinct `ReadId`s reserved by any read table.
    pub reserved_reads: usize,
    pub fatal_state: GroupFatalState,
}
