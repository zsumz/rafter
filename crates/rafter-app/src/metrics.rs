//! Stable group metrics exposed by the embedded driver.

use rafter::{LogIndex, MembershipConfig, NodeId, ReplicationProgress, Role, Term};

use crate::group::GroupFatalState;

/// Metrics snapshot for one embedded Raft group.
///
/// An observation, never protocol state: it is cloned and compared on every
/// step that asks for one, so every field is a plain comparable value and
/// nothing here carries an error or a payload. Reading it drives no protocol
/// and proves nothing about other replicas — a follower's snapshot describes
/// what that follower believes, which may be stale.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftGroupMetrics<G> {
    /// The group this snapshot describes.
    pub group_id: G,
    /// The local node this snapshot was taken on.
    pub node_id: NodeId,
    /// This node's current role.
    ///
    /// `Leader` here is this node's own belief in its current term, not proof
    /// that it still holds leadership; a partitioned leader reports `Leader`
    /// until it learns of a higher term.
    pub role: Role,
    /// The current term.
    pub term: Term,
    /// The leader this node last heard from, if any. A hint for redirecting a
    /// client, never an authorization decision.
    pub leader_hint: Option<NodeId>,
    /// The highest index known committed. Committed entries are durable and
    /// will not be overwritten.
    pub commit_index: LogIndex,
    /// The highest index the state machine has applied.
    ///
    /// At or below the commit index in general, and it may legitimately exceed
    /// the last committed *application* entry after a snapshot install.
    pub applied_index: LogIndex,
    /// The highest index in this node's local log, committed or not.
    pub last_log_index: LogIndex,
    /// The index this node's log is compacted through.
    pub snapshot_index: LogIndex,
    /// The effective membership, which may be a joint configuration mid-change.
    pub membership: MembershipConfig,
    /// Per-follower replication progress. Populated on a leader; empty
    /// elsewhere.
    pub replication: Vec<ReplicationProgress>,
    /// Locally submitted proposals awaiting a terminal outcome.
    pub pending_proposals: usize,
    /// Alias carrying the same value as
    /// [`RaftGroupMetrics::pending_read_barriers`].
    ///
    /// Prefer the specific read metrics below; this field exists only because
    /// it predates them and is slated for removal before 1.0.
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
