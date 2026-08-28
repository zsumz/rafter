//! The cluster snapshot captured alongside a model-check failure.
//!
//! A summary records per-node term, role, commit index, and log geometry — the
//! minimum a reader needs to orient in a counterexample without carrying the
//! whole cluster. It reports a state; it is never the state itself.

use rafter::{LogIndex, NodeId, Role, Term};

/// Node state summary captured with a model-checking failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeSummary {
    pub node_id: NodeId,
    pub term: Term,
    pub role: Role,
    pub commit_index: LogIndex,
    pub snapshot_index: LogIndex,
    pub first_log_index: LogIndex,
    pub last_log_index: LogIndex,
    pub retained_log_len: usize,
}

/// Cluster state summary captured with a model-checking failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateSummary {
    pub nodes: Vec<NodeSummary>,
}
