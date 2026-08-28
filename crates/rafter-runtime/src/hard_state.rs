//! Projection of kernel state into the hard-state image a store persists.
//!
//! One place decides what the durable term, vote, commit index, and committed
//! configuration are for a given kernel — including the cap that keeps a
//! published commit index from naming an entry the log segment does not yet
//! hold. It writes nothing; publication is the stepping path's obligation.

use rafter::{LogIndex, Node as RaftNode};
use rafter_storage::{RaftHardState, RaftLogSegment};

pub(crate) fn hard_state_for_node(node: &RaftNode) -> RaftHardState {
    hard_state_for_node_capped_at(node, node.commit_index())
}

pub(crate) fn hard_state_for_node_capped_at(
    node: &RaftNode,
    durable_commit_index: LogIndex,
) -> RaftHardState {
    let commit_index = node.commit_index().min(durable_commit_index);
    RaftHardState {
        current_term: node.current_term(),
        voted_for: node.voted_for(),
        commit_index,
        committed_configuration: node.committed_configuration_state_at(commit_index),
    }
}

pub(crate) fn durable_last_log_index<L: RaftLogSegment>(log_segment: &L) -> LogIndex {
    LogIndex(log_segment.next_index().0.saturating_sub(1))
}
