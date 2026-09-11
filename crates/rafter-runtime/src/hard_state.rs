//! Projection of kernel state into its final durable hard-state image.
//!
//! The stepping path publishes this commit metadata only after the matching
//! snapshot and log entries are durable. Its earlier term/vote fence retains
//! the previously durable commit metadata.

use rafter::Node as RaftNode;
use rafter_storage::RaftHardState;

pub(crate) fn hard_state_for_node(node: &RaftNode) -> RaftHardState {
    RaftHardState {
        current_term: node.current_term(),
        voted_for: node.voted_for(),
        commit_index: node.commit_index(),
        committed_configuration: node.committed_configuration_state_at(node.commit_index()),
    }
}
