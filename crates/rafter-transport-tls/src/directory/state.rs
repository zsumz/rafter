//! Private peer-directory state.

use std::collections::{BTreeMap, BTreeSet};

use rafter::NodeId;

use crate::PeerId;

#[derive(Debug)]
pub(super) struct DirectoryState<G> {
    pub(super) groups: BTreeMap<G, GroupState>,
}

#[derive(Debug, Default)]
pub(super) struct GroupState {
    pub(super) node_to_peer: BTreeMap<NodeId, PeerId>,
    pub(super) peer_to_node: BTreeMap<PeerId, NodeId>,
    pub(super) policy: Option<GroupPolicy>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GroupPolicy {
    pub(super) authorized_peers: BTreeSet<PeerId>,
    pub(super) authorized_nodes: BTreeSet<NodeId>,
    pub(super) retirement_floor: Option<NodeId>,
}

pub(super) fn maximum_floor(left: Option<NodeId>, right: Option<NodeId>) -> Option<NodeId> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}
