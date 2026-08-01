//! Managed-service validator integration.

use std::fmt;

use rafter::NodeId;
use rafter_service::AuthenticatedPeerValidator;

use crate::PeerId;

use super::TlsPeerDirectory;

impl<G> fmt::Debug for TlsPeerDirectory<G>
where
    G: Ord,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.state.read() {
            Ok(state) => formatter
                .debug_struct("TlsPeerDirectory")
                .field("limits", &self.limits)
                .field("groups", &state.groups.len())
                .finish(),
            Err(_) => formatter
                .debug_struct("TlsPeerDirectory")
                .field("limits", &self.limits)
                .field("state", &"poisoned")
                .finish(),
        }
    }
}

impl<G> AuthenticatedPeerValidator<G, PeerId> for TlsPeerDirectory<G>
where
    G: Ord + Send + Sync + 'static,
{
    fn is_known_group(&self, group_id: &G) -> bool {
        self.state
            .read()
            .is_ok_and(|state| state.groups.contains_key(group_id))
    }

    fn node_for_authenticated_peer(&self, group_id: &G, peer: &PeerId) -> Option<NodeId> {
        self.state.read().ok().and_then(|state| {
            state
                .groups
                .get(group_id)
                .and_then(|group| group.peer_to_node.get(peer).copied())
        })
    }

    fn principal_for_node(&self, group_id: &G, node_id: NodeId) -> Option<PeerId> {
        self.state.read().ok().and_then(|state| {
            state
                .groups
                .get(group_id)
                .and_then(|group| group.node_to_peer.get(&node_id).cloned())
        })
    }

    fn is_authorized_peer(&self, group_id: &G, node_id: NodeId) -> bool {
        self.state.read().is_ok_and(|state| {
            state
                .groups
                .get(group_id)
                .and_then(|group| group.policy.as_ref())
                .is_some_and(|policy| policy.authorized_nodes.contains(&node_id))
        })
    }

    fn is_retired_peer(&self, group_id: &G, node_id: NodeId) -> bool {
        self.state.read().is_ok_and(|state| {
            state
                .groups
                .get(group_id)
                .and_then(|group| group.policy.as_ref())
                .is_some_and(|policy| {
                    policy
                        .retirement_floor
                        .is_some_and(|floor| node_id <= floor)
                        && !policy.authorized_nodes.contains(&node_id)
                })
        })
    }
}
