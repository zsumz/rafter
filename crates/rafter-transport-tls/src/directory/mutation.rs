//! Atomic group, binding, and authorization-policy mutation.

use std::collections::BTreeSet;

use rafter::NodeId;
use rafter_service::PeerPolicy;

use crate::PeerId;

use super::state::{maximum_floor, GroupPolicy, GroupState};
use super::AuthorizationLease;
use super::{DirectoryError, InstalledPeerPolicy, TlsPeerDirectory};

impl<G> TlsPeerDirectory<G>
where
    G: Ord,
{
    /// Adds an empty known group.
    ///
    /// This is required for a single-node group with no remote bindings: known
    /// group identity and authorized peer count are separate facts.
    ///
    /// Returns `false` when the group was already known.
    ///
    /// # Errors
    ///
    /// Returns [`DirectoryError`] when the group bound is exhausted or shared
    /// state is poisoned.
    pub fn insert_group(&self, group_id: G) -> Result<bool, DirectoryError> {
        let mut state = self.state.write().map_err(|_| DirectoryError::Poisoned)?;
        if state.groups.contains_key(&group_id) {
            return Ok(false);
        }
        if state.groups.len() >= self.limits.max_groups() {
            return Err(DirectoryError::GroupLimit {
                maximum: self.limits.max_groups(),
            });
        }

        state.groups.insert(group_id, GroupState::default());
        Ok(true)
    }

    /// Installs one stable principal/node binding, creating the group if needed.
    ///
    /// Repeating the same binding is idempotent. Changing either half of a live
    /// binding is refused. A node already classified as retired cannot be bound
    /// again beneath the monotonic floor.
    ///
    /// # Errors
    ///
    /// Returns [`DirectoryError`] for conflicting or retired bindings,
    /// exhausted limits, or poisoned state.
    pub fn bind(
        &self,
        group_id: G,
        node_id: NodeId,
        peer_id: PeerId,
    ) -> Result<(), DirectoryError> {
        let mut state = self.state.write().map_err(|_| DirectoryError::Poisoned)?;
        let group_is_new = !state.groups.contains_key(&group_id);
        if group_is_new && state.groups.len() >= self.limits.max_groups() {
            return Err(DirectoryError::GroupLimit {
                maximum: self.limits.max_groups(),
            });
        }

        let group = state.groups.entry(group_id).or_default();
        validate_new_binding(
            group,
            node_id,
            &peer_id,
            self.limits.max_bindings_per_group(),
        )?;
        group
            .binding_leases
            .entry(node_id)
            .or_insert_with(AuthorizationLease::new);
        group.node_to_peer.insert(node_id, peer_id.clone());
        group.peer_to_node.insert(peer_id, node_id);
        Ok(())
    }

    /// Forgets one no-longer-authorized node binding.
    ///
    /// Returns the removed principal, or `None` when the group or node was
    /// absent. A currently authorized binding must first disappear from the
    /// complete installed policy.
    ///
    /// # Errors
    ///
    /// Returns [`DirectoryError::AuthorizedNodeUnbind`] for a live authorized
    /// node, or [`DirectoryError::Poisoned`] for poisoned shared state.
    pub fn unbind(&self, group_id: &G, node_id: NodeId) -> Result<Option<PeerId>, DirectoryError> {
        let mut state = self.state.write().map_err(|_| DirectoryError::Poisoned)?;
        let Some(group) = state.groups.get_mut(group_id) else {
            return Ok(None);
        };
        if group
            .policy
            .as_ref()
            .is_some_and(|policy| policy.authorized_nodes.contains(&node_id))
        {
            return Err(DirectoryError::AuthorizedNodeUnbind { node_id });
        }

        let Some(peer_id) = group.node_to_peer.get(&node_id).cloned() else {
            return Ok(None);
        };
        let lease = group
            .binding_leases
            .get(&node_id)
            .cloned()
            .ok_or(DirectoryError::Poisoned)?;
        group.node_to_peer.remove(&node_id);
        group.binding_leases.remove(&node_id);
        lease.revoke();
        group.peer_to_node.remove(&peer_id);
        Ok(Some(peer_id))
    }

    /// Removes all routing and authorization state for one local group.
    ///
    /// # Errors
    ///
    /// Returns [`DirectoryError::Poisoned`] when shared state is poisoned.
    pub fn remove_group(&self, group_id: &G) -> Result<bool, DirectoryError> {
        let mut state = self.state.write().map_err(|_| DirectoryError::Poisoned)?;
        let Some(group) = state.groups.remove(group_id) else {
            return Ok(false);
        };
        revoke_all(&group);
        Ok(true)
    }

    /// Atomically replaces one group's complete authorization policy.
    ///
    /// Every principal must already have a stable node binding. The authorized
    /// set is replaced whole, while the retirement floor retains the greatest
    /// value ever accepted. Once a prior installed policy classified a node as
    /// retired, a later policy cannot authorize that identity again.
    ///
    /// # Errors
    ///
    /// Returns [`DirectoryError`] for an unknown group, duplicate or unmapped
    /// principals, retired reauthorization, or poisoned state.
    pub fn replace_policy(
        &self,
        group_id: &G,
        policy: PeerPolicy<PeerId>,
    ) -> Result<(), DirectoryError> {
        let mut state = self.state.write().map_err(|_| DirectoryError::Poisoned)?;
        let group = state
            .groups
            .get_mut(group_id)
            .ok_or(DirectoryError::UnknownGroup)?;
        let incoming_floor = policy.retirement_floor();
        let (authorized_peers, authorized_nodes) = resolve_policy(group, policy.into_peers())?;
        refuse_retired_reauthorization(group, &authorized_nodes)?;

        let retirement_floor = maximum_floor(
            group
                .policy
                .as_ref()
                .and_then(|current| current.retirement_floor),
            incoming_floor,
        );
        let leases = authorization_leases(group, &authorized_nodes);
        group.policy = Some(GroupPolicy {
            authorized_peers,
            authorized_nodes,
            leases,
            retirement_floor,
        });
        Ok(())
    }

    /// Returns the policy currently enforced for one group.
    ///
    /// # Errors
    ///
    /// Returns [`DirectoryError::Poisoned`] when shared state is poisoned.
    pub fn policy(&self, group_id: &G) -> Result<Option<InstalledPeerPolicy>, DirectoryError> {
        let state = self.state.read().map_err(|_| DirectoryError::Poisoned)?;
        Ok(state.groups.get(group_id).and_then(|group| {
            group.policy.as_ref().map(|policy| InstalledPeerPolicy {
                authorized_peers: policy.authorized_peers.iter().cloned().collect(),
                retirement_floor: policy.retirement_floor,
            })
        }))
    }
}

fn authorization_leases(
    group: &GroupState,
    authorized_nodes: &BTreeSet<NodeId>,
) -> std::collections::BTreeMap<NodeId, AuthorizationLease> {
    let mut leases = std::collections::BTreeMap::new();
    for node_id in authorized_nodes {
        let lease = group
            .policy
            .as_ref()
            .and_then(|policy| policy.leases.get(node_id))
            .cloned()
            .unwrap_or_else(AuthorizationLease::new);
        leases.insert(*node_id, lease);
    }
    if let Some(current) = &group.policy {
        for (node_id, lease) in &current.leases {
            if !authorized_nodes.contains(node_id) {
                lease.revoke();
            }
        }
    }
    leases
}

fn revoke_all(group: &GroupState) {
    for lease in group.binding_leases.values() {
        lease.revoke();
    }
    if let Some(policy) = &group.policy {
        for lease in policy.leases.values() {
            lease.revoke();
        }
    }
}

fn validate_new_binding(
    group: &GroupState,
    node_id: NodeId,
    peer_id: &PeerId,
    maximum: usize,
) -> Result<(), DirectoryError> {
    if let Some(existing) = group.node_to_peer.get(&node_id) {
        if existing == peer_id {
            return Ok(());
        }
        return Err(DirectoryError::NodeAlreadyBound {
            node_id,
            existing: existing.clone(),
            requested: peer_id.clone(),
        });
    }
    if let Some(existing) = group.peer_to_node.get(peer_id) {
        if *existing == node_id {
            return Ok(());
        }
        return Err(DirectoryError::PeerAlreadyBound {
            peer_id: peer_id.clone(),
            existing: *existing,
            requested: node_id,
        });
    }
    if let Some(policy) = &group.policy {
        if let Some(retirement_floor) = policy.retirement_floor {
            if node_id <= retirement_floor && !policy.authorized_nodes.contains(&node_id) {
                return Err(DirectoryError::RetiredNodeBinding {
                    node_id,
                    retirement_floor,
                });
            }
        }
    }
    if group.node_to_peer.len() >= maximum {
        return Err(DirectoryError::BindingLimit { maximum });
    }
    Ok(())
}

fn resolve_policy(
    group: &GroupState,
    peers: Vec<PeerId>,
) -> Result<(BTreeSet<PeerId>, BTreeSet<NodeId>), DirectoryError> {
    let mut authorized_peers = BTreeSet::new();
    let mut authorized_nodes = BTreeSet::new();
    for peer_id in peers {
        if !authorized_peers.insert(peer_id.clone()) {
            return Err(DirectoryError::DuplicatePolicyPeer { peer_id });
        }
        let node_id = group.peer_to_node.get(&peer_id).copied().ok_or_else(|| {
            DirectoryError::UnknownPolicyPeer {
                peer_id: peer_id.clone(),
            }
        })?;
        authorized_nodes.insert(node_id);
    }
    Ok((authorized_peers, authorized_nodes))
}

fn refuse_retired_reauthorization(
    group: &GroupState,
    authorized_nodes: &BTreeSet<NodeId>,
) -> Result<(), DirectoryError> {
    let Some(current) = &group.policy else {
        return Ok(());
    };
    let Some(retirement_floor) = current.retirement_floor else {
        return Ok(());
    };
    if let Some(node_id) = authorized_nodes
        .iter()
        .copied()
        .find(|node_id| *node_id <= retirement_floor && !current.authorized_nodes.contains(node_id))
    {
        return Err(DirectoryError::RetiredNodeReauthorization {
            node_id,
            retirement_floor,
        });
    }
    Ok(())
}
