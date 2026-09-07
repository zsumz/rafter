//! Pure admission checks for bindings and replacement policies.
//!
//! This module owns the questions a mutation must answer before it touches
//! group state: whether a binding conflicts or reaches beneath the retirement
//! floor, and whether an incoming policy names principals this group can
//! resolve. It only inspects state; every write stays with the mutation.

use std::collections::BTreeSet;

use rafter::NodeId;

use crate::PeerId;

use super::{DirectoryError, GroupState};

pub(super) fn validate_new_binding(
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

pub(super) fn resolve_policy(
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

pub(super) fn refuse_retired_reauthorization(
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
