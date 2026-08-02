//! Read-only routing and admission queries used on transport hot paths.

use rafter::NodeId;

use crate::PeerId;

use super::{AuthorizationLease, DirectoryError, TlsPeerDirectory};

/// Current per-group admission classification for one Raft identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PeerAuthorization {
    /// The installed policy currently authorizes this identity.
    Authorized,
    /// The identity is not currently authorized but may be authorized later.
    Unauthorized,
    /// A committed removal permanently retired this identity.
    Retired,
}

#[derive(Clone, Debug)]
pub(crate) enum OutboundRoute {
    UnknownGroup,
    LocalIdentityMismatch,
    UnknownNode,
    Unauthorized,
    Retired,
    Authorized {
        peer: PeerId,
        lease: AuthorizationLease,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InboundRoute {
    UnknownGroup,
    IdentityMismatch,
    Unauthorized,
    Retired,
    Authorized,
}

impl<G> TlsPeerDirectory<G>
where
    G: Ord,
{
    /// Returns whether the local runtime currently knows `group_id`.
    ///
    /// # Errors
    ///
    /// Returns [`DirectoryError::Poisoned`] when shared state is poisoned.
    pub fn contains_group(&self, group_id: &G) -> Result<bool, DirectoryError> {
        self.state
            .read()
            .map(|state| state.groups.contains_key(group_id))
            .map_err(|_| DirectoryError::Poisoned)
    }

    /// Returns the stable physical principal bound to one group node.
    ///
    /// # Errors
    ///
    /// Returns [`DirectoryError::Poisoned`] when shared state is poisoned.
    pub fn peer_for_node(
        &self,
        group_id: &G,
        node_id: NodeId,
    ) -> Result<Option<PeerId>, DirectoryError> {
        let state = self.state.read().map_err(|_| DirectoryError::Poisoned)?;
        Ok(state
            .groups
            .get(group_id)
            .and_then(|group| group.node_to_peer.get(&node_id).cloned()))
    }

    /// Returns the group-specific Raft identity bound to one physical peer.
    ///
    /// # Errors
    ///
    /// Returns [`DirectoryError::Poisoned`] when shared state is poisoned.
    pub fn node_for_peer(
        &self,
        group_id: &G,
        peer_id: &PeerId,
    ) -> Result<Option<NodeId>, DirectoryError> {
        let state = self.state.read().map_err(|_| DirectoryError::Poisoned)?;
        Ok(state
            .groups
            .get(group_id)
            .and_then(|group| group.peer_to_node.get(peer_id).copied()))
    }

    pub(crate) fn outbound_route(
        &self,
        group_id: &G,
        local_peer: &PeerId,
        from: NodeId,
        to: NodeId,
    ) -> Result<OutboundRoute, DirectoryError> {
        let state = self.state.read().map_err(|_| DirectoryError::Poisoned)?;
        let Some(group) = state.groups.get(group_id) else {
            return Ok(OutboundRoute::UnknownGroup);
        };
        if group.node_to_peer.get(&from) != Some(local_peer) {
            return Ok(OutboundRoute::LocalIdentityMismatch);
        }
        let Some(peer) = group.node_to_peer.get(&to) else {
            return Ok(OutboundRoute::UnknownNode);
        };
        Ok(match authorization_for(group, to) {
            PeerAuthorization::Authorized => OutboundRoute::Authorized {
                peer: peer.clone(),
                lease: group
                    .policy
                    .as_ref()
                    .and_then(|policy| policy.leases.get(&to))
                    .cloned()
                    .ok_or(DirectoryError::Poisoned)?,
            },
            PeerAuthorization::Unauthorized => OutboundRoute::Unauthorized,
            PeerAuthorization::Retired => OutboundRoute::Retired,
        })
    }

    pub(crate) fn inbound_route(
        &self,
        group_id: &G,
        local_peer: &PeerId,
        authenticated_peer: &PeerId,
        from: NodeId,
        to: NodeId,
    ) -> Result<InboundRoute, DirectoryError> {
        let state = self.state.read().map_err(|_| DirectoryError::Poisoned)?;
        let Some(group) = state.groups.get(group_id) else {
            return Ok(InboundRoute::UnknownGroup);
        };
        if group.peer_to_node.get(authenticated_peer) != Some(&from)
            || group.node_to_peer.get(&to) != Some(local_peer)
        {
            return Ok(InboundRoute::IdentityMismatch);
        }
        Ok(match authorization_for(group, from) {
            PeerAuthorization::Authorized => InboundRoute::Authorized,
            PeerAuthorization::Unauthorized => InboundRoute::Unauthorized,
            PeerAuthorization::Retired => InboundRoute::Retired,
        })
    }

    /// Classifies one group identity under the complete installed policy.
    ///
    /// A known binding with no installed policy is unauthorized. Retirement is
    /// reported before authorization because retired identities are absent from
    /// the authorized set by definition.
    ///
    /// # Errors
    ///
    /// Returns [`DirectoryError::UnknownGroup`] for an unknown group or
    /// [`DirectoryError::Poisoned`] when shared state is poisoned.
    pub fn authorization(
        &self,
        group_id: &G,
        node_id: NodeId,
    ) -> Result<PeerAuthorization, DirectoryError> {
        let state = self.state.read().map_err(|_| DirectoryError::Poisoned)?;
        let group = state
            .groups
            .get(group_id)
            .ok_or(DirectoryError::UnknownGroup)?;
        Ok(authorization_for(group, node_id))
    }
}

fn authorization_for(group: &super::state::GroupState, node_id: NodeId) -> PeerAuthorization {
    let Some(policy) = &group.policy else {
        return PeerAuthorization::Unauthorized;
    };
    if policy
        .retirement_floor
        .is_some_and(|floor| node_id <= floor)
        && !policy.authorized_nodes.contains(&node_id)
    {
        return PeerAuthorization::Retired;
    }
    if policy.authorized_nodes.contains(&node_id) {
        PeerAuthorization::Authorized
    } else {
        PeerAuthorization::Unauthorized
    }
}
