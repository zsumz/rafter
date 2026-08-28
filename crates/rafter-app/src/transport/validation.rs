//! The check that turns an authenticated envelope into a routed one.
//!
//! Runs the deployment's admission policy in a fixed order — retirement
//! before authorization, because a retired identity is unauthorized by
//! construction and only the order decides which refusal an operator
//! reads.

use rafter::NodeId;

use super::{
    message_sender, AuthenticatedPeerEnvelope, AuthenticatedPeerEnvelopeError,
    AuthenticatedPeerValidator, PeerEnvelope,
};

impl<G, P> AuthenticatedPeerEnvelope<G, P> {
    /// Validates this authenticated envelope against the local node and group
    /// authorization policy.
    ///
    /// # Errors
    ///
    /// Returns an envelope error when the group is unknown, the authenticated
    /// peer does not map to `raft_from`, the message targets another local
    /// node, the peer is retired or unauthorized, or the embedded Raft message
    /// sender does not match the envelope sender.
    pub fn validate<V>(
        &self,
        local_node_id: NodeId,
        validator: &V,
    ) -> Result<(), AuthenticatedPeerEnvelopeError>
    where
        V: AuthenticatedPeerValidator<G, P>,
    {
        if !validator.is_known_group(&self.group_id) {
            return Err(AuthenticatedPeerEnvelopeError::UnknownGroup);
        }
        let Some(mapped_node_id) =
            validator.node_for_authenticated_peer(&self.group_id, &self.authenticated_peer)
        else {
            return Err(AuthenticatedPeerEnvelopeError::AuthenticatedPeerNotMapped);
        };
        if mapped_node_id != self.raft_from {
            return Err(AuthenticatedPeerEnvelopeError::AuthenticatedPeerMismatch {
                expected: mapped_node_id,
                actual: self.raft_from,
            });
        }
        if self.raft_to != local_node_id {
            return Err(AuthenticatedPeerEnvelopeError::WrongRecipient {
                expected: local_node_id,
                actual: self.raft_to,
            });
        }
        // **Retirement first, and the order is the whole of what makes the
        // distinction sayable.** Both answers come out of one published policy —
        // authorized means "in the set", retired means "beneath the floor and
        // not in the set" — so retirement *implies* unauthorized for every
        // directory that follows the contract. Asking about authorization first
        // therefore answered every retired peer with the repairable variant, and
        // the permanent one was dead code at the boundary.
        if validator.is_retired_peer(&self.group_id, self.raft_from) {
            return Err(AuthenticatedPeerEnvelopeError::RetiredPeer {
                node_id: self.raft_from,
            });
        }
        if !validator.is_authorized_peer(&self.group_id, self.raft_from) {
            return Err(AuthenticatedPeerEnvelopeError::UnauthorizedPeer {
                node_id: self.raft_from,
            });
        }
        let message_from = message_sender(&self.message);
        if message_from != self.raft_from {
            return Err(AuthenticatedPeerEnvelopeError::SenderMismatch {
                envelope_from: self.raft_from,
                message_from,
            });
        }
        Ok(())
    }

    /// Validates and converts this authenticated envelope into a plain peer
    /// envelope for the group driver.
    ///
    /// # Errors
    ///
    /// Returns the same validation errors as
    /// [`AuthenticatedPeerEnvelope::validate`].
    pub fn try_into_peer_envelope<V>(
        self,
        local_node_id: NodeId,
        validator: &V,
    ) -> Result<PeerEnvelope<G>, AuthenticatedPeerEnvelopeError>
    where
        V: AuthenticatedPeerValidator<G, P>,
    {
        self.validate(local_node_id, validator)?;
        Ok(PeerEnvelope {
            group_id: self.group_id,
            from: self.raft_from,
            to: self.raft_to,
            message: self.message,
        })
    }
}
