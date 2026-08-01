//! Durable session-state envelope types.

use crate::{ClusterId, PeerId, SessionStoreLimits};

use super::super::{PeerSessionState, TransportSessionState};

/// Magic bytes beginning every durable transport session-state file.
pub const SESSION_STATE_MAGIC: [u8; 8] = *b"RFTSESSN";
/// Durable transport session-state format emitted by this crate.
pub const SESSION_STATE_VERSION: u16 = 1;

/// Identity field carried by the durable session-state envelope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SessionIdentityField {
    /// Deployment boundary owning the state file.
    Cluster,
    /// Local authenticated transport principal owning the state file.
    LocalPeer,
    /// Remote peer named by one high-water record.
    RemotePeer,
}

/// Complete logical contents of one durable session-state file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedTransportSessionState {
    cluster_id: ClusterId,
    local_peer_id: PeerId,
    state: TransportSessionState,
}

impl PersistedTransportSessionState {
    /// Creates an envelope from validated identity and pure session state.
    #[must_use]
    pub fn new(cluster_id: ClusterId, local_peer_id: PeerId, state: TransportSessionState) -> Self {
        Self {
            cluster_id,
            local_peer_id,
            state,
        }
    }

    /// Deployment boundary that owns this state.
    #[must_use]
    pub const fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    /// Local authenticated principal that owns this state.
    #[must_use]
    pub const fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }

    /// Durable physical-peer record bound encoded in the file.
    #[must_use]
    pub const fn limits(&self) -> SessionStoreLimits {
        self.state.limits()
    }

    /// Number of retained physical-peer records.
    #[must_use]
    pub fn peer_count(&self) -> usize {
        self.state.peer_count()
    }

    /// Returns one remote peer's retained high-water marks.
    #[must_use]
    pub fn peer_state(&self, peer: &PeerId) -> PeerSessionState {
        self.state.peer_state(peer)
    }

    /// Returns the pure session state carried by this envelope.
    #[must_use]
    pub const fn state(&self) -> &TransportSessionState {
        &self.state
    }

    /// Consumes the envelope into its identity and pure-state parts.
    #[must_use]
    pub fn into_parts(self) -> (ClusterId, PeerId, TransportSessionState) {
        (self.cluster_id, self.local_peer_id, self.state)
    }
}
