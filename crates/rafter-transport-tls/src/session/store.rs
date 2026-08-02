//! Durable session-store contract.

use std::error::Error;

use crate::{PeerId, SessionStoreLimits};

use super::{ConnectionSession, InboundSessionDecision, PeerSessionState};

/// Durable allocator and replay high-water store for transport sessions.
///
/// Implementations must publish an outbound allocation before returning it and
/// publish a newly accepted inbound session before returning an accepted
/// decision. A stale inbound session must not mutate durable state.
pub trait TransportSessionStore: Send + Sync + 'static {
    /// Typed store failure.
    type Error: Error + Send + Sync + 'static;

    /// Finite peer-record bound enforced by this store.
    fn limits(&self) -> SessionStoreLimits;

    /// Verifies that `peer` can participate without mutating durable state.
    ///
    /// This must check both readability and capacity for an absent peer.
    ///
    /// # Errors
    ///
    /// Returns the implementation error when a first session for `peer` could
    /// not be recorded or the store is no longer a valid recovery oracle.
    fn preflight_peer(&self, peer: &PeerId) -> Result<(), Self::Error>;

    /// Verifies aggregate capacity for every peer a runtime will configure.
    ///
    /// # Errors
    ///
    /// Returns the implementation error when the complete distinct set could
    /// not be retained without mutation.
    fn preflight_peers(&self, peers: &[PeerId]) -> Result<(), Self::Error>;

    /// Durably allocates the next outbound connection session for `peer`.
    ///
    /// # Errors
    ///
    /// Returns the implementation error when the session cannot be allocated
    /// and durably published.
    fn allocate_outbound_session(&self, peer: &PeerId) -> Result<ConnectionSession, Self::Error>;

    /// Durably accepts `session` only when it is newer than the peer high-water.
    ///
    /// # Errors
    ///
    /// Returns the implementation error when a new accepted high-water cannot
    /// be durably published. Stale sessions are ordinary decisions, not errors.
    fn accept_inbound_session(
        &self,
        peer: &PeerId,
        session: ConnectionSession,
    ) -> Result<InboundSessionDecision, Self::Error>;

    /// Reads one peer's current durable high-water marks.
    ///
    /// # Errors
    ///
    /// Returns the implementation error when this handle is no longer a valid
    /// recovery oracle.
    fn peer_session_state(&self, peer: &PeerId) -> Result<PeerSessionState, Self::Error>;
}
