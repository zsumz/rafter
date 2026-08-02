//! Authenticated client-hello validation and durable inbound-session admission.

use std::num::NonZeroU16;

use crate::{
    highest_common_version, AuthenticatedTlsPeer, ClientHello, InboundSessionDecision, ServerHello,
    ServerRefusal, TransportSessionStore,
};

use super::{TlsHandshakeConfig, TlsHandshakeStoreError, MIN_PEER_FRAME_BYTES};

impl TlsHandshakeConfig {
    /// Validates one authenticated client hello and durably admits its session.
    ///
    /// Every ordinary incompatibility is returned as a canonical typed refusal.
    /// The session store is called only after identity, cluster, version, and
    /// frame checks succeed, and it must durably publish a newly accepted
    /// inbound high-water before this method returns an accepted hello.
    ///
    /// # Errors
    ///
    /// Returns [`TlsHandshakeStoreError`] only when durable session admission
    /// fails. A stale session is a normal [`ServerRefusal::StaleSession`].
    pub fn accept_client_hello<S>(
        &self,
        authenticated: &AuthenticatedTlsPeer,
        hello: &ClientHello,
        sessions: &S,
    ) -> Result<ServerHello, TlsHandshakeStoreError<S::Error>>
    where
        S: TransportSessionStore,
    {
        if hello.claimed_peer_id() != authenticated.peer_id() {
            return Ok(self.refusal(ServerRefusal::IdentityMismatch));
        }
        if hello.cluster_id() != self.cluster_id() {
            return Ok(self.refusal(ServerRefusal::ClusterMismatch));
        }
        let Some(transport_version) =
            highest_common_version(self.transport_versions(), hello.transport_versions())
                .and_then(NonZeroU16::new)
        else {
            return Ok(self.refusal(ServerRefusal::TransportVersionMismatch));
        };
        let Some(peer_codec_version) =
            highest_common_version(self.peer_codec_versions(), hello.peer_codec_versions())
                .and_then(NonZeroU16::new)
        else {
            return Ok(self.refusal(ServerRefusal::PeerCodecVersionMismatch));
        };
        if hello.max_send_frame_bytes().get() < MIN_PEER_FRAME_BYTES
            || hello.max_send_frame_bytes() > self.max_frame_bytes()
        {
            return Ok(self.refusal(ServerRefusal::FrameLimitRejected));
        }
        let accepted_frame_bytes = hello.max_send_frame_bytes();

        let decision = sessions
            .accept_inbound_session(authenticated.peer_id(), hello.connection_session())
            .map_err(TlsHandshakeStoreError::new)?;
        if matches!(decision, InboundSessionDecision::Stale { .. }) {
            return Ok(self.refusal(ServerRefusal::StaleSession));
        }

        Ok(ServerHello::accepted(
            transport_version,
            peer_codec_version,
            self.cluster_id().clone(),
            self.local_peer_id().clone(),
            accepted_frame_bytes,
        ))
    }
}
