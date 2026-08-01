//! Durable outbound-session allocation and authenticated server-hello checks.

use crate::{
    AuthenticatedTlsPeer, ClientHello, PeerId, ServerHello, ServerHelloStatus,
    TransportSessionStore,
};

use super::{
    NegotiatedTlsHandshake, TlsClientHandshakeError, TlsHandshakeConfig, TlsHandshakeStoreError,
    MIN_PEER_FRAME_BYTES,
};

impl TlsHandshakeConfig {
    /// Durably allocates a connection session and builds the client hello.
    ///
    /// The returned session has already been published by the store. Callers
    /// may lose it by failing before write; they must never reuse it.
    ///
    /// # Errors
    ///
    /// Returns [`TlsHandshakeStoreError`] when the next outbound session cannot
    /// be durably allocated.
    pub fn begin_client_hello<S>(
        &self,
        remote_peer: &PeerId,
        sessions: &S,
    ) -> Result<ClientHello, TlsHandshakeStoreError<S::Error>>
    where
        S: TransportSessionStore,
    {
        let session = sessions
            .allocate_outbound_session(remote_peer)
            .map_err(TlsHandshakeStoreError::new)?;
        Ok(ClientHello::new(
            self.transport_versions(),
            self.peer_codec_versions(),
            self.cluster_id().clone(),
            self.local_peer_id().clone(),
            session,
            self.max_frame_bytes(),
        ))
    }

    /// Validates an authenticated server hello against the dial target.
    ///
    /// # Errors
    ///
    /// Returns [`TlsClientHandshakeError`] for certificate/target mismatch,
    /// claimed server mismatch, cross-cluster response, typed refusal,
    /// unoffered version, or an invalid negotiated frame bound.
    pub fn validate_server_hello(
        &self,
        expected_peer: &PeerId,
        authenticated: &AuthenticatedTlsPeer,
        hello: &ServerHello,
    ) -> Result<NegotiatedTlsHandshake, TlsClientHandshakeError> {
        if authenticated.peer_id() != expected_peer {
            return Err(TlsClientHandshakeError::AuthenticatedPeerMismatch {
                expected: expected_peer.clone(),
                actual: authenticated.peer_id().clone(),
            });
        }
        if hello.server_peer_id() != authenticated.peer_id() {
            return Err(TlsClientHandshakeError::ServerIdentityMismatch {
                authenticated: authenticated.peer_id().clone(),
                claimed: hello.server_peer_id().clone(),
            });
        }
        if hello.cluster_id() != self.cluster_id() {
            return Err(TlsClientHandshakeError::ClusterMismatch {
                expected: self.cluster_id().clone(),
                actual: hello.cluster_id().clone(),
            });
        }
        if let ServerHelloStatus::Refused(reason) = hello.status() {
            return Err(TlsClientHandshakeError::Refused { reason });
        }

        let Some(transport_version) = hello.selected_transport_version() else {
            return Err(TlsClientHandshakeError::NonCanonicalAccepted);
        };
        if !self.transport_versions().contains(transport_version.get()) {
            return Err(TlsClientHandshakeError::TransportVersionNotOffered {
                selected: transport_version.get(),
            });
        }
        let Some(peer_codec_version) = hello.selected_peer_codec_version() else {
            return Err(TlsClientHandshakeError::NonCanonicalAccepted);
        };
        if !self
            .peer_codec_versions()
            .contains(peer_codec_version.get())
        {
            return Err(TlsClientHandshakeError::PeerCodecVersionNotOffered {
                selected: peer_codec_version.get(),
            });
        }
        let Some(frame_bytes) = hello.accepted_frame_bytes() else {
            return Err(TlsClientHandshakeError::NonCanonicalAccepted);
        };
        if frame_bytes.get() < MIN_PEER_FRAME_BYTES || frame_bytes > self.max_frame_bytes() {
            return Err(TlsClientHandshakeError::FrameLimitInvalid {
                accepted: frame_bytes.get(),
                minimum: MIN_PEER_FRAME_BYTES,
                maximum: self.max_frame_bytes().get(),
            });
        }

        Ok(NegotiatedTlsHandshake::new(
            authenticated.peer_id().clone(),
            transport_version,
            peer_codec_version,
            frame_bytes,
        ))
    }
}
