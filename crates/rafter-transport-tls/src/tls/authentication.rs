//! Completed-rustls-connection to stable-principal authentication.

use rustls::{client::ClientConnection, pki_types::CertificateDer, server::ServerConnection};

use crate::{
    CertificateDirectory, CertificateFingerprint, PeerId, TlsPeerAuthenticationError,
    TLS_ALPN_PROTOCOL,
};

/// Stable principal proved by a completed TLS connection and explicit leaf map.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticatedTlsPeer {
    peer_id: PeerId,
    leaf_fingerprint: CertificateFingerprint,
}

impl AuthenticatedTlsPeer {
    /// Creates authenticated peer evidence from an already-verified mapping.
    ///
    /// This constructor is intentionally crate-private: public callers obtain
    /// evidence only through completed rustls connections.
    pub(crate) fn new(peer_id: PeerId, leaf_fingerprint: CertificateFingerprint) -> Self {
        Self {
            peer_id,
            leaf_fingerprint,
        }
    }

    /// Stable physical transport principal.
    #[must_use]
    pub const fn peer_id(&self) -> &PeerId {
        &self.peer_id
    }

    /// SHA-256 fingerprint of the authenticated leaf certificate.
    #[must_use]
    pub const fn leaf_fingerprint(&self) -> CertificateFingerprint {
        self.leaf_fingerprint
    }
}

/// Authenticates the server certificate on one completed client connection.
///
/// # Errors
///
/// Returns [`TlsPeerAuthenticationError`] before handshake completion, when
/// `rafter/1` was not negotiated, when no peer leaf is available, or when the
/// validated leaf is absent from the explicit certificate directory.
pub fn authenticate_client_connection(
    connection: &ClientConnection,
    directory: &CertificateDirectory,
) -> Result<AuthenticatedTlsPeer, TlsPeerAuthenticationError> {
    authenticate_connection(
        connection.is_handshaking(),
        connection.alpn_protocol(),
        connection.peer_certificates(),
        directory,
    )
}

/// Authenticates the client certificate on one completed server connection.
///
/// # Errors
///
/// Returns [`TlsPeerAuthenticationError`] before handshake completion, when
/// `rafter/1` was not negotiated, when no peer leaf is available, or when the
/// validated leaf is absent from the explicit certificate directory.
pub fn authenticate_server_connection(
    connection: &ServerConnection,
    directory: &CertificateDirectory,
) -> Result<AuthenticatedTlsPeer, TlsPeerAuthenticationError> {
    authenticate_connection(
        connection.is_handshaking(),
        connection.alpn_protocol(),
        connection.peer_certificates(),
        directory,
    )
}

fn authenticate_connection(
    is_handshaking: bool,
    alpn_protocol: Option<&[u8]>,
    certificates: Option<&[CertificateDer<'static>]>,
    directory: &CertificateDirectory,
) -> Result<AuthenticatedTlsPeer, TlsPeerAuthenticationError> {
    if is_handshaking {
        return Err(TlsPeerAuthenticationError::HandshakeIncomplete);
    }
    match alpn_protocol {
        Some(selected) if selected == TLS_ALPN_PROTOCOL => {}
        Some(selected) => {
            return Err(TlsPeerAuthenticationError::UnexpectedAlpn {
                selected: selected.to_vec(),
            });
        }
        None => return Err(TlsPeerAuthenticationError::MissingAlpn),
    }
    let leaf = certificates
        .and_then(|chain| chain.first())
        .ok_or(TlsPeerAuthenticationError::MissingPeerCertificate)?;
    let fingerprint = CertificateFingerprint::from_der(leaf.as_ref());
    let peer_id = directory
        .peer_for_fingerprint(&fingerprint)
        .cloned()
        .ok_or(TlsPeerAuthenticationError::UnknownCertificate { fingerprint })?;
    Ok(AuthenticatedTlsPeer::new(peer_id, fingerprint))
}
