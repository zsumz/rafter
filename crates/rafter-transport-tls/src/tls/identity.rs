//! Strict PEM loading and TLS 1.3 mutual-authentication configuration.

use std::{num::NonZeroUsize, path::Path, sync::Arc};

use rustls::{
    client::{ClientConfig, ClientConnection, Resumption},
    crypto::ring,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
    server::{NoServerSessionStorage, ServerConfig, ServerConnection, WebPkiClientVerifier},
    RootCertStore,
};

use super::pem::{build_root_store, parse_certificates, parse_private_key, read_identity_file};

use crate::{
    CertificateDirectory, CertificateFingerprint, LocalTlsIdentityError, PeerId, TlsConfigSide,
    TlsConnectionError, TlsIdentityError, TlsIdentityFile, TlsServerName, TLS_ALPN_PROTOCOL,
};

/// Loaded local certificate, key, trust roots, and hardened rustls configs.
///
/// Both sides use TLS 1.3 only, require certificates, advertise only
/// `rafter/1`, disable client resumption and early data, and issue no server
/// session tickets. The cryptographic provider is selected explicitly rather
/// than through rustls process-global state.
#[derive(Clone)]
pub struct TlsIdentity {
    client: Arc<ClientConfig>,
    server: Arc<ServerConfig>,
    leaf_fingerprint: CertificateFingerprint,
    certificate_chain_len: NonZeroUsize,
    trust_root_count: NonZeroUsize,
}

impl std::fmt::Debug for TlsIdentity {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TlsIdentity")
            .field("leaf_fingerprint", &self.leaf_fingerprint)
            .field("certificate_chain_len", &self.certificate_chain_len)
            .field("trust_root_count", &self.trust_root_count)
            .finish_non_exhaustive()
    }
}

impl TlsIdentity {
    /// Loads three PEM files and constructs one strict mutual-TLS identity.
    ///
    /// # Errors
    ///
    /// Returns [`TlsIdentityError`] for file I/O, malformed or missing PEM
    /// material, invalid trust roots, key/certificate mismatch, or unsupported
    /// TLS 1.3 provider configuration.
    pub fn from_pem_files(
        certificate_chain_path: impl AsRef<Path>,
        private_key_path: impl AsRef<Path>,
        trust_roots_path: impl AsRef<Path>,
    ) -> Result<Self, TlsIdentityError> {
        let certificate_chain = read_identity_file(
            TlsIdentityFile::CertificateChain,
            certificate_chain_path.as_ref(),
        )?;
        let private_key =
            read_identity_file(TlsIdentityFile::PrivateKey, private_key_path.as_ref())?;
        let trust_roots =
            read_identity_file(TlsIdentityFile::TrustRoots, trust_roots_path.as_ref())?;
        Self::from_pem(&certificate_chain, &private_key, &trust_roots)
    }

    /// Parses PEM bytes and constructs one strict mutual-TLS identity.
    ///
    /// # Errors
    ///
    /// Returns [`TlsIdentityError`] for malformed or missing PEM material,
    /// invalid trust roots, key/certificate mismatch, or unsupported TLS 1.3
    /// provider configuration.
    pub fn from_pem(
        certificate_chain_pem: &[u8],
        private_key_pem: &[u8],
        trust_roots_pem: &[u8],
    ) -> Result<Self, TlsIdentityError> {
        let certificate_chain =
            parse_certificates(TlsIdentityFile::CertificateChain, certificate_chain_pem)?;
        let certificate_chain_len = NonZeroUsize::new(certificate_chain.len())
            .ok_or(TlsIdentityError::EmptyCertificateChain)?;
        let leaf_fingerprint = CertificateFingerprint::from_der(certificate_chain[0].as_ref());
        let private_key = parse_private_key(private_key_pem)?;
        let trust_certificates = parse_certificates(TlsIdentityFile::TrustRoots, trust_roots_pem)?;
        let trust_root_count =
            NonZeroUsize::new(trust_certificates.len()).ok_or(TlsIdentityError::EmptyTrustRoots)?;
        let roots = build_root_store(trust_certificates)?;
        Self::build(
            certificate_chain,
            private_key,
            roots,
            leaf_fingerprint,
            certificate_chain_len,
            trust_root_count,
        )
    }

    /// SHA-256 fingerprint of the loaded local leaf certificate.
    #[must_use]
    pub const fn leaf_fingerprint(&self) -> CertificateFingerprint {
        self.leaf_fingerprint
    }

    /// Number of certificates sent as the local chain.
    #[must_use]
    pub const fn certificate_chain_len(&self) -> usize {
        self.certificate_chain_len.get()
    }

    /// Number of strict trust anchors loaded.
    #[must_use]
    pub const fn trust_root_count(&self) -> usize {
        self.trust_root_count.get()
    }

    /// Requires the local leaf fingerprint to map to `expected_peer`.
    ///
    /// # Errors
    ///
    /// Returns [`LocalTlsIdentityError`] when the leaf is unconfigured or maps
    /// to a different stable principal.
    pub fn validate_local_peer(
        &self,
        expected_peer: &PeerId,
        directory: &CertificateDirectory,
    ) -> Result<(), LocalTlsIdentityError> {
        let Some(actual) = directory.peer_for_fingerprint(&self.leaf_fingerprint) else {
            return Err(LocalTlsIdentityError::UnknownCertificate {
                fingerprint: self.leaf_fingerprint,
            });
        };
        if actual != expected_peer {
            return Err(LocalTlsIdentityError::PeerMismatch {
                expected: expected_peer.clone(),
                actual: actual.clone(),
            });
        }
        Ok(())
    }

    /// Creates an outbound rustls connection with mandatory server-name checks.
    ///
    /// # Errors
    ///
    /// Returns [`TlsConnectionError`] if rustls cannot represent the validated
    /// name or instantiate the connection.
    pub fn client_connection(
        &self,
        server_name: &TlsServerName,
    ) -> Result<ClientConnection, TlsConnectionError> {
        let name = ServerName::try_from(server_name.as_str().to_owned()).map_err(|_| {
            TlsConnectionError::InvalidServerName {
                name: server_name.clone(),
            }
        })?;
        ClientConnection::new(Arc::clone(&self.client), name).map_err(TlsConnectionError::Rustls)
    }

    /// Creates an inbound rustls connection requiring a trusted client chain.
    ///
    /// # Errors
    ///
    /// Returns [`TlsConnectionError`] if rustls cannot instantiate the
    /// connection.
    pub fn server_connection(&self) -> Result<ServerConnection, TlsConnectionError> {
        ServerConnection::new(Arc::clone(&self.server)).map_err(TlsConnectionError::Rustls)
    }

    fn build(
        certificate_chain: Vec<CertificateDer<'static>>,
        private_key: PrivateKeyDer<'static>,
        roots: RootCertStore,
        leaf_fingerprint: CertificateFingerprint,
        certificate_chain_len: NonZeroUsize,
        trust_root_count: NonZeroUsize,
    ) -> Result<Self, TlsIdentityError> {
        let provider = Arc::new(ring::default_provider());
        let mut client = ClientConfig::builder_with_provider(Arc::clone(&provider))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|source| TlsIdentityError::Tls13Configuration {
                side: TlsConfigSide::Client,
                source,
            })?
            .with_root_certificates(roots.clone())
            .with_client_auth_cert(certificate_chain.clone(), private_key.clone_key())
            .map_err(|source| TlsIdentityError::ClientIdentity { source })?;
        client.alpn_protocols = vec![TLS_ALPN_PROTOCOL.to_vec()];
        client.resumption = Resumption::disabled();
        client.enable_early_data = false;

        let verifier =
            WebPkiClientVerifier::builder_with_provider(Arc::new(roots), Arc::clone(&provider))
                .build()
                .map_err(|source| TlsIdentityError::ClientVerifier { source })?;
        let mut server = ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|source| TlsIdentityError::Tls13Configuration {
                side: TlsConfigSide::Server,
                source,
            })?
            .with_client_cert_verifier(verifier)
            .with_single_cert(certificate_chain, private_key)
            .map_err(|source| TlsIdentityError::ServerIdentity { source })?;
        server.alpn_protocols = vec![TLS_ALPN_PROTOCOL.to_vec()];
        server.session_storage = Arc::new(NoServerSessionStorage {});
        server.send_tls13_tickets = 0;
        server.max_tls13_tickets = 0;
        server.max_early_data_size = 0;
        server.send_half_rtt_data = false;

        Ok(Self {
            client: Arc::new(client),
            server: Arc::new(server),
            leaf_fingerprint,
            certificate_chain_len,
            trust_root_count,
        })
    }
}
