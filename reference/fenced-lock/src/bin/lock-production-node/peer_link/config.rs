//! Strict TLS identity and explicit certificate-directory loading.

use std::{collections::BTreeMap, path::PathBuf};

use rafter::NodeId;
use rafter_reference_fenced_lock::production::transport_peer_id;
use rafter_transport_tls::{CertificateDirectory, PeerId, TlsIdentity, TlsServerName};

/// Paths and certificate-to-node mapping for one mutual-TLS endpoint.
#[derive(Clone, Debug)]
pub struct PeerTlsPaths {
    /// Trust roots accepted for both TLS directions.
    pub ca: PathBuf,
    /// Local certificate chain.
    pub certificate: PathBuf,
    /// Local private key.
    pub private_key: PathBuf,
    /// Explicit leaf certificate assigned to each provisioned replica.
    pub peer_certificates: BTreeMap<NodeId, PathBuf>,
}

/// Loaded public transport identity and explicit certificate directory.
#[derive(Clone, Debug)]
pub struct PeerTlsConfig {
    identity: TlsIdentity,
    certificates: CertificateDirectory,
    peer_by_node: BTreeMap<NodeId, PeerId>,
    server_name: TlsServerName,
}

impl PeerTlsConfig {
    /// Loads strict TLS 1.3 mutual authentication and exact leaf mappings.
    ///
    /// # Errors
    ///
    /// Returns a reason when PEM material, certificate mappings, or the local
    /// principal are inconsistent.
    pub fn load(local_node: NodeId, paths: &PeerTlsPaths) -> Result<Self, String> {
        let identity =
            TlsIdentity::from_pem_files(&paths.certificate, &paths.private_key, &paths.ca)
                .map_err(|error| error.to_string())?;
        let mut builder = CertificateDirectory::builder();
        let mut peer_by_node = BTreeMap::new();
        for (node_id, path) in &paths.peer_certificates {
            let peer_id = transport_peer_id(*node_id);
            builder = builder
                .map_pem_certificate_file(path, peer_id.clone())
                .map_err(|error| error.to_string())?;
            peer_by_node.insert(*node_id, peer_id);
        }
        let certificates = builder.build();
        let local_peer = transport_peer_id(local_node);
        identity
            .validate_local_peer(&local_peer, &certificates)
            .map_err(|error| error.to_string())?;
        let server_name = TlsServerName::new("rafter-peer").map_err(|error| error.to_string())?;
        Ok(Self {
            identity,
            certificates,
            peer_by_node,
            server_name,
        })
    }

    pub(super) fn identity(&self) -> TlsIdentity {
        self.identity.clone()
    }

    pub(super) fn certificates(&self) -> CertificateDirectory {
        self.certificates.clone()
    }

    pub(super) fn server_name(&self) -> TlsServerName {
        self.server_name.clone()
    }

    pub(super) fn peer_map(&self) -> BTreeMap<NodeId, PeerId> {
        self.peer_by_node.clone()
    }

    pub(super) fn peer_id(&self, node_id: NodeId) -> Option<&PeerId> {
        self.peer_by_node.get(&node_id)
    }
}
