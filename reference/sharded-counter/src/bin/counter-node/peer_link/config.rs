//! Strict TLS identity and explicit certificate-to-principal mapping.

use std::{collections::BTreeMap, path::PathBuf};

use rafter::NodeId;
use rafter_transport_tls::{
    CertificateDirectory, CertificateDirectoryLimits, PeerId, TlsIdentity, TlsServerName,
};

use super::transport_peer_id;

/// Caller-owned paths for one mutually authenticated peer endpoint.
#[derive(Clone, Debug)]
pub struct PeerTlsPaths {
    pub ca: PathBuf,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub peer_certificates: BTreeMap<NodeId, PathBuf>,
}

/// Loaded identity and immutable explicit leaf-certificate directory.
#[derive(Clone, Debug)]
pub(super) struct PeerTlsConfig {
    identity: TlsIdentity,
    certificates: CertificateDirectory,
    peer_by_node: BTreeMap<NodeId, PeerId>,
    server_name: TlsServerName,
}

impl PeerTlsConfig {
    pub(super) fn load(
        local_node: NodeId,
        paths: &PeerTlsPaths,
        limits: CertificateDirectoryLimits,
    ) -> Result<Self, String> {
        let identity =
            TlsIdentity::from_pem_files(&paths.certificate, &paths.private_key, &paths.ca)
                .map_err(|error| error.to_string())?;
        let mut builder = CertificateDirectory::builder_with_limits(limits);
        let mut peer_by_node = BTreeMap::new();
        for (node_id, certificate) in &paths.peer_certificates {
            let peer_id = transport_peer_id(*node_id);
            builder = builder
                .map_pem_certificate_file(certificate, peer_id.clone())
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

    pub(super) fn peer_map(&self) -> BTreeMap<NodeId, PeerId> {
        self.peer_by_node.clone()
    }

    pub(super) fn server_name(&self) -> TlsServerName {
        self.server_name.clone()
    }
}
