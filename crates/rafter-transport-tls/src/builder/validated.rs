//! Complete dependency validation before runtime state is assembled.

use std::{net::TcpListener, sync::Arc};

use crate::runtime::SessionStoreHandle;
use crate::snapshot::SnapshotResolverHandle;
use crate::{
    CertificateDirectory, EndpointBook, GroupIdCodec, PeerFrameCodec, PeerId, TlsHandshakeConfig,
    TlsIdentity, TlsPeerDirectory, TlsPeerTransportBuilder, TlsTransportBuildError,
    TransportConfig,
};

pub(super) struct ValidatedBuilder<G, C> {
    pub(super) config: TransportConfig,
    pub(super) identity: TlsIdentity,
    pub(super) certificates: CertificateDirectory,
    pub(super) directory: TlsPeerDirectory<G>,
    pub(super) endpoints: EndpointBook,
    pub(super) sessions: SessionStoreHandle,
    pub(super) snapshot_resolver: Option<SnapshotResolverHandle<G>>,
    pub(super) codec: Arc<PeerFrameCodec<G, C>>,
    pub(super) handshake: TlsHandshakeConfig,
    pub(super) listener: TcpListener,
    pub(super) peers: Vec<PeerId>,
}

impl<G, C> ValidatedBuilder<G, C>
where
    G: Ord,
    C: GroupIdCodec<G>,
{
    pub(super) fn new(
        builder: TlsPeerTransportBuilder<G, C>,
    ) -> Result<Self, TlsTransportBuildError> {
        let TlsPeerTransportBuilder {
            config,
            group_codec,
            identity,
            certificates,
            directory,
            endpoints,
            sessions,
            snapshot_resolver,
        } = builder;
        let identity = identity.ok_or(TlsTransportBuildError::MissingIdentity)?;
        let certificates = certificates.ok_or(TlsTransportBuildError::MissingCertificates)?;
        let directory = directory.ok_or(TlsTransportBuildError::MissingDirectory)?;
        let endpoints = endpoints.ok_or(TlsTransportBuildError::MissingEndpoints)?;
        let sessions = sessions.ok_or(TlsTransportBuildError::MissingSessionStore)?;

        validate_dependency_limits(&config, &certificates, &directory, &endpoints, &sessions)?;
        validate_receive_memory(&config)?;

        identity
            .validate_local_peer(config.local_peer_id(), &certificates)
            .map_err(|source| TlsTransportBuildError::LocalIdentity { source })?;
        let peers = endpoints
            .peer_ids()
            .map_err(|source| TlsTransportBuildError::EndpointBook { source })?;
        validate_remote_peers(&config, &certificates, &peers)?;
        let session_peers: Vec<_> = certificates
            .peer_ids()
            .filter(|peer| *peer != config.local_peer_id())
            .cloned()
            .collect();
        sessions
            .preflight_peers(&session_peers)
            .map_err(|source| TlsTransportBuildError::SessionStore { source })?;

        let codec = Arc::new(
            PeerFrameCodec::new(group_codec, config.limits().wire())
                .map_err(|source| TlsTransportBuildError::FrameCodec { source })?,
        );
        let handshake = TlsHandshakeConfig::current(
            config.cluster_id().clone(),
            config.local_peer_id().clone(),
            config.limits().wire(),
        )
        .map_err(|source| TlsTransportBuildError::HandshakeConfig { source })?;
        let listener = bind_listener(&config)?;

        Ok(Self {
            config,
            identity,
            certificates,
            directory,
            endpoints,
            sessions,
            snapshot_resolver,
            codec,
            handshake,
            listener,
            peers,
        })
    }
}

fn validate_receive_memory(config: &TransportConfig) -> Result<(), TlsTransportBuildError> {
    let limits = config.limits();
    let memory = limits.runtime().receive_memory();
    let required = limits
        .wire()
        .max_frame_bytes()
        .saturating_mul(memory.decode_amplification());
    let maximum = memory.bytes_global();
    if required > maximum {
        return Err(TlsTransportBuildError::ReceiveMemoryTooSmall { required, maximum });
    }
    Ok(())
}

fn validate_dependency_limits<G: Ord>(
    config: &TransportConfig,
    certificates: &CertificateDirectory,
    directory: &TlsPeerDirectory<G>,
    endpoints: &EndpointBook,
    sessions: &SessionStoreHandle,
) -> Result<(), TlsTransportBuildError> {
    let limits = config.limits();
    for (component, matches) in [
        (
            "certificate directory",
            certificates.limits() == limits.certificates(),
        ),
        ("peer directory", directory.limits() == limits.directory()),
        ("endpoint book", endpoints.limits() == limits.endpoints()),
        ("session store", sessions.limits() == limits.sessions()),
    ] {
        if !matches {
            return Err(TlsTransportBuildError::DependencyLimitsMismatch { component });
        }
    }
    Ok(())
}

fn validate_remote_peers(
    config: &TransportConfig,
    certificates: &CertificateDirectory,
    peers: &[PeerId],
) -> Result<(), TlsTransportBuildError> {
    for peer in peers {
        if peer == config.local_peer_id() {
            return Err(TlsTransportBuildError::LocalPeerEndpoint { peer: peer.clone() });
        }
        if !certificates.contains_peer(peer) {
            return Err(TlsTransportBuildError::UnconfiguredCertificate { peer: peer.clone() });
        }
    }
    Ok(())
}

fn bind_listener(config: &TransportConfig) -> Result<TcpListener, TlsTransportBuildError> {
    let listener =
        TcpListener::bind(config.bind_addr()).map_err(|source| TlsTransportBuildError::Bind {
            address: config.bind_addr(),
            source,
        })?;
    listener
        .set_nonblocking(true)
        .map_err(|source| TlsTransportBuildError::ConfigureListener { source })?;
    Ok(listener)
}
