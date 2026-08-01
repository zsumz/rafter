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

        identity
            .validate_local_peer(config.local_peer_id(), &certificates)
            .map_err(|source| TlsTransportBuildError::LocalIdentity { source })?;
        sessions
            .preflight(config.local_peer_id())
            .map_err(|source| TlsTransportBuildError::SessionStore {
                peer: config.local_peer_id().clone(),
                source,
            })?;
        let peers = endpoints
            .peer_ids()
            .map_err(|source| TlsTransportBuildError::EndpointBook { source })?;
        validate_remote_peers(&config, &certificates, &sessions, &peers)?;

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

fn validate_remote_peers(
    config: &TransportConfig,
    certificates: &CertificateDirectory,
    sessions: &SessionStoreHandle,
    peers: &[PeerId],
) -> Result<(), TlsTransportBuildError> {
    for peer in peers {
        if peer == config.local_peer_id() {
            return Err(TlsTransportBuildError::LocalPeerEndpoint { peer: peer.clone() });
        }
        if !certificates.contains_peer(peer) {
            return Err(TlsTransportBuildError::UnconfiguredCertificate { peer: peer.clone() });
        }
        sessions
            .preflight(peer)
            .map_err(|source| TlsTransportBuildError::SessionStore {
                peer: peer.clone(),
                source,
            })?;
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
