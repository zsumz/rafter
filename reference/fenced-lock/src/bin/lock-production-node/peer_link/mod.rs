//! Thin fenced-lock adapter over the public `rafter-transport-tls` runtime.
//!
//! The fixture owns only deployment composition: stable `PeerId` naming, one
//! canonical group codec, filesystem-to-`EndpointBook` discovery, and the
//! location of durable transport state. TLS, framing, sessions, bounded queues,
//! persistent streams, authorization, diagnostics, and shutdown belong to the
//! public crate.

mod api;
mod config;
mod diagnostics;
mod endpoints;
mod group_codec;
mod lifecycle;
mod limits;

use std::{
    net::SocketAddr,
    path::Path,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use rafter::NodeId;
use rafter_reference_fenced_lock::production::{
    transport_cluster_id, transport_peer_id, transport_session_path, ReopeningTransportSessionStore,
};
use rafter_transport_tls::{
    EndpointBook, PeerId, TlsInbound, TlsPeerDirectory, TlsPeerTransport, TlsSender,
    TransportConfig,
};

pub use config::{PeerTlsConfig, PeerTlsPaths};
pub use diagnostics::LinkDiagnostics;
pub use group_codec::LockGroupCodec;
pub use limits::{
    GLOBAL_INBOUND_QUEUE_LEN, MAX_PEER_CONNECTIONS, PEER_INBOUND_QUEUE_LEN, PEER_SEND_QUEUE_LEN,
};

use endpoints::{install_placeholders, remote_peers};
use lifecycle::EndpointLifecycle;
use limits::{transport_limits, transport_timeouts, MAX_FRAME_BYTES};

use super::replica::{LockGroupId, GROUP_ID};

/// Stable certificate-authenticated principal used by the managed driver.
pub type PeerPrincipal = PeerId;
/// Public per-group principal/node directory used as the inbound validator.
pub type PeerDirectory = TlsPeerDirectory<LockGroupId>;
/// Public nonblocking `RaftTransport` handle used by the managed driver.
pub type TcpPeerTransport = TlsSender<LockGroupId, LockGroupCodec>;

type RunningTransport = TlsPeerTransport<LockGroupId, LockGroupCodec>;

/// Returns the exact accepted complete peer-frame bound.
#[must_use]
pub const fn max_frame_bytes() -> usize {
    MAX_FRAME_BYTES
}

/// Public TLS runtime plus the fixture-owned address publication adapter.
pub struct PeerLink {
    local_addr: SocketAddr,
    inbound: TlsInbound<LockGroupId>,
    transport: TcpPeerTransport,
    directory: PeerDirectory,
    runtime: Mutex<Option<RunningTransport>>,
    endpoints: EndpointLifecycle,
    failure: Arc<Mutex<Option<String>>>,
    remote_peers: usize,
}

impl std::fmt::Debug for PeerLink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PeerLink")
            .field("local_addr", &self.local_addr)
            .field("remote_peers", &self.remote_peers)
            .field("terminal_failure", &self.terminal_failure())
            .finish_non_exhaustive()
    }
}

impl PeerLink {
    /// Binds the public authenticated transport with every worker paused.
    ///
    /// # Errors
    ///
    /// Returns a reason when identity, durable state, bounds, listener setup,
    /// or worker construction fails. No worker performs network or session I/O
    /// before [`PeerLink::start`] is called after replica recovery.
    pub fn bind(
        bind_addr: &str,
        node_id: NodeId,
        cluster_dir: &Path,
        node_dir: &Path,
        tls: &PeerTlsConfig,
    ) -> Result<Self, String> {
        let bind_addr = bind_addr
            .parse()
            .map_err(|error| format!("peer listen address is invalid: {error}"))?;
        let local_peer = transport_peer_id(node_id);
        if tls.peer_id(node_id) != Some(&local_peer) {
            return Err(format!("TLS principal map omits local node {}", node_id.0));
        }

        let peer_map = tls.peer_map();
        let remote_map = remote_peers(node_id, &peer_map);
        let limits = transport_limits()?;
        let directory = PeerDirectory::new(limits.directory());
        for (candidate, peer) in &peer_map {
            directory
                .bind(GROUP_ID, *candidate, peer.clone())
                .map_err(|error| error.to_string())?;
        }

        let endpoint_book = EndpointBook::new(limits.endpoints());
        install_placeholders(&endpoint_book, &remote_map, &tls.server_name())?;
        let cluster_id = transport_cluster_id(GROUP_ID.0);
        let sessions = ReopeningTransportSessionStore::new(
            transport_session_path(node_dir),
            cluster_id.clone(),
            local_peer.clone(),
        );
        let config = TransportConfig::new(
            cluster_id,
            local_peer,
            bind_addr,
            limits,
            transport_timeouts()?,
        );
        let runtime = TlsPeerTransport::builder(config, LockGroupCodec)
            .identity(tls.identity())
            .certificates(tls.certificates())
            .directory(directory.clone())
            .endpoints(endpoint_book.clone())
            .session_store(sessions)
            .bind_paused()
            .map_err(|error| error.to_string())?;
        let local_addr = runtime.local_addr();
        let inbound = runtime.inbound();
        let transport = runtime.sender();
        let failure = Arc::new(Mutex::new(None));
        let endpoints = EndpointLifecycle::new(
            cluster_dir,
            remote_map.clone(),
            endpoint_book,
            tls.server_name(),
        );

        Ok(Self {
            local_addr,
            inbound,
            transport,
            directory,
            runtime: Mutex::new(Some(runtime)),
            endpoints,
            failure,
            remote_peers: remote_map.len(),
        })
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
