//! Thin sharded-counter adapter over the public authenticated TLS transport.
//!
//! This module owns only deployment composition: stable host principals, the
//! counter's `(group, incarnation)` route codec, filesystem endpoint discovery,
//! and the durable session-state location. TLS, framing, replay protection,
//! queueing, authorization, diagnostics, and worker lifecycle remain in
//! `rafter-transport-tls`.

mod config;
mod endpoints;
mod group_codec;
mod limits;
mod session;

use std::{
    error::Error,
    fmt,
    net::SocketAddr,
    path::Path,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use rafter::{Message, NodeId};
use rafter_reference_sharded_counter::{GroupId, GroupIncarnation};
use rafter_service::{PeerEnvelope, PeerPolicy, RaftTransport};
use rafter_transport_tls::{
    EndpointBook, TlsInbound, TlsPeerDirectory, TlsPeerTransport, TlsSender, TlsTransportError,
    TransportConfig, TransportDiagnostics,
};

use config::PeerTlsConfig;
pub use config::PeerTlsPaths;
use endpoints::{install_placeholders, remote_peers, EndpointLifecycle};
use group_codec::{PeerGroupCodec, PeerGroupId};
use limits::{transport_limits, transport_timeouts, GLOBAL_INBOUND_QUEUE_LEN};
use session::{open_transport_state, transport_cluster_id, transport_peer_id};

type PeerDirectory = TlsPeerDirectory<PeerGroupId>;
type Sender = TlsSender<PeerGroupId, PeerGroupCodec>;
type Runtime = TlsPeerTransport<PeerGroupId, PeerGroupCodec>;

/// One peer frame in the counter consumer's lifecycle vocabulary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerFrame {
    pub group_id: GroupId,
    pub incarnation: GroupIncarnation,
    pub from: NodeId,
    pub to: NodeId,
    pub message: Message,
}

/// Typed synchronous admission refusal from the public transport.
#[derive(Debug)]
pub struct LinkError {
    source: TlsTransportError,
}

impl fmt::Display for LinkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "authenticated peer transport refused the frame: {}",
            self.source
        )
    }
}

impl Error for LinkError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

/// Stable process-facing projection of the public diagnostic vocabulary.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LinkCounters {
    pub outbound_full: u64,
    pub inbound_full: u64,
    pub malformed: u64,
    pub identity_refused: u64,
    pub inbound_connection_full: u64,
    pub tls_handshakes: u64,
    pub tls_failures: u64,
    pub stale_sessions: u64,
    pub active_outbound_connections: usize,
    pub active_inbound_connections: usize,
}

/// Bounded multi-group TLS link owned by one counter host process.
pub struct PeerLink {
    local_node: NodeId,
    members: Vec<NodeId>,
    local_addr: SocketAddr,
    inbound: TlsInbound<PeerGroupId>,
    sender: Sender,
    directory: PeerDirectory,
    runtime: Mutex<Option<Runtime>>,
    endpoints: EndpointLifecycle,
    failure: Arc<Mutex<Option<String>>>,
}

impl fmt::Debug for PeerLink {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PeerLink")
            .field("local_node", &self.local_node)
            .field("local_addr", &self.local_addr)
            .field("members", &self.members)
            .field("terminal_failure", &self.terminal_failure())
            .finish_non_exhaustive()
    }
}

impl PeerLink {
    /// Binds finite transport resources in a paused state.
    ///
    /// # Errors
    ///
    /// Returns a reason when identity, durable state, bounds, listener setup,
    /// or worker construction fails. No worker performs network or session I/O
    /// before [`Self::start`] follows successful group recovery.
    pub fn bind(
        cluster_dir: &Path,
        host_dir: &Path,
        node_id: NodeId,
        members: &[NodeId],
        tls_paths: &PeerTlsPaths,
    ) -> Result<Self, String> {
        let limits = transport_limits()?;
        let tls = PeerTlsConfig::load(node_id, tls_paths, limits.certificates())?;
        let local_peer = transport_peer_id(node_id);
        let peer_map = tls.peer_map();
        let remote_map = remote_peers(node_id, &peer_map);
        let endpoints = EndpointBook::new(limits.endpoints());
        install_placeholders(&endpoints, &remote_map, &tls.server_name())?;
        let directory = PeerDirectory::new(limits.directory());
        let cluster_id = transport_cluster_id();
        let sessions = open_transport_state(host_dir, &cluster_id, &local_peer, limits.sessions())?;
        let config = TransportConfig::new(
            cluster_id,
            local_peer,
            "127.0.0.1:0"
                .parse()
                .expect("the fixed loopback bind address is valid"),
            limits,
            transport_timeouts()?,
        );
        let runtime = TlsPeerTransport::builder(config, PeerGroupCodec)
            .identity(tls.identity())
            .certificates(tls.certificates())
            .directory(directory.clone())
            .endpoints(endpoints.clone())
            .session_store(sessions)
            .bind_paused()
            .map_err(|error| error.to_string())?;
        let local_addr = runtime.local_addr();
        let inbound = runtime.inbound();
        let sender = runtime.sender();

        Ok(Self {
            local_node: node_id,
            members: members.to_vec(),
            local_addr,
            inbound,
            sender,
            directory,
            runtime: Mutex::new(Some(runtime)),
            endpoints: EndpointLifecycle::new(
                cluster_dir,
                remote_map,
                endpoints,
                tls.server_name(),
            ),
            failure: Arc::new(Mutex::new(None)),
        })
    }

    /// Installs one live incarnation's complete route and peer policy.
    ///
    /// # Errors
    ///
    /// Returns a reason when a binding conflicts, a finite directory bound is
    /// exhausted, or the complete policy cannot be installed atomically.
    pub fn configure_group(
        &self,
        group_id: GroupId,
        incarnation: GroupIncarnation,
    ) -> Result<(), String> {
        let route = PeerGroupId::new(group_id, incarnation);
        for member in &self.members {
            self.directory
                .bind(route, *member, transport_peer_id(*member))
                .map_err(|error| error.to_string())?;
        }
        let peers = self
            .members
            .iter()
            .copied()
            .filter(|member| *member != self.local_node)
            .map(transport_peer_id)
            .collect::<Vec<_>>();
        let retirement_floor = self.members.iter().copied().max();
        self.sender
            .update_peers(&route, PeerPolicy::new(peers, retirement_floor))
            .map_err(|error| error.to_string())
    }

    /// Removes one retired incarnation from transport routing and admission.
    ///
    /// # Errors
    ///
    /// Returns a reason when the shared directory is poisoned.
    pub fn remove_group(
        &self,
        group_id: GroupId,
        incarnation: GroupIncarnation,
    ) -> Result<(), String> {
        self.directory
            .remove_group(&PeerGroupId::new(group_id, incarnation))
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    /// Starts caller-owned discovery and activates transport workers.
    ///
    /// # Errors
    ///
    /// Returns a reason when discovery cannot start or the paused runtime can
    /// no longer be activated.
    pub fn start(&self) -> Result<(), String> {
        self.endpoints.start(Arc::clone(&self.failure))?;
        let result = lock(&self.runtime)
            .as_ref()
            .ok_or_else(|| "transport runtime is already stopped".to_string())?
            .start();
        if let Err(error) = result {
            if let Err(stop_error) = self.endpoints.stop() {
                self.latch(stop_error);
            }
            return Err(error.to_string());
        }
        Ok(())
    }

    /// Publishes this process's effective peer address atomically.
    pub fn publish_address(&self, host_dir: &Path) -> Result<(), std::io::Error> {
        endpoints::publish_address(host_dir, self.local_addr)
    }

    /// Effective listener address, including its OS-selected port.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Synchronously admits one bounded peer message without network or disk I/O.
    pub fn send(&self, frame: PeerFrame) -> Result<(), LinkError> {
        self.sender
            .send(PeerEnvelope {
                group_id: PeerGroupId::new(frame.group_id, frame.incarnation),
                from: frame.from,
                to: frame.to,
                message: frame.message,
            })
            .map_err(|source| LinkError { source })
    }

    /// Drains already authenticated and authorized peer frames.
    #[must_use]
    pub fn drain_inbound(&self, limit: usize) -> Vec<PeerFrame> {
        match self.inbound.drain(limit.min(GLOBAL_INBOUND_QUEUE_LEN)) {
            Ok(envelopes) => envelopes
                .into_iter()
                .map(|envelope| PeerFrame {
                    group_id: envelope.group_id.group_id(),
                    incarnation: envelope.group_id.incarnation(),
                    from: envelope.raft_from,
                    to: envelope.raft_to,
                    message: envelope.message,
                })
                .collect(),
            Err(error) => {
                self.latch(format!("authenticated inbound queue failed: {error}"));
                Vec::new()
            }
        }
    }

    /// Stable compatibility counters backed by public diagnostics.
    #[must_use]
    pub fn counters(&self) -> LinkCounters {
        self.public_diagnostics()
            .map_or_else(LinkCounters::default, LinkCounters::from)
    }

    /// Returns the first caller-adapter or public-runtime terminal failure.
    #[must_use]
    pub fn terminal_failure(&self) -> Option<String> {
        if let Some(failure) = lock(&self.failure).clone() {
            return Some(failure);
        }
        lock(&self.runtime)
            .as_ref()
            .and_then(TlsPeerTransport::terminal_failure)
    }

    /// Stops discovery, drains accepted work, and joins every public worker.
    pub fn shut_down(&self) {
        let runtime = lock(&self.runtime).take();
        if let Some(runtime) = runtime.as_ref() {
            runtime.shutdown();
        }
        if let Err(error) = self.endpoints.stop() {
            self.latch(error);
        }
        if let Some(runtime) = runtime {
            if let Err(error) = runtime.join() {
                self.latch(error.to_string());
            }
        }
    }

    fn public_diagnostics(&self) -> Option<TransportDiagnostics> {
        lock(&self.runtime)
            .as_ref()
            .map(TlsPeerTransport::diagnostics)
    }

    fn latch(&self, detail: String) {
        lock(&self.failure).get_or_insert(detail);
    }
}

impl From<TransportDiagnostics> for LinkCounters {
    fn from(diagnostics: TransportDiagnostics) -> Self {
        Self {
            outbound_full: diagnostics.queue_full,
            inbound_full: diagnostics.inbound_full,
            malformed: diagnostics
                .malformed_frames
                .saturating_add(diagnostics.frame_too_large),
            identity_refused: diagnostics
                .unknown_certificates
                .saturating_add(diagnostics.identity_mismatches)
                .saturating_add(diagnostics.cluster_mismatches)
                .saturating_add(diagnostics.version_mismatches)
                .saturating_add(diagnostics.unauthorized_frames)
                .saturating_add(diagnostics.retired_peer_frames),
            inbound_connection_full: diagnostics.connection_full,
            tls_handshakes: diagnostics.tls_handshakes,
            tls_failures: diagnostics.tls_failures,
            stale_sessions: diagnostics.stale_sessions,
            active_outbound_connections: diagnostics.active_outbound_connections,
            active_inbound_connections: diagnostics.active_inbound_connections,
        }
    }
}

impl Drop for PeerLink {
    fn drop(&mut self) {
        self.shut_down();
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
