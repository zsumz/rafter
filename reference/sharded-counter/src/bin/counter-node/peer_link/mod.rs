//! Thin sharded-counter adapter over the public authenticated TLS transport.
//!
//! This module owns only deployment composition: stable host principals, the
//! counter's `(group, incarnation)` route codec, filesystem endpoint discovery,
//! and the durable session-state location. TLS, framing, replay protection,
//! queueing, authorization, diagnostics, and worker lifecycle remain in
//! `rafter-transport-tls`.

mod api;
mod config;
mod diagnostics;
mod endpoints;
mod group_codec;
mod limits;
mod session;

use std::{
    fmt,
    net::SocketAddr,
    path::Path,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use rafter::NodeId;
use rafter_reference_sharded_counter::{GroupId, GroupIncarnation};
use rafter_service::{PeerEnvelope, PeerPolicy, RaftTransport};
use rafter_transport_tls::{
    EndpointBook, TlsInbound, TlsPeerDirectory, TlsPeerTransport, TlsSender, TransportConfig,
    TransportDiagnostics,
};

pub use api::{LinkError, PeerFrame};
use config::PeerTlsConfig;
pub use config::PeerTlsPaths;
pub use diagnostics::LinkCounters;
use endpoints::{install_placeholders, remote_peers, EndpointLifecycle};
use group_codec::{PeerGroupCodec, PeerGroupId};
use limits::{transport_limits, transport_timeouts, GLOBAL_INBOUND_QUEUE_LEN};
use session::{open_transport_state, transport_cluster_id, transport_peer_id};

type PeerDirectory = TlsPeerDirectory<PeerGroupId>;
type Sender = TlsSender<PeerGroupId, PeerGroupCodec>;
type Runtime = TlsPeerTransport<PeerGroupId, PeerGroupCodec>;

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
        let local_peer = transport_peer_id(node_id)?;
        let peer_map = tls.peer_map();
        let remote_map = remote_peers(node_id, &peer_map);
        let endpoints = EndpointBook::new(limits.endpoints());
        install_placeholders(&endpoints, &remote_map, &tls.server_name())?;
        let directory = PeerDirectory::new(limits.directory());
        let cluster_id = transport_cluster_id()?;
        let sessions = open_transport_state(host_dir, &cluster_id, &local_peer, limits.sessions())?;
        let config = TransportConfig::new(
            cluster_id,
            local_peer,
            SocketAddr::from(([127, 0, 0, 1], 0)),
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
                .bind(route, *member, transport_peer_id(*member)?)
                .map_err(|error| error.to_string())?;
        }
        let peers = self
            .members
            .iter()
            .copied()
            .filter(|member| *member != self.local_node)
            .map(transport_peer_id)
            .collect::<Result<Vec<_>, _>>()?;
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
            .map_err(LinkError::new)
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

impl Drop for PeerLink {
    fn drop(&mut self) {
        self.shut_down();
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
