//! Process-facing lifecycle, diagnostics, and queue access.

use std::{net::SocketAddr, path::Path, sync::Arc};

use rafter::NodeId;
use rafter_service::AuthenticatedPeerEnvelope;

use super::{
    diagnostics, endpoints, lock, LinkDiagnostics, LockGroupId, PeerDirectory, PeerLink,
    PeerPrincipal, TcpPeerTransport, GLOBAL_INBOUND_QUEUE_LEN, GROUP_ID,
};
use rafter_reference_fenced_lock::production::open_transport_state;

impl PeerLink {
    /// Effective listener address, including an OS-assigned port.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Cloneable public transport handle for the managed driver.
    #[must_use]
    pub fn transport(&self) -> TcpPeerTransport {
        self.transport.clone()
    }

    /// Cloneable public authenticated peer validator.
    #[must_use]
    pub fn validator(&self) -> PeerDirectory {
        self.directory.clone()
    }

    /// Verifies durable state, starts discovery, and activates transport workers.
    ///
    /// The fixture binds its client and peer listeners before acquiring the
    /// replica directory so a contender can expose fail-closed recovery state.
    /// Activation therefore happens only after application-directory ownership
    /// and recovery succeed, and before the peer address or readiness gate is
    /// published.
    ///
    /// # Errors
    ///
    /// Returns a reason when session state is missing, corrupt, concurrently
    /// owned, or identity-mismatched; discovery cannot start; or the paused
    /// public runtime can no longer be activated.
    pub fn start(&self, node_dir: &Path, node_id: NodeId) -> Result<(), String> {
        let state = open_transport_state(node_dir, GROUP_ID.0, node_id)
            .map_err(|error| error.to_string())?;
        drop(state);
        self.endpoints.start(Arc::clone(&self.failure))?;
        let start = lock(&self.runtime)
            .as_ref()
            .ok_or_else(|| String::from("transport runtime is already stopped"))?
            .start();
        if let Err(error) = start {
            if let Err(stop_error) = self.endpoints.stop() {
                self.latch(stop_error);
            }
            return Err(error.to_string());
        }
        Ok(())
    }

    /// Atomically publishes this process's resolved peer address.
    ///
    /// # Errors
    ///
    /// Returns the underlying filesystem error if publication fails.
    pub fn publish_address(&self, node_dir: &Path) -> std::io::Result<()> {
        endpoints::publish_address(node_dir, self.local_addr)
    }

    /// Drains already authenticated and authorized inbound envelopes.
    #[must_use]
    pub fn drain_inbound(&self) -> Vec<AuthenticatedPeerEnvelope<LockGroupId, PeerPrincipal>> {
        match self.inbound.drain(GLOBAL_INBOUND_QUEUE_LEN) {
            Ok(envelopes) => envelopes,
            Err(error) => {
                self.latch(format!("authenticated inbound queue failed: {error}"));
                Vec::new()
            }
        }
    }

    /// Legacy process counters backed by the public diagnostic vocabulary.
    #[must_use]
    pub fn counts(&self) -> (u64, u64, u64) {
        self.public_diagnostics()
            .as_ref()
            .map_or((0, 0, 0), diagnostics::frame_counts)
    }

    /// Stable process-facing projection of public runtime diagnostics.
    #[must_use]
    pub fn diagnostics(&self) -> LinkDiagnostics {
        self.public_diagnostics()
            .map_or_else(LinkDiagnostics::default, LinkDiagnostics::from)
    }

    /// Returns the first adapter or public-runtime terminal failure.
    #[must_use]
    pub fn terminal_failure(&self) -> Option<String> {
        if let Some(failure) = lock(&self.failure).clone() {
            return Some(failure);
        }
        lock(&self.runtime)
            .as_ref()
            .and_then(rafter_transport_tls::TlsPeerTransport::terminal_failure)
    }

    /// Number of configured remote principals with durable session records.
    #[must_use]
    pub const fn replay_peer_windows(&self) -> usize {
        self.remote_peers
    }

    /// Aggregate outbound and inbound frame counts.
    #[must_use]
    pub fn queue_depths(&self) -> (usize, usize) {
        let runtime = lock(&self.runtime);
        let Some(runtime) = runtime.as_ref() else {
            return (0, 0);
        };
        match runtime.queue_depths() {
            Ok(depths) => diagnostics::frame_depths(depths),
            Err(error) => {
                self.latch(format!("transport queue accounting failed: {error}"));
                (0, 0)
            }
        }
    }

    /// Stops discovery, drains accepted work, and joins all transport workers.
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

    fn public_diagnostics(&self) -> Option<rafter_transport_tls::TransportDiagnostics> {
        let runtime = lock(&self.runtime);
        let runtime = runtime.as_ref()?;
        Some(runtime.diagnostics())
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
