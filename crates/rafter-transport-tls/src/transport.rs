//! Owned blocking listener, workers, handles, diagnostics, and shutdown.

use std::{collections::BTreeMap, fmt, net::SocketAddr, sync::Arc, thread::JoinHandle};

use crate::connection::ReceiverRegistry;
use crate::diagnostics::{Counters, PeerCounterMap};
use crate::queue::{InboundQueue, OutboundQueue};
use crate::runtime::{InboundEpochs, RuntimeControl};
use crate::{
    GroupIdCodec, PeerDiagnostics, PeerId, QueueDepths, TlsInbound, TlsInboundError,
    TlsPeerTransportBuilder, TlsSender, TlsTransportJoinError, TlsTransportStartError,
    TransportConfig, TransportDiagnostics, TransportHealth,
};

pub(crate) struct NamedWorker {
    pub(crate) name: String,
    pub(crate) handle: Option<JoinHandle<()>>,
}

impl fmt::Debug for NamedWorker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NamedWorker")
            .field("name", &self.name)
            .field("joined", &self.handle.is_none())
            .finish()
    }
}

impl NamedWorker {
    pub(crate) fn new(name: String, handle: JoinHandle<()>) -> Self {
        Self {
            name,
            handle: Some(handle),
        }
    }

    pub(crate) fn join(&mut self, panicked: &mut Vec<String>) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        if handle.join().is_err() {
            panicked.push(self.name.clone());
        }
    }
}

/// Owned bounded blocking TLS peer transport.
///
/// [`TlsPeerTransportBuilder::bind`] returns an activated runtime.
/// [`TlsPeerTransportBuilder::bind_paused`] returns the same owned resources in
/// [`TransportHealth::Starting`], allowing caller-owned recovery and directory
/// ownership to complete before any worker performs network or session I/O.
///
/// One persistent sender worker is owned for every peer present in the endpoint
/// book at bind time. Existing endpoint sets may be replaced or removed live;
/// adding a new physical peer requires rebuilding the runtime so thread and
/// queue ownership remain explicit and finite.
pub struct TlsPeerTransport<G, C> {
    pub(crate) config: TransportConfig,
    pub(crate) local_addr: SocketAddr,
    pub(crate) sender: TlsSender<G, C>,
    pub(crate) inbound: TlsInbound<G>,
    pub(crate) control: Arc<RuntimeControl>,
    pub(crate) counters: Arc<Counters>,
    pub(crate) queues: Arc<BTreeMap<PeerId, Arc<OutboundQueue<G>>>>,
    pub(crate) peer_counters: Arc<PeerCounterMap>,
    pub(crate) inbound_queue: Arc<InboundQueue<G>>,
    pub(crate) epochs: Arc<InboundEpochs>,
    pub(crate) receivers: Arc<ReceiverRegistry>,
    pub(crate) acceptor: Option<NamedWorker>,
    pub(crate) senders: Vec<NamedWorker>,
}

impl<G, C> fmt::Debug for TlsPeerTransport<G, C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TlsPeerTransport")
            .field("local_addr", &self.local_addr)
            .field("local_peer", self.config.local_peer_id())
            .field("configured_peers", &self.queues.len())
            .field("health", &self.control.health())
            .finish_non_exhaustive()
    }
}

impl<G, C> TlsPeerTransport<G, C>
where
    G: Ord + Send + Sync + 'static,
    C: GroupIdCodec<G>,
{
    /// Starts a typed builder with no implicit identity, PKI, discovery, or
    /// session-store defaults.
    #[must_use]
    pub fn builder(config: TransportConfig, group_codec: C) -> TlsPeerTransportBuilder<G, C> {
        TlsPeerTransportBuilder::new(config, group_codec)
    }

    /// Activates workers bound through
    /// [`TlsPeerTransportBuilder::bind_paused`].
    ///
    /// Activation is idempotent. Work and peer policies accepted while paused
    /// remain bounded and are processed only after this transition succeeds.
    ///
    /// # Errors
    ///
    /// Returns [`TlsTransportStartError`] when shutdown or a terminal failure
    /// won the lifecycle race before activation.
    pub fn start(&self) -> Result<(), TlsTransportStartError> {
        self.control.start()
    }

    /// Effective bound listener address, including an OS-assigned port.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Immutable runtime configuration.
    #[must_use]
    pub const fn config(&self) -> &TransportConfig {
        &self.config
    }

    /// Cloneable nonblocking `RaftTransport<G>` admission handle.
    #[must_use]
    pub fn sender(&self) -> TlsSender<G, C> {
        self.sender.clone()
    }

    /// Cloneable bounded authenticated inbound receiver.
    #[must_use]
    pub fn inbound(&self) -> TlsInbound<G> {
        self.inbound.clone()
    }

    /// Current aggregate runtime standing.
    #[must_use]
    pub fn health(&self) -> TransportHealth {
        self.control.health()
    }

    /// First terminal local failure, when one has been latched.
    #[must_use]
    pub fn terminal_failure(&self) -> Option<String> {
        self.control.terminal_failure()
    }

    /// Stable aggregate counters and resource state.
    #[must_use]
    pub fn diagnostics(&self) -> TransportDiagnostics {
        self.counters.snapshot(self.health())
    }

    /// One configured physical peer's diagnostics.
    ///
    /// # Errors
    ///
    /// Returns [`TlsInboundError::Poisoned`] when queue accounting is poisoned.
    pub fn peer_diagnostics(
        &self,
        peer: &PeerId,
    ) -> Result<Option<PeerDiagnostics>, TlsInboundError> {
        let Some(counters) = self.peer_counters.get(peer) else {
            return Ok(None);
        };
        let Some(queue) = self.queues.get(peer) else {
            return Ok(None);
        };
        let depth = queue.depth().map_err(|_| TlsInboundError::Poisoned)?;
        Ok(Some(counters.snapshot(
            peer.clone(),
            depth.frames,
            depth.bytes,
        )))
    }

    /// Aggregate count-and-byte queue occupancy.
    ///
    /// # Errors
    ///
    /// Returns [`TlsInboundError::Poisoned`] when queue accounting is poisoned.
    pub fn queue_depths(&self) -> Result<QueueDepths, TlsInboundError> {
        let mut outbound_frames = 0_usize;
        let mut outbound_bytes = 0_usize;
        for queue in self.queues.values() {
            let depth = queue.depth().map_err(|_| TlsInboundError::Poisoned)?;
            outbound_frames = outbound_frames.saturating_add(depth.frames);
            outbound_bytes = outbound_bytes.saturating_add(depth.bytes);
        }
        let inbound = self
            .inbound_queue
            .depth()
            .map_err(|_| TlsInboundError::Poisoned)?;
        Ok(QueueDepths {
            outbound_frames,
            outbound_bytes,
            inbound_frames: inbound.frames,
            inbound_bytes: inbound.bytes,
        })
    }

    /// Begins idempotent graceful shutdown.
    ///
    /// New sends are refused immediately. Accepted outbound work drains until
    /// queues empty or the configured grace period expires. Inbound sockets are
    /// closed so receiver workers wake promptly.
    pub fn shutdown(&self) {
        self.control.request_shutdown();
        for queue in self.queues.values() {
            if queue.close().is_err() {
                self.control.fail("outbound queue state is poisoned");
            }
        }
        // Receiver admission holds the epoch lock before the inbound queue
        // lock. Shutdown follows the same order so it cannot invert those
        // locks while closing a live connection.
        if self.epochs.shutdown_all().is_err() {
            self.control.fail("inbound epoch state is poisoned");
        }
        if self.inbound_queue.close().is_err() {
            self.control.fail("inbound queue state is poisoned");
        }
        if self.receivers.shutdown_all().is_err() {
            self.control.fail("receiver registry state is poisoned");
        }
    }

    /// Shuts down and joins every currently owned worker.
    ///
    /// # Errors
    ///
    /// Returns [`TlsTransportJoinError`] naming workers that panicked.
    pub fn join(mut self) -> Result<(), TlsTransportJoinError> {
        self.shutdown();
        let mut panicked = Vec::new();
        if let Some(acceptor) = self.acceptor.as_mut() {
            acceptor.join(&mut panicked);
        }
        for sender in &mut self.senders {
            sender.join(&mut panicked);
        }
        if let Ok(receivers) = self.receivers.join_all() {
            panicked.extend(receivers);
        } else {
            self.control.fail("receiver registry state is poisoned");
            panicked.push("rafter-tls-receiver-registry".to_owned());
        }
        self.control.mark_stopped();
        if panicked.is_empty() {
            Ok(())
        } else {
            Err(TlsTransportJoinError::new(panicked))
        }
    }
}

impl<G, C> Drop for TlsPeerTransport<G, C> {
    fn drop(&mut self) {
        self.control.request_shutdown();
        for queue in self.queues.values() {
            let _ = queue.close();
        }
        let _ = self.epochs.shutdown_all();
        let _ = self.inbound_queue.close();
        let _ = self.receivers.shutdown_all();
    }
}
