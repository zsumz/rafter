//! Saturating atomic counters retained by the blocking runtime.

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use crate::{PeerConnectionState, PeerDiagnostics, PeerId, TransportDiagnostics, TransportHealth};

#[derive(Debug, Default)]
pub(crate) struct Counters {
    pub(crate) active_outbound: AtomicUsize,
    pub(crate) active_inbound: AtomicUsize,
    pub(crate) frames_enqueued: AtomicU64,
    pub(crate) frames_sent: AtomicU64,
    pub(crate) frames_received: AtomicU64,
    pub(crate) frames_dropped: AtomicU64,
    pub(crate) snapshot_directives_enqueued: AtomicU64,
    pub(crate) snapshot_chunks_resolved: AtomicU64,
    pub(crate) snapshot_source_refusals: AtomicU64,
    pub(crate) snapshot_resolve_failures: AtomicU64,
    pub(crate) snapshot_resolution_mismatches: AtomicU64,
    pub(crate) queue_full: AtomicU64,
    pub(crate) inbound_full: AtomicU64,
    pub(crate) inbound_peer_full: AtomicU64,
    pub(crate) inbound_global_full: AtomicU64,
    pub(crate) inbound_memory_full: AtomicU64,
    pub(crate) tls_handshakes: AtomicU64,
    pub(crate) tls_failures: AtomicU64,
    pub(crate) unknown_certificates: AtomicU64,
    pub(crate) identity_mismatches: AtomicU64,
    pub(crate) cluster_mismatches: AtomicU64,
    pub(crate) version_mismatches: AtomicU64,
    pub(crate) unauthorized_frames: AtomicU64,
    pub(crate) retired_peer_frames: AtomicU64,
    pub(crate) retired_queued_frames: AtomicU64,
    pub(crate) retry_exhausted_frames: AtomicU64,
    pub(crate) stale_sessions: AtomicU64,
    pub(crate) sequence_violations: AtomicU64,
    pub(crate) reconnects: AtomicU64,
    pub(crate) endpoint_failures: AtomicU64,
    pub(crate) session_store_failures: AtomicU64,
    pub(crate) malformed_frames: AtomicU64,
    pub(crate) frame_too_large: AtomicU64,
    pub(crate) listener_failures: AtomicU64,
    pub(crate) connection_full: AtomicU64,
    pub(crate) configuration_blocks: AtomicU64,
}

impl Counters {
    pub(crate) fn snapshot(&self, health: TransportHealth) -> TransportDiagnostics {
        TransportDiagnostics {
            health,
            active_outbound_connections: self.active_outbound.load(Ordering::Relaxed),
            active_inbound_connections: self.active_inbound.load(Ordering::Relaxed),
            frames_enqueued: load(&self.frames_enqueued),
            frames_sent: load(&self.frames_sent),
            frames_received: load(&self.frames_received),
            frames_dropped: load(&self.frames_dropped),
            snapshot_directives_enqueued: load(&self.snapshot_directives_enqueued),
            snapshot_chunks_resolved: load(&self.snapshot_chunks_resolved),
            snapshot_source_refusals: load(&self.snapshot_source_refusals),
            snapshot_resolve_failures: load(&self.snapshot_resolve_failures),
            snapshot_resolution_mismatches: load(&self.snapshot_resolution_mismatches),
            queue_full: load(&self.queue_full),
            inbound_full: load(&self.inbound_full),
            inbound_peer_full: load(&self.inbound_peer_full),
            inbound_global_full: load(&self.inbound_global_full),
            inbound_memory_full: load(&self.inbound_memory_full),
            tls_handshakes: load(&self.tls_handshakes),
            tls_failures: load(&self.tls_failures),
            unknown_certificates: load(&self.unknown_certificates),
            identity_mismatches: load(&self.identity_mismatches),
            cluster_mismatches: load(&self.cluster_mismatches),
            version_mismatches: load(&self.version_mismatches),
            unauthorized_frames: load(&self.unauthorized_frames),
            retired_peer_frames: load(&self.retired_peer_frames),
            retired_queued_frames: load(&self.retired_queued_frames),
            retry_exhausted_frames: load(&self.retry_exhausted_frames),
            stale_sessions: load(&self.stale_sessions),
            sequence_violations: load(&self.sequence_violations),
            reconnects: load(&self.reconnects),
            endpoint_failures: load(&self.endpoint_failures),
            session_store_failures: load(&self.session_store_failures),
            malformed_frames: load(&self.malformed_frames),
            frame_too_large: load(&self.frame_too_large),
            listener_failures: load(&self.listener_failures),
            connection_full: load(&self.connection_full),
            configuration_blocks: load(&self.configuration_blocks),
        }
    }
}

#[derive(Debug)]
pub(crate) struct PeerCounters {
    connected: AtomicBool,
    configuration_blocked: AtomicBool,
    last_error: Mutex<Option<String>>,
    frames_sent: AtomicU64,
    frames_dropped: AtomicU64,
    snapshot_chunks_resolved: AtomicU64,
    snapshot_source_refusals: AtomicU64,
    snapshot_resolve_failures: AtomicU64,
    snapshot_resolution_mismatches: AtomicU64,
    reconnects: AtomicU64,
    endpoint_failures: AtomicU64,
}

impl Default for PeerCounters {
    fn default() -> Self {
        Self {
            connected: AtomicBool::new(false),
            configuration_blocked: AtomicBool::new(false),
            last_error: Mutex::new(None),
            frames_sent: AtomicU64::new(0),
            frames_dropped: AtomicU64::new(0),
            snapshot_chunks_resolved: AtomicU64::new(0),
            snapshot_source_refusals: AtomicU64::new(0),
            snapshot_resolve_failures: AtomicU64::new(0),
            snapshot_resolution_mismatches: AtomicU64::new(0),
            reconnects: AtomicU64::new(0),
            endpoint_failures: AtomicU64::new(0),
        }
    }
}

impl PeerCounters {
    pub(crate) fn set_connected(&self, connected: bool) {
        self.connected.store(connected, Ordering::Relaxed);
        if connected {
            self.configuration_blocked.store(false, Ordering::Relaxed);
            *self
                .last_error
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        }
    }

    pub(crate) fn record_failure(&self, message: String, blocked: bool) {
        self.connected.store(false, Ordering::Relaxed);
        self.configuration_blocked.store(blocked, Ordering::Relaxed);
        *self
            .last_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(message);
    }

    pub(crate) fn sent(&self) {
        increment(&self.frames_sent);
    }

    pub(crate) fn dropped(&self) {
        increment(&self.frames_dropped);
    }

    pub(crate) fn dropped_many(&self, amount: usize) {
        add(&self.frames_dropped, amount);
    }

    pub(crate) fn snapshot_resolved(&self) {
        increment(&self.snapshot_chunks_resolved);
    }

    pub(crate) fn snapshot_source_refused(&self) {
        increment(&self.snapshot_source_refusals);
    }

    pub(crate) fn snapshot_resolve_failed(&self) {
        increment(&self.snapshot_resolve_failures);
    }

    pub(crate) fn snapshot_resolution_mismatched(&self) {
        increment(&self.snapshot_resolution_mismatches);
    }

    pub(crate) fn reconnected(&self) {
        increment(&self.reconnects);
    }

    pub(crate) fn endpoint_failed(&self) {
        increment(&self.endpoint_failures);
    }

    pub(crate) fn snapshot(
        &self,
        peer_id: PeerId,
        queued_frames: usize,
        queued_bytes: usize,
    ) -> PeerDiagnostics {
        let connected = self.connected.load(Ordering::Relaxed);
        PeerDiagnostics {
            peer_id,
            connected,
            connection_state: if connected {
                PeerConnectionState::Connected
            } else if self.configuration_blocked.load(Ordering::Relaxed) {
                PeerConnectionState::ConfigurationBlocked
            } else {
                PeerConnectionState::Disconnected
            },
            last_error: self
                .last_error
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
            queued_frames,
            queued_bytes,
            frames_sent: load(&self.frames_sent),
            frames_dropped: load(&self.frames_dropped),
            snapshot_chunks_resolved: load(&self.snapshot_chunks_resolved),
            snapshot_source_refusals: load(&self.snapshot_source_refusals),
            snapshot_resolve_failures: load(&self.snapshot_resolve_failures),
            snapshot_resolution_mismatches: load(&self.snapshot_resolution_mismatches),
            reconnects: load(&self.reconnects),
            endpoint_failures: load(&self.endpoint_failures),
        }
    }
}

pub(crate) type PeerCounterMap = BTreeMap<PeerId, Arc<PeerCounters>>;

pub(crate) fn increment(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_add(1))
    });
}

pub(crate) fn add(counter: &AtomicU64, amount: usize) {
    let amount = u64::try_from(amount).unwrap_or(u64::MAX);
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_add(amount))
    });
}

fn load(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}
