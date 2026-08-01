//! Stable process-facing projection of public transport diagnostics.

use rafter_transport_tls::TransportDiagnostics;

/// Compatibility counters consumed by the counter process.
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
