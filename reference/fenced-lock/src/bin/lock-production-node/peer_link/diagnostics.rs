//! Compatibility view over the public transport's richer diagnostics.

use rafter_transport_tls::{QueueDepths, TransportDiagnostics};

/// Stable structured link diagnostics used by the process evidence record.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LinkDiagnostics {
    pub authenticated_connections: u64,
    pub authentication_failed: u64,
    pub unknown_certificate: u64,
    pub identity_mismatch: u64,
    pub unauthorized_peer: u64,
    pub replay_duplicate: u64,
    pub replay_stale_session: u64,
    pub replay_outside_window: u64,
    pub malformed_frame: u64,
    pub inbound_peer_full: u64,
    pub inbound_global_full: u64,
    pub connection_full: u64,
}

impl From<TransportDiagnostics> for LinkDiagnostics {
    fn from(diagnostics: TransportDiagnostics) -> Self {
        Self {
            authenticated_connections: diagnostics.tls_handshakes,
            authentication_failed: diagnostics.tls_failures,
            unknown_certificate: diagnostics.unknown_certificates,
            identity_mismatch: diagnostics.identity_mismatches,
            unauthorized_peer: diagnostics
                .unauthorized_frames
                .saturating_add(diagnostics.retired_peer_frames),
            // The public stream accepts exactly the next sequence. The legacy
            // fixture schema keeps both names, so both project the same fact.
            replay_duplicate: diagnostics.sequence_violations,
            replay_stale_session: diagnostics.stale_sessions,
            replay_outside_window: diagnostics.sequence_violations,
            malformed_frame: diagnostics
                .malformed_frames
                .saturating_add(diagnostics.frame_too_large),
            inbound_peer_full: diagnostics.inbound_peer_full,
            inbound_global_full: diagnostics.inbound_global_full,
            connection_full: diagnostics.connection_full,
        }
    }
}

pub(super) fn frame_counts(diagnostics: &TransportDiagnostics) -> (u64, u64, u64) {
    (
        diagnostics.frames_dropped,
        diagnostics.frame_too_large,
        diagnostics
            .snapshot_source_refusals
            .saturating_add(diagnostics.snapshot_resolve_failures)
            .saturating_add(diagnostics.snapshot_resolution_mismatches),
    )
}

pub(super) const fn frame_depths(depths: QueueDepths) -> (usize, usize) {
    (depths.outbound_frames, depths.inbound_frames)
}
