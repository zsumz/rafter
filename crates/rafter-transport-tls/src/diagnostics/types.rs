//! Stable transport, queue, and per-peer diagnostic snapshots.

use crate::PeerId;

/// Current lifecycle and operational standing of a transport runtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TransportHealth {
    /// Listener and finite workers are bound but paused before activation.
    Starting,
    /// Runtime is accepting work with no currently degraded outbound peer.
    Ready,
    /// Runtime remains usable while at least one peer is disconnected.
    Degraded,
    /// A terminal local invariant or session-store failure stopped the runtime.
    Failed,
    /// Shutdown was requested and accepted work is being drained.
    Stopping,
    /// Every owned worker has terminated.
    Stopped,
}

/// Current outbound connection state for one configured peer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PeerConnectionState {
    /// No stream is established; the worker will retry with bounded backoff.
    Disconnected,
    /// A persistent mutually authenticated stream is established.
    Connected,
    /// A permanent handshake incompatibility blocks retries until endpoints change.
    ConfigurationBlocked,
}

/// Aggregate count-and-byte queue occupancy.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QueueDepths {
    /// Outbound frames retained across all physical peers, including in-flight work.
    pub outbound_frames: usize,
    /// Outbound complete-frame bytes retained across all physical peers.
    pub outbound_bytes: usize,
    /// Inbound authenticated envelopes waiting for the caller.
    pub inbound_frames: usize,
    /// Inbound complete-frame bytes waiting for the caller.
    pub inbound_bytes: usize,
    /// Weighted receive memory held by readers, decoders, and queued envelopes.
    pub inbound_memory_bytes: usize,
}

/// Stable aggregate runtime counters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransportDiagnostics {
    /// Current runtime standing.
    pub health: TransportHealth,
    /// Established persistent outbound connections.
    pub active_outbound_connections: usize,
    /// Established or handshaking inbound connections.
    pub active_inbound_connections: usize,
    /// Frames accepted into outbound queues.
    pub frames_enqueued: u64,
    /// Frames written successfully to TLS streams.
    pub frames_sent: u64,
    /// Frames accepted into the authenticated inbound queue.
    pub frames_received: u64,
    /// Frames abandoned after bounds, connection, or shutdown decisions.
    pub frames_dropped: u64,
    /// Snapshot directives accepted into bounded outbound queues.
    pub snapshot_directives_enqueued: u64,
    /// Snapshot directives materialized into complete peer frames.
    pub snapshot_chunks_resolved: u64,
    /// Snapshot directives dropped because the source no longer served them.
    pub snapshot_source_refusals: u64,
    /// Snapshot directives dropped because their resolver returned an error.
    pub snapshot_resolve_failures: u64,
    /// Snapshot directives dropped because resolved bytes violated their bounds.
    pub snapshot_resolution_mismatches: u64,
    /// Synchronous outbound queue refusals.
    pub queue_full: u64,
    /// Inbound per-peer or global queue refusals.
    pub inbound_full: u64,
    /// Inbound frames refused by one authenticated peer's count or byte bound.
    pub inbound_peer_full: u64,
    /// Inbound frames refused by the aggregate count or byte bound.
    pub inbound_global_full: u64,
    /// Frames refused before allocation by the runtime-wide receive-memory budget.
    pub inbound_memory_full: u64,
    /// Completed mutual-TLS handshakes in either direction.
    pub tls_handshakes: u64,
    /// TLS setup, handshake, or stream failures.
    pub tls_failures: u64,
    /// CA-valid leaves absent from the explicit certificate directory.
    pub unknown_certificates: u64,
    /// Certificate, hello, or frame principal disagreement.
    pub identity_mismatches: u64,
    /// Rafter hello cluster mismatches.
    pub cluster_mismatches: u64,
    /// Outer or peer-codec version mismatches.
    pub version_mismatches: u64,
    /// Frames refused because the group did not authorize the sender.
    pub unauthorized_frames: u64,
    /// Frames refused because a committed removal retired the sender.
    pub retired_peer_frames: u64,
    /// Accepted outbound frames discarded after a route authorization was revoked.
    pub invalidated_queued_frames: u64,
    /// Bulk frames abandoned after the bounded ambiguous-write retry count.
    pub retry_exhausted_frames: u64,
    /// Durable connection sessions refused as stale.
    pub stale_sessions: u64,
    /// Duplicate, skipped, reordered, superseded, or exhausted sequences.
    pub sequence_violations: u64,
    /// Outbound streams re-established after an earlier successful stream.
    pub reconnects: u64,
    /// Failed endpoint connection attempts.
    pub endpoint_failures: u64,
    /// Terminal durable session-store failures.
    pub session_store_failures: u64,
    /// Malformed handshake or peer-frame inputs.
    pub malformed_frames: u64,
    /// Frames incompatible with a local or negotiated bound.
    pub frame_too_large: u64,
    /// Listener accept failures that terminated the acceptor.
    pub listener_failures: u64,
    /// Connections refused because the configured concurrency bound was full.
    pub connection_full: u64,
    /// Endpoint attempts blocked by permanent peer-configuration incompatibility.
    pub configuration_blocks: u64,
}

/// One physical peer's persistent sender state and counters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerDiagnostics {
    /// Stable physical transport principal.
    pub peer_id: PeerId,
    /// Whether a persistent outbound TLS stream is established.
    pub connected: bool,
    /// More precise connection and retry classification.
    pub connection_state: PeerConnectionState,
    /// Most recent connection failure, cleared after a successful handshake.
    pub last_error: Option<String>,
    /// Frames retained by this peer's queue, including current in-flight work.
    pub queued_frames: usize,
    /// Complete-frame bytes retained by this peer's queue.
    pub queued_bytes: usize,
    /// Frames successfully written for this peer.
    pub frames_sent: u64,
    /// Frames abandoned for this peer.
    pub frames_dropped: u64,
    /// Snapshot directives materialized into complete peer frames.
    pub snapshot_chunks_resolved: u64,
    /// Snapshot directives dropped because the source no longer served them.
    pub snapshot_source_refusals: u64,
    /// Snapshot directives dropped because their resolver returned an error.
    pub snapshot_resolve_failures: u64,
    /// Snapshot directives dropped because resolved bytes violated their bounds.
    pub snapshot_resolution_mismatches: u64,
    /// Outbound streams re-established after an earlier successful stream.
    pub reconnects: u64,
    /// Failed endpoint connection attempts.
    pub endpoint_failures: u64,
}
