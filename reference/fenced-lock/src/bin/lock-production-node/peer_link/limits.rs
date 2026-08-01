//! Public-transport limits chosen by the bounded production fixture.

use std::time::Duration;

use rafter_transport_tls::{
    CertificateDirectoryLimits, DirectoryLimits, EndpointBookLimits, InboundQueueLimits,
    OutboundQueueLimits, RuntimeLimits, TransportIoTimeouts, TransportLimits,
    TransportRuntimeTimeouts, TransportTimeouts, WireLimits,
};

/// Outbound frames one physical peer may retain.
pub const PEER_SEND_QUEUE_LEN: usize = 256;
/// Authenticated frames one physical peer may retain inbound.
pub const PEER_INBOUND_QUEUE_LEN: usize = 128;
/// Authenticated frames all peers may retain inbound together.
pub const GLOBAL_INBOUND_QUEUE_LEN: usize = 512;
/// Concurrent authenticated or handshaking inbound connections.
pub const MAX_PEER_CONNECTIONS: usize = 16;
/// Largest complete public peer frame accepted by this fixture.
pub const MAX_FRAME_BYTES: usize = 2_163_089;

pub(super) fn transport_limits() -> Result<TransportLimits, String> {
    let outbound =
        OutboundQueueLimits::new(PEER_SEND_QUEUE_LEN, 16 * 1024 * 1024, 32, 1024 * 1024, 16)
            .map_err(|error| error.to_string())?;
    let inbound = InboundQueueLimits::new(
        PEER_INBOUND_QUEUE_LEN,
        8 * 1024 * 1024,
        GLOBAL_INBOUND_QUEUE_LEN,
        32 * 1024 * 1024,
    )
    .map_err(|error| error.to_string())?;
    let runtime = RuntimeLimits::new(outbound, inbound, MAX_PEER_CONNECTIONS)
        .map_err(|error| error.to_string())?;
    let wire = WireLimits::new(MAX_FRAME_BYTES - 4, 8).map_err(|error| error.to_string())?;
    Ok(TransportLimits::new(
        DirectoryLimits::default(),
        EndpointBookLimits::default(),
        CertificateDirectoryLimits::default(),
        wire,
    )
    .with_runtime(runtime))
}

pub(super) fn transport_timeouts() -> Result<TransportTimeouts, String> {
    let io = TransportIoTimeouts::new(
        Duration::from_millis(200),
        Duration::from_secs(2),
        Duration::from_secs(2),
        Duration::from_millis(500),
    )
    .map_err(|error| error.to_string())?;
    let runtime = TransportRuntimeTimeouts::new(
        Duration::from_millis(20),
        Duration::from_millis(25),
        Duration::from_secs(3),
    )
    .map_err(|error| error.to_string())?;
    Ok(TransportTimeouts::new(io, runtime))
}
