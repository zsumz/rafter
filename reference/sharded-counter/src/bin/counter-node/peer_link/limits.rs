//! Explicit finite limits for the counter process composition.

use std::time::Duration;

use rafter_transport_tls::{
    CertificateDirectoryLimits, DirectoryLimits, EndpointBookLimits, InboundQueueLimits,
    OutboundQueueLimits, RuntimeLimits, TransportLimits, TransportTimeouts, WireLimits,
};

pub(super) const GLOBAL_INBOUND_QUEUE_LEN: usize = 4_096;
const MAX_GROUPS: usize = 4_096;
const MAX_BINDINGS_PER_GROUP: usize = 3;
const MAX_PHYSICAL_PEERS: usize = 3;
const MAX_CERTIFICATE_FINGERPRINTS: usize = 6;
const MAX_ENDPOINTS_PER_PEER: usize = 1;
const OUTBOUND_FRAMES_PER_PEER: usize = 256;
const INBOUND_FRAMES_PER_PEER: usize = 2_048;
const MAX_INBOUND_CONNECTIONS: usize = 64;
const MAX_FRAME_BYTES: usize = 2_163_089;

pub(super) fn transport_limits() -> Result<TransportLimits, String> {
    let directory = DirectoryLimits::new(MAX_GROUPS, MAX_BINDINGS_PER_GROUP)
        .map_err(|error| error.to_string())?;
    let endpoints = EndpointBookLimits::new(MAX_PHYSICAL_PEERS, MAX_ENDPOINTS_PER_PEER)
        .map_err(|error| error.to_string())?;
    let certificates =
        CertificateDirectoryLimits::new(MAX_CERTIFICATE_FINGERPRINTS, MAX_PHYSICAL_PEERS)
            .map_err(|error| error.to_string())?;
    let outbound = OutboundQueueLimits::new(
        OUTBOUND_FRAMES_PER_PEER,
        32 * 1024 * 1024,
        32,
        2 * 1024 * 1024,
        16,
    )
    .map_err(|error| error.to_string())?;
    let inbound = InboundQueueLimits::new(
        INBOUND_FRAMES_PER_PEER,
        16 * 1024 * 1024,
        GLOBAL_INBOUND_QUEUE_LEN,
        32 * 1024 * 1024,
    )
    .map_err(|error| error.to_string())?;
    let runtime = RuntimeLimits::new(outbound, inbound, MAX_INBOUND_CONNECTIONS)
        .map_err(|error| error.to_string())?;
    let wire = WireLimits::new(MAX_FRAME_BYTES - 4, 8).map_err(|error| error.to_string())?;
    Ok(TransportLimits::new(directory, endpoints, certificates, wire).with_runtime(runtime))
}

pub(super) fn transport_timeouts() -> Result<TransportTimeouts, String> {
    TransportTimeouts::new(
        Duration::from_millis(200),
        Duration::from_secs(2),
        Duration::from_secs(2),
        Duration::from_millis(500),
        Duration::from_millis(20),
        Duration::from_millis(20),
        Duration::from_secs(3),
    )
    .map_err(|error| error.to_string())
}
