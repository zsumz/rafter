//! Immutable blocking-runtime configuration and finite timeouts.

mod timeouts;

use std::net::SocketAddr;

use crate::{ClusterId, PeerId, TransportLimits};

pub use timeouts::{
    TimeoutKind, TransportIoTimeouts, TransportRuntimeTimeouts, TransportTimeoutError,
    TransportTimeouts,
};

/// Immutable identity, listener, bound, and timeout configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransportConfig {
    cluster_id: ClusterId,
    local_peer_id: PeerId,
    bind_addr: SocketAddr,
    limits: TransportLimits,
    timeouts: TransportTimeouts,
}

impl TransportConfig {
    /// Creates one blocking peer-runtime configuration.
    #[must_use]
    pub fn new(
        cluster_id: ClusterId,
        local_peer_id: PeerId,
        bind_addr: SocketAddr,
        limits: TransportLimits,
        timeouts: TransportTimeouts,
    ) -> Self {
        Self {
            cluster_id,
            local_peer_id,
            bind_addr,
            limits,
            timeouts,
        }
    }

    /// Exact deployment boundary negotiated after TLS.
    #[must_use]
    pub const fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    /// Stable local TLS transport principal.
    #[must_use]
    pub const fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }

    /// Requested TCP listener address.
    #[must_use]
    pub const fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }

    /// Complete finite transport limits.
    #[must_use]
    pub const fn limits(&self) -> TransportLimits {
        self.limits
    }

    /// Blocking I/O deadlines and retry pacing.
    #[must_use]
    pub const fn timeouts(&self) -> TransportTimeouts {
        self.timeouts
    }
}
