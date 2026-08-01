//! Immutable blocking-runtime configuration and finite timeouts.

use std::{error::Error, fmt, net::SocketAddr, time::Duration};

use crate::{ClusterId, PeerId, TransportLimits};

/// Which runtime timeout was configured as zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TimeoutKind {
    /// TCP connect deadline.
    Connect,
    /// TLS and Rafter handshake I/O deadline.
    Handshake,
    /// Established connection read deadline.
    Read,
    /// Established connection write deadline.
    Write,
    /// Delay before another endpoint dial round.
    Redial,
    /// Sender and listener shutdown polling interval.
    Poll,
    /// Grace period for draining accepted outbound work.
    ShutdownGrace,
}

impl fmt::Display for TimeoutKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Connect => "connect",
            Self::Handshake => "handshake",
            Self::Read => "read",
            Self::Write => "write",
            Self::Redial => "redial",
            Self::Poll => "poll",
            Self::ShutdownGrace => "shutdown grace",
        })
    }
}

/// Invalid blocking-runtime timeout configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransportTimeoutError {
    kind: TimeoutKind,
}

impl TransportTimeoutError {
    /// Timeout whose zero duration was refused.
    #[must_use]
    pub const fn kind(self) -> TimeoutKind {
        self.kind
    }
}

impl fmt::Display for TransportTimeoutError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "transport {} timeout must be nonzero", self.kind)
    }
}

impl Error for TransportTimeoutError {}

/// Finite deadlines and retry pacing for the blocking connection runtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransportTimeouts {
    connect: Duration,
    handshake: Duration,
    read: Duration,
    write: Duration,
    redial: Duration,
    poll: Duration,
    shutdown_grace: Duration,
}

impl TransportTimeouts {
    /// Validates all runtime durations.
    ///
    /// # Errors
    ///
    /// Returns [`TransportTimeoutError`] when any duration is zero.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        connect: Duration,
        handshake: Duration,
        read: Duration,
        write: Duration,
        redial: Duration,
        poll: Duration,
        shutdown_grace: Duration,
    ) -> Result<Self, TransportTimeoutError> {
        for (kind, value) in [
            (TimeoutKind::Connect, connect),
            (TimeoutKind::Handshake, handshake),
            (TimeoutKind::Read, read),
            (TimeoutKind::Write, write),
            (TimeoutKind::Redial, redial),
            (TimeoutKind::Poll, poll),
            (TimeoutKind::ShutdownGrace, shutdown_grace),
        ] {
            if value.is_zero() {
                return Err(TransportTimeoutError { kind });
            }
        }
        Ok(Self {
            connect,
            handshake,
            read,
            write,
            redial,
            poll,
            shutdown_grace,
        })
    }

    /// TCP connect deadline for each resolved endpoint.
    #[must_use]
    pub const fn connect(self) -> Duration {
        self.connect
    }

    /// Read and write deadline while TLS and Rafter handshakes run.
    #[must_use]
    pub const fn handshake(self) -> Duration {
        self.handshake
    }

    /// Established inbound read deadline.
    #[must_use]
    pub const fn read(self) -> Duration {
        self.read
    }

    /// Established outbound write deadline.
    #[must_use]
    pub const fn write(self) -> Duration {
        self.write
    }

    /// Delay between failed endpoint rounds.
    #[must_use]
    pub const fn redial(self) -> Duration {
        self.redial
    }

    /// Runtime polling interval used for shutdown responsiveness.
    #[must_use]
    pub const fn poll(self) -> Duration {
        self.poll
    }

    /// Maximum graceful outbound drain period after shutdown begins.
    #[must_use]
    pub const fn shutdown_grace(self) -> Duration {
        self.shutdown_grace
    }
}

impl Default for TransportTimeouts {
    fn default() -> Self {
        Self {
            connect: Duration::from_millis(500),
            handshake: Duration::from_secs(3),
            read: Duration::from_secs(15),
            write: Duration::from_secs(3),
            redial: Duration::from_millis(100),
            poll: Duration::from_millis(25),
            shutdown_grace: Duration::from_secs(3),
        }
    }
}

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
