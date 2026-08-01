//! Separately validated I/O deadlines and runtime pacing.

use std::{error::Error, fmt, time::Duration};

/// Which runtime timeout was configured as zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TimeoutKind {
    /// TCP connect deadline.
    Connect,
    /// Complete TLS and Rafter handshake deadline.
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

/// Network I/O deadlines for one blocking peer connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransportIoTimeouts {
    connect: Duration,
    handshake: Duration,
    read: Duration,
    write: Duration,
}

impl TransportIoTimeouts {
    /// Validates TCP, handshake, and established-stream I/O deadlines.
    ///
    /// # Errors
    ///
    /// Returns [`TransportTimeoutError`] when any duration is zero.
    pub fn new(
        connect: Duration,
        handshake: Duration,
        read: Duration,
        write: Duration,
    ) -> Result<Self, TransportTimeoutError> {
        validate([
            (TimeoutKind::Connect, connect),
            (TimeoutKind::Handshake, handshake),
            (TimeoutKind::Read, read),
            (TimeoutKind::Write, write),
        ])?;
        Ok(Self {
            connect,
            handshake,
            read,
            write,
        })
    }
}

impl Default for TransportIoTimeouts {
    fn default() -> Self {
        Self {
            connect: Duration::from_millis(500),
            handshake: Duration::from_secs(3),
            read: Duration::from_secs(15),
            write: Duration::from_secs(3),
        }
    }
}

/// Retry pacing, shutdown polling, and graceful-drain duration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransportRuntimeTimeouts {
    redial: Duration,
    poll: Duration,
    shutdown_grace: Duration,
}

impl TransportRuntimeTimeouts {
    /// Validates retry and lifecycle durations.
    ///
    /// # Errors
    ///
    /// Returns [`TransportTimeoutError`] when any duration is zero.
    pub fn new(
        redial: Duration,
        poll: Duration,
        shutdown_grace: Duration,
    ) -> Result<Self, TransportTimeoutError> {
        validate([
            (TimeoutKind::Redial, redial),
            (TimeoutKind::Poll, poll),
            (TimeoutKind::ShutdownGrace, shutdown_grace),
        ])?;
        Ok(Self {
            redial,
            poll,
            shutdown_grace,
        })
    }
}

impl Default for TransportRuntimeTimeouts {
    fn default() -> Self {
        Self {
            redial: Duration::from_millis(100),
            poll: Duration::from_millis(25),
            shutdown_grace: Duration::from_secs(3),
        }
    }
}

/// Finite deadlines and retry pacing for the blocking connection runtime.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TransportTimeouts {
    io: TransportIoTimeouts,
    runtime: TransportRuntimeTimeouts,
}

impl TransportTimeouts {
    /// Combines independently validated I/O and runtime durations.
    #[must_use]
    pub const fn new(io: TransportIoTimeouts, runtime: TransportRuntimeTimeouts) -> Self {
        Self { io, runtime }
    }

    /// Complete network I/O deadline group.
    #[must_use]
    pub const fn io(self) -> TransportIoTimeouts {
        self.io
    }

    /// Complete retry and lifecycle duration group.
    #[must_use]
    pub const fn runtime(self) -> TransportRuntimeTimeouts {
        self.runtime
    }

    /// TCP connect deadline for each resolved endpoint.
    #[must_use]
    pub const fn connect(self) -> Duration {
        self.io.connect
    }

    /// End-to-end TLS and Rafter handshake deadline.
    #[must_use]
    pub const fn handshake(self) -> Duration {
        self.io.handshake
    }

    /// Established inbound read deadline.
    #[must_use]
    pub const fn read(self) -> Duration {
        self.io.read
    }

    /// Established outbound write deadline.
    #[must_use]
    pub const fn write(self) -> Duration {
        self.io.write
    }

    /// Delay between failed endpoint rounds.
    #[must_use]
    pub const fn redial(self) -> Duration {
        self.runtime.redial
    }

    /// Runtime polling interval used for shutdown responsiveness.
    #[must_use]
    pub const fn poll(self) -> Duration {
        self.runtime.poll
    }

    /// Maximum graceful outbound drain period after shutdown begins.
    #[must_use]
    pub const fn shutdown_grace(self) -> Duration {
        self.runtime.shutdown_grace
    }
}

fn validate<const N: usize>(
    values: [(TimeoutKind, Duration); N],
) -> Result<(), TransportTimeoutError> {
    for (kind, value) in values {
        if value.is_zero() {
            return Err(TransportTimeoutError { kind });
        }
    }
    Ok(())
}
