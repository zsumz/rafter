//! Separately validated I/O deadlines and runtime pacing.

use std::{error::Error, fmt, time::Duration};

pub(crate) const MAX_REDIAL_DELAY: Duration = Duration::from_secs(30);

/// Which runtime timeout violated its supported range.
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
    /// Delay before sparsely probing an unchanged configuration-blocked endpoint.
    ConfigurationReprobe,
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
            Self::ConfigurationReprobe => "configuration reprobe",
            Self::Poll => "poll",
            Self::ShutdownGrace => "shutdown grace",
        })
    }
}

/// Invalid blocking-runtime timeout configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransportTimeoutError {
    kind: TimeoutKind,
    constraint: TimeoutConstraint,
}

/// Supported-range constraint violated by a transport timeout.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TimeoutConstraint {
    /// The timeout must be greater than zero.
    NonZero,
    /// The timeout must not exceed this duration.
    Maximum(Duration),
}

impl TransportTimeoutError {
    /// Timeout whose supported range was violated.
    #[must_use]
    pub const fn kind(self) -> TimeoutKind {
        self.kind
    }

    /// Supported-range constraint that was violated.
    #[must_use]
    pub const fn constraint(self) -> TimeoutConstraint {
        self.constraint
    }
}

impl fmt::Display for TransportTimeoutError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.constraint {
            TimeoutConstraint::NonZero => {
                write!(formatter, "transport {} timeout must be nonzero", self.kind)
            }
            TimeoutConstraint::Maximum(maximum) => write!(
                formatter,
                "transport {} timeout must not exceed {maximum:?}",
                self.kind
            ),
        }
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
    configuration_reprobe: Duration,
    poll: Duration,
    shutdown_grace: Duration,
}

impl TransportRuntimeTimeouts {
    /// Validates retry and lifecycle durations.
    ///
    /// # Errors
    ///
    /// Returns [`TransportTimeoutError`] when any duration is zero or `redial`
    /// exceeds the 30-second retry ceiling.
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
        if redial > MAX_REDIAL_DELAY {
            return Err(TransportTimeoutError {
                kind: TimeoutKind::Redial,
                constraint: TimeoutConstraint::Maximum(MAX_REDIAL_DELAY),
            });
        }
        Ok(Self {
            redial,
            configuration_reprobe: Duration::from_secs(5 * 60),
            poll,
            shutdown_grace,
        })
    }

    /// Replaces the sparse recovery interval for an unchanged blocked endpoint.
    ///
    /// The default base is five minutes. Each local-to-remote peer pair receives
    /// deterministic jitter below 25 percent of this value. Discovery can
    /// recover immediately with [`crate::EndpointBook::refresh`]; this interval
    /// is the fail-safe when no local discovery event accompanies remote repair.
    ///
    /// # Errors
    ///
    /// Returns [`TransportTimeoutError`] when `configuration_reprobe` is zero.
    pub fn with_configuration_reprobe(
        mut self,
        configuration_reprobe: Duration,
    ) -> Result<Self, TransportTimeoutError> {
        validate([(TimeoutKind::ConfigurationReprobe, configuration_reprobe)])?;
        self.configuration_reprobe = configuration_reprobe;
        Ok(self)
    }
}

impl Default for TransportRuntimeTimeouts {
    fn default() -> Self {
        Self {
            redial: Duration::from_millis(100),
            configuration_reprobe: Duration::from_secs(5 * 60),
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

    /// Initial exponential retry window after a failed endpoint round.
    ///
    /// Each local-to-remote pair receives deterministic equal jitter between
    /// half and all of the current window, capped below 30 seconds.
    #[must_use]
    pub const fn redial(self) -> Duration {
        self.runtime.redial
    }

    /// Sparse retry base for a configuration-blocked endpoint generation.
    ///
    /// The runtime adds deterministic local-to-remote pair jitter below 25 percent.
    #[must_use]
    pub const fn configuration_reprobe(self) -> Duration {
        self.runtime.configuration_reprobe
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
            return Err(TransportTimeoutError {
                kind,
                constraint: TimeoutConstraint::NonZero,
            });
        }
    }
    Ok(())
}
