//! The single timeout group carried through the blocking runtime.
//!
//! This module owns the pairing of already-validated I/O deadlines with
//! already-validated runtime pacing, and the flat reads every worker uses. It
//! validates nothing itself: each half is admitted by its own constructor.

use std::time::Duration;

use super::{TransportIoTimeouts, TransportRuntimeTimeouts};

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
