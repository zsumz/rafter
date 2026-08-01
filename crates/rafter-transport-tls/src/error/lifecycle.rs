//! Inbound queue and owned-worker join failures.

use std::{error::Error, fmt};

/// Failure while draining authenticated inbound envelopes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TlsInboundError {
    /// A poisoned queue made count-and-byte accounting untrustworthy.
    Poisoned,
}

impl fmt::Display for TlsInboundError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("authenticated inbound queue is poisoned")
    }
}

impl Error for TlsInboundError {}

/// Failure while activating a transport bound in the paused state.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TlsTransportStartError {
    /// Graceful shutdown began before activation.
    Stopping,
    /// The runtime already reached its terminal stopped state.
    Stopped,
    /// An owned worker or durable dependency failed before activation.
    Failed {
        /// First terminal failure recorded by the runtime, when available.
        message: Option<String>,
    },
}

impl fmt::Display for TlsTransportStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stopping => {
                formatter.write_str("TLS transport cannot start after shutdown was requested")
            }
            Self::Stopped => formatter.write_str("TLS transport is already stopped"),
            Self::Failed {
                message: Some(message),
            } => {
                write!(formatter, "TLS transport failed before start: {message}")
            }
            Self::Failed { message: None } => {
                formatter.write_str("TLS transport failed before start")
            }
        }
    }
}

impl Error for TlsTransportStartError {}

/// One or more owned worker threads panicked while joining the runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct TlsTransportJoinError {
    panicked_workers: Vec<String>,
}

impl TlsTransportJoinError {
    pub(crate) fn new(panicked_workers: Vec<String>) -> Self {
        Self { panicked_workers }
    }

    /// Worker roles that terminated by panic.
    #[must_use]
    pub fn panicked_workers(&self) -> &[String] {
        &self.panicked_workers
    }
}

impl fmt::Display for TlsTransportJoinError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} TLS transport worker(s) panicked: {}",
            self.panicked_workers.len(),
            self.panicked_workers.join(", ")
        )
    }
}

impl Error for TlsTransportJoinError {}
