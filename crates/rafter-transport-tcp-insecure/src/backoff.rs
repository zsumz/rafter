//! Bounded reconnect policy for outbound demo connections.
//!
//! This module owns how many connect attempts a send may make and how long it
//! waits between them. It performs no I/O of its own: the caller supplies the
//! connect and sleep effects, which is what lets the policy be driven without
//! a socket.

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use rafter::NodeId;

use crate::TcpTransportError;

/// Bounded reconnect policy for outbound TCP sends.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReconnectBackoff {
    /// Total connection attempts, including the initial attempt.
    pub max_attempts: usize,
    /// Delay before the first reconnect attempt.
    pub initial_delay: Duration,
    /// Maximum delay between successive reconnect attempts.
    pub max_delay: Duration,
}

impl Default for ReconnectBackoff {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(250),
        }
    }
}

impl ReconnectBackoff {
    /// Returns a policy that tries exactly once.
    #[must_use]
    pub const fn once() -> Self {
        Self {
            max_attempts: 1,
            initial_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
        }
    }
}

pub(super) fn connect_with_backoff_using<T>(
    peer: NodeId,
    addr: SocketAddr,
    backoff: ReconnectBackoff,
    mut connect: impl FnMut(SocketAddr) -> io::Result<T>,
    mut sleep: impl FnMut(Duration),
) -> Result<T, TcpTransportError> {
    let attempts = backoff.max_attempts.max(1);
    let mut delay = backoff.initial_delay;
    let mut last_error = None;
    for attempt in 0..attempts {
        match connect(addr) {
            Ok(stream) => return Ok(stream),
            Err(error) => {
                last_error = Some(error);
                if attempt + 1 < attempts && !delay.is_zero() {
                    sleep(delay);
                    delay = delay.saturating_mul(2).min(backoff.max_delay);
                }
            }
        }
    }
    let Some(source) = last_error else {
        return Err(TcpTransportError::Connect {
            peer,
            source: io::Error::other("no TCP connect attempts were made"),
        });
    };
    Err(TcpTransportError::Connect { peer, source })
}
