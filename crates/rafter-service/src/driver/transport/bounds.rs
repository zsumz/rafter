#![allow(clippy::wildcard_imports)]

//! What one driver is allowed to accumulate before it refuses.
//!
//! Its own file because the bound here is not about the driver's public
//! operations: it caps client waiters, so that a driver whose transport is down
//! fails closed rather than growing. `transport.rs` states what a driver *does*;
//! this states what it will not let itself become.
//!
//! **A second bound used to sit beside it** — a service threshold on the fence
//! backlog, which decided when a link layer that had stopped accepting admission
//! controls should stop being given client work. It left with the backlog. A
//! driver's retirement statement is now a floor republished from state it still
//! holds, so there is no queue to grow, nothing to threshold, and no degraded
//! state to leave.

use super::super::*;

/// Bounds on one driver's local work.
///
/// Every bound is a refusal rather than an unbounded wait, so a stalled
/// protocol surfaces as a typed error instead of a hang.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct TransportDriverOptions {
    /// Refuses to enqueue more than this many unresolved client waiters of
    /// each kind, so a driver whose transport is down fails closed rather than
    /// growing.
    ///
    /// Defaults to 1024. The transport contract already requires bounded
    /// queues of a transport; a driver that buffered without a bound would
    /// move the unbounded growth one layer up.
    ///
    /// A waiter stops counting the moment it resolves, including when
    /// [`TransportRaftDriver::abandon_write`] or
    /// [`TransportRaftDriver::abandon_read`] resolves it, so a caller that stops
    /// waiting gets its slot back without waiting for the client to poll.
    pub max_pending_waiters: usize,
}

impl TransportDriverOptions {
    /// Returns the shipped defaults.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_pending_waiters: 1024,
        }
    }

    /// Sets [`TransportDriverOptions::max_pending_waiters`].
    ///
    /// A setter rather than struct-update syntax, because the type is
    /// `#[non_exhaustive]`: an embedder outside this crate cannot name every
    /// field, and a later field must not break their construction.
    #[must_use]
    pub const fn with_max_pending_waiters(mut self, max_pending_waiters: usize) -> Self {
        self.max_pending_waiters = max_pending_waiters;
        self
    }

    /// Fails closed on a bound that would make an operation impossible.
    ///
    /// Zero is meaningless rather than merely small: a driver that admits no
    /// waiters refuses every write, which is a driver that cannot serve
    /// anything, discovered at the first request.
    pub(super) fn validate(self) -> Result<Self, ManagedDriverError> {
        if self.max_pending_waiters == 0 {
            return Err(ManagedDriverError::InvalidOptions {
                field: "max_pending_waiters",
                reason: "a driver that admits no waiters cannot serve any operation",
            });
        }
        Ok(self)
    }
}

impl Default for TransportDriverOptions {
    fn default() -> Self {
        Self::new()
    }
}
