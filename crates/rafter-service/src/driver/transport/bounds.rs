#![allow(clippy::wildcard_imports)]

//! What one driver is allowed to accumulate before it refuses.
//!
//! Its own file because the two bounds here answer to different pressures and
//! neither is about the driver's public operations: one caps client waiters, and
//! one decides when a link layer has fallen far enough behind that this driver
//! should stop taking client work at all. `transport.rs` states what a driver
//! *does*; this states what it will not let itself become.

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
    /// How many committed removals may be waiting on the link layer before this
    /// driver stops admitting client work.
    ///
    /// Defaults to 1024, the same order as
    /// [`TransportDriverOptions::max_pending_waiters`] and far above any real
    /// group's committed-removal count — a deployment that reaches it has a link
    /// layer that has refused every fence for the life of the group.
    ///
    /// **A service threshold, and the name now says so.** This was called
    /// `max_pending_fences` and documented as a bound, and it is neither: a fence
    /// obligation comes from a committed fact, and a committed fact is not a
    /// request, so there is nothing to refuse and nowhere to push back.
    /// Discarding an obligation on overflow would be exactly the forgotten fence
    /// the peer control plane exists to prevent, with a capacity limit attached
    /// as an excuse — so the structure deliberately grows past this number while
    /// the old name promised it would not.
    ///
    /// What it decides is when a driver whose link layer has stopped accepting
    /// admission controls should stop accepting client work. Past it the driver
    /// keeps stepping, keeps flushing, and refuses new writes and reads with
    /// [`DriverServiceState::FenceBacklog`], which ends by itself as soon as the
    /// backlog is back under the threshold.
    ///
    /// **Retention is the embedder's, explicitly.** The backlog grows by one
    /// [`NodeId`] per committed removal the link layer has not fenced, and
    /// nothing here ever discards one. An embedder that persists
    /// [`crate::PeerControlPlaneCheckpoint`] owns that set across restarts and
    /// owns any policy over it; this crate has none, because there is no correct
    /// one it could pick — an obligation dropped is an authorization the cluster
    /// retracted and nobody enforced.
    pub fence_backlog_service_threshold: usize,
}

impl TransportDriverOptions {
    /// Returns the shipped defaults.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_pending_waiters: 1024,
            fence_backlog_service_threshold: 1024,
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

    /// Sets [`TransportDriverOptions::fence_backlog_service_threshold`].
    #[must_use]
    pub const fn with_fence_backlog_service_threshold(mut self, threshold: usize) -> Self {
        self.fence_backlog_service_threshold = threshold;
        self
    }

    /// Fails closed on a bound that would make an operation impossible.
    ///
    /// Zero is meaningless rather than merely small: a driver that admits no
    /// waiters refuses every write, which is a driver that cannot serve
    /// anything, discovered at the first request. A driver whose fence backlog
    /// may not reach one is the same shape one configuration change later — the
    /// first committed removal would put it permanently past its threshold — so
    /// both are answered here rather than at the change.
    pub(super) fn validate(self) -> Result<Self, ManagedDriverError> {
        if self.max_pending_waiters == 0 {
            return Err(ManagedDriverError::InvalidOptions {
                field: "max_pending_waiters",
                reason: "a driver that admits no waiters cannot serve any operation",
            });
        }
        if self.fence_backlog_service_threshold == 0 {
            return Err(ManagedDriverError::InvalidOptions {
                field: "fence_backlog_service_threshold",
                reason: "a driver that stops serving at its first unfenced removal \
                         is degraded by its first committed removal",
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
