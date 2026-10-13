//! Independent write and read limits for one owned backend.

use crate::durable::DurableWriteLimits;
use rafter_runtime::service::ServiceOptions;
use std::num::NonZeroUsize;

/// Bounds spanning queued queries, quorum ownership, and uncollected results.
#[derive(Clone, Copy, Debug)]
pub struct DurableReadLimits {
    /// Maximum retained queries, including canceled rounds still owned by Raft.
    pub reads: NonZeroUsize,
    /// Maximum sum of encoded query `Vec` capacities until result collection.
    /// Arbitrary query results and the configured query adapter are not bounded
    /// by this value. Bound their sizes in the application.
    pub payload_bytes: NonZeroUsize,
}

/// Current retained query credits; the counters are observational.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableReadStats {
    /// Queries retained by the owner or their uncollected result futures.
    pub reads: usize,
    /// Encoded query capacities charged to those queries.
    pub payload_bytes: usize,
}

/// Worker and client limits for a read-capable durable owner.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct DurableClientOptions {
    /// Backend persistence, application, and dispatch options.
    pub service: ServiceOptions,
    /// Bounded encoded writes and their results.
    pub writes: DurableWriteLimits,
    /// Bounded encoded reads and their results.
    pub reads: DurableReadLimits,
}
impl DurableClientOptions {
    /// Configures independently bounded write and read admission.
    #[must_use]
    pub const fn new(
        service: ServiceOptions,
        writes: DurableWriteLimits,
        reads: DurableReadLimits,
    ) -> Self {
        Self {
            service,
            writes,
            reads,
        }
    }
}
