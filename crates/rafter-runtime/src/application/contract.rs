//! Application and worker limit contracts.

use rafter::LogIndex;
use std::error::Error;

/// One committed item whose durable application is ordered by log index.
pub trait ApplicationEntry: Send + 'static {
    /// Returns the Raft log index represented by this item.
    ///
    /// This value must remain stable for the item's lifetime.
    fn log_index(&self) -> LogIndex;

    /// Returns a conservative bound for memory retained through completion.
    ///
    /// Include the entry, its eventual outcome, and referenced or heap-backed
    /// bytes. The worker uses this application-supplied value for admission; it
    /// cannot discover allocations hidden behind an arbitrary entry type.
    /// This value must remain stable for the item's lifetime.
    fn retained_bytes(&self) -> usize;

    /// Returns the application's work-size estimate for batch construction.
    ///
    /// This can be smaller than [`Self::retained_bytes`] when an outcome or
    /// other owner metadata remains live through completion but does not add
    /// bytes to one storage operation. The default preserves the simpler
    /// single-estimate contract. This value must remain stable for the item's
    /// lifetime.
    fn batch_bytes(&self) -> usize {
        self.retained_bytes()
    }
}

/// Durable application state owned by an [`super::ApplicationWorker`].
pub trait DurableApplication<T>: Send + 'static
where
    T: ApplicationEntry,
{
    /// One result corresponding to one applied entry.
    type Outcome: Send + 'static;

    /// A durable application failure.
    type Error: Error + Send + 'static;

    /// Returns the application's authoritative durable applied floor.
    fn applied_through(&self) -> LogIndex;

    /// Applies one nonempty contiguous batch in order.
    ///
    /// `Ok` must mean both the application bytes and the final applied floor
    /// are durable as one crash-consistency unit. The returned vector must have
    /// exactly one outcome per entry. An error has an application-defined
    /// durability disposition; the live worker stops and requires recovery.
    ///
    /// # Errors
    ///
    /// Returns the application-defined durability failure. The worker treats
    /// every error as fatal to the live owner and returns all accepted work.
    fn apply(&mut self, entries: &[T]) -> Result<Vec<Self::Outcome>, Self::Error>;
}

/// Entry, retained-byte, and application-batch limits for one worker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ApplicationWorkerOptions {
    pub(super) inflight_entries: usize,
    pub(super) inflight_bytes: usize,
    pub(super) batch_entries: usize,
    pub(super) batch_bytes: usize,
}

impl ApplicationWorkerOptions {
    /// Returns bounded defaults used by the reference durable service.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            inflight_entries: 4_096,
            inflight_bytes: 16 * 1024 * 1024,
            batch_entries: 64,
            batch_bytes: 256 * 1024,
        }
    }

    /// Replaces all queue and per-apply batch limits.
    ///
    /// `inflight_bytes` bounds memory retained through event consumption;
    /// `batch_bytes` bounds the application work represented by one call to
    /// [`DurableApplication::apply`]. Each uses its matching estimate from
    /// [`ApplicationEntry`].
    #[must_use]
    pub const fn with_limits(
        mut self,
        inflight_entries: usize,
        inflight_bytes: usize,
        batch_entries: usize,
        batch_bytes: usize,
    ) -> Self {
        self.inflight_entries = inflight_entries;
        self.inflight_bytes = inflight_bytes;
        self.batch_entries = batch_entries;
        self.batch_bytes = batch_bytes;
        self
    }

    pub(super) fn validate(self) -> std::io::Result<Self> {
        if self.inflight_entries == 0
            || self.inflight_bytes == 0
            || self.batch_entries == 0
            || self.batch_bytes == 0
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "application worker limits must be nonzero",
            ));
        }
        if self.batch_entries > self.inflight_entries {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "application batch entry limit exceeds inflight entry limit",
            ));
        }
        Ok(self)
    }
}

impl Default for ApplicationWorkerOptions {
    fn default() -> Self {
        Self::new()
    }
}
