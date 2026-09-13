//! Owned application completion and failure events.

use rafter::LogIndex;

/// One durably applied batch returned to the service owner.
#[derive(Debug)]
pub struct ApplicationCompletion<T, O> {
    pub(super) entries: Vec<T>,
    pub(super) outcomes: Vec<O>,
    pub(super) retained_bytes: usize,
}

impl<T, O> ApplicationCompletion<T, O> {
    /// Returns the durably applied entries in log order.
    #[must_use]
    pub fn entries(&self) -> &[T] {
        &self.entries
    }

    /// Returns one application outcome per entry.
    #[must_use]
    pub fn outcomes(&self) -> &[O] {
        &self.outcomes
    }

    /// Splits the owned entries from their corresponding outcomes.
    #[must_use]
    pub fn into_parts(self) -> (Vec<T>, Vec<O>) {
        (self.entries, self.outcomes)
    }
}

/// Why a worker stopped after attempting application durability.
#[derive(Debug)]
#[non_exhaustive]
pub enum ApplicationFailureKind<E> {
    /// The application store returned an error. Its durability disposition is
    /// application-defined, so recovery is required before retrying.
    Store(E),
    /// A successful store call returned the wrong number of outcomes.
    OutcomeCount {
        /// Number of applied entries.
        expected: usize,
        /// Number of returned outcomes.
        actual: usize,
    },
    /// A successful store call did not publish the final batch index.
    DurableFloor {
        /// Final applied index required by the batch.
        expected: LogIndex,
        /// Durable floor reported by the application after success.
        actual: LogIndex,
    },
}

/// The attempted batch and all accepted work not attempted after a failure.
#[derive(Debug)]
pub struct ApplicationFailure<T, E> {
    pub(super) attempted: Vec<T>,
    pub(super) unattempted: Vec<T>,
    pub(super) kind: ApplicationFailureKind<E>,
    pub(super) retained_bytes: usize,
}

impl<T, E> ApplicationFailure<T, E> {
    /// Returns the batch whose durable disposition must be recovered.
    #[must_use]
    pub fn attempted(&self) -> &[T] {
        &self.attempted
    }

    /// Returns accepted entries that the stopped worker did not apply.
    #[must_use]
    pub fn unattempted(&self) -> &[T] {
        &self.unattempted
    }

    /// Returns the failure classification.
    #[must_use]
    pub const fn kind(&self) -> &ApplicationFailureKind<E> {
        &self.kind
    }

    /// Splits all owned work from the failure classification.
    #[must_use]
    pub fn into_parts(self) -> (Vec<T>, Vec<T>, ApplicationFailureKind<E>) {
        (self.attempted, self.unattempted, self.kind)
    }
}

/// One worker result, consumed in original application order.
#[derive(Debug)]
#[non_exhaustive]
pub enum ApplicationEvent<T, O, E> {
    /// One batch crossed the durable application fence.
    Applied(ApplicationCompletion<T, O>),
    /// The worker stopped; the live service must recover before more input.
    Failed(ApplicationFailure<T, E>),
}

impl<T, O, E> ApplicationEvent<T, O, E> {
    pub(super) const fn retained_bytes(&self) -> usize {
        match self {
            Self::Applied(completion) => completion.retained_bytes,
            Self::Failed(failure) => failure.retained_bytes,
        }
    }

    pub(super) fn entry_count(&self) -> usize {
        match self {
            Self::Applied(completion) => completion.entries.len(),
            Self::Failed(failure) => failure.attempted.len() + failure.unattempted.len(),
        }
    }
}
