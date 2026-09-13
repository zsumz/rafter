//! Nonblocking application admission errors that return entry ownership.

use rafter::LogIndex;
use std::{error::Error, fmt};

/// Why an application batch was refused before worker ownership transferred.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ApplicationSubmitRejection {
    /// An application batch must contain at least one entry.
    Empty,
    /// The first or a subsequent log index was not the required next index.
    NonContiguous {
        /// Index required at the refused position.
        expected: LogIndex,
        /// Index supplied at the refused position.
        actual: LogIndex,
    },
    /// No log index exists after the worker's accepted boundary.
    IndexExhausted {
        /// Last accepted or submitted index.
        after: LogIndex,
    },
    /// One batch exceeded the configured per-apply entry or byte limit.
    BatchTooLarge {
        /// Submitted entry count.
        entries: usize,
        /// Submitted retained-byte estimate.
        bytes: usize,
        /// Configured per-apply entry limit.
        max_entries: usize,
        /// Configured per-apply byte limit.
        max_bytes: usize,
    },
    /// Accepted work still owns too many entry or byte credits.
    Full {
        /// Currently available entry credits.
        available_entries: usize,
        /// Currently available byte credits.
        available_bytes: usize,
    },
    /// The worker no longer accepts application work.
    Stopped,
}

/// A refused submission and the exact entries retained by the caller.
pub struct ApplicationSubmitError<T> {
    pub(super) entries: Vec<T>,
    pub(super) rejection: ApplicationSubmitRejection,
}

impl<T> ApplicationSubmitError<T> {
    /// Returns why the batch was refused.
    #[must_use]
    pub const fn rejection(&self) -> ApplicationSubmitRejection {
        self.rejection
    }

    /// Returns the refused entries.
    #[must_use]
    pub fn entries(&self) -> &[T] {
        &self.entries
    }

    /// Returns ownership of every refused entry.
    #[must_use]
    pub fn into_entries(self) -> Vec<T> {
        self.entries
    }
}

impl<T> fmt::Debug for ApplicationSubmitError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApplicationSubmitError")
            .field("entry_count", &self.entries.len())
            .field("rejection", &self.rejection)
            .finish()
    }
}

impl<T> fmt::Display for ApplicationSubmitError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "application batch with {} entries was refused: {:?}",
            self.entries.len(),
            self.rejection
        )
    }
}

impl<T> Error for ApplicationSubmitError<T> {}
