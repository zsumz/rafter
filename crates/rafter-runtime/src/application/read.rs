//! Verified application views and ownership-preserving read refusals.

use rafter::LogIndex;
use std::{error::Error, fmt};

/// An owned query result and the verified durable floor of its application view.
#[derive(Debug)]
pub struct ApplicationRead<R> {
    pub(super) durable_through: LogIndex,
    pub(super) output: R,
}

impl<R> ApplicationRead<R> {
    /// Returns the durable application floor observed under the same borrow.
    #[must_use]
    pub const fn durable_through(&self) -> LogIndex {
        self.durable_through
    }

    /// Returns the query result.
    #[must_use]
    pub const fn output(&self) -> &R {
        &self.output
    }

    /// Returns ownership of the query result.
    #[must_use]
    pub fn into_output(self) -> R {
        self.output
    }
}

/// Why a query was returned without being invoked.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ApplicationReadRejection {
    /// The application is currently being updated or validated.
    Busy,
    /// The verified application state has not reached the requested floor.
    Behind {
        /// Minimum application floor required by the caller.
        required: LogIndex,
        /// Current verified durable application floor.
        durable_through: LogIndex,
    },
    /// The worker failed, panicked, or was shut down; recovery is required.
    Stopped,
}

/// A refused read and its unchanged callback, including captured query inputs.
pub struct ApplicationReadError<Q> {
    pub(super) query: Q,
    pub(super) rejection: ApplicationReadRejection,
}

impl<Q> ApplicationReadError<Q> {
    /// Returns why the query was not invoked.
    #[must_use]
    pub const fn rejection(&self) -> ApplicationReadRejection {
        self.rejection
    }

    /// Returns the original callback for retry or disposal by its owner.
    #[must_use]
    pub fn into_query(self) -> Q {
        self.query
    }
}

impl<Q> fmt::Debug for ApplicationReadError<Q> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApplicationReadError")
            .field("rejection", &self.rejection)
            .finish_non_exhaustive()
    }
}
impl<Q> fmt::Display for ApplicationReadError<Q> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "application read was refused: {:?}",
            self.rejection
        )
    }
}
impl<Q> Error for ApplicationReadError<Q> {}
