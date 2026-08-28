#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

//! What can go wrong around a managed group rather than inside one.
//!
//! Opening a metrics watch and shutting a service down are lifecycle operations,
//! not protocol ones: neither proposes anything, neither reads the state
//! machine, and neither can fail for a reason outside the driver. That is why
//! both types here stay this small, and why one of them keeps equality.

use super::*;

/// Errors returned while opening a managed metrics watch.
///
/// This one keeps `Copy` and equality, unlike its siblings: it carries no
/// cause, so there is nothing whose equality would be dishonest. Opening a
/// watch reads driver-local state and cannot fail for any reason outside the
/// driver, which is why the type stays this small.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MetricsError {
    /// The request named a group this driver does not own.
    WrongGroup,
}

impl fmt::Display for MetricsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongGroup => formatter.write_str("metrics watch targets the wrong group"),
        }
    }
}

impl Error for MetricsError {}

/// Errors returned by managed service shutdown.
///
/// None of these leaves a waiter pending. A driver that refuses shutdown either
/// never owned the group or had already released its waiters, so a caller that
/// sees one of these has nothing left to drain.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ShutdownError {
    /// The request named a group this driver does not own.
    WrongGroup,
    /// The driver could not route or deliver the work shutdown required.
    Transport {
        /// Preserved typed transport failure.
        cause: ErrorCause,
    },
    /// Shutdown had already completed.
    ///
    /// Reported rather than succeeding again so a supervisor can tell "I
    /// stopped this" from "it was already stopped". Shutdown is terminal: a
    /// driver that has shut down refuses every operation, including adoption.
    AlreadyShutDown,
}

impl ShutdownError {
    /// Projects this error to its stable category.
    ///
    /// The label to aggregate on. See [`WriteError::kind`] for why the variant
    /// itself is not one.
    #[must_use]
    pub const fn kind(&self) -> ShutdownErrorKind {
        match self {
            Self::WrongGroup => ShutdownErrorKind::WrongGroup,
            Self::Transport { .. } => ShutdownErrorKind::Transport,
            Self::AlreadyShutDown => ShutdownErrorKind::AlreadyShutDown,
        }
    }
}

impl fmt::Display for ShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongGroup => {
                formatter.write_str("shutdown targets a group this driver does not own")
            }
            Self::Transport { .. } => formatter.write_str("shutdown transport failed"),
            Self::AlreadyShutDown => formatter.write_str("service is already shut down"),
        }
    }
}

impl Error for ShutdownError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Transport { cause } => Some(cause.as_error()),
            Self::WrongGroup | Self::AlreadyShutDown => None,
        }
    }
}
