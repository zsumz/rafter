//! Group-driver abstraction for many-group hosts.

use std::{error::Error, fmt};

use rafter_app::{
    error::ErrorCause,
    group::{GroupInput, GroupStepReport},
    metrics::RaftGroupMetrics,
};

/// Why a group driver could not complete a step.
///
/// This kind answers **permanence, and nothing else**: may this group be
/// stepped again, or is it finished? It is deliberately not a statement about
/// what a failed proposal's fate was. That question is answered per proposal by
/// [`rafter_app::proposal::ProposalEvent`] in the step report, and a second,
/// coarser answer here would be a place for the two to disagree.
///
/// New kinds are additive. A caller aggregating by kind keeps a bucket for
/// kinds it does not recognize rather than dropping them.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum DriverErrorKind {
    /// The group will never make progress again. Retire it with
    /// [`crate::MultiRaftHost::remove_group`].
    Poisoned,
    /// The step failed and the group has not declared itself permanently
    /// unusable.
    ///
    /// This is the *absence* of a poison, not a promise that a retry succeeds.
    Transient,
}

impl DriverErrorKind {
    /// Whether this failure retires the group.
    ///
    /// Written as a positive test for [`DriverErrorKind::Poisoned`] so a kind
    /// a caller does not recognize reads as *not* permanent. That is the safe
    /// direction: continuing to tick a group that is finished wastes a
    /// scheduling opportunity, while retiring one that is not destroys a
    /// driver that still owns committed state.
    #[must_use]
    pub const fn is_permanent(self) -> bool {
        matches!(self, Self::Poisoned)
    }
}

/// A group driver's failure: how permanent it is, and what actually failed.
///
/// A Rafter error names a stable category; the cause names what actually
/// failed. Rendering the cause into the category loses the second and does not
/// improve the first, which is what this type replaced — the driver traits used
/// to return `String`, and the crate's own blanket implementation produced it
/// by `Debug`-formatting a [`rafter_app::error::GroupError`] with twenty typed
/// variants and a `source()` chain.
///
/// Equality is deliberately absent: an error carrying a `dyn Error` has no
/// honest equality, and comparing rendered output would rebuild exactly the
/// stringly-typed semantics this type exists to remove.
#[derive(Clone, Debug)]
pub struct DriverError {
    kind: DriverErrorKind,
    cause: ErrorCause,
}

impl DriverError {
    /// Reports a driver failure of `kind`, preserving `cause`.
    #[must_use]
    pub const fn new(kind: DriverErrorKind, cause: ErrorCause) -> Self {
        Self { kind, cause }
    }

    /// How permanent this failure is.
    #[must_use]
    pub const fn kind(&self) -> DriverErrorKind {
        self.kind
    }

    /// The preserved error, from which a caller recovers its own type with
    /// [`ErrorCause::downcast_ref`].
    #[must_use]
    pub const fn cause(&self) -> &ErrorCause {
        &self.cause
    }

    /// Takes the preserved error.
    #[must_use]
    pub fn into_cause(self) -> ErrorCause {
        self.cause
    }
}

impl fmt::Display for DriverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            DriverErrorKind::Poisoned => {
                write!(formatter, "group driver is poisoned: {}", self.cause)
            }
            DriverErrorKind::Transient => {
                write!(formatter, "group driver failed its step: {}", self.cause)
            }
        }
    }
}

impl Error for DriverError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.cause.as_error())
    }
}

/// Object-safe driver surface used by [`crate::host::MultiRaftHost`].
pub trait GroupDriver<G>: fmt::Debug {
    /// Steps one group input and returns explicit side effects.
    ///
    /// # Errors
    ///
    /// Returns a [`DriverError`] carrying the permanence the driver observed
    /// and the typed error that caused it. An implementation reports
    /// [`DriverErrorKind::Poisoned`] only when it observed that the group is
    /// finished — never because a category implies it.
    fn step(
        &mut self,
        input: GroupInput<G, Vec<u8>>,
    ) -> Result<GroupStepReport<G, Vec<u8>>, DriverError>;

    fn metrics(&self) -> RaftGroupMetrics<G>;
}
