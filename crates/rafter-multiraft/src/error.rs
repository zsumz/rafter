//! Error types for many-group hosts.

use std::{error::Error, fmt};

use rafter_app::error::ErrorCause;

use crate::driver::DriverErrorKind;

/// Stable, payload-free category of a [`MultiRaftError`].
///
/// `MultiRaftError<G>` is generic over a caller-defined group key and carries
/// those keys in its payloads, so it is neither a bounded metric label nor a
/// map key: a host running thousands of groups would produce one label per
/// group. This is the projection to aggregate by — `Copy`, totally ordered,
/// hashable, and free of payload.
///
/// New categories are additive. A caller that aggregates by kind keeps a
/// bucket for kinds it does not recognize rather than dropping them.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum MultiRaftErrorKind {
    /// A key was opened twice.
    GroupAlreadyOpen,
    /// A key is not open, either because it was retired or because it never
    /// existed.
    UnknownGroup,
    /// A group ID that had to match the host key did not, and nothing was
    /// stepped.
    WrongGroup,
    /// A driver stepped and then returned a report naming another group.
    InvalidReport,
    /// A driver returned a variant this host does not recognize.
    UnrecognizedEvent,
    /// A driver failed permanently.
    DriverPoisoned,
    /// A driver failed without declaring itself finished.
    DriverTransient,
}

/// Errors returned by a many-group host.
///
/// Equality is deliberately absent. An error carrying a `dyn Error` has no
/// honest equality: comparing `Arc` pointers makes two errors built from the
/// same failure unequal, and comparing rendered output rebuilds the
/// stringly-typed semantics this surface exists to remove. Branch on
/// [`MultiRaftError::kind`] instead, which is what a caller should have been
/// branching on all along.
///
/// This enum is `#[non_exhaustive]`: a host gains failure modes as it gains
/// responsibilities, and the projection above is the stable thing to match on.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum MultiRaftError<G> {
    /// A driver is already registered under `group_id`.
    GroupAlreadyOpen {
        /// The key that is already taken.
        group_id: G,
    },
    /// No driver is registered under `group_id`.
    ///
    /// A retired key and a key that never existed answer identically; this
    /// host keeps no tombstones. See
    /// [`crate::MultiRaftHost::remove_group`].
    UnknownGroup {
        /// The key that is not open.
        group_id: G,
    },
    /// A group ID that had to match the host key did not, and **nothing was
    /// stepped**.
    ///
    /// Raised for a caller's input that names another group, and for a driver
    /// whose metrics snapshot names another group. In both cases no driver
    /// mutated and no effect occurred — which is what separates this from
    /// [`MultiRaftError::InvalidReport`].
    WrongGroup {
        /// The host key the input or driver was routed under.
        expected: G,
        /// The group the input or driver named instead.
        actual: G,
    },
    /// The driver stepped, and then returned a report naming another group.
    ///
    /// **The driver has already mutated itself**; whatever the report
    /// described has happened. The report is discarded rather than returned,
    /// because a driver that cannot name its own group has forfeited the
    /// contract that made its `applied` list mean anything. That is a real
    /// loss of committed effects, it is not silent, and the repair is to
    /// retire the group with [`crate::MultiRaftHost::remove_group`].
    InvalidReport {
        /// The host key that was stepped.
        group_id: G,
        /// The report field that carried the wrong group.
        field: &'static str,
        /// The group that field named.
        reported: G,
    },
    /// The report carried a `#[non_exhaustive]` variant this host does not
    /// recognize.
    ///
    /// The host failed to understand the report; the driver did nothing wrong.
    /// As with [`MultiRaftError::InvalidReport`], the driver has already
    /// stepped.
    UnrecognizedEvent {
        /// The host key that was stepped.
        group_id: G,
        /// The report field that carried the unrecognized variant.
        field: &'static str,
    },
    /// The group's driver could not complete the step.
    ///
    /// `kind` and `cause` are carried flat rather than as a nested
    /// [`crate::DriverError`] so that [`Error::source`] walks one link per
    /// real failure: this error, then the preserved driver error, then
    /// whatever that error's own source was.
    Driver {
        /// The host key that was stepped.
        group_id: G,
        /// How permanent the driver said the failure is.
        kind: DriverErrorKind,
        /// The preserved error, from which a caller recovers its own type with
        /// [`ErrorCause::downcast_ref`].
        cause: ErrorCause,
    },
}

impl<G> MultiRaftError<G> {
    /// This error's stable category.
    #[must_use]
    pub const fn kind(&self) -> MultiRaftErrorKind {
        match self {
            Self::GroupAlreadyOpen { .. } => MultiRaftErrorKind::GroupAlreadyOpen,
            Self::UnknownGroup { .. } => MultiRaftErrorKind::UnknownGroup,
            Self::WrongGroup { .. } => MultiRaftErrorKind::WrongGroup,
            Self::InvalidReport { .. } => MultiRaftErrorKind::InvalidReport,
            Self::UnrecognizedEvent { .. } => MultiRaftErrorKind::UnrecognizedEvent,
            Self::Driver { kind, .. } => match kind {
                DriverErrorKind::Poisoned => MultiRaftErrorKind::DriverPoisoned,
                DriverErrorKind::Transient => MultiRaftErrorKind::DriverTransient,
            },
        }
    }
}

impl<G> fmt::Display for MultiRaftError<G>
where
    G: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GroupAlreadyOpen { group_id } => {
                write!(formatter, "group {group_id:?} is already open")
            }
            Self::UnknownGroup { group_id } => {
                write!(formatter, "group {group_id:?} is not open")
            }
            Self::WrongGroup { expected, actual } => write!(
                formatter,
                "group {actual:?} was routed under host key {expected:?}; nothing was stepped"
            ),
            Self::InvalidReport {
                group_id,
                field,
                reported,
            } => write!(
                formatter,
                "group {group_id:?} stepped and then reported group {reported:?} in `{field}`; \
                 its report was discarded"
            ),
            Self::UnrecognizedEvent { group_id, field } => write!(
                formatter,
                "group {group_id:?} stepped and then reported a variant in `{field}` that this \
                 host does not recognize; its report was discarded"
            ),
            Self::Driver {
                group_id,
                kind,
                cause,
            } => match kind {
                DriverErrorKind::Poisoned => {
                    write!(formatter, "group {group_id:?} is poisoned: {cause}")
                }
                DriverErrorKind::Transient => {
                    write!(formatter, "group {group_id:?} failed its step: {cause}")
                }
            },
        }
    }
}

impl<G> Error for MultiRaftError<G>
where
    G: fmt::Debug,
{
    /// Transparent to the preserved cause: a chain printer walks one link per
    /// real failure rather than one per boundary crossed.
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Driver { cause, .. } => Some(cause.as_error()),
            Self::GroupAlreadyOpen { .. }
            | Self::UnknownGroup { .. }
            | Self::WrongGroup { .. }
            | Self::InvalidReport { .. }
            | Self::UnrecognizedEvent { .. } => None,
        }
    }
}

/// A refused `open_group`, carrying the caller's driver back.
///
/// A driver owns a runtime, a state machine, and open storage handles.
/// Destroying one because the caller passed the wrong key is a data-loss bug
/// wearing a validation error's clothes, so the refusal hands it back — the
/// same reason `std::sync::mpsc::SendError` and `String::from_utf8` return the
/// value they could not accept.
#[derive(Debug)]
pub struct OpenGroupRejected<G, D> {
    /// Why the open was refused.
    pub error: MultiRaftError<G>,
    /// The driver the caller passed in, unmodified.
    pub driver: D,
}

impl<G, D> OpenGroupRejected<G, D> {
    /// Takes the driver back.
    #[must_use]
    pub fn into_driver(self) -> D {
        self.driver
    }

    /// Why the open was refused.
    #[must_use]
    pub const fn error(&self) -> &MultiRaftError<G> {
        &self.error
    }

    /// The refusal's stable category.
    #[must_use]
    pub const fn kind(&self) -> MultiRaftErrorKind {
        self.error.kind()
    }
}

impl<G, D> fmt::Display for OpenGroupRejected<G, D>
where
    G: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.error, formatter)
    }
}

impl<G, D> Error for OpenGroupRejected<G, D>
where
    G: fmt::Debug + 'static,
    D: fmt::Debug,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.error)
    }
}
