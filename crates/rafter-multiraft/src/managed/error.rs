use std::{error::Error, fmt};

use super::{DispatchId, WorkId};

/// Stable scheduler identity space was exhausted.
///
/// This enum is exhaustive because pass and dispatch IDs are the complete
/// scheduler identities allocated after admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentityError {
    /// No further pass identity can be assigned.
    PassExhausted,
    /// No further dispatch identity can be assigned.
    DispatchExhausted,
}

/// Why queue admission failed.
///
/// This enum is exhaustive so callers can distinguish an unknown route, each
/// configured queue bound, and permanent identity exhaustion without parsing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdmissionRejection<G> {
    /// The group is not registered.
    UnknownGroup(G),
    /// The selected group has reached its queue bound.
    GroupQueueFull {
        /// Selected group.
        group_id: G,
        /// Configured bound.
        bound: usize,
    },
    /// The host has reached its total queue bound.
    GlobalQueueFull {
        /// Configured bound.
        bound: usize,
    },
    /// No further stable work identity can be assigned.
    WorkIdentityExhausted,
}

/// Refused admission that returns the caller's payload.
#[derive(Debug)]
pub struct AdmissionRejected<G, T> {
    /// Typed refusal reason.
    pub reason: AdmissionRejection<G>,
    /// Payload that took no queue slot.
    pub payload: T,
}

/// A group registration was refused.
///
/// This enum is exhaustive because duplicate ownership is the only refusal
/// after bounds have been validated in [`super::ManagedConfig`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RegisterError<G> {
    /// The group is already registered.
    AlreadyRegistered(G),
}

/// A group availability update was refused.
///
/// This enum is exhaustive because availability is a boolean update whose only
/// precondition is a registered group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GroupStateError<G> {
    /// The group is not registered.
    UnknownGroup(G),
}

/// A group could not be removed from scheduling.
///
/// This enum is exhaustive because queued and in-flight work are the complete
/// scheduler-owned reasons an existing group cannot be removed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RemoveError<G> {
    /// Accepted work remains queued.
    Queued {
        /// Selected group.
        group_id: G,
        /// Remaining queued items.
        items: usize,
    },
    /// A dispatch is still in flight.
    InFlight(G),
}

/// An exact dispatch completion was refused.
///
/// This enum is exhaustive because completion validates scheduler authority,
/// live dispatch identity, item count, and item identity in that order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CompletionError {
    /// The dispatch belongs to another scheduler instance.
    ForeignDispatch(DispatchId),
    /// The dispatch is not in flight.
    UnknownDispatch(DispatchId),
    /// The disposition count differs from the dispatch item count.
    WrongItemCount {
        /// Dispatch being completed.
        dispatch_id: DispatchId,
        /// Required count.
        expected: usize,
        /// Supplied count.
        actual: usize,
    },
    /// A disposition names the wrong work item.
    WrongWork {
        /// Dispatch being completed.
        dispatch_id: DispatchId,
        /// Item identity held by the scheduler.
        expected: WorkId,
        /// Item identity supplied by the caller.
        actual: WorkId,
    },
}

macro_rules! debug_display {
    ($type:ident<$generic:ident>) => {
        impl<$generic: fmt::Debug> fmt::Display for $type<$generic> {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "{self:?}")
            }
        }
        impl<$generic: fmt::Debug> Error for $type<$generic> {}
    };
}

debug_display!(RegisterError<G>);
debug_display!(GroupStateError<G>);
debug_display!(RemoveError<G>);

impl fmt::Display for CompletionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl Error for CompletionError {}

impl fmt::Display for IdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl Error for IdentityError {}
