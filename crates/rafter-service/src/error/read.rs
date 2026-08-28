#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

//! The managed read failure surface.
//!
//! The read half of what [`super::write`] owes a caller, less the fate: a read
//! takes no effect, so nothing here leaves a later outcome to be uncertain
//! about. It renders through [`super`]'s shared helpers and decides nothing
//! about which failures a driver may raise.

use super::*;

/// Errors returned by managed reads.
///
/// A read that fails takes no effect, so there is no [`WriteFate`] here and no
/// later outcome for a client to be uncertain about. Equality is absent for the
/// same reason it is absent on [`WriteError`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ReadError {
    /// The local replica is not the leader required for this read.
    NotLeader {
        /// Current leader, when known.
        leader_hint: Option<NodeId>,
        /// Term in which the replica rejected the read.
        term: Term,
    },
    /// The Raft runtime rejected a linearizable read barrier.
    Rejected {
        /// Reserved read identifier, when reservation completed.
        read_id: Option<ReadId>,
        /// Protocol reason the barrier could not start.
        reason: ReadIndexRejection,
        /// Current leader, when known.
        leader_hint: Option<NodeId>,
    },
    /// An active linearizable read barrier was canceled.
    Canceled {
        /// Identifier of the canceled read.
        read_id: ReadId,
        /// Protocol reason the barrier was canceled.
        reason: ReadIndexCancelReason,
        /// Current leader, when known.
        leader_hint: Option<NodeId>,
    },
    /// The requested consistency mode is not supported by this path.
    UnsupportedConsistency {
        /// Rejected consistency mode.
        consistency: ReadConsistency,
    },
    /// The local state machine cannot prove the requested freshness.
    FreshnessUnavailable {
        /// The local read ID for an abandoned linearizable read. Local
        /// freshness gaps do not consume a read ID and report `None`.
        read_id: Option<ReadId>,
        /// Minimum applied index required by the completed barrier.
        required_applied_index: LogIndex,
        /// Applied index currently reported by the local state machine.
        local_applied_index: LogIndex,
    },
    /// The driver stopped waiting for this barrier and released it.
    ///
    /// The barrier was cancelled through
    /// [`rafter_app::group::RaftGroup::cancel_read`] before this error was
    /// returned, so no local read state leaks and no later step will report an
    /// outcome for `read_id`. That `ReadId` is spent: a retry issues a new
    /// read, and reusing this one is
    /// [`rafter_app::error::GroupError::NonMonotonicReadId`].
    ///
    /// Unlike an abandoned write, an abandoned read has no outcome that can
    /// still occur — a read takes no effect — so this is a terminal error
    /// rather than an unknown outcome. A caller learns nothing about the
    /// queried state, which is the correct result when freshness cannot be
    /// proved.
    Abandoned {
        /// Identifier of the barrier the driver stopped awaiting.
        read_id: ReadId,
        /// Boundary at which the driver abandoned the read.
        reason: ReadAbandonReason,
    },
    /// The request named a group this driver does not own.
    WrongGroup,
    /// The application state machine failed.
    StateMachine {
        /// State-machine callback that failed.
        operation: StateMachineOperation,
        /// Preserved typed application failure.
        cause: ErrorCause,
    },
    /// The Raft runtime failed to persist or query local durable state.
    Storage {
        /// Preserved typed storage failure.
        cause: ErrorCause,
    },
    /// The driver could not route or deliver the work this read required.
    Transport {
        /// Preserved typed transport failure.
        cause: ErrorCause,
    },
    /// The driver refused the read on its own standing rather than on any
    /// failure.
    ///
    /// Both consistency levels are refused for every reason this carries, and
    /// [`DriverUnavailableReason::NotMember`] is why the read side has this
    /// variant at all: a replica the cluster is not replicating to answers a
    /// local read from a view with no bound on how stale it is, and that is the
    /// one refusal a client could not otherwise tell from a fresh answer.
    Unavailable {
        /// Driver condition that refused the read.
        reason: DriverUnavailableReason,
    },
    /// Service shutdown began before the read completed.
    ShuttingDown,
    /// The group is permanently poisoned.
    ///
    /// `cause` is the error that poisoned the group, when the poison came from
    /// a typed failure.
    Poisoned {
        /// Stable explanation retained when the group poisoned.
        reason: String,
        /// Preserved typed cause, when poisoning originated in a callback.
        cause: Option<ErrorCause>,
    },
    /// No read identifier exists above the driver's durable watermark.
    ReadIdExhausted,
    /// The driver violated one of its own documented invariants.
    ///
    /// As on [`WriteError::ManagedInvariantViolation`], this is the one variant
    /// whose message is authored rather than rendered: a driver reporting its
    /// own bug has no underlying error to preserve. There is no fate here
    /// because a read takes no effect.
    ManagedInvariantViolation {
        /// Stable diagnostic for the violated invariant.
        message: String,
    },
}

impl ReadError {
    /// Returns this error's stable category.
    #[must_use]
    pub const fn kind(&self) -> ReadErrorKind {
        match self {
            Self::NotLeader { .. } => ReadErrorKind::NotLeader,
            Self::Rejected { .. } => ReadErrorKind::Rejected,
            Self::Canceled { .. } => ReadErrorKind::Canceled,
            Self::UnsupportedConsistency { .. } => ReadErrorKind::UnsupportedConsistency,
            Self::FreshnessUnavailable { .. } => ReadErrorKind::FreshnessUnavailable,
            Self::Abandoned { .. } => ReadErrorKind::Abandoned,
            Self::WrongGroup => ReadErrorKind::WrongGroup,
            Self::StateMachine { .. } => ReadErrorKind::StateMachine,
            Self::Storage { .. } => ReadErrorKind::Storage,
            Self::Transport { .. } => ReadErrorKind::Transport,
            Self::Unavailable { .. } => ReadErrorKind::Unavailable,
            Self::ShuttingDown => ReadErrorKind::ShuttingDown,
            Self::Poisoned { .. } => ReadErrorKind::Poisoned,
            Self::ReadIdExhausted => ReadErrorKind::ReadIdExhausted,
            Self::ManagedInvariantViolation { .. } => ReadErrorKind::ManagedInvariantViolation,
        }
    }
}

impl fmt::Display for ReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLeader { leader_hint, term } => {
                write!(
                    formatter,
                    "read rejected: this node is not leader in term {term}"
                )?;
                write_leader_hint(formatter, *leader_hint)
            }
            Self::Rejected {
                read_id,
                reason,
                leader_hint,
            } => {
                write!(formatter, "read barrier {read_id:?} rejected: {reason}")?;
                write_leader_hint(formatter, *leader_hint)
            }
            Self::Canceled {
                read_id,
                reason,
                leader_hint,
            } => {
                write!(
                    formatter,
                    "read barrier {read_id} canceled: {}",
                    read_cancel_reason_message(*reason)
                )?;
                write_leader_hint(formatter, *leader_hint)
            }
            Self::UnsupportedConsistency { consistency } => {
                write!(formatter, "unsupported read consistency {consistency:?}")
            }
            Self::FreshnessUnavailable {
                read_id,
                required_applied_index,
                local_applied_index,
            } => write!(
                formatter,
                "read barrier {read_id:?} requires applied index {required_applied_index}, but the local app is at {local_applied_index}"
            ),
            Self::Abandoned { read_id, reason } => write!(
                formatter,
                "read barrier {read_id} was abandoned by the driver: {reason}"
            ),
            Self::WrongGroup => {
                formatter.write_str("read rejected: this driver does not own the requested group")
            }
            Self::StateMachine { operation, .. } => {
                write!(formatter, "read state machine {operation} failed")
            }
            Self::Storage { .. } => formatter.write_str("read storage failed"),
            Self::Transport { .. } => formatter.write_str("read transport failed"),
            Self::Unavailable { reason } => {
                write!(formatter, "read refused by this driver: {reason}")
            }
            Self::ShuttingDown => {
                formatter.write_str("read rejected because the service is shutting down")
            }
            Self::Poisoned { reason, .. } => write!(
                formatter,
                "read rejected because the group is poisoned: {reason}"
            ),
            Self::ReadIdExhausted => {
                formatter.write_str("read rejected because read ids are exhausted")
            }
            Self::ManagedInvariantViolation { message } => {
                write!(formatter, "managed read invariant violation: {message}")
            }
        }
    }
}

impl Error for ReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::StateMachine { cause, .. }
            | Self::Storage { cause }
            | Self::Transport { cause } => Some(cause.as_error()),
            Self::Poisoned { cause, .. } => match cause {
                Some(cause) => Some(cause.as_error()),
                None => None,
            },
            Self::NotLeader { .. }
            | Self::Rejected { .. }
            | Self::Canceled { .. }
            | Self::UnsupportedConsistency { .. }
            | Self::FreshnessUnavailable { .. }
            | Self::Abandoned { .. }
            | Self::WrongGroup
            | Self::Unavailable { .. }
            | Self::ShuttingDown
            | Self::ReadIdExhausted
            | Self::ManagedInvariantViolation { .. } => None,
        }
    }
}
