//! The managed write failure surface.
//!
//! One type for every way a write can fail, and it owes the caller three
//! separable answers: the category it projects to, the fate it proves, and the
//! preserved cause. It renders through [`super`]'s shared helpers so a write, a
//! read, and a transfer report a leader hint identically, and it decides nothing
//! about which failures a driver may raise.

#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

use super::*;

/// Errors returned by managed writes.
///
/// Equality is deliberately absent. An error carrying a `dyn Error` has no
/// honest equality: comparing `Arc` pointers makes two errors built from the
/// same failure unequal, and comparing rendered output rebuilds the
/// stringly-typed semantics this surface exists to remove. `Clone` is kept,
/// because one failure fans out to every entry of a write batch.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum WriteError {
    /// The local replica is not the leader and did not append the command.
    NotLeader {
        /// Current leader, when known.
        leader_hint: Option<NodeId>,
        /// Term in which the replica rejected the write.
        term: Term,
    },
    /// The Raft runtime rejected the proposal before appending it.
    Rejected {
        /// Protocol reason the proposal could not start.
        reason: ProposalRejection,
    },
    /// The encoded command exceeds the configured payload limit.
    PayloadTooLarge {
        /// Maximum accepted encoded payload size in bytes.
        max: usize,
        /// Actual encoded payload size in bytes.
        actual: usize,
    },
    /// The operation may or may not have committed and applied.
    ///
    /// Retry only with application-level idempotency if duplicate effects
    /// matter.
    UnknownOutcome {
        /// Local proposal identifier whose outcome was lost.
        local_proposal_id: LocalProposalId,
        /// Application request identity, when the caller supplied one.
        client_request_id: Option<ClientRequestId>,
        /// Boundary at which the driver lost certainty.
        reason: UnknownOutcomeReason,
    },
    /// The request named a group this driver does not own.
    ///
    /// The command was never handed to a group, so its request identity is
    /// still unused.
    WrongGroup,
    /// The application state machine failed.
    ///
    /// `operation` is the callback that surfaced the failure, and it is
    /// load-bearing: encoding a command, reading an applied index, and applying
    /// a batch fail for unrelated reasons and at unrelated moments.
    StateMachine {
        /// State-machine callback that failed.
        operation: StateMachineOperation,
        /// Strongest fate the driver can prove for the command.
        fate: WriteFate,
        /// Preserved typed application failure.
        cause: ErrorCause,
    },
    /// The Raft runtime failed to persist or query local durable state.
    Storage {
        /// Strongest fate the driver can prove for the command.
        fate: WriteFate,
        /// Preserved typed storage failure.
        cause: ErrorCause,
    },
    /// The driver could not route or deliver the work this write required.
    Transport {
        /// Strongest fate the driver can prove for the command.
        fate: WriteFate,
        /// Preserved typed transport failure.
        cause: ErrorCause,
    },
    /// The driver refused the write on its own standing rather than on any
    /// failure.
    ///
    /// Nothing was proposed — the driver never touched the group — so this
    /// answers [`WriteFate::NotAppended`] from the variant alone and the
    /// caller's request identity is still unused. `reason` is a typed category,
    /// not a rendered string; see [`DriverUnavailableReason`].
    Unavailable {
        /// Driver condition that refused the write.
        reason: DriverUnavailableReason,
    },
    /// Service shutdown began before the command was appended.
    ShuttingDown,
    /// The group is permanently poisoned.
    ///
    /// `cause` is the error that poisoned the group, when the poison came from
    /// a typed failure. It is `None` for a poison with no underlying error,
    /// such as a malformed snapshot output.
    Poisoned {
        /// Strongest fate the driver can prove for the command.
        fate: WriteFate,
        /// Stable explanation retained when the group poisoned.
        reason: String,
        /// Preserved typed cause, when poisoning originated in a callback.
        cause: Option<ErrorCause>,
    },
    /// No proposal identifier exists above the driver's durable watermark.
    LocalProposalIdExhausted,
    /// The driver violated one of its own documented invariants.
    ///
    /// This is the one variant whose message is authored rather than rendered:
    /// a driver reporting its own bug has no underlying error to preserve.
    ManagedInvariantViolation {
        /// Strongest fate the driver can prove for the command.
        fate: WriteFate,
        /// Stable diagnostic for the violated invariant.
        message: String,
    },
}

impl WriteError {
    /// Returns what this error proves about the command's fate.
    ///
    /// Variants that describe a refusal — not leader, rejected, payload too
    /// large, unavailable, shutting down, wrong group, exhausted local IDs —
    /// answer [`WriteFate::NotAppended`] from the variant alone, because
    /// reaching them is the proof. [`WriteError::UnknownOutcome`] answers
    /// [`WriteFate::Unresolved`] for the same reason. The remaining variants
    /// carry the fate the driver observed, because the same fault can occur on
    /// either side of the local append.
    ///
    /// [`WriteError::Unavailable`] belongs to the first group provably rather
    /// than by convention: every reason it carries is taken before the driver
    /// hands anything to the group, so the command reached no log and cannot
    /// commit later.
    #[must_use]
    pub const fn fate(&self) -> WriteFate {
        match self {
            Self::NotLeader { .. }
            | Self::Rejected { .. }
            | Self::PayloadTooLarge { .. }
            | Self::WrongGroup
            | Self::Unavailable { .. }
            | Self::ShuttingDown
            | Self::LocalProposalIdExhausted => WriteFate::NotAppended,
            Self::UnknownOutcome { .. } => WriteFate::Unresolved,
            Self::StateMachine { fate, .. }
            | Self::Storage { fate, .. }
            | Self::Transport { fate, .. }
            | Self::Poisoned { fate, .. }
            | Self::ManagedInvariantViolation { fate, .. } => *fate,
        }
    }

    /// Returns this error's stable category.
    #[must_use]
    pub const fn kind(&self) -> WriteErrorKind {
        match self {
            Self::NotLeader { .. } => WriteErrorKind::NotLeader,
            Self::Rejected { .. } => WriteErrorKind::Rejected,
            Self::PayloadTooLarge { .. } => WriteErrorKind::PayloadTooLarge,
            Self::UnknownOutcome { .. } => WriteErrorKind::UnknownOutcome,
            Self::WrongGroup => WriteErrorKind::WrongGroup,
            Self::StateMachine { .. } => WriteErrorKind::StateMachine,
            Self::Storage { .. } => WriteErrorKind::Storage,
            Self::Transport { .. } => WriteErrorKind::Transport,
            Self::Unavailable { .. } => WriteErrorKind::Unavailable,
            Self::ShuttingDown => WriteErrorKind::ShuttingDown,
            Self::Poisoned { .. } => WriteErrorKind::Poisoned,
            Self::LocalProposalIdExhausted => WriteErrorKind::LocalProposalIdExhausted,
            Self::ManagedInvariantViolation { .. } => WriteErrorKind::ManagedInvariantViolation,
        }
    }
}

impl fmt::Display for WriteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLeader { leader_hint, term } => {
                write!(
                    formatter,
                    "write rejected: this node is not leader in term {term}"
                )?;
                write_leader_hint(formatter, *leader_hint)
            }
            Self::Rejected { reason } => write!(formatter, "write rejected: {reason}"),
            Self::PayloadTooLarge { max, actual } => write!(
                formatter,
                "write payload is {actual} bytes, exceeding the {max} byte maximum"
            ),
            Self::UnknownOutcome {
                local_proposal_id,
                client_request_id,
                reason,
            } => write!(
                formatter,
                "write outcome is unknown for local proposal {local_proposal_id} and client request {client_request_id:?}: {reason}"
            ),
            Self::WrongGroup => {
                formatter.write_str("write rejected: this driver does not own the requested group")
            }
            Self::StateMachine {
                operation, fate, ..
            } => {
                write!(formatter, "write state machine {operation} failed")?;
                write_write_fate(formatter, *fate)
            }
            Self::Storage { fate, .. } => {
                formatter.write_str("write storage failed")?;
                write_write_fate(formatter, *fate)
            }
            Self::Transport { fate, .. } => {
                formatter.write_str("write transport failed")?;
                write_write_fate(formatter, *fate)
            }
            Self::Unavailable { reason } => {
                write!(formatter, "write refused by this driver: {reason}")?;
                write_write_fate(formatter, WriteFate::NotAppended)
            }
            Self::ShuttingDown => {
                formatter.write_str("write rejected because the service is shutting down")
            }
            Self::Poisoned { fate, reason, .. } => {
                write!(
                    formatter,
                    "write rejected because the group is poisoned: {reason}"
                )?;
                write_write_fate(formatter, *fate)
            }
            Self::LocalProposalIdExhausted => {
                formatter.write_str("write rejected because local proposal ids are exhausted")
            }
            Self::ManagedInvariantViolation { fate, message } => {
                write!(formatter, "managed write invariant violation: {message}")?;
                write_write_fate(formatter, *fate)
            }
        }
    }
}

impl Error for WriteError {
    /// Returns the preserved error, not the [`ErrorCause`] wrapper.
    ///
    /// A chain printer therefore shows one link per real failure rather than
    /// one per boundary crossed, which is why `ErrorCause` is not itself an
    /// `Error`.
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::StateMachine { cause, .. }
            | Self::Storage { cause, .. }
            | Self::Transport { cause, .. } => Some(cause.as_error()),
            Self::Poisoned { cause, .. } => match cause {
                Some(cause) => Some(cause.as_error()),
                None => None,
            },
            Self::NotLeader { .. }
            | Self::Rejected { .. }
            | Self::PayloadTooLarge { .. }
            | Self::UnknownOutcome { .. }
            | Self::WrongGroup
            | Self::Unavailable { .. }
            | Self::ShuttingDown
            | Self::LocalProposalIdExhausted
            | Self::ManagedInvariantViolation { .. } => None,
        }
    }
}
