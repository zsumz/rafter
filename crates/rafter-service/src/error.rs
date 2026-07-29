//! Error types for the managed service layer.
//!
//! A failed managed operation answers three different questions, and this
//! module keeps them apart because they have different types, different
//! lifetimes, and different audiences:
//!
//! - *What kind of failure was this?* — the variant, projected to a `Copy`
//!   category through [`WriteError::kind`], [`ReadError::kind`],
//!   [`TransferLeadershipError::kind`], and [`ShutdownError::kind`]. A metric
//!   label, a map key, or a structured-log field. Every operation surface a
//!   driver can fail on projects to one, so an operator aggregating driver
//!   failures never has to key on a rendered string.
//! - *May the command still take effect?* — a reported [`WriteFate`], never an
//!   inference from the category.
//! - *What actually failed?* — the typed [`ErrorCause`], reached through
//!   `source()`.
//!
//! Collapsing any two of them back together loses one of the three answers.
//!
//! The *vocabulary* those answers are given in — the diagnostic reasons, the
//! fate, the reason a driver refuses on its own standing, and the four category
//! projections — lives in the private `vocabulary` module and is re-exported from
//! here. The split
//! is the module's own sentence read literally: this file holds the error types,
//! and that one holds what they say. They are read at different times, too: an
//! embedder matching on a failure reads the types, and an operator wiring a
//! metric label reads the categories.

use std::{error::Error, fmt};

use rafter::{
    LeadershipTransferRejection, LocalProposalId, LogIndex, NodeId, ProposalRejection, ReadId,
    ReadIndexCancelReason, ReadIndexRejection, Term,
};
use rafter_app::proposal::ClientRequestId;
use rafter_app::read::ReadConsistency;

/// Types this module's errors carry across the `rafter-app` boundary.
///
/// They are re-exported rather than redeclared: a caller must be able to
/// compare the value it receives here with the one `rafter-app` produced, so
/// there can be only one type.
pub use rafter_app::error::{ErrorCause, StateMachineOperation};

mod vocabulary;

pub use vocabulary::{
    DriverUnavailableReason, ReadAbandonReason, ReadErrorKind, ShutdownErrorKind,
    TransferLeadershipErrorKind, UnknownOutcomeReason, WriteErrorKind, WriteFate,
};

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

/// Errors returned by managed leadership transfer.
///
/// A transfer is a request, not an outcome: `Ok(())` from the driver means the
/// request was accepted and its immediate effects were routed, so every variant
/// here reports a refusal to start rather than a transfer that failed part-way.
/// There is no [`WriteFate`] and no unknown outcome, because a transfer commits
/// no entry of its own.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum TransferLeadershipError {
    /// Only a leader can hand leadership over, and this node is not one.
    NotLeader {
        /// Current leader, when known.
        leader_hint: Option<NodeId>,
        /// Term in which the replica rejected the transfer.
        term: Term,
    },
    /// The leader refused the transfer; `reason` names which precondition failed.
    Rejected {
        /// Protocol reason the transfer could not start.
        reason: LeadershipTransferRejection,
        /// Current leader, when known.
        leader_hint: Option<NodeId>,
    },
    /// The request named a group this driver does not own.
    WrongGroup,
    /// The Raft runtime failed to persist or query local durable state.
    Storage {
        /// Preserved typed storage failure.
        cause: ErrorCause,
    },
    /// The driver could not route or deliver the work this transfer required.
    Transport {
        /// Preserved typed transport failure.
        cause: ErrorCause,
    },
    /// The service is shutting down and started no transfer.
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
}

impl TransferLeadershipError {
    /// Projects this error to its stable category.
    ///
    /// The label to aggregate on. See [`WriteError::kind`] for why the variant
    /// itself is not one.
    #[must_use]
    pub const fn kind(&self) -> TransferLeadershipErrorKind {
        match self {
            Self::NotLeader { .. } => TransferLeadershipErrorKind::NotLeader,
            Self::Rejected { .. } => TransferLeadershipErrorKind::Rejected,
            Self::WrongGroup => TransferLeadershipErrorKind::WrongGroup,
            Self::Storage { .. } => TransferLeadershipErrorKind::Storage,
            Self::Transport { .. } => TransferLeadershipErrorKind::Transport,
            Self::ShuttingDown => TransferLeadershipErrorKind::ShuttingDown,
            Self::Poisoned { .. } => TransferLeadershipErrorKind::Poisoned,
        }
    }
}

impl fmt::Display for TransferLeadershipError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLeader { leader_hint, term } => {
                write!(
                    formatter,
                    "leadership transfer rejected: this node is not leader in term {term}"
                )?;
                write_leader_hint(formatter, *leader_hint)
            }
            Self::Rejected {
                reason,
                leader_hint,
            } => {
                write!(formatter, "leadership transfer rejected: {reason}")?;
                write_leader_hint(formatter, *leader_hint)
            }
            Self::WrongGroup => formatter.write_str(
                "leadership transfer rejected: this driver does not own the requested group",
            ),
            Self::Storage { .. } => formatter.write_str("leadership transfer storage failed"),
            Self::Transport { .. } => formatter.write_str("leadership transfer transport failed"),
            Self::ShuttingDown => formatter
                .write_str("leadership transfer rejected because the service is shutting down"),
            Self::Poisoned { reason, .. } => write!(
                formatter,
                "leadership transfer rejected because the group is poisoned: {reason}"
            ),
        }
    }
}

impl Error for TransferLeadershipError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Storage { cause } | Self::Transport { cause } => Some(cause.as_error()),
            Self::Poisoned { cause, .. } => match cause {
                Some(cause) => Some(cause.as_error()),
                None => None,
            },
            Self::NotLeader { .. }
            | Self::Rejected { .. }
            | Self::WrongGroup
            | Self::ShuttingDown => None,
        }
    }
}

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

fn write_leader_hint(
    formatter: &mut fmt::Formatter<'_>,
    leader_hint: Option<NodeId>,
) -> fmt::Result {
    if let Some(leader_hint) = leader_hint {
        write!(formatter, "; leader hint is {leader_hint}")?;
    }
    Ok(())
}

/// Renders the fate without rendering the cause.
///
/// The fate is the one thing a client branches on, so it belongs in the
/// message. The cause does not: a `Display` that interpolated it would
/// reproduce today's message in a place a caller cannot parse, and a chain
/// printer would print it twice.
fn write_write_fate(formatter: &mut fmt::Formatter<'_>, fate: WriteFate) -> fmt::Result {
    formatter.write_str(match fate {
        WriteFate::NotAppended => "; the command was not appended",
        WriteFate::Unresolved => "; the command may still commit and apply",
    })
}

const fn read_cancel_reason_message(reason: ReadIndexCancelReason) -> &'static str {
    match reason {
        ReadIndexCancelReason::LeadershipLost => "leadership was lost",
        ReadIndexCancelReason::LeaderStateReset => "leader state was reset",
        ReadIndexCancelReason::LeadershipTransfer { .. } => "leadership transfer started",
    }
}

#[cfg(test)]
#[path = "error/tests.rs"]
mod tests;
