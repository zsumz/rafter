//! Error types for the managed service layer.
//!
//! A failed managed operation answers three different questions, and this
//! module keeps them apart because they have different types, different
//! lifetimes, and different audiences:
//!
//! - *What kind of failure was this?* — the variant, projected to a `Copy`
//!   category through [`WriteError::kind`] and [`ReadError::kind`]. A metric
//!   label, a map key, or a structured-log field.
//! - *May the command still take effect?* — a reported [`WriteFate`], never an
//!   inference from the category.
//! - *What actually failed?* — the typed [`ErrorCause`], reached through
//!   `source()`.
//!
//! Collapsing any two of them back together loses one of the three answers.

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

/// Diagnostic cause for a managed write with unknown outcome.
///
/// This reason explains why the managed service lost the final write outcome.
/// It does not make the operation's commit/apply result known.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnknownOutcomeReason {
    /// The driver had no more locally queued work before observing a terminal
    /// proposal result.
    EmptyNetwork,
    /// The managed operation reached its configured drive-step bound before
    /// observing a terminal proposal result.
    DriveBoundReached,
    /// The driver observed local append and then hit a driver/routing error
    /// before observing the final proposal result.
    PostAppendDriverError,
    /// The app/runtime layer reported that local proposal tracking was dropped
    /// before the final proposal result was known.
    RuntimeDroppedProposal,
    /// The group entered a poisoned state before the final proposal result was
    /// known.
    GroupPoisoned,
    /// The driver released the group that held this proposal's waiter before
    /// the final proposal result was known.
    ///
    /// This is the in-process restart and shutdown case. The incarnation that
    /// accepted the proposal is gone; a proposal already appended is still in
    /// the durable log and may commit and apply under the next incarnation,
    /// which is exactly what an unknown outcome means.
    ///
    /// It is distinct from [`UnknownOutcomeReason::RuntimeDroppedProposal`],
    /// which reports that the app or runtime layer itself declared local
    /// proposal tracking lost while the driver kept running. The two point at
    /// different layers and lead to different investigations.
    DriverReleased,
}

impl fmt::Display for UnknownOutcomeReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyNetwork => "the driver had no queued work before a terminal result",
            Self::DriveBoundReached => "the managed drive-step bound was reached",
            Self::PostAppendDriverError => {
                "a driver error occurred after the proposal was appended locally"
            }
            Self::RuntimeDroppedProposal => "the app/runtime layer dropped local proposal tracking",
            Self::GroupPoisoned => "the Raft group was poisoned before the outcome was known",
            Self::DriverReleased => "the driver released the group holding this proposal",
        })
    }
}

/// Why a managed read barrier was abandoned without an answer.
///
/// Abandonment is the driver's own decision, so every variant names something
/// the driver did. None of them says anything about the cluster: a read that
/// was refused reports [`ReadError::Rejected`], and a barrier the cluster
/// invalidated reports [`ReadError::Canceled`].
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ReadAbandonReason {
    /// The managed read reached its configured drive-step bound before the
    /// barrier resolved.
    DriveBoundReached,
    /// The driver released the group that held this barrier.
    DriverReleased,
}

impl fmt::Display for ReadAbandonReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::DriveBoundReached => "the managed drive-step bound was reached",
            Self::DriverReleased => "the driver released the group holding this barrier",
        })
    }
}

/// What a failed managed write proves about the command's fate.
///
/// This is the retry question, and it is the only part of a write error a
/// client may branch on when deciding whether a request identity is still
/// unused. It is separate from the error's category because the two answer
/// different questions: a storage failure before the local append and a
/// storage failure after it are the same fault and different facts.
///
/// A driver reports the fate it observed. It never infers one from a category,
/// and a caller must not either — the category says what broke, not when.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum WriteFate {
    /// The command was refused before it reached the local Raft log. It cannot
    /// commit, now or later, and its request identity is still unused.
    ///
    /// A driver reports this only when it observed the refusal itself.
    NotAppended,
    /// The command may or may not commit and apply.
    ///
    /// Retry only under the same request identity, and only with
    /// application-level idempotency if duplicate effects matter. A driver that
    /// cannot prove [`WriteFate::NotAppended`] reports this.
    ///
    /// A locally appended entry that was never sent is unresolved rather than
    /// refused, and that is the truth rather than caution: the entry is on
    /// disk, and a node reopened over the same durable log can still replicate
    /// and commit it under a later incarnation. `NotAppended` would be
    /// unprovable there.
    Unresolved,
}

impl WriteFate {
    /// Returns whether the command may still take effect.
    ///
    /// Written as the negation of [`WriteFate::NotAppended`] so a future
    /// variant reads as unresolved until a caller is updated to interpret it.
    /// This is the safe direction, and it is the only direction this enum is
    /// meant to be tested in.
    #[must_use]
    pub const fn may_commit(self) -> bool {
        !matches!(self, Self::NotAppended)
    }
}

/// Stable category of a [`WriteError`].
///
/// This is the low-cardinality projection of the error: `Copy`, totally
/// ordered, hashable, and free of payload, so it can be a metric label, a map
/// key, or a structured-log field. The variants themselves carry indices, node
/// IDs, and messages, so neither `Display` nor `Debug` is bounded enough to
/// label with.
///
/// New categories are additive. A caller that aggregates by kind must keep a
/// bucket for kinds it does not recognize rather than dropping them.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum WriteErrorKind {
    NotLeader,
    Rejected,
    PayloadTooLarge,
    UnknownOutcome,
    WrongGroup,
    StateMachine,
    Storage,
    Transport,
    ShuttingDown,
    Poisoned,
    LocalProposalIdExhausted,
    ManagedInvariantViolation,
}

/// Stable category of a [`ReadError`].
///
/// The same low-cardinality projection [`WriteErrorKind`] is, for the same
/// reasons and with the same rule for unrecognized values.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ReadErrorKind {
    NotLeader,
    Rejected,
    Canceled,
    UnsupportedConsistency,
    FreshnessUnavailable,
    Abandoned,
    WrongGroup,
    StateMachine,
    Storage,
    Transport,
    ShuttingDown,
    Poisoned,
    ReadIdExhausted,
    ManagedInvariantViolation,
}

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
    NotLeader {
        leader_hint: Option<NodeId>,
        term: Term,
    },
    Rejected {
        reason: ProposalRejection,
    },
    PayloadTooLarge {
        max: usize,
        actual: usize,
    },
    /// The operation may or may not have committed and applied.
    ///
    /// Retry only with application-level idempotency if duplicate effects
    /// matter.
    UnknownOutcome {
        local_proposal_id: LocalProposalId,
        client_request_id: Option<ClientRequestId>,
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
        operation: StateMachineOperation,
        fate: WriteFate,
        cause: ErrorCause,
    },
    /// The Raft runtime failed to persist or query local durable state.
    Storage {
        fate: WriteFate,
        cause: ErrorCause,
    },
    /// The driver could not route or deliver the work this write required.
    Transport {
        fate: WriteFate,
        cause: ErrorCause,
    },
    ShuttingDown,
    /// The group is permanently poisoned.
    ///
    /// `cause` is the error that poisoned the group, when the poison came from
    /// a typed failure. It is `None` for a poison with no underlying error,
    /// such as a malformed snapshot output.
    Poisoned {
        fate: WriteFate,
        reason: String,
        cause: Option<ErrorCause>,
    },
    LocalProposalIdExhausted,
    /// The driver violated one of its own documented invariants.
    ///
    /// This is the one variant whose message is authored rather than rendered:
    /// a driver reporting its own bug has no underlying error to preserve.
    ManagedInvariantViolation {
        fate: WriteFate,
        message: String,
    },
}

impl WriteError {
    /// Returns what this error proves about the command's fate.
    ///
    /// Variants that describe a refusal — not leader, rejected, payload too
    /// large, shutting down, wrong group, exhausted local IDs — answer
    /// [`WriteFate::NotAppended`] from the variant alone, because reaching them
    /// is the proof. [`WriteError::UnknownOutcome`] answers
    /// [`WriteFate::Unresolved`] for the same reason. The remaining variants
    /// carry the fate the driver observed, because the same fault can occur on
    /// either side of the local append.
    #[must_use]
    pub const fn fate(&self) -> WriteFate {
        match self {
            Self::NotLeader { .. }
            | Self::Rejected { .. }
            | Self::PayloadTooLarge { .. }
            | Self::WrongGroup
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
    NotLeader {
        leader_hint: Option<NodeId>,
        term: Term,
    },
    Rejected {
        read_id: Option<ReadId>,
        reason: ReadIndexRejection,
        leader_hint: Option<NodeId>,
    },
    Canceled {
        read_id: ReadId,
        reason: ReadIndexCancelReason,
        leader_hint: Option<NodeId>,
    },
    UnsupportedConsistency {
        consistency: ReadConsistency,
    },
    FreshnessUnavailable {
        /// The local read ID for an abandoned linearizable read. Local
        /// freshness gaps do not consume a read ID and report `None`.
        read_id: Option<ReadId>,
        required_applied_index: LogIndex,
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
        read_id: ReadId,
        reason: ReadAbandonReason,
    },
    /// The request named a group this driver does not own.
    WrongGroup,
    /// The application state machine failed.
    StateMachine {
        operation: StateMachineOperation,
        cause: ErrorCause,
    },
    /// The Raft runtime failed to persist or query local durable state.
    Storage {
        cause: ErrorCause,
    },
    /// The driver could not route or deliver the work this read required.
    Transport {
        cause: ErrorCause,
    },
    ShuttingDown,
    /// The group is permanently poisoned.
    ///
    /// `cause` is the error that poisoned the group, when the poison came from
    /// a typed failure.
    Poisoned {
        reason: String,
        cause: Option<ErrorCause>,
    },
    ReadIdExhausted,
    /// The driver violated one of its own documented invariants.
    ///
    /// As on [`WriteError::ManagedInvariantViolation`], this is the one variant
    /// whose message is authored rather than rendered: a driver reporting its
    /// own bug has no underlying error to preserve. There is no fate here
    /// because a read takes no effect.
    ManagedInvariantViolation {
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
        leader_hint: Option<NodeId>,
        term: Term,
    },
    /// The leader refused the transfer; `reason` names which precondition failed.
    Rejected {
        reason: LeadershipTransferRejection,
        leader_hint: Option<NodeId>,
    },
    /// The request named a group this driver does not own.
    WrongGroup,
    /// The Raft runtime failed to persist or query local durable state.
    Storage { cause: ErrorCause },
    /// The driver could not route or deliver the work this transfer required.
    Transport { cause: ErrorCause },
    /// The service is shutting down and started no transfer.
    ShuttingDown,
    /// The group is permanently poisoned.
    ///
    /// `cause` is the error that poisoned the group, when the poison came from
    /// a typed failure.
    Poisoned {
        reason: String,
        cause: Option<ErrorCause>,
    },
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
    Transport { cause: ErrorCause },
    /// Shutdown had already completed.
    ///
    /// Reported rather than succeeding again so a supervisor can tell "I
    /// stopped this" from "it was already stopped". Shutdown is terminal: a
    /// driver that has shut down refuses every operation, including adoption.
    AlreadyShutDown,
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
