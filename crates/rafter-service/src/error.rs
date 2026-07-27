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

/// Why a driver refused a client operation on its own standing.
///
/// **A typed cause where there used to be a rendered string.** These refusals
/// used to ride [`WriteError::Transport`] with a crate-private cause, which was
/// wrong twice: no transport operation failed, and an external caller could only
/// reach the reason by formatting the error and reading it. Both surfaces now
/// carry this value directly.
///
/// The companion of [`crate::DriverServiceState`] and deliberately not the same
/// type. That one is an *observation* an operator polls, and carries the detail
/// an investigation needs — which node, how many fences, against what threshold.
/// This one is a *cause on a client's error*: `Copy`, payload-free, and
/// low-cardinality, so it can be a metric label or a map key exactly like
/// [`WriteErrorKind`]. A client branching on why it was refused needs the
/// category; an operator needing the numbers reads
/// [`crate::TransportRaftDriver::service_state`].
///
/// Every variant means the same thing about the command: **nothing was
/// proposed.** The driver refused before touching the group, so a write refused
/// for one of these reports [`WriteFate::NotAppended`] and its request identity
/// is still unused.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum DriverUnavailableReason {
    /// A committed configuration change removed this driver's own replica, and
    /// the identity it served is spent.
    Decommissioned,
    /// No configuration this driver knows names its replica, and no committed
    /// removal spent it either — a rolled-back joiner, or one whose addition has
    /// not been proposed yet.
    ///
    /// Reads are refused as well as writes, and that is the whole reason this
    /// state exists: the replica receives no replication, so answering a local
    /// read from it is answering from a view with no bound on how stale it is.
    NotMember,
    /// The link layer has left more committed removals unfenced than
    /// [`crate::TransportDriverOptions::fence_backlog_service_threshold`]
    /// allows. It clears when the backlog drains.
    FenceBacklog,
    /// The driver released its group and has not adopted another.
    Released,
    /// The driver has shut down, which is terminal.
    ///
    /// Reachable through [`DriverUnavailableReason::from_service_state`], which
    /// is what a consumer rendering [`crate::DriverServiceState`] uses. It does
    /// not reach a client through [`WriteError`] or [`ReadError`]: both answered
    /// shutdown with a dedicated variant before this enum existed, every
    /// consumer already matches on that variant, and one fact with two spellings
    /// on one surface is worse than a projection that stays total.
    ShuttingDown,
}

impl DriverUnavailableReason {
    /// Projects a driver's service state to the reason a client would be
    /// refused for it, or `None` when it would not be refused.
    ///
    /// `None` for [`crate::DriverServiceState::Serving`], which is the only
    /// state that is not a refusal. Both enums are `#[non_exhaustive]` and grow
    /// together, so a caller outside this crate keeps a bucket for a reason it
    /// does not recognize rather than dropping it — the rule
    /// [`WriteErrorKind`] states.
    #[must_use]
    pub const fn from_service_state(state: crate::DriverServiceState) -> Option<Self> {
        match state {
            crate::DriverServiceState::Serving => None,
            crate::DriverServiceState::Decommissioned { .. } => Some(Self::Decommissioned),
            crate::DriverServiceState::NotMember { .. } => Some(Self::NotMember),
            crate::DriverServiceState::FenceBacklog { .. } => Some(Self::FenceBacklog),
            crate::DriverServiceState::Released => Some(Self::Released),
            crate::DriverServiceState::ShuttingDown => Some(Self::ShuttingDown),
        }
    }
}

impl fmt::Display for DriverUnavailableReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Decommissioned => "a committed change removed this replica",
            Self::NotMember => "no configuration this driver knows names this replica",
            Self::FenceBacklog => "the link layer owes more peer fences than this driver allows",
            Self::Released => "the driver released its group and holds none",
            Self::ShuttingDown => "the driver has shut down",
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
    Unavailable,
    ShuttingDown,
    Poisoned,
    LocalProposalIdExhausted,
    ManagedInvariantViolation,
}

/// Stable category of a [`TransferLeadershipError`].
///
/// The same low-cardinality projection [`WriteErrorKind`] is, for the same
/// reasons and with the same rule for unrecognized values. This surface is
/// smaller than the write one and it is projected for exactly the same reason:
/// an operator aggregating failures across a driver has four operations to
/// aggregate, and one that could not be projected would be counted as a string
/// or not counted at all.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum TransferLeadershipErrorKind {
    NotLeader,
    Rejected,
    WrongGroup,
    Storage,
    Transport,
    ShuttingDown,
    Poisoned,
}

/// Stable category of a [`ShutdownError`].
///
/// The same projection again, over the smallest of the four surfaces. Three
/// variants is still three buckets, and a caller that aggregates by kind should
/// not have to special-case one operation out of the four.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ShutdownErrorKind {
    WrongGroup,
    Transport,
    AlreadyShutDown,
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
    Unavailable,
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
    /// The driver refused the write on its own standing rather than on any
    /// failure.
    ///
    /// Nothing was proposed — the driver never touched the group — so this
    /// answers [`WriteFate::NotAppended`] from the variant alone and the
    /// caller's request identity is still unused. `reason` is a typed category,
    /// not a rendered string; see [`DriverUnavailableReason`].
    Unavailable {
        reason: DriverUnavailableReason,
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
    /// The driver refused the read on its own standing rather than on any
    /// failure.
    ///
    /// Both consistency levels are refused for every reason this carries, and
    /// [`DriverUnavailableReason::NotMember`] is why the read side has this
    /// variant at all: a replica the cluster is not replicating to answers a
    /// local read from a view with no bound on how stale it is, and that is the
    /// one refusal a client could not otherwise tell from a fresh answer.
    Unavailable {
        reason: DriverUnavailableReason,
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
