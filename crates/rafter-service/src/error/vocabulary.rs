//! What a managed failure is described *in*.
//!
//! Four shapes, and none of them is an error: the diagnostic reason a write or
//! a read ended without an answer, what a failed write proves about its
//! command's fate, why a driver is refusing on its own standing, and the
//! low-cardinality category each error surface projects to.
//!
//! Split from [`super`] along that file's own sentence — it keeps three
//! questions apart, and this holds the answers while it holds the types. They
//! are also read at different times. An embedder matching on a failure reads the
//! error types; an operator wiring a metric label or a map key reads only what
//! is here, and everything here is `Copy`, totally ordered, hashable, and free
//! of payload so that it can be one.
//!
//! Every enum here is `#[non_exhaustive]` and grows additively. A caller
//! aggregating by any of them keeps a bucket for a value it does not recognize
//! rather than dropping it.

use std::fmt;

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
/// was refused reports [`ReadError::Rejected`](super::ReadError::Rejected),
/// and a barrier the cluster invalidated reports
/// [`ReadError::Canceled`](super::ReadError::Canceled).
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
/// used to ride [`WriteError::Transport`](super::WriteError::Transport) with a
/// crate-private cause, which was
/// wrong twice: no transport operation failed, and an external caller could only
/// reach the reason by formatting the error and reading it. Both surfaces now
/// carry this value directly.
///
/// The companion of [`crate::DriverServiceState`] and deliberately not the same
/// type. That one is an *observation* an operator polls, and carries the detail
/// an investigation needs — which node, at which log position.
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
    /// Two observations of the committed membership at one log position
    /// disagree about it, so this driver has no trustworthy statement to make
    /// about who may speak. Terminal for the incarnation; the supervisor
    /// releases and reseeds deliberately.
    ContradictoryCurrentState,
    /// A committed transition declares a predecessor this driver's own record is
    /// not, at the position immediately below the transition.
    ///
    /// The same refusal as [`DriverUnavailableReason::ContradictoryCurrentState`]
    /// and a different diagnosis: the log's own account of its own history is one
    /// side of it, so the record beside the log is what an operator has to look
    /// at rather than either of two observations.
    ContradictoryTransitionPredecessor,
    /// The driver released its group and has not adopted another.
    Released,
    /// The driver has shut down, which is terminal.
    ///
    /// Reachable through [`DriverUnavailableReason::from_service_state`], which
    /// is what a consumer rendering [`crate::DriverServiceState`] uses. It does
    /// not reach a client through [`WriteError`](super::WriteError) or
    /// [`ReadError`](super::ReadError): both answered
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
            crate::DriverServiceState::ContradictoryCurrentState { .. } => {
                Some(Self::ContradictoryCurrentState)
            }
            crate::DriverServiceState::ContradictoryTransitionPredecessor { .. } => {
                Some(Self::ContradictoryTransitionPredecessor)
            }
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
            Self::ContradictoryCurrentState => {
                "two observations of the committed membership at one position disagree"
            }
            Self::ContradictoryTransitionPredecessor => {
                "a committed transition declares a predecessor this driver's record is not"
            }
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

/// Stable category of a [`WriteError`](super::WriteError).
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
    /// The local replica was not leader.
    NotLeader,
    /// The Raft runtime rejected the proposal.
    Rejected,
    /// The encoded command exceeded the configured limit.
    PayloadTooLarge,
    /// The driver cannot prove whether the command took effect.
    UnknownOutcome,
    /// The request named another group.
    WrongGroup,
    /// An application state-machine callback failed.
    StateMachine,
    /// Durable storage failed.
    Storage,
    /// Message transport failed.
    Transport,
    /// The driver refused work because of its current service state.
    Unavailable,
    /// Service shutdown is in progress.
    ShuttingDown,
    /// The group is permanently poisoned.
    Poisoned,
    /// The local proposal-identifier space is exhausted.
    LocalProposalIdExhausted,
    /// The managed driver violated its own invariant.
    ManagedInvariantViolation,
}

/// Stable category of a [`TransferLeadershipError`](super::TransferLeadershipError).
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
    /// The local replica was not leader.
    NotLeader,
    /// The Raft runtime rejected the transfer.
    Rejected,
    /// The request named another group.
    WrongGroup,
    /// Durable storage failed.
    Storage,
    /// Message transport failed.
    Transport,
    /// Service shutdown is in progress.
    ShuttingDown,
    /// The group is permanently poisoned.
    Poisoned,
}

/// Stable category of a [`ShutdownError`](super::ShutdownError).
///
/// The same projection again, over the smallest of the four surfaces. Three
/// variants is still three buckets, and a caller that aggregates by kind should
/// not have to special-case one operation out of the four.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ShutdownErrorKind {
    /// The request named another group.
    WrongGroup,
    /// Message transport failed.
    Transport,
    /// Shutdown had already completed.
    AlreadyShutDown,
}

/// Stable category of a [`ReadError`](super::ReadError).
///
/// The same low-cardinality projection [`WriteErrorKind`] is, for the same
/// reasons and with the same rule for unrecognized values.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ReadErrorKind {
    /// The local replica was not leader.
    NotLeader,
    /// The Raft runtime rejected the read barrier.
    Rejected,
    /// An active read barrier was canceled.
    Canceled,
    /// The requested consistency mode is unsupported.
    UnsupportedConsistency,
    /// Local state could not satisfy the required freshness.
    FreshnessUnavailable,
    /// The driver abandoned an active read barrier.
    Abandoned,
    /// The request named another group.
    WrongGroup,
    /// An application state-machine callback failed.
    StateMachine,
    /// Durable storage failed.
    Storage,
    /// Message transport failed.
    Transport,
    /// The driver refused work because of its current service state.
    Unavailable,
    /// Service shutdown is in progress.
    ShuttingDown,
    /// The group is permanently poisoned.
    Poisoned,
    /// The local read-identifier space is exhausted.
    ReadIdExhausted,
    /// The managed driver violated its own invariant.
    ManagedInvariantViolation,
}
