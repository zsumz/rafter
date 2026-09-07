//! The low-cardinality label each error surface projects to.
//!
//! One enum per operation a driver can fail, and every one of them is `Copy`,
//! totally ordered, hashable, and payload-free so it can be a metric label, a
//! map key, or a structured-log field. Nothing here carries detail: the detail
//! is on the error, and a projection that leaked any would stop being a label.

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
