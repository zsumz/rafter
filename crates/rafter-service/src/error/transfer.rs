//! The managed leadership-transfer failure surface.
//!
//! A transfer request may be refused, accepted, or lose confirmation when its
//! driver is released. None of these confirms the target became leader. A
//! transfer commits no application entry and carries no write fate.

#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

use super::*;

/// Errors returned by managed leadership transfer.
///
/// A transfer is a request, not an outcome: `Ok(())` from the driver means the
/// request was accepted and its immediate effects were routed. Most errors
/// report refusal; [`Self::Unconfirmed`] preserves uncertainty after backend
/// release. No variant confirms who is now leader or carries a [`WriteFate`].
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
    /// The managed control slot is still retained by queued work or a result.
    Backpressure,
    /// The driver released the incarnation before confirming the request.
    ///
    /// A transfer may have begun. Observe actual leadership under a fresh
    /// incarnation; this does not report a protocol refusal or successful handoff.
    Unconfirmed,
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
            Self::Backpressure => TransferLeadershipErrorKind::Backpressure,
            Self::Unconfirmed => TransferLeadershipErrorKind::Unconfirmed,
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
            Self::Backpressure => formatter.write_str("leadership transfer admission is full"),
            Self::Unconfirmed => formatter.write_str(
                "leadership transfer request outcome was not confirmed before backend release",
            ),
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
            | Self::Backpressure
            | Self::Unconfirmed
            | Self::WrongGroup
            | Self::ShuttingDown => None,
        }
    }
}
