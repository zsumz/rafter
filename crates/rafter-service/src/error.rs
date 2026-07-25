//! Error types for the managed service layer.

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
        })
    }
}

/// Errors returned by managed writes.
#[derive(Clone, Debug, Eq, PartialEq)]
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
    ApplyFailed {
        message: String,
    },
    Storage {
        message: String,
    },
    Transport {
        message: String,
    },
    ShuttingDown,
    Poisoned {
        reason: String,
    },
    LocalProposalIdExhausted,
    ManagedInvariantViolation {
        message: String,
    },
}

impl fmt::Display for WriteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLeader { leader_hint, term } => {
                write!(formatter, "write rejected: this node is not leader in term {term}")?;
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
            Self::ApplyFailed { message } => write!(formatter, "write apply failed: {message}"),
            Self::Storage { message } => write!(formatter, "write storage failed: {message}"),
            Self::Transport { message } => write!(formatter, "write transport failed: {message}"),
            Self::ShuttingDown => formatter.write_str("write rejected because the service is shutting down"),
            Self::Poisoned { reason } => write!(formatter, "write rejected because the group is poisoned: {reason}"),
            Self::LocalProposalIdExhausted => formatter.write_str("write rejected because local proposal ids are exhausted"),
            Self::ManagedInvariantViolation { message } => {
                write!(formatter, "managed write invariant violation: {message}")
            }
        }
    }
}

impl Error for WriteError {}

/// Errors returned by managed reads.
#[derive(Clone, Debug, Eq, PartialEq)]
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
    ApplyFailed {
        message: String,
    },
    Storage {
        message: String,
    },
    Transport {
        message: String,
    },
    ShuttingDown,
    Poisoned {
        reason: String,
    },
    ReadIdExhausted,
    ManagedInvariantViolation {
        message: String,
    },
}

impl fmt::Display for ReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLeader { leader_hint, term } => {
                write!(formatter, "read rejected: this node is not leader in term {term}")?;
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
            Self::ApplyFailed { message } => write!(formatter, "read apply failed: {message}"),
            Self::Storage { message } => write!(formatter, "read storage failed: {message}"),
            Self::Transport { message } => write!(formatter, "read transport failed: {message}"),
            Self::ShuttingDown => formatter.write_str("read rejected because the service is shutting down"),
            Self::Poisoned { reason } => write!(formatter, "read rejected because the group is poisoned: {reason}"),
            Self::ReadIdExhausted => formatter.write_str("read rejected because read ids are exhausted"),
            Self::ManagedInvariantViolation { message } => {
                write!(formatter, "managed read invariant violation: {message}")
            }
        }
    }
}

impl Error for ReadError {}

/// Errors returned by managed leadership transfer.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TransferLeadershipError {
    NotLeader {
        leader_hint: Option<NodeId>,
        term: Term,
    },
    Rejected {
        reason: LeadershipTransferRejection,
        leader_hint: Option<NodeId>,
    },
    Storage {
        message: String,
    },
    Transport {
        message: String,
    },
    ShuttingDown,
    Poisoned {
        reason: String,
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
            Self::Storage { message } => {
                write!(formatter, "leadership transfer storage failed: {message}")
            }
            Self::Transport { message } => {
                write!(formatter, "leadership transfer transport failed: {message}")
            }
            Self::ShuttingDown => formatter
                .write_str("leadership transfer rejected because the service is shutting down"),
            Self::Poisoned { reason } => write!(
                formatter,
                "leadership transfer rejected because the group is poisoned: {reason}"
            ),
        }
    }
}

impl Error for TransferLeadershipError {}

/// Errors returned while opening a managed metrics watch.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MetricsError {
    WrongGroup,
    Transport { message: String },
}

impl fmt::Display for MetricsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongGroup => formatter.write_str("metrics watch targets the wrong group"),
            Self::Transport { message } => write!(formatter, "metrics transport failed: {message}"),
        }
    }
}

impl Error for MetricsError {}

/// Errors returned by managed service shutdown.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ShutdownError {
    Transport { message: String },
    AlreadyShutDown,
}

impl fmt::Display for ShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport { message } => {
                write!(formatter, "shutdown transport failed: {message}")
            }
            Self::AlreadyShutDown => formatter.write_str("service is already shut down"),
        }
    }
}

impl Error for ShutdownError {}

fn write_leader_hint(
    formatter: &mut fmt::Formatter<'_>,
    leader_hint: Option<NodeId>,
) -> fmt::Result {
    if let Some(leader_hint) = leader_hint {
        write!(formatter, "; leader hint is {leader_hint}")?;
    }
    Ok(())
}

const fn read_cancel_reason_message(reason: ReadIndexCancelReason) -> &'static str {
    match reason {
        ReadIndexCancelReason::LeadershipLost => "leadership was lost",
        ReadIndexCancelReason::LeaderStateReset => "leader state was reset",
        ReadIndexCancelReason::LeadershipTransfer { .. } => "leadership transfer started",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_error(error: &(dyn Error + 'static)) -> String {
        error.to_string()
    }

    #[test]
    fn write_error_is_a_standard_error_with_display_message() {
        let error = WriteError::Storage {
            message: "disk full".to_owned(),
        };

        assert_eq!(assert_error(&error), "write storage failed: disk full");
    }

    #[test]
    fn read_error_formats_leader_hint_without_debug_dump() {
        let error = ReadError::NotLeader {
            leader_hint: Some(NodeId(2)),
            term: Term(7),
        };

        assert_eq!(
            error.to_string(),
            "read rejected: this node is not leader in term 7; leader hint is node-2"
        );
    }

    #[test]
    fn shutdown_error_is_a_standard_error() {
        let error = ShutdownError::AlreadyShutDown;

        assert_eq!(assert_error(&error), "service is already shut down");
    }
}
