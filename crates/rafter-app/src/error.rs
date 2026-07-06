//! Error types for the embedded application/group driver.

use std::{error::Error, fmt};

use rafter::{LocalProposalId, LogIndex, NodeId, ReadId, Term};

use crate::read::ReadConsistency;

/// State-machine operation that surfaced an application error.
///
/// This enum is exhaustive for the operations currently issued by
/// `rafter-app`; new state-machine callbacks may add variants before 1.0.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateMachineOperation {
    AppliedIndex,
    EncodeCommand,
    DecodeCommand,
    ApplyBatch,
    Read,
    InstallSnapshot,
}

impl fmt::Display for StateMachineOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AppliedIndex => "applied-index lookup",
            Self::EncodeCommand => "command encoding",
            Self::DecodeCommand => "command decoding",
            Self::ApplyBatch => "batch apply",
            Self::Read => "read",
            Self::InstallSnapshot => "snapshot install",
        })
    }
}

/// Errors returned by the synchronous app/group driver.
#[derive(Debug)]
#[non_exhaustive]
pub enum GroupError<E, R> {
    Runtime(R),
    StateMachine {
        operation: StateMachineOperation,
        source: E,
    },
    ApplyResultCountMismatch {
        expected: usize,
        actual: usize,
    },
    ApplyResultMetadataMismatch {
        expected_index: LogIndex,
        actual_index: LogIndex,
        expected_term: Term,
        actual_term: Term,
        expected_local_proposal_id: Option<LocalProposalId>,
        actual_local_proposal_id: Option<LocalProposalId>,
    },
    ApplyEntryAlreadyApplied {
        entry_index: LogIndex,
        app_applied_index: LogIndex,
        group_applied_index: LogIndex,
    },
    AppliedIndexBehind {
        required: LogIndex,
        actual: LogIndex,
    },
    MalformedSnapshot {
        reason: String,
    },
    Poisoned {
        reason: String,
    },
    WrongGroup,
    WrongRecipient {
        expected: NodeId,
        actual: NodeId,
    },
    NonMonotonicLocalProposalId {
        local_proposal_id: LocalProposalId,
        last_seen_local_proposal_id: LocalProposalId,
    },
    DuplicateReadId {
        read_id: ReadId,
    },
    NonMonotonicReadId {
        read_id: ReadId,
        last_seen_read_id: ReadId,
    },
    ProposalDidNotStart {
        local_proposal_id: LocalProposalId,
    },
    UnsupportedReadConsistency {
        consistency: ReadConsistency,
    },
    UnsupportedOutput {
        output: &'static str,
    },
}

impl<E, R> fmt::Display for GroupError<E, R>
where
    E: fmt::Display,
    R: fmt::Display,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(error) => write!(formatter, "Raft runtime failed: {error}"),
            Self::StateMachine { operation, source } => {
                write!(formatter, "state machine {operation} failed: {source}")
            }
            Self::ApplyResultCountMismatch { expected, actual } => write!(
                formatter,
                "state machine returned {actual} apply results for {expected} committed entries"
            ),
            Self::ApplyResultMetadataMismatch {
                expected_index,
                actual_index,
                expected_term,
                actual_term,
                expected_local_proposal_id,
                actual_local_proposal_id,
            } => write!(
                formatter,
                "state machine apply result metadata mismatch: expected index {expected_index}, term {expected_term}, local proposal {expected_local_proposal_id:?}; got index {actual_index}, term {actual_term}, local proposal {actual_local_proposal_id:?}"
            ),
            Self::ApplyEntryAlreadyApplied {
                entry_index,
                app_applied_index,
                group_applied_index,
            } => write!(
                formatter,
                "refusing to apply entry {entry_index} because the app reports applied index {app_applied_index} while the group reports {group_applied_index}"
            ),
            Self::AppliedIndexBehind { required, actual } => write!(
                formatter,
                "state machine applied index {actual} is behind required index {required}"
            ),
            Self::MalformedSnapshot { reason } => write!(formatter, "malformed snapshot: {reason}"),
            Self::Poisoned { reason } => write!(formatter, "Raft group is poisoned: {reason}"),
            Self::WrongGroup => formatter.write_str("input targets a different Raft group"),
            Self::WrongRecipient { expected, actual } => write!(
                formatter,
                "peer message targets {actual}, but this group is node {expected}"
            ),
            Self::NonMonotonicLocalProposalId {
                local_proposal_id,
                last_seen_local_proposal_id,
            } => write!(
                formatter,
                "local proposal id {local_proposal_id} is not greater than last seen id {last_seen_local_proposal_id}"
            ),
            Self::DuplicateReadId { read_id } => {
                write!(formatter, "read id {read_id} is already pending")
            }
            Self::NonMonotonicReadId {
                read_id,
                last_seen_read_id,
            } => write!(
                formatter,
                "read id {read_id} is not greater than last seen id {last_seen_read_id}"
            ),
            Self::ProposalDidNotStart { local_proposal_id } => write!(
                formatter,
                "proposal {local_proposal_id} did not emit a start event"
            ),
            Self::UnsupportedReadConsistency { consistency } => {
                write!(formatter, "unsupported read consistency {consistency:?}")
            }
            Self::UnsupportedOutput { output } => {
                write!(formatter, "unsupported Raft output in app layer: {output}")
            }
        }
    }
}

impl<E, R> Error for GroupError<E, R>
where
    E: Error + 'static,
    R: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Runtime(error) => Some(error),
            Self::StateMachine { source, .. } => Some(source),
            Self::ApplyResultCountMismatch { .. }
            | Self::ApplyResultMetadataMismatch { .. }
            | Self::ApplyEntryAlreadyApplied { .. }
            | Self::AppliedIndexBehind { .. }
            | Self::MalformedSnapshot { .. }
            | Self::Poisoned { .. }
            | Self::WrongGroup
            | Self::WrongRecipient { .. }
            | Self::NonMonotonicLocalProposalId { .. }
            | Self::DuplicateReadId { .. }
            | Self::NonMonotonicReadId { .. }
            | Self::ProposalDidNotStart { .. }
            | Self::UnsupportedReadConsistency { .. }
            | Self::UnsupportedOutput { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct TestError(&'static str);

    impl fmt::Display for TestError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(self.0)
        }
    }

    impl Error for TestError {}

    #[test]
    fn group_error_display_uses_underlying_error_messages() {
        let error = GroupError::<TestError, TestError>::StateMachine {
            operation: StateMachineOperation::ApplyBatch,
            source: TestError("apply failed"),
        };

        assert_eq!(
            error.to_string(),
            "state machine batch apply failed: apply failed"
        );
    }

    #[test]
    fn group_error_sources_are_available_for_underlying_errors() {
        let error = GroupError::<TestError, TestError>::Runtime(TestError("disk unavailable"));

        assert_eq!(
            error
                .source()
                .expect("runtime error is exposed as source")
                .to_string(),
            "disk unavailable"
        );
    }
}
