//! Error types for the embedded application/group driver.

use std::{error::Error, fmt, sync::Arc};

use rafter::{LocalProposalId, LogIndex, NodeId, ReadId, Term};

use crate::read::ReadConsistency;

/// A typed error preserved across a layer boundary.
///
/// A Rafter error names a stable category; the cause names what actually
/// failed. Both are needed: the category is what a caller branches on and what
/// a metric labels, and the cause is what an operator reads. Rendering the
/// cause into the category's message loses the second and does not improve the
/// first.
///
/// The cause is shared rather than owned because one failure fans out to every
/// entry of a write batch, and a `Box<dyn Error>` cannot be cloned. It is
/// type-erased rather than a type parameter because the boundary it crosses is
/// a client boundary: a driver that reaches its group over a network holds its
/// own transport error, not the leader's application error, and a client type
/// parameterized over the leader's error type would be a promise no networked
/// driver can keep.
///
/// This type is deliberately not itself a [`std::error::Error`]. It is a
/// handle, and it is transparent to `source()`: an error carrying a cause
/// returns the *inner* error from its own `source()`, so a chain printer walks
/// one link per real failure rather than one per boundary crossed.
#[derive(Clone)]
pub struct ErrorCause(Arc<dyn Error + Send + Sync + 'static>);

impl ErrorCause {
    /// Preserves `error` as the cause of a Rafter error.
    #[must_use]
    pub fn new<E>(error: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self(Arc::new(error))
    }

    /// Returns the preserved error.
    #[must_use]
    pub fn as_error(&self) -> &(dyn Error + Send + Sync + 'static) {
        self.0.as_ref()
    }

    /// Preserves an already-shared `error` as the cause of a Rafter error.
    ///
    /// This is the constructor for a failure with two owners. A group that
    /// poisons retains the state machine's error as its poison cause *and*
    /// hands the same error back to the caller inside
    /// [`GroupError::StateMachine`], and [`ReplicatedStateMachine::Error`] is
    /// deliberately not `Clone`, so one allocation is shared rather than two
    /// values produced.
    ///
    /// [`ReplicatedStateMachine::Error`]: crate::state_machine::ReplicatedStateMachine::Error
    #[must_use]
    pub fn from_shared<E>(error: Arc<E>) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self(error)
    }

    /// Returns the preserved error when it is of type `E`.
    ///
    /// An embedder whose own state machine or runtime produced the failure
    /// recovers its exact type here, which is what makes a typed recovery path
    /// writable. A caller on the far side of a transport recovers whatever
    /// *that* driver preserved, which is that driver's error and not the
    /// leader's — a cause is preserved across one boundary, not serialized
    /// across the network.
    #[must_use]
    pub fn downcast_ref<E>(&self) -> Option<&E>
    where
        E: Error + 'static,
    {
        let error: &(dyn Error + 'static) = self.0.as_ref();
        error.downcast_ref::<E>()
    }
}

impl fmt::Debug for ErrorCause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_error(), formatter)
    }
}

impl fmt::Display for ErrorCause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.as_error(), formatter)
    }
}

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
    /// The application state machine failed.
    ///
    /// `source` is shared rather than owned because a failure that poisons the
    /// group has two owners: the group retains it as its
    /// [`crate::group::RaftGroup::poison_cause`] so every later refusal can
    /// report what broke, and the same error travels here to the caller that
    /// triggered it. `ReplicatedStateMachine::Error` is deliberately not
    /// `Clone`, so the two share one allocation.
    StateMachine {
        operation: StateMachineOperation,
        source: Arc<E>,
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
    /// A Raft-driven snapshot install reached a state machine that declared
    /// [`crate::state_machine::SnapshotSupport::Unsupported`].
    ///
    /// The state machine was not called. This replica has fallen behind the
    /// leader's compacted prefix and cannot catch up, so the group poisons.
    SnapshotsUnsupported {
        snapshot_index: LogIndex,
    },
    /// A state machine that declared
    /// [`crate::state_machine::SnapshotSupport::Supported`] refused the
    /// install as unsupported, which means it inherited a provided method body
    /// while declaring support.
    SnapshotSupportMisdeclared {
        snapshot_index: LogIndex,
    },
    /// The group is permanently poisoned.
    ///
    /// `cause` is the error that poisoned the group, when the poison came from
    /// a typed failure. It is `None` for a poison with no underlying error,
    /// such as a malformed snapshot output or a state machine that broke an
    /// apply-result invariant.
    Poisoned {
        reason: String,
        cause: Option<ErrorCause>,
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
            Self::SnapshotsUnsupported { snapshot_index } => write!(
                formatter,
                "refusing snapshot install at index {snapshot_index}: the state machine declares no application snapshot support"
            ),
            Self::SnapshotSupportMisdeclared { snapshot_index } => write!(
                formatter,
                "state machine declares application snapshot support but refused the install at index {snapshot_index} as unsupported"
            ),
            Self::Poisoned { reason, .. } => write!(formatter, "Raft group is poisoned: {reason}"),
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
            Self::StateMachine { source, .. } => Some(&**source),
            // Transparent to the preserved cause: a chain printer walks one
            // link per real failure rather than one per boundary crossed.
            Self::Poisoned { cause, .. } => match cause {
                Some(cause) => Some(cause.as_error()),
                None => None,
            },
            Self::ApplyResultCountMismatch { .. }
            | Self::ApplyResultMetadataMismatch { .. }
            | Self::ApplyEntryAlreadyApplied { .. }
            | Self::AppliedIndexBehind { .. }
            | Self::MalformedSnapshot { .. }
            | Self::SnapshotsUnsupported { .. }
            | Self::SnapshotSupportMisdeclared { .. }
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
            source: Arc::new(TestError("apply failed")),
        };

        assert_eq!(
            error.to_string(),
            "state machine batch apply failed: apply failed"
        );
    }

    /// The shared source is the same object the group kept as its poison cause,
    /// and it stays reachable as a typed `source()` link.
    #[test]
    fn a_state_machine_group_error_exposes_its_shared_source() {
        let source = Arc::new(TestError("apply failed"));
        let error = GroupError::<TestError, TestError>::StateMachine {
            operation: StateMachineOperation::ApplyBatch,
            source: Arc::clone(&source),
        };
        let cause = ErrorCause::from_shared(source);

        assert_eq!(
            error
                .source()
                .expect("the state machine error is exposed")
                .to_string(),
            "apply failed"
        );
        assert!(cause.downcast_ref::<TestError>().is_some());
    }

    #[test]
    fn a_preserved_cause_downcasts_to_the_error_the_caller_kept() {
        let cause = ErrorCause::new(TestError("apply failed"));

        assert_eq!(
            cause
                .downcast_ref::<TestError>()
                .expect("the preserved error keeps its own type")
                .0,
            "apply failed"
        );
        assert!(cause.downcast_ref::<fmt::Error>().is_none());
    }

    #[test]
    fn a_preserved_cause_renders_as_the_error_it_holds() {
        let cause = ErrorCause::new(TestError("apply failed"));

        assert_eq!(cause.to_string(), "apply failed");
        assert_eq!(
            format!("{cause:?}"),
            format!("{:?}", TestError("apply failed"))
        );
        assert_eq!(cause.as_error().to_string(), "apply failed");
    }

    /// The cause is a handle rather than an error, so a chain printer walks one
    /// link per real failure: `source()` reaches the preserved error directly
    /// and not an `ErrorCause` wrapper that renders the same text twice.
    #[test]
    fn a_poisoned_group_error_exposes_the_preserved_cause_as_its_source() {
        let error = GroupError::<TestError, TestError>::Poisoned {
            reason: "ApplyBatch failed".to_owned(),
            cause: Some(ErrorCause::new(TestError("apply failed"))),
        };

        let source = error.source().expect("the poison cause is exposed");

        assert_eq!(source.to_string(), "apply failed");
        assert!(source.downcast_ref::<TestError>().is_some());
        assert!(source.source().is_none());
    }

    /// The `Option` is not decoration: a poison with no underlying error must
    /// not invent one.
    #[test]
    fn a_poisoned_group_error_without_a_cause_has_no_source() {
        let error = GroupError::<TestError, TestError>::Poisoned {
            reason: "malformed snapshot output: snapshot last included index is zero".to_owned(),
            cause: None,
        };

        assert!(error.source().is_none());
    }

    /// The category is what a caller branches on; the cause is reached through
    /// `source()`. A `Display` that interpolated the cause would print it twice
    /// in any chain-aware report.
    #[test]
    fn poisoned_display_states_the_category_without_repeating_the_cause() {
        let error = GroupError::<TestError, TestError>::Poisoned {
            reason: "ApplyBatch failed".to_owned(),
            cause: Some(ErrorCause::new(TestError("disk unavailable"))),
        };

        let rendered = error.to_string();

        assert_eq!(rendered, "Raft group is poisoned: ApplyBatch failed");
        assert!(!rendered.contains("disk unavailable"));
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
