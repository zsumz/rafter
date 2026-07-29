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
/// This diagnostic vocabulary is `#[non_exhaustive]`: new state-machine
/// callbacks may add operations, and a caller that does not recognize one can
/// still preserve and report the underlying error without reclassifying it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum StateMachineOperation {
    /// Reading the durable applied-index marker.
    AppliedIndex,
    /// Encoding an application command for the replicated log.
    EncodeCommand,
    /// Decoding an application command from the replicated log.
    DecodeCommand,
    /// Applying a committed batch to the application state machine.
    ApplyBatch,
    /// Reading application state.
    Read,
    /// Installing application snapshot bytes.
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
    /// The underlying Raft runtime failed.
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
        /// Callback that failed.
        operation: StateMachineOperation,
        /// Exact application error returned by the callback.
        source: Arc<E>,
    },
    /// The state machine returned the wrong number of apply results.
    ApplyResultCountMismatch {
        /// Number of committed entries presented to the state machine.
        expected: usize,
        /// Number of results returned by the state machine.
        actual: usize,
    },
    /// An apply result did not preserve the committed entry's identity.
    ApplyResultMetadataMismatch {
        /// Committed log index.
        expected_index: LogIndex,
        /// Log index reported by the state machine.
        actual_index: LogIndex,
        /// Committed log term.
        expected_term: Term,
        /// Log term reported by the state machine.
        actual_term: Term,
        /// Local proposal identifier attached to the committed entry.
        expected_local_proposal_id: Option<LocalProposalId>,
        /// Local proposal identifier reported by the state machine.
        actual_local_proposal_id: Option<LocalProposalId>,
    },
    /// The state machine claims an entry awaiting apply was already applied.
    ApplyEntryAlreadyApplied {
        /// Index of the entry the group attempted to apply.
        entry_index: LogIndex,
        /// Durable applied index reported by the state machine.
        app_applied_index: LogIndex,
        /// Applied index previously accepted by the group.
        group_applied_index: LogIndex,
    },
    /// The state machine's applied index is behind the group's required floor.
    AppliedIndexBehind {
        /// Minimum applied index required by the group.
        required: LogIndex,
        /// Durable applied index reported by the state machine.
        actual: LogIndex,
    },
    /// The state machine is below the runtime's snapshot boundary, so the
    /// entries it is missing are compacted out of the Raft log and will never
    /// be delivered to it.
    ///
    /// The kernel raises a declared applied floor to its snapshot boundary,
    /// because it can neither emit the covered entries nor restore a state
    /// machine from a snapshot whose bytes it does not hold. That raise is
    /// silent, and this group refuses to run on top of it: every
    /// [`crate::group::RaftGroup::step`] and every
    /// [`crate::group::RaftGroup::begin_proposal`] would advance the protocol
    /// for, and every [`crate::group::RaftGroup::read`] would answer from, a
    /// state machine missing acknowledged entries, with nothing reporting the
    /// gap.
    ///
    /// [`crate::group::RaftGroup::apply_raft_outputs`] never returns this
    /// error, and neither does [`crate::group::RaftGroup::metrics`]. A replica
    /// that crashed between promoting an inbound snapshot and installing it is
    /// legitimately below its boundary while it drains its recovery outputs,
    /// and metrics reporting `applied_index` beside `snapshot_index` is how an
    /// operator sees the gap. The refusal falls on the first step or read
    /// after a restore that never came.
    ///
    /// The repair is to restore the state machine from the snapshot the
    /// boundary names, before or after constructing the group, or to discard
    /// this replica's Raft state so it rejoins empty and is sent one.
    AppliedIndexBelowSnapshotBoundary {
        /// Durable applied index reported by the state machine.
        app_applied_index: LogIndex,
        /// Compacted snapshot boundary retained by the Raft runtime.
        snapshot_index: LogIndex,
    },
    /// The Raft runtime emitted snapshot metadata that the group cannot apply.
    MalformedSnapshot {
        /// Stable explanation of the invalid snapshot output.
        reason: String,
    },
    /// A Raft-driven snapshot install reached a state machine that declared
    /// [`crate::state_machine::SnapshotSupport::Unsupported`].
    ///
    /// The state machine was not called. This replica has fallen behind the
    /// leader's compacted prefix and cannot catch up, so the group poisons.
    SnapshotsUnsupported {
        /// Index of the snapshot that must be installed.
        snapshot_index: LogIndex,
    },
    /// A state machine that declared
    /// [`crate::state_machine::SnapshotSupport::Supported`] refused the
    /// install as unsupported, which means it inherited a provided method body
    /// while declaring support.
    SnapshotSupportMisdeclared {
        /// Index of the snapshot the state machine refused.
        snapshot_index: LogIndex,
    },
    /// The group is permanently poisoned.
    ///
    /// `cause` is the error that poisoned the group, when the poison came from
    /// a typed failure. It is `None` for a poison with no underlying error,
    /// such as a malformed snapshot output or a state machine that broke an
    /// apply-result invariant.
    Poisoned {
        /// Stable explanation of the failure that poisoned the group.
        reason: String,
        /// Preserved typed cause, when the poison originated in a callback.
        cause: Option<ErrorCause>,
    },
    /// An input names another Raft group.
    WrongGroup,
    /// An inbound message targets another local replica.
    WrongRecipient {
        /// Local node identifier required by this group.
        expected: NodeId,
        /// Recipient carried by the inbound message.
        actual: NodeId,
    },
    /// A proposal identifier did not increase over the prior local proposal.
    NonMonotonicLocalProposalId {
        /// Reused or decreasing proposal identifier.
        local_proposal_id: LocalProposalId,
        /// Greatest proposal identifier previously accepted locally.
        last_seen_local_proposal_id: LocalProposalId,
    },
    /// A read identifier is already active.
    DuplicateReadId {
        /// Identifier already owned by an in-flight read.
        read_id: ReadId,
    },
    /// A read identifier did not increase over the prior local read.
    NonMonotonicReadId {
        /// Reused or decreasing read identifier.
        read_id: ReadId,
        /// Greatest read identifier previously accepted locally.
        last_seen_read_id: ReadId,
    },
    /// The synchronous group does not implement the requested read mode.
    UnsupportedReadConsistency {
        /// Read mode rejected by the group.
        consistency: ReadConsistency,
    },
    /// The runtime emitted an output that this group integration cannot handle.
    UnsupportedOutput {
        /// Stable name of the unsupported output variant.
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
            Self::AppliedIndexBelowSnapshotBoundary {
                app_applied_index,
                snapshot_index,
            } => write!(
                formatter,
                "state machine applied index {app_applied_index} is below the snapshot boundary {snapshot_index}, whose covered entries are compacted and can never be applied"
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
            | Self::AppliedIndexBelowSnapshotBoundary { .. }
            | Self::MalformedSnapshot { .. }
            | Self::SnapshotsUnsupported { .. }
            | Self::SnapshotSupportMisdeclared { .. }
            | Self::WrongGroup
            | Self::WrongRecipient { .. }
            | Self::NonMonotonicLocalProposalId { .. }
            | Self::DuplicateReadId { .. }
            | Self::NonMonotonicReadId { .. }
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
