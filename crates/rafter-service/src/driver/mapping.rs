#![allow(clippy::wildcard_imports)]

use std::{error::Error, fmt};

use super::*;

/// Error returned while constructing or manually driving a managed service
/// driver.
///
/// Both shipped drivers report through this type, and they do not reach the
/// same variants. The cluster-shaped ones — [`ManagedDriverError::EmptyCluster`],
/// [`ManagedDriverError::MissingPrimary`], [`ManagedDriverError::MissingNode`],
/// [`ManagedDriverError::DuplicateNode`], and [`ManagedDriverError::Stalled`] —
/// describe a set of replicas and can only come from
/// [`crate::InMemoryRaftDriver`], which owns one. The incarnation-shaped ones —
/// [`ManagedDriverError::NoGroup`], [`ManagedDriverError::GroupAlreadyAdopted`],
/// and [`ManagedDriverError::InvalidOptions`] — describe a single replica's slot
/// and can only come from [`crate::TransportRaftDriver`], which has one. The
/// rest are adoption and stepping faults that either driver reports, including
/// [`ManagedDriverError::MixedGroups`]: each driver serves one group ID, and
/// each refuses a group that does not belong to it.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum ManagedDriverError {
    /// No groups were supplied, so there is no cluster to drive.
    EmptyCluster,
    /// The node named as primary is not among the supplied groups.
    ///
    /// The primary is the replica the in-memory driver proposes through, so a
    /// driver without one cannot serve a write at all.
    MissingPrimary { node_id: NodeId },
    /// A frame was addressed to a node this driver does not own.
    ///
    /// The in-memory network routes by node ID, so this is a routing fault
    /// rather than a cluster-membership one.
    MissingNode { node_id: NodeId },
    /// Two supplied groups claim the same node ID.
    ///
    /// Refused rather than deduplicated: the driver correlates outcomes by node,
    /// and two replicas answering to one ID make that correspondence undefined.
    DuplicateNode { node_id: NodeId },
    /// A group offered for adoption is poisoned, or still holds waiters a poison
    /// captured.
    ///
    /// A poisoned group emits no further events for those waiters, so adopting
    /// one would install clients that can never be resolved.
    PoisonedGroup { node_id: NodeId, reason: String },
    /// A group offered for adoption still tracks proposals or reads.
    ///
    /// A driver resolves only the waiters it created, so a waiter arriving with
    /// the group could never be resolved. [`crate::TransportRaftDriver::adopt_group`]
    /// is the one exception, and only for proposals: a released group's writes
    /// were already answered, and its entries are durable.
    NonQuiescentGroup {
        node_id: NodeId,
        pending_proposals: usize,
        reserved_reads: usize,
    },
    /// The adopted local proposal ID watermark cannot be advanced.
    ///
    /// Generated IDs must stay strictly above every ID the group has seen, and
    /// there is no ID above this one.
    LocalProposalIdExhausted {
        node_id: NodeId,
        last_seen_local_proposal_id: LocalProposalId,
    },
    /// The adopted read ID watermark cannot be advanced, for the reason
    /// [`ManagedDriverError::LocalProposalIdExhausted`] gives.
    ReadIdExhausted {
        node_id: NodeId,
        last_seen_read_id: ReadId,
    },
    /// A group offered to a driver does not belong to the group ID that driver
    /// serves.
    ///
    /// A driver serves exactly one group. For
    /// [`crate::InMemoryRaftDriver::new`] that means the supplied groups must
    /// all share one ID, or its handles would name only some of the replicas.
    /// For [`crate::TransportRaftDriver::adopt_group`] it means the incoming
    /// group must serve the ID the driver was built with, or client commands
    /// addressed to that ID would be proposed into another group's log.
    MixedGroups,
    /// The driver made no progress within its drive bound.
    ///
    /// A refusal rather than an unbounded wait, so a protocol that cannot
    /// advance surfaces as a typed error instead of a hang.
    Stalled { max_steps: usize },
    /// The driver has shut down, which is terminal.
    ///
    /// A supervisor that wants to serve again builds a driver; adopting a group
    /// into a shut-down one is refused.
    ShuttingDown,
    /// The driver released its group and has not adopted a new one.
    ///
    /// Every operation refuses in this state; nothing panics, because a slot
    /// with a typed empty state is the point of having one.
    NoGroup,
    /// The driver still holds a group, so it cannot adopt another.
    GroupAlreadyAdopted,
    /// A [`crate::TransportDriverOptions`] field was outside its valid range.
    InvalidOptions {
        field: &'static str,
        reason: &'static str,
    },
    /// A group operation failed while the driver was driving it.
    ///
    /// The category is the variant and the detail is the preserved cause; there
    /// is no free-text message field, so nothing downstream can be tempted to
    /// match on rendered text.
    Group { cause: ErrorCause },
}

impl fmt::Display for ManagedDriverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyCluster => formatter.write_str("managed driver requires at least one group"),
            Self::MissingPrimary { node_id } => {
                write!(formatter, "managed driver primary node {node_id} is missing")
            }
            Self::MissingNode { node_id } => {
                write!(formatter, "managed driver node {node_id} is missing")
            }
            Self::DuplicateNode { node_id } => {
                write!(formatter, "managed driver has duplicate node {node_id}")
            }
            Self::PoisonedGroup { node_id, reason } => write!(
                formatter,
                "managed driver group for node {node_id} is poisoned: {reason}"
            ),
            Self::NonQuiescentGroup {
                node_id,
                pending_proposals,
                reserved_reads,
            } => write!(
                formatter,
                "managed driver cannot adopt node {node_id}: {pending_proposals} pending proposals and {reserved_reads} reserved reads remain"
            ),
            Self::LocalProposalIdExhausted {
                node_id,
                last_seen_local_proposal_id,
            } => write!(
                formatter,
                "managed driver node {node_id} exhausted local proposal ids after {last_seen_local_proposal_id}"
            ),
            Self::ReadIdExhausted {
                node_id,
                last_seen_read_id,
            } => write!(
                formatter,
                "managed driver node {node_id} exhausted read ids after {last_seen_read_id}"
            ),
            Self::MixedGroups => formatter.write_str("managed driver cannot adopt mixed group ids"),
            Self::Stalled { max_steps } => write!(
                formatter,
                "managed driver made no progress within {max_steps} drive steps"
            ),
            Self::ShuttingDown => formatter.write_str("managed driver is shutting down"),
            Self::NoGroup => {
                formatter.write_str("managed driver has released its group and holds none")
            }
            Self::GroupAlreadyAdopted => {
                formatter.write_str("managed driver already holds a group")
            }
            Self::InvalidOptions { field, reason } => {
                write!(formatter, "managed driver option {field} is invalid: {reason}")
            }
            Self::Group { .. } => formatter.write_str("managed driver group operation failed"),
        }
    }
}

impl Error for ManagedDriverError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Group { cause } => Some(cause.as_error()),
            Self::EmptyCluster
            | Self::MissingPrimary { .. }
            | Self::MissingNode { .. }
            | Self::DuplicateNode { .. }
            | Self::PoisonedGroup { .. }
            | Self::NonQuiescentGroup { .. }
            | Self::LocalProposalIdExhausted { .. }
            | Self::ReadIdExhausted { .. }
            | Self::MixedGroups
            | Self::Stalled { .. }
            | Self::ShuttingDown
            | Self::NoGroup
            | Self::GroupAlreadyAdopted
            | Self::InvalidOptions { .. } => None,
        }
    }
}

/// Internal error carried between driver stages before it reaches a client.
///
/// `WrongGroup` is a driver fact rather than a delivery failure, which is why
/// it is a variant here instead of a synthesized transport error.
#[derive(Debug)]
pub(super) enum ManagedOperationError<E, RE> {
    MissingNode { node_id: NodeId },
    WrongGroup,
    DriveBoundReached { max_steps: usize },
    ShuttingDown,
    Write(WriteError),
    Read(ReadError),
    Transfer(TransferLeadershipError),
    Group(GroupError<E, RE>),
}

/// A driver stage that could not route its own work.
///
/// This is the driver reporting on itself, so it is the one place the service
/// layer authors an error object rather than preserving somebody else's.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DriverRoutingError {
    /// A frame was addressed to a node this driver does not own.
    MissingNode { node_id: NodeId },
    /// The driver stopped routing at its own bound rather than looping forever.
    DriveBoundReached { max_steps: usize },
    /// The driver already holds its configured maximum of unresolved waiters.
    ///
    /// Failing closed rather than growing: the operation was refused before
    /// anything was proposed, so nothing is in flight to be uncertain about.
    PendingWaiterLimit { max_pending_waiters: usize },
    /// The driver released its group, so the operation was never started.
    ///
    /// This is a refusal rather than a lost outcome, and it is here rather
    /// than fabricated into an `UnknownOutcome` or an `Abandoned` because
    /// those variants carry an ID, and an operation that never started has
    /// none.
    NoGroup,
}

impl fmt::Display for DriverRoutingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingNode { node_id } => {
                write!(formatter, "managed driver node {node_id} is missing")
            }
            Self::DriveBoundReached { max_steps } => write!(
                formatter,
                "managed driver did not drain within {max_steps} drive steps"
            ),
            Self::PendingWaiterLimit {
                max_pending_waiters,
            } => write!(
                formatter,
                "managed driver already holds {max_pending_waiters} unresolved waiters"
            ),
            Self::NoGroup => {
                formatter.write_str("managed driver has released its group and holds none")
            }
        }
    }
}

impl Error for DriverRoutingError {}

impl<E, RE> ManagedOperationError<E, RE>
where
    E: Error + Send + Sync + 'static,
    RE: Error + Send + Sync + 'static,
{
    /// Maps a staged error into the write surface.
    ///
    /// `fate` is the fate the driver observed for this write, and it is passed
    /// in rather than inferred: the same fault can occur on either side of the
    /// local append, and only the caller knows which side this one was on.
    pub(super) fn into_write_error(self, fate: WriteFate) -> WriteError {
        match self {
            Self::Write(error) => error,
            Self::Read(error) => WriteError::Transport {
                fate,
                cause: ErrorCause::new(error),
            },
            Self::Transfer(error) => WriteError::Transport {
                fate,
                cause: ErrorCause::new(error),
            },
            Self::MissingNode { node_id } => WriteError::Transport {
                fate,
                cause: ErrorCause::new(DriverRoutingError::MissingNode { node_id }),
            },
            Self::DriveBoundReached { max_steps } => WriteError::Transport {
                fate,
                cause: ErrorCause::new(DriverRoutingError::DriveBoundReached { max_steps }),
            },
            Self::WrongGroup => WriteError::WrongGroup,
            Self::ShuttingDown => WriteError::ShuttingDown,
            Self::Group(error) => write_error_from_group(error, fate),
        }
    }

    pub(super) fn into_read_error(self) -> ReadError {
        match self {
            Self::Read(error) => error,
            Self::Write(error) => ReadError::Transport {
                cause: ErrorCause::new(error),
            },
            Self::Transfer(error) => ReadError::Transport {
                cause: ErrorCause::new(error),
            },
            Self::MissingNode { node_id } => ReadError::Transport {
                cause: ErrorCause::new(DriverRoutingError::MissingNode { node_id }),
            },
            Self::DriveBoundReached { max_steps } => ReadError::Transport {
                cause: ErrorCause::new(DriverRoutingError::DriveBoundReached { max_steps }),
            },
            Self::WrongGroup => ReadError::WrongGroup,
            Self::ShuttingDown => ReadError::ShuttingDown,
            Self::Group(error) => read_error_from_group(error),
        }
    }

    pub(super) fn into_transfer_error(self) -> TransferLeadershipError {
        match self {
            Self::Transfer(error) => error,
            Self::Write(error) => TransferLeadershipError::Transport {
                cause: ErrorCause::new(error),
            },
            Self::Read(error) => TransferLeadershipError::Transport {
                cause: ErrorCause::new(error),
            },
            Self::MissingNode { node_id } => TransferLeadershipError::Transport {
                cause: ErrorCause::new(DriverRoutingError::MissingNode { node_id }),
            },
            Self::DriveBoundReached { max_steps } => TransferLeadershipError::Transport {
                cause: ErrorCause::new(DriverRoutingError::DriveBoundReached { max_steps }),
            },
            Self::WrongGroup => TransferLeadershipError::WrongGroup,
            Self::ShuttingDown => TransferLeadershipError::ShuttingDown,
            Self::Group(error) => transfer_error_from_group(error),
        }
    }
}

impl<E, RE> From<GroupError<E, RE>> for ManagedOperationError<E, RE> {
    fn from(error: GroupError<E, RE>) -> Self {
        Self::Group(error)
    }
}

impl<E, RE> From<ManagedOperationError<E, RE>> for ManagedDriverError
where
    E: Error + Send + Sync + 'static,
    RE: Error + Send + Sync + 'static,
{
    fn from(error: ManagedOperationError<E, RE>) -> Self {
        match error {
            ManagedOperationError::MissingNode { node_id } => Self::MissingNode { node_id },
            ManagedOperationError::DriveBoundReached { max_steps } => Self::Group {
                cause: ErrorCause::new(DriverRoutingError::DriveBoundReached { max_steps }),
            },
            ManagedOperationError::WrongGroup => Self::Group {
                cause: ErrorCause::new(WriteError::WrongGroup),
            },
            ManagedOperationError::ShuttingDown => Self::ShuttingDown,
            ManagedOperationError::Write(error) => Self::Group {
                cause: ErrorCause::new(error),
            },
            ManagedOperationError::Read(error) => Self::Group {
                cause: ErrorCause::new(error),
            },
            ManagedOperationError::Transfer(error) => Self::Group {
                cause: ErrorCause::new(error),
            },
            ManagedOperationError::Group(error) => Self::Group {
                cause: ErrorCause::new(error),
            },
        }
    }
}

pub(super) fn write_error_from_group<E, RE>(error: GroupError<E, RE>, fate: WriteFate) -> WriteError
where
    E: Error + Send + Sync + 'static,
    RE: Error + Send + Sync + 'static,
{
    match error {
        GroupError::Poisoned { reason, cause } => WriteError::Poisoned {
            fate,
            reason,
            cause,
        },
        GroupError::NonMonotonicLocalProposalId {
            local_proposal_id,
            last_seen_local_proposal_id,
        } => {
            // Provably pre-append: the group refuses before it proposes.
            WriteError::ManagedInvariantViolation {
                fate: WriteFate::NotAppended,
                message: format!(
                    "managed driver local-ID invariant violation: generated non-monotonic local proposal id {local_proposal_id} after {last_seen_local_proposal_id}"
                ),
            }
        }
        GroupError::WrongGroup => WriteError::WrongGroup,
        // The operation is load-bearing and is no longer folded away: encoding
        // a command touches no storage, and reporting it as a storage failure
        // pointed an operator at the wrong subsystem.
        GroupError::StateMachine { operation, source } => WriteError::StateMachine {
            operation,
            fate,
            cause: ErrorCause::from_shared(source),
        },
        GroupError::Runtime(error) => WriteError::Storage {
            fate,
            cause: ErrorCause::new(error),
        },
        error => WriteError::Transport {
            fate,
            cause: ErrorCause::new(error),
        },
    }
}

/// The client answer a routed [`ReadEvent`] carries, when it ends the barrier.
///
/// Both shipped drivers route read events, so both need the same reading of
/// one. `Rejected` and `Canceled` are terminal: the app layer cleared the
/// barrier's local waiter state before emitting them, so the event is the whole
/// answer and nothing may ask the group again — a retry against a spent
/// `ReadId` gets [`GroupError::NonMonotonicReadId`], which a driver can only
/// report as an invariant violation of its own.
///
/// The rest are `None` because they are not answers. `Granted` leaves the proof
/// cached for a read call to consume, `FreshnessUnavailable` leaves the barrier
/// reserved until the applied index catches up, and a variant neither driver
/// knows is not something to resolve a client with. In all three the caller
/// keeps waiting.
pub(super) fn terminal_read_error<G>(event: &ReadEvent<G>) -> Option<(ReadId, ReadError)> {
    match event {
        ReadEvent::Rejected {
            read_id,
            reason,
            leader_hint,
        } => Some((
            *read_id,
            ReadError::Rejected {
                read_id: Some(*read_id),
                reason: *reason,
                leader_hint: *leader_hint,
            },
        )),
        ReadEvent::Canceled {
            read_id,
            reason,
            leader_hint,
        } => Some((
            *read_id,
            ReadError::Canceled {
                read_id: *read_id,
                reason: *reason,
                leader_hint: *leader_hint,
            },
        )),
        _ => None,
    }
}

pub(super) fn read_error_from_group<E, RE>(error: GroupError<E, RE>) -> ReadError
where
    E: Error + Send + Sync + 'static,
    RE: Error + Send + Sync + 'static,
{
    match error {
        GroupError::Poisoned { reason, cause } => ReadError::Poisoned { reason, cause },
        GroupError::DuplicateReadId { read_id } => ReadError::ManagedInvariantViolation {
            message: format!(
                "managed driver local-ID invariant violation: generated duplicate read id {read_id}"
            ),
        },
        GroupError::NonMonotonicReadId {
            read_id,
            last_seen_read_id,
        } => ReadError::ManagedInvariantViolation {
            message: format!(
                "managed driver local-ID invariant violation: generated non-monotonic read id {read_id} after {last_seen_read_id}"
            ),
        },
        GroupError::WrongGroup => ReadError::WrongGroup,
        GroupError::StateMachine { operation, source } => ReadError::StateMachine {
            operation,
            cause: ErrorCause::from_shared(source),
        },
        GroupError::Runtime(error) => ReadError::Storage {
            cause: ErrorCause::new(error),
        },
        GroupError::UnsupportedReadConsistency { consistency } => {
            ReadError::UnsupportedConsistency { consistency }
        }
        error => ReadError::Transport {
            cause: ErrorCause::new(error),
        },
    }
}

pub(super) fn transfer_error_from_group<E, RE>(error: GroupError<E, RE>) -> TransferLeadershipError
where
    E: Error + Send + Sync + 'static,
    RE: Error + Send + Sync + 'static,
{
    match error {
        GroupError::Poisoned { reason, cause } => {
            TransferLeadershipError::Poisoned { reason, cause }
        }
        GroupError::WrongGroup => TransferLeadershipError::WrongGroup,
        GroupError::Runtime(error) => TransferLeadershipError::Storage {
            cause: ErrorCause::new(error),
        },
        error => TransferLeadershipError::Transport {
            cause: ErrorCause::new(error),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rafter_app::error::StateMachineOperation;

    use super::*;

    #[derive(Debug)]
    struct MappingRuntimeError;

    impl fmt::Display for MappingRuntimeError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("mapping runtime error")
        }
    }

    impl Error for MappingRuntimeError {}

    #[derive(Debug)]
    struct MappingAppError;

    impl fmt::Display for MappingAppError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("mapping app error")
        }
    }

    impl Error for MappingAppError {}

    #[test]
    fn non_monotonic_local_proposal_id_maps_to_managed_invariant_write_error() {
        let error = write_error_from_group::<MappingAppError, MappingRuntimeError>(
            GroupError::NonMonotonicLocalProposalId {
                local_proposal_id: LocalProposalId(7),
                last_seen_local_proposal_id: LocalProposalId(9),
            },
            WriteFate::Unresolved,
        );

        let WriteError::ManagedInvariantViolation { fate, message } = &error else {
            panic!("expected a managed invariant violation, got {error:?}");
        };
        assert_eq!(
            message,
            "managed driver local-ID invariant violation: generated non-monotonic local proposal id local-proposal-7 after local-proposal-9"
        );
        assert_eq!(
            *fate,
            WriteFate::NotAppended,
            "the group refuses a non-monotonic id before it proposes"
        );
    }

    #[test]
    fn duplicate_read_id_maps_to_managed_invariant_read_error() {
        let error = read_error_from_group::<MappingAppError, MappingRuntimeError>(
            GroupError::DuplicateReadId { read_id: ReadId(8) },
        );

        let ReadError::ManagedInvariantViolation { message } = &error else {
            panic!("expected a managed invariant violation, got {error:?}");
        };
        assert_eq!(
            message,
            "managed driver local-ID invariant violation: generated duplicate read id read-8"
        );
    }

    #[test]
    fn non_monotonic_read_id_maps_to_managed_invariant_read_error() {
        let error = read_error_from_group::<MappingAppError, MappingRuntimeError>(
            GroupError::NonMonotonicReadId {
                read_id: ReadId(8),
                last_seen_read_id: ReadId(10),
            },
        );

        let ReadError::ManagedInvariantViolation { message } = &error else {
            panic!("expected a managed invariant violation, got {error:?}");
        };
        assert_eq!(
            message,
            "managed driver local-ID invariant violation: generated non-monotonic read id read-8 after read-10"
        );
    }

    /// The old mapping folded six operations into two variants and got one
    /// wrong: `EncodeCommand` was reported as a storage failure, and encoding a
    /// command touches no storage.
    #[test]
    fn a_state_machine_error_keeps_the_operation_that_surfaced_it() {
        let error = write_error_from_group::<MappingAppError, MappingRuntimeError>(
            GroupError::StateMachine {
                operation: StateMachineOperation::EncodeCommand,
                source: Arc::new(MappingAppError),
            },
            WriteFate::NotAppended,
        );

        let WriteError::StateMachine {
            operation, cause, ..
        } = &error
        else {
            panic!("expected a state machine error, got {error:?}");
        };
        assert_eq!(*operation, StateMachineOperation::EncodeCommand);
        assert!(cause.downcast_ref::<MappingAppError>().is_some());
    }

    #[test]
    fn managed_driver_error_is_a_standard_error_with_display_message() {
        let error = ManagedDriverError::MissingNode { node_id: NodeId(9) };
        let standard_error: &(dyn Error + 'static) = &error;

        assert_eq!(
            standard_error.to_string(),
            "managed driver node node-9 is missing"
        );
    }

    /// The category is the variant; the detail is the preserved cause. There is
    /// no message field to render into.
    #[test]
    fn a_group_driver_error_preserves_its_cause() {
        let error = ManagedDriverError::from(ManagedOperationError::<
            MappingAppError,
            MappingRuntimeError,
        >::Group(GroupError::Runtime(
            MappingRuntimeError,
        )));

        let source = error.source().expect("the group error is preserved");

        assert!(source
            .downcast_ref::<GroupError<MappingAppError, MappingRuntimeError>>()
            .is_some());
    }
}
