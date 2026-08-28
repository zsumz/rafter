//! How an app-layer failure reaches a client.
//!
//! One mapping per client surface, shared by both shipped drivers so a group
//! error and an ended read barrier read the same whichever driver observed
//! them. Nothing here inspects driver state: the fate a write mapping stamps is
//! passed in by the caller that observed it, because the same fault can occur on
//! either side of the local append.

#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

use std::error::Error;

use super::*;

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
