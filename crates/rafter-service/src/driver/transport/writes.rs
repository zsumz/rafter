#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

//! Admitting one write.
//!
//! Every refusal a driver can raise before it proposes, in the order that keeps
//! them provable, and the waiter registered before the group is stepped so a
//! terminal event emitted inside that very step finds someone listening. The
//! fate a failing step reports is never inferred from the absence of an observed
//! append: only the group errors that are themselves the whole event prove one.

use std::error::Error;

use crate::error::StateMachineOperation;
use crate::transport::{AuthenticatedPeerValidator, RaftTransport};

use super::state::{StepFailure, TransportDriverState};
use super::waiters::WriteWaiter;
use super::*;

impl<G, A, R, T, V> TransportDriverState<G, A, R, T, V>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine + Send + 'static,
    A::Command: Send + 'static,
    A::CommandResult: Clone + Send + 'static,
    A::Query: Clone + Send + 'static,
    A::QueryResult: Send + 'static,
    R: PersistedRaftRuntime + Send + 'static,
    T: RaftTransport<G>,
    V: AuthenticatedPeerValidator<G, T::PeerPrincipal> + Send + Sync + 'static,
{
    pub(super) fn begin_write(
        &mut self,
        group_id: &G,
        command: A::Command,
        options: WriteOptions,
    ) -> Result<LocalProposalId, WriteError> {
        if self.shutting_down {
            return Err(WriteError::ShuttingDown);
        }
        if group_id != &self.group_id {
            return Err(WriteError::WrongGroup);
        }
        // One gate for every reason this driver refuses on its own standing, a
        // released group included: nothing was proposed for any of them, so
        // every one is `NotAppended`, which `WriteError::Unavailable` answers
        // from the variant alone. This used to be two checks reporting a
        // *transport* failure with a crate-private cause, and no transport
        // operation had failed.
        if let Err(reason) = self.reject_if_not_serving() {
            return Err(WriteError::Unavailable { reason });
        }
        let unresolved = self
            .write_waiters
            .values()
            .filter(|waiter| waiter.outcome.is_none())
            .count();
        if unresolved >= self.options.max_pending_waiters {
            // Nothing was proposed, so the refusal is observed.
            return Err(WriteError::Transport {
                fate: WriteFate::NotAppended,
                cause: ErrorCause::new(DriverRoutingError::PendingWaiterLimit {
                    max_pending_waiters: self.options.max_pending_waiters,
                }),
            });
        }
        let next = self
            .next_proposal_id
            .ok_or(WriteError::LocalProposalIdExhausted)?;
        self.next_proposal_id = next.checked_add(1);
        let local_proposal_id = LocalProposalId(next);
        // Registered before the step, so a terminal event emitted inside the
        // very step that starts the proposal resolves this waiter rather than
        // arriving before anything is listening.
        self.write_waiters.insert(
            local_proposal_id,
            WriteWaiter {
                options,
                outcome: None,
                waker: None,
            },
        );
        let proposal = Proposal {
            local_proposal_id,
            client_request_id: options.client_request_id,
            command,
        };
        if let Err(failure) = self.step_group(GroupInput::Proposal { proposal }) {
            // `step_group` already drained any waiter the poison captured, and
            // `resolve_write` keeps the first outcome, so a captured proposal
            // keeps its `GroupPoisoned` answer and never reaches the mapping
            // below. That ordering is the whole mechanism, and it is the one
            // `InMemoryRaftDriver::finish_failed_write_batch` uses.
            self.resolve_write(local_proposal_id, Err(write_failure(failure)));
        }
        Ok(local_proposal_id)
    }
}

/// Maps one failed proposing step onto the fate the driver can prove.
///
/// The driver never infers `NotAppended` from the absence of an observed append.
/// A step that failed after the group was asked to propose is unresolved,
/// because the entry may be on disk and a node reopened over the same durable
/// log can still replicate and commit it. `NotAppended` survives only where the
/// refusal is itself the whole event, and [`pre_proposal_fate`] is the list of
/// group errors that are.
fn write_failure<E, RE>(failure: StepFailure<E, RE>) -> WriteError
where
    E: Error + Send + Sync + 'static,
    RE: Error + Send + Sync + 'static,
{
    match failure {
        StepFailure::NoGroup => WriteError::Unavailable {
            reason: DriverUnavailableReason::Released,
        },
        StepFailure::Group(error) => {
            let fate = pre_proposal_fate(&error);
            write_error_from_group(error, fate)
        }
    }
}

/// Whether one group error proves the proposal never reached the log.
///
/// The rule is the entry's own — `NotAppended` is reported only where the
/// refusal is the thing that happened — and these are the two group errors that
/// satisfy it beside `NonMonotonicLocalProposalId`, which
/// `write_error_from_group` stamps itself:
///
/// - [`GroupError::Poisoned`] is produced by `reject_if_poisoned`, which is
///   `RaftGroup::step_with_options`'s first statement and the variant's only
///   producer. A step that *becomes* poisoned reports the state-machine or
///   malformed-snapshot error that poisoned it instead, so this variant means
///   the group refused before it looked at the proposal. It is also the most
///   travelled failing-write path a poisoned replica has — every write after
///   the first — and `Unresolved` there tells a caller its request identity may
///   be spent, foreclosing the retry that is in fact the safe thing to do.
/// - [`StateMachineOperation::EncodeCommand`] runs before the group records the
///   proposal and before it hands anything to the runtime. Every other
///   state-machine operation reachable from a proposing step runs after the
///   append, on an entry the log already holds, which is the case this driver
///   reports `Unresolved` for.
fn pre_proposal_fate<E, RE>(error: &GroupError<E, RE>) -> WriteFate {
    match error {
        GroupError::Poisoned { .. }
        | GroupError::StateMachine {
            operation: StateMachineOperation::EncodeCommand,
            ..
        } => WriteFate::NotAppended,
        _ => WriteFate::Unresolved,
    }
}
