#![allow(clippy::wildcard_imports)]

//! The client-waiter tables behind one transport driver.
//!
//! A second impl block on [`TransportDriverState`] rather than a second type:
//! the waiter tables and the step loop share one lock and one group, and the
//! split is by what a reader came for. `state.rs` answers "what does a step
//! do"; this answers "what happens to the client".

use std::{
    error::Error,
    task::{Context, Poll, Waker},
};

use crate::error::StateMachineOperation;
use crate::transport::{AuthenticatedPeerValidator, RaftTransport};

use super::super::*;
use super::state::{StartedRead, StepFailure, TransportDriverState, WaiterId};

pub(super) struct WriteWaiter<R> {
    pub(super) options: WriteOptions,
    pub(super) outcome: Option<Result<WriteReceipt<R>, WriteError>>,
    pub(super) waker: Option<Waker>,
}

pub(super) struct ReadWaiter<G, Q, QR> {
    pub(super) request: ReadRequest<G, Q>,
    /// Whether a routed [`ReadEvent::Granted`] said this barrier's proof is
    /// cached and waiting for a read call to consume it.
    ///
    /// This is the whole retry policy. A read against a barrier the group
    /// already tracks returns an unstepped report, so a second attempt without
    /// a grant in between cannot see a different answer.
    pub(super) proof_ready: bool,
    pub(super) outcome: Option<Result<QueryReceipt<G, QR>, ReadError>>,
    pub(super) waker: Option<Waker>,
}

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
    /// Resolves a barrier the group itself ended, and records one it granted.
    ///
    /// The terminal mapping is [`terminal_read_error`], which
    /// [`InMemoryRaftState::route_report`] also uses, so a barrier the group
    /// ended reads the same on either driver. The mapping agrees with
    /// [`TransportDriverState::handle_read_outcome`] too, so a barrier resolves
    /// identically whichever step observed its end. The terminal arm never
    /// touches the group again: the event carries the whole answer, and the
    /// group has already dropped that barrier's state.
    ///
    /// `Granted` is the one event this driver reads further. It is not terminal
    /// — the proof is cached in the group and a later read call consumes it —
    /// and recording it is what lets `drive_pending_reads` attempt exactly the
    /// barriers whose answer can have changed.
    pub(super) fn observe_read_event(&mut self, event: &ReadEvent<G>) {
        if let ReadEvent::Granted { read_id, .. } = event {
            if let Some(waiter) = self.read_waiters.get_mut(read_id) {
                waiter.proof_ready = true;
            }
            return;
        }
        if let Some((read_id, error)) = terminal_read_error(event) {
            self.resolve_read(read_id, Err(error));
        }
    }

    pub(super) fn observe_proposal_event(&mut self, event: &ProposalEvent<A::CommandResult>) {
        let (local_proposal_id, outcome) = match event {
            // A local append is not a terminal outcome and is not recorded.
            // The driver used to keep it as a fate discriminator; it could not
            // be one, because "no append was observed" is not "no append
            // happened", and the fate this driver reports is only ever the one
            // it observed.
            ProposalEvent::Applied {
                local_proposal_id,
                index,
                term,
                result,
            } => (
                *local_proposal_id,
                Ok(WriteReceipt {
                    index: *index,
                    term: *term,
                    result: result.clone(),
                }),
            ),
            ProposalEvent::Rejected {
                local_proposal_id,
                reason,
                leader_hint,
            } => (
                *local_proposal_id,
                Err(write_error_from_rejection(reason.clone(), *leader_hint)),
            ),
            ProposalEvent::UnknownOutcome {
                local_proposal_id,
                client_request_id,
                reason,
            } => {
                let options = self
                    .write_waiters
                    .get(local_proposal_id)
                    .map_or_else(WriteOptions::default, |waiter| waiter.options);
                (
                    *local_proposal_id,
                    Err(WriteError::UnknownOutcome {
                        local_proposal_id: *local_proposal_id,
                        client_request_id: client_request_id.or(options.client_request_id),
                        reason: managed_unknown_reason_from_app(reason),
                    }),
                )
            }
            _ => return,
        };
        self.resolve_write(local_proposal_id, outcome);
    }

    pub(super) fn resolve_write(
        &mut self,
        local_proposal_id: LocalProposalId,
        outcome: Result<WriteReceipt<A::CommandResult>, WriteError>,
    ) {
        let Some(waiter) = self.write_waiters.get_mut(&local_proposal_id) else {
            return;
        };
        if waiter.outcome.is_some() {
            return;
        }
        waiter.outcome = Some(outcome);
        if let Some(waker) = waiter.waker.take() {
            waker.wake();
        }
    }

    pub(super) fn resolve_read(
        &mut self,
        read_id: ReadId,
        outcome: Result<QueryReceipt<G, A::QueryResult>, ReadError>,
    ) {
        let Some(waiter) = self.read_waiters.get_mut(&read_id) else {
            return;
        };
        if waiter.outcome.is_some() {
            return;
        }
        waiter.outcome = Some(outcome);
        if let Some(waker) = waiter.waker.take() {
            waker.wake();
        }
    }

    /// Stops waiting for one write and resolves its client.
    ///
    /// Resolving rather than removing: a caller that abandons may still hold the
    /// future, and a future that answered `ManagedInvariantViolation` because its
    /// own caller abandoned it would be a worse answer than the one it asked
    /// for. The slot is freed either way, because `max_pending_waiters` counts
    /// unresolved waiters.
    pub(super) fn abandon_write(&mut self, local_proposal_id: LocalProposalId) -> bool {
        let Some(waiter) = self.write_waiters.get(&local_proposal_id) else {
            return false;
        };
        if waiter.outcome.is_some() {
            return false;
        }
        let client_request_id = waiter.options.client_request_id;
        self.resolve_write(
            local_proposal_id,
            Err(WriteError::UnknownOutcome {
                local_proposal_id,
                client_request_id,
                reason: UnknownOutcomeReason::DriveBoundReached,
            }),
        );
        true
    }

    /// Stops waiting for one read, cancelling its barrier through the group
    /// first so `reserved_reads` returns to its previous value.
    pub(super) fn abandon_read(&mut self, read_id: ReadId) -> bool {
        let Some(waiter) = self.read_waiters.get(&read_id) else {
            return false;
        };
        if waiter.outcome.is_some() {
            return false;
        }
        if let Some(group) = self.group.as_mut() {
            group.cancel_read(read_id);
        }
        self.resolve_read(
            read_id,
            Err(ReadError::Abandoned {
                read_id,
                reason: ReadAbandonReason::DriveBoundReached,
            }),
        );
        true
    }

    /// Removes one dropped client future's waiter, whichever kind it is.
    ///
    /// The entry point the reclamation path uses, so that a deferred waiter and
    /// an immediately reclaimed one take exactly the same route.
    pub(super) fn discard(&mut self, waiter: WaiterId) {
        match waiter {
            WaiterId::Write(local_proposal_id) => self.discard_write(local_proposal_id),
            WaiterId::Read(read_id) => self.discard_read(read_id),
        }
    }

    /// Removes a waiter whose client future was dropped.
    ///
    /// The future is the only thing that can consume a resolved outcome, so a
    /// dropped future is the moment a waiter provably has no reader. Until this
    /// existed, a client that timed out and dropped its future left its entry —
    /// and, for a read, its cloned request — in the table for the life of the
    /// driver. The bound was never the leak: `max_pending_waiters` counts
    /// unresolved waiters, so the slot came back and the entry did not.
    ///
    /// This does not replace abandonment. `abandon_write` and `abandon_read`
    /// resolve rather than remove, so a caller that abandons and still holds its
    /// future gets its answer on the next poll; only the future's own drop
    /// removes the entry, and a future cannot be dropped and polled.
    pub(super) fn discard_write(&mut self, local_proposal_id: LocalProposalId) {
        self.write_waiters.remove(&local_proposal_id);
    }

    /// Removes a dropped read's waiter, cancelling its barrier first.
    ///
    /// A client that stopped listening must not leave a barrier reserved in the
    /// group any more than it leaves a waiter in the driver, so an unresolved
    /// read gives its `reserved_reads` slot back on the way out. A resolved one
    /// has no barrier left to cancel.
    pub(super) fn discard_read(&mut self, read_id: ReadId) {
        let Some(waiter) = self.read_waiters.remove(&read_id) else {
            return;
        };
        if waiter.outcome.is_some() {
            return;
        }
        if let Some(group) = self.group.as_mut() {
            group.cancel_read(read_id);
        }
        self.publish_metrics();
    }

    pub(super) fn pending_writes(&self) -> Vec<PendingWrite> {
        self.write_waiters
            .iter()
            .filter(|(_, waiter)| waiter.outcome.is_none())
            .map(|(local_proposal_id, waiter)| PendingWrite {
                local_proposal_id: *local_proposal_id,
                client_request_id: waiter.options.client_request_id,
            })
            .collect()
    }

    pub(super) fn pending_reads(&self) -> Vec<ReadId> {
        self.read_waiters
            .iter()
            .filter(|(_, waiter)| waiter.outcome.is_none())
            .map(|(read_id, _)| *read_id)
            .collect()
    }

    /// Resolves every outstanding waiter as the incarnation lets go of them.
    ///
    /// Writes are unknown rather than refused: a proposal already appended is
    /// still in the durable log and may commit under the next incarnation.
    /// Reads are terminal, and their barriers are cancelled through the group
    /// first so the retired group is quiescent *in reads*. The group's proposal
    /// table is left alone, so the released group is not quiescent in the sense
    /// [`super::TransportRaftDriver::new`] requires.
    pub(super) fn release_waiters(&mut self) {
        let local_proposal_ids = self.write_waiters.keys().copied().collect::<Vec<_>>();
        for local_proposal_id in local_proposal_ids {
            let options = self
                .write_waiters
                .get(&local_proposal_id)
                .map_or_else(WriteOptions::default, |waiter| waiter.options);
            self.resolve_write(
                local_proposal_id,
                Err(WriteError::UnknownOutcome {
                    local_proposal_id,
                    client_request_id: options.client_request_id,
                    reason: UnknownOutcomeReason::DriverReleased,
                }),
            );
        }
        let read_ids = self.read_waiters.keys().copied().collect::<Vec<_>>();
        for read_id in read_ids {
            if let Some(group) = self.group.as_mut() {
                group.cancel_read(read_id);
            }
            self.resolve_read(
                read_id,
                Err(ReadError::Abandoned {
                    read_id,
                    reason: ReadAbandonReason::DriverReleased,
                }),
            );
        }
    }

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
        if self.group.is_none() {
            // A refusal, not an unknown outcome: nothing was proposed, so there
            // is no proposal to be unknown about and no ID that names one. The
            // fabricated `LocalProposalId(0)` this used to carry was a value a
            // caller could compare against a real allocation.
            return Err(WriteError::Transport {
                fate: WriteFate::NotAppended,
                cause: ErrorCause::new(DriverRoutingError::NoGroup),
            });
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
            self.resolve_write(local_proposal_id, Err(write_failure(failure, options)));
        }
        Ok(local_proposal_id)
    }

    pub(super) fn begin_read(
        &mut self,
        group_id: &G,
        query: A::Query,
        consistency: ReadConsistency,
        options: ReadOptions,
    ) -> Result<StartedRead<G, A::QueryResult>, ReadError> {
        self.reject_read_before_start(group_id)?;
        match consistency {
            // Answered here rather than through a waiter, and before the waiter
            // limit is consulted: a local read registers nothing, so it cannot
            // contribute to the condition that bound would be refusing for.
            ReadConsistency::Local => Ok(StartedRead::Answered(self.read_local(query, options))),
            ReadConsistency::Linearizable => {
                self.begin_barrier(query, options).map(StartedRead::Barrier)
            }
            // Every remaining level, including `LeaseRead`, which the app layer
            // itself refuses. Forwarding it would spend a group step to reach
            // the same answer with a `GroupError` in the middle.
            _ => Err(ReadError::UnsupportedConsistency { consistency }),
        }
    }

    /// Starts a barrier for a caller that will hold its [`ReadId`].
    ///
    /// The same body [`TransportDriverState::begin_read`] runs for
    /// [`ReadConsistency::Linearizable`], reached without a consistency argument
    /// because a caller that wants the ID wants the level that has one.
    pub(super) fn begin_linearizable_read(
        &mut self,
        group_id: &G,
        query: A::Query,
        options: ReadOptions,
    ) -> Result<ReadId, ReadError> {
        self.reject_read_before_start(group_id)?;
        self.begin_barrier(query, options)
    }

    /// The refusals that precede every read, whatever level it asked for.
    ///
    /// The `NoGroup` refusal is here rather than inside the linearizable branch
    /// for the reason the write side gives: no barrier was reserved, so there is
    /// no `ReadId` to abandon and `ReadId(0)` named one that never existed. It
    /// covers a local read too, which has no state machine to read either.
    fn reject_read_before_start(&self, group_id: &G) -> Result<(), ReadError> {
        if self.shutting_down {
            return Err(ReadError::ShuttingDown);
        }
        if group_id != &self.group_id {
            return Err(ReadError::WrongGroup);
        }
        if self.group.is_none() {
            return Err(ReadError::Transport {
                cause: ErrorCause::new(DriverRoutingError::NoGroup),
            });
        }
        Ok(())
    }

    fn begin_barrier(
        &mut self,
        query: A::Query,
        options: ReadOptions,
    ) -> Result<ReadId, ReadError> {
        let unresolved = self
            .read_waiters
            .values()
            .filter(|waiter| waiter.outcome.is_none())
            .count();
        if unresolved >= self.options.max_pending_waiters {
            return Err(ReadError::Transport {
                cause: ErrorCause::new(DriverRoutingError::PendingWaiterLimit {
                    max_pending_waiters: self.options.max_pending_waiters,
                }),
            });
        }
        let next = self.next_read_id.ok_or(ReadError::ReadIdExhausted)?;
        self.next_read_id = next.checked_add(1);
        let read_id = ReadId(next);
        // Registered before the barrier starts, for the reason a write waiter
        // is: a terminal event emitted inside the very step that starts the
        // barrier must find a waiter listening. The request is stored whole
        // because `drive_pending_reads` retries with it: the app layer refuses
        // a retry whose freshness or context moved, so the caller's floor has
        // to survive on the waiter rather than be rebuilt per attempt.
        self.read_waiters.insert(
            read_id,
            ReadWaiter {
                request: ReadRequest::Linearizable {
                    group_id: self.group_id.clone(),
                    read_id,
                    query,
                    min_applied_index: options.min_applied_index,
                    context: Vec::new(),
                },
                proof_ready: false,
                outcome: None,
                waker: None,
            },
        );
        if let Err(error) = self.attempt_read(read_id) {
            self.resolve_read(
                read_id,
                Err(ReadError::Transport {
                    cause: ErrorCause::new(error),
                }),
            );
        }
        self.publish_metrics();
        Ok(read_id)
    }

    /// Answers one read from this replica's own applied state.
    ///
    /// [`RaftGroup::read`] is the whole implementation, and going through it
    /// rather than around it is the point: it refuses a poisoned group and a
    /// state machine below the runtime's snapshot boundary before it reads
    /// anything, and it honors the caller's `min_applied_index` verbatim. A
    /// projection taken through [`TransportRaftDriver::with_group`] gets none of
    /// those.
    ///
    /// The report is routed even though a local read never steps the runtime and
    /// the report is therefore empty for this group. Routing it unconditionally
    /// keeps this path from being the one exception to the driver's discipline,
    /// and an empty report costs a walk over five empty lists.
    fn read_local(
        &mut self,
        query: A::Query,
        options: ReadOptions,
    ) -> Result<QueryReceipt<G, A::QueryResult>, ReadError> {
        let request = ReadRequest::Local {
            group_id: self.group_id.clone(),
            query,
            min_applied_index: options.min_applied_index,
        };
        // `reject_read_before_start` already refused a released driver, so this
        // holds a group. Mapped rather than unwrapped because the caller gets a
        // typed refusal either way and a panic here would be the driver's own
        // invariant, not the caller's fault.
        let read = self.group_mut().map_err(|_| ReadError::Transport {
            cause: ErrorCause::new(DriverRoutingError::NoGroup),
        })?;
        let answered = match read.read(request) {
            Ok(read) => {
                self.route_report(read.report);
                local_read_outcome(read.outcome)
            }
            Err(error) => Err(read_error_from_group(error)),
        };
        // The drain runs whichever way the read went, for the reason
        // `attempt_read` gives: a poison captured during the call hands this
        // driver waiters it must resolve, and this read owns none of them.
        self.drain_poisoned_waiters();
        self.publish_metrics();
        answered
    }

    pub(super) fn poll_write(
        &mut self,
        local_proposal_id: LocalProposalId,
        context: &Context<'_>,
    ) -> Poll<Result<WriteReceipt<A::CommandResult>, WriteError>> {
        let Some(waiter) = self.write_waiters.get_mut(&local_proposal_id) else {
            return Poll::Ready(Err(WriteError::ManagedInvariantViolation {
                fate: WriteFate::Unresolved,
                message: format!("no write waiter remains for {local_proposal_id}"),
            }));
        };
        if waiter.outcome.is_some() {
            let outcome = self
                .write_waiters
                .remove(&local_proposal_id)
                .and_then(|waiter| waiter.outcome);
            return Poll::Ready(outcome.unwrap_or_else(|| {
                Err(WriteError::ManagedInvariantViolation {
                    fate: WriteFate::Unresolved,
                    message: format!("write {local_proposal_id} finished without an outcome"),
                })
            }));
        }
        waiter.waker = Some(context.waker().clone());
        Poll::Pending
    }

    pub(super) fn poll_read(
        &mut self,
        read_id: ReadId,
        context: &Context<'_>,
    ) -> Poll<Result<QueryReceipt<G, A::QueryResult>, ReadError>> {
        let Some(waiter) = self.read_waiters.get_mut(&read_id) else {
            return Poll::Ready(Err(ReadError::ManagedInvariantViolation {
                message: format!("no read waiter remains for {read_id}"),
            }));
        };
        if waiter.outcome.is_some() {
            let outcome = self
                .read_waiters
                .remove(&read_id)
                .and_then(|waiter| waiter.outcome);
            return Poll::Ready(outcome.unwrap_or_else(|| {
                Err(ReadError::ManagedInvariantViolation {
                    message: format!("read {read_id} finished without an outcome"),
                })
            }));
        }
        waiter.waker = Some(context.waker().clone());
        Poll::Pending
    }
}

/// Maps one failed proposing step onto the fate the driver can prove.
///
/// The arms are `InMemoryRaftDriver::finish_failed_write_batch`'s, in its order
/// and for its reasons, with one change: the driver no longer infers
/// `NotAppended` from the absence of an observed append. A step that failed
/// after the group was asked to propose is unresolved, because the entry may be
/// on disk and a node reopened over the same durable log can still replicate and
/// commit it. `NotAppended` survives only where the refusal is itself the whole
/// event, and [`pre_proposal_fate`] is the list of group errors that are.
fn write_failure<E, RE>(failure: StepFailure<E, RE>, options: WriteOptions) -> WriteError
where
    E: Error + Send + Sync + 'static,
    RE: Error + Send + Sync + 'static,
{
    match failure {
        StepFailure::NoGroup => WriteError::Transport {
            fate: WriteFate::NotAppended,
            cause: ErrorCause::new(DriverRoutingError::NoGroup),
        },
        // The app layer's own name for "the runtime said nothing": it states
        // that the layer below does not know what happened to the proposal,
        // which is exactly what an unknown outcome reports.
        StepFailure::Group(GroupError::ProposalDidNotStart { local_proposal_id }) => {
            WriteError::UnknownOutcome {
                local_proposal_id,
                client_request_id: options.client_request_id,
                reason: UnknownOutcomeReason::RuntimeDroppedProposal,
            }
        }
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

/// Maps the two outcomes a local read can produce.
///
/// It produces exactly two. `RaftGroup::read_local` returns `Ready` or
/// `LocalFreshnessUnavailable` and reaches no other arm, because it reserves no
/// barrier: there is nothing to leave pending, reject, or cancel. The catch-all
/// is therefore an invariant violation rather than a case, and it says so in the
/// same vocabulary the barrier path uses.
fn local_read_outcome<G, QR>(
    outcome: ReadOutcome<G, QR>,
) -> Result<QueryReceipt<G, QR>, ReadError> {
    match outcome {
        ReadOutcome::Ready { result, proof } => Ok(QueryReceipt { result, proof }),
        ReadOutcome::LocalFreshnessUnavailable {
            required_applied_index,
            local_applied_index,
        } => Err(ReadError::FreshnessUnavailable {
            read_id: None,
            required_applied_index,
            local_applied_index,
        }),
        _ => Err(ReadError::ManagedInvariantViolation {
            message: "managed driver received unsupported app-layer read outcome variant"
                .to_owned(),
        }),
    }
}
