#![allow(clippy::wildcard_imports)]

//! Waiter tables and the step/route loop behind one transport driver.
//!
//! Everything here is private to the driver. It is a separate file because
//! the public surface and the mechanism behind it are read for different
//! reasons: one is a contract, the other is a loop.

use std::{
    error::Error,
    task::{Context, Poll, Waker},
};

use crate::transport::{AuthenticatedPeerValidator, RaftTransport};

use super::super::*;
use super::TransportDriverOptions;

pub(super) struct WriteWaiter<R> {
    options: WriteOptions,
    outcome: Option<Result<WriteReceipt<R>, WriteError>>,
    waker: Option<Waker>,
}

pub(super) struct ReadWaiter<G, Q, QR> {
    request: ReadRequest<G, Q>,
    /// Whether a routed [`ReadEvent::Granted`] said this barrier's proof is
    /// cached and waiting for a read call to consume it.
    ///
    /// This is the whole retry policy. A read against a barrier the group
    /// already tracks returns an unstepped report, so a second attempt without
    /// a grant in between cannot see a different answer.
    proof_ready: bool,
    outcome: Option<Result<QueryReceipt<G, QR>, ReadError>>,
    waker: Option<Waker>,
}

/// Why one group step did not run, or did not finish.
///
/// The group error travels unwrapped so each caller can report it in its own
/// vocabulary: a driver hears `ManagedDriverError`, and a client hears the
/// typed write or read category the same mapping gives `InMemoryRaftDriver`.
pub(super) enum StepFailure<E, RE> {
    NoGroup,
    Group(GroupError<E, RE>),
}

/// The driver's state, shared by every clone and every client future.
pub(super) type SharedState<G, A, R, T, V> = Arc<Mutex<TransportDriverState<G, A, R, T, V>>>;

pub(super) struct TransportDriverState<G, A, R, T, V>
where
    A: ReplicatedStateMachine,
{
    pub(super) group_id: G,
    pub(super) node_id: NodeId,
    pub(super) group: Option<RaftGroup<G, A, R>>,
    pub(super) transport: T,
    pub(super) validator: V,
    pub(super) options: TransportDriverOptions,
    pub(super) metrics: MetricsPublisher<G>,
    pub(super) next_proposal_id: Option<u64>,
    pub(super) next_read_id: Option<u64>,
    pub(super) write_waiters: BTreeMap<LocalProposalId, WriteWaiter<A::CommandResult>>,
    pub(super) read_waiters: BTreeMap<ReadId, ReadWaiter<G, A::Query, A::QueryResult>>,
    pub(super) refused_sends: u64,
    pub(super) shutting_down: bool,
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
    pub(super) fn reject_if_shutting_down(&self) -> Result<(), ManagedDriverError> {
        if self.shutting_down {
            return Err(ManagedDriverError::ShuttingDown);
        }
        Ok(())
    }

    pub(super) fn group_mut(&mut self) -> Result<&mut RaftGroup<G, A, R>, ManagedDriverError> {
        self.group.as_mut().ok_or(ManagedDriverError::NoGroup)
    }

    /// Steps the group and routes everything the step produced or captured.
    ///
    /// The group error survives here rather than being wrapped, because the
    /// caller decides how a client hears about it: `tick` and `deliver` report
    /// to a driver, and `begin_write` reports to a client through the typed
    /// write mapping. A single wrapped error could serve only one of them.
    ///
    /// The poison drain runs on both paths. A step that poisons can return
    /// `Ok`, and a step that fails is the likeliest place for a poison to have
    /// happened; draining only where the report exists would strand exactly the
    /// waiters a poison captured.
    pub(super) fn step_group(
        &mut self,
        input: GroupInput<G, A::Command>,
    ) -> Result<(), StepFailure<A::Error, R::Error>> {
        let Some(group) = self.group.as_mut() else {
            return Err(StepFailure::NoGroup);
        };
        let stepped = group.step_with_options(input, StepReportOptions::without_metrics());
        let result = match stepped {
            Ok(report) => {
                self.route_report(report);
                Ok(())
            }
            Err(error) => Err(StepFailure::Group(error)),
        };
        self.drain_poisoned_waiters();
        self.publish_metrics();
        result
    }

    pub(super) fn step(
        &mut self,
        input: GroupInput<G, A::Command>,
    ) -> Result<(), ManagedDriverError> {
        self.step_group(input).map_err(|failure| match failure {
            StepFailure::NoGroup => ManagedDriverError::NoGroup,
            StepFailure::Group(error) => ManagedDriverError::Group {
                cause: ErrorCause::new(error),
            },
        })
    }

    pub(super) fn apply_recovery_outputs(
        &mut self,
        outputs: Vec<RaftOutput>,
    ) -> Result<(), ManagedDriverError> {
        let applied = self.group_mut()?.apply_raft_outputs(outputs);
        let result = match applied {
            Ok(report) => {
                self.route_report(report);
                Ok(())
            }
            Err(error) => Err(ManagedDriverError::Group {
                cause: ErrorCause::new(error),
            }),
        };
        self.drain_poisoned_waiters();
        self.publish_metrics();
        result
    }

    /// Resolves every waiter the group handed over when it poisoned.
    ///
    /// A poison is not an event stream. `RaftGroup::enter_poisoned` moves every
    /// pending proposal and every reserved read out of the group's live tables
    /// into `poisoned_waiters` and emits nothing further for them, so a driver
    /// that routes reports and nothing else leaves those clients waiting
    /// forever while every later call raises the same refusal.
    ///
    /// Writes resolve as unknown rather than refused, for the reason a released
    /// driver's do: the entry may be in the durable log, and an incarnation
    /// reopened over that log can still commit it. Reads resolve as poisoned,
    /// which is the whole truth about them — a barrier the group dropped
    /// produces no answer, ever.
    pub(super) fn drain_poisoned_waiters(&mut self) {
        let Some(group) = self.group.as_mut() else {
            return;
        };
        if group.poisoned_waiters().is_empty() {
            return;
        }
        let GroupFatalState::Poisoned { reason } = group.fatal_state().clone() else {
            // Adoption refuses a healthy group holding poisoned waiters, so
            // there is no poison to report and nothing honest to say.
            return;
        };
        let cause = group.poison_cause().cloned();
        let waiters = group.drain_poisoned_waiters();
        for (local_proposal_id, client_request_id) in waiters.proposals {
            let client_request_id = client_request_id.or_else(|| {
                self.write_waiters
                    .get(&local_proposal_id)
                    .and_then(|waiter| waiter.options.client_request_id)
            });
            self.resolve_write(
                local_proposal_id,
                Err(WriteError::UnknownOutcome {
                    local_proposal_id,
                    client_request_id,
                    reason: UnknownOutcomeReason::GroupPoisoned,
                }),
            );
        }
        for read_id in waiters.reads {
            self.resolve_read(
                read_id,
                Err(ReadError::Poisoned {
                    reason: reason.clone(),
                    cause: cause.clone(),
                }),
            );
        }
    }

    /// Hands the report's peer messages to the transport and its lifecycle
    /// events to the waiters they belong to.
    ///
    /// A refused send is counted rather than propagated: Raft tolerates drops
    /// and re-sends, so a write must not fail because one heartbeat could not
    /// be delivered.
    ///
    /// Read events are routed here for the same reason proposal events are: the
    /// app layer ends a barrier in whichever step observes the cause, and for a
    /// leadership change that step is a tick or a delivery rather than a read
    /// call. A driver that read only its own read calls' outcomes would leave
    /// that client waiting forever and then ask the group to re-reserve a spent
    /// `ReadId`.
    pub(super) fn route_report(&mut self, report: DriverStepReport<G, A>) {
        for envelope in report.peer_messages {
            if self.transport.send(envelope).is_err() {
                self.refused_sends = self.refused_sends.saturating_add(1);
            }
        }
        for event in &report.proposal_events {
            self.observe_proposal_event(event);
        }
        for event in &report.read_events {
            self.observe_read_event(event);
        }
    }

    /// Resolves a barrier the group itself ended, and records one it granted.
    ///
    /// The terminal mapping is the one
    /// [`TransportDriverState::handle_read_outcome`] uses, so a barrier resolves
    /// identically whichever step observed its end. Neither terminal arm touches
    /// the group again: the event carries the whole answer, and the group has
    /// already dropped that barrier's state.
    pub(super) fn observe_read_event(&mut self, event: &ReadEvent<G>) {
        match event {
            // Not terminal. The proof is cached in the group and a later read
            // call consumes it; recording the grant is what lets
            // `drive_pending_reads` attempt exactly the barriers whose answer
            // can have changed.
            ReadEvent::Granted { read_id, .. } => {
                if let Some(waiter) = self.read_waiters.get_mut(read_id) {
                    waiter.proof_ready = true;
                }
            }
            ReadEvent::Rejected {
                read_id,
                reason,
                leader_hint,
            } => self.resolve_read(
                *read_id,
                Err(ReadError::Rejected {
                    read_id: Some(*read_id),
                    reason: *reason,
                    leader_hint: *leader_hint,
                }),
            ),
            ReadEvent::Canceled {
                read_id,
                reason,
                leader_hint,
            } => self.resolve_read(
                *read_id,
                Err(ReadError::Canceled {
                    read_id: *read_id,
                    reason: *reason,
                    leader_hint: *leader_hint,
                }),
            ),
            // `FreshnessUnavailable` is not terminal either: the barrier stays
            // reserved, and the same app-layer path emits `Granted` once the
            // applied index catches up. A variant this driver does not know
            // falls here for the same reason — it is not an answer, so the
            // waiter keeps waiting for one.
            _ => {}
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

    pub(super) fn publish_metrics(&self) {
        if let Some(group) = self.group.as_ref() {
            let _ = self.metrics.publish(group.metrics());
        }
    }

    /// Collects every barrier whose proof a routed grant said is ready.
    ///
    /// Barriers still waiting on a quorum round, and granted barriers this
    /// replica has not applied through, are left alone. They cannot answer
    /// differently until the group emits a [`ReadEvent::Granted`] for them, and
    /// attempting them anyway would spin: a read against a barrier the group
    /// already tracks returns an unstepped report.
    ///
    /// One barrier's fault never denies service to the rest. Every group error a
    /// read call can raise is about the one `ReadId` that call named, so it
    /// resolves that barrier's client and the pass continues; nothing but a
    /// released group leaves this method as an error.
    pub(super) fn drive_pending_reads(&mut self) -> Result<(), ManagedDriverError> {
        if self.group.is_none() {
            return Err(ManagedDriverError::NoGroup);
        }
        let ready = self
            .read_waiters
            .iter()
            .filter(|(_, waiter)| waiter.outcome.is_none() && waiter.proof_ready)
            .map(|(read_id, _)| *read_id)
            .collect::<Vec<_>>();
        for read_id in ready {
            self.attempt_read(read_id)?;
        }
        self.publish_metrics();
        Ok(())
    }

    /// Runs one read call for one barrier and resolves whatever it produced.
    ///
    /// This both starts a barrier, when [`TransportDriverState::begin_read`]
    /// calls it, and consumes a granted one afterwards. Only the first of those
    /// steps the group.
    ///
    /// A failure is the barrier's own. `RaftGroup::read` is called with one
    /// request naming one `ReadId`, so there is no second barrier the error
    /// could be about, and it reaches the client through the same category
    /// mapping `InMemoryRaftDriver` uses: a state machine that refuses a query
    /// is reported as a state-machine failure with its own error preserved, not
    /// as a driver invariant violation naming a routing defect that did not
    /// occur. The two ID variants still map to that violation, because for them
    /// it is what happened.
    pub(super) fn attempt_read(&mut self, read_id: ReadId) -> Result<(), ManagedDriverError> {
        let Some(waiter) = self.read_waiters.get(&read_id) else {
            return Ok(());
        };
        if waiter.outcome.is_some() {
            return Ok(());
        }
        let request = waiter.request.clone();
        let read = self.group_mut()?.read(request);
        match read {
            Ok(read) => {
                self.route_report(read.report);
                self.drain_poisoned_waiters();
                self.handle_read_outcome(read_id, read.outcome);
            }
            Err(error) => {
                // The drain runs first so a barrier the group handed over keeps
                // the poison's own answer; `resolve_read` keeps the first
                // outcome either way, and both arms say `Poisoned`.
                self.drain_poisoned_waiters();
                self.resolve_read(read_id, Err(read_error_from_group(error)));
            }
        }
        Ok(())
    }

    /// Resolves one barrier's outcome, or leaves it waiting for a grant.
    pub(super) fn handle_read_outcome(
        &mut self,
        read_id: ReadId,
        outcome: ReadOutcome<G, A::QueryResult>,
    ) {
        match outcome {
            ReadOutcome::Ready { result, proof } => {
                self.resolve_read(read_id, Ok(QueryReceipt { result, proof }));
            }
            // Both are waits rather than retries. The quorum round needs an
            // inbound frame only `deliver` can bring, and a granted barrier
            // ahead of this replica's applied index needs an apply; either way
            // the group announces the change with a `ReadEvent::Granted` that
            // `route_report` records.
            ReadOutcome::Pending { .. } | ReadOutcome::LinearizableFreshnessUnavailable { .. } => {}
            ReadOutcome::Rejected {
                read_id: rejected,
                reason,
                leader_hint,
            } => self.resolve_read(
                read_id,
                Err(ReadError::Rejected {
                    read_id: Some(rejected),
                    reason,
                    leader_hint,
                }),
            ),
            ReadOutcome::Canceled {
                read_id: canceled,
                reason,
                leader_hint,
            } => self.resolve_read(
                read_id,
                Err(ReadError::Canceled {
                    read_id: canceled,
                    reason,
                    leader_hint,
                }),
            ),
            ReadOutcome::LocalFreshnessUnavailable {
                required_applied_index,
                local_applied_index,
            } => self.resolve_read(
                read_id,
                Err(ReadError::FreshnessUnavailable {
                    read_id: None,
                    required_applied_index,
                    local_applied_index,
                }),
            ),
            _ => self.resolve_read(
                read_id,
                Err(ReadError::ManagedInvariantViolation {
                    message: "managed driver received unsupported app-layer read outcome variant"
                        .to_owned(),
                }),
            ),
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
    /// first so the retired group is quiescent.
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
    ) -> Result<ReadId, ReadError> {
        if self.shutting_down {
            return Err(ReadError::ShuttingDown);
        }
        if group_id != &self.group_id {
            return Err(ReadError::WrongGroup);
        }
        if self.group.is_none() {
            // A refusal, for the reason the write side gives: no barrier was
            // reserved, so there is no `ReadId` to abandon and `ReadId(0)` named
            // one that never existed.
            return Err(ReadError::Transport {
                cause: ErrorCause::new(DriverRoutingError::NoGroup),
            });
        }
        if !matches!(consistency, ReadConsistency::Linearizable) {
            return Err(ReadError::UnsupportedConsistency { consistency });
        }
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
/// The three arms are `InMemoryRaftDriver::finish_failed_write_batch`'s, in its
/// order and for its reasons. What changed here is the last one: the driver no
/// longer infers `NotAppended` from the absence of an observed append. A step
/// that failed after the group was asked to propose is unresolved, because the
/// entry may be on disk and a node reopened over the same durable log can still
/// replicate and commit it. `NotAppended` survives only where the refusal is
/// itself the event — a pre-proposal driver refusal, a `ProposalEvent::Rejected`
/// whose mapped variants carry no fate at all, and
/// `GroupError::NonMonotonicLocalProposalId`, which `write_error_from_group`
/// stamps itself because the group refuses before it proposes.
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
        StepFailure::Group(error) => write_error_from_group(error, WriteFate::Unresolved),
    }
}
