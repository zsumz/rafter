#![allow(clippy::wildcard_imports)]

//! Waiter tables and the step/route loop behind one transport driver.
//!
//! Everything here is private to the driver. It is a separate file because
//! the public surface and the mechanism behind it are read for different
//! reasons: one is a contract, the other is a loop.

use std::task::{Context, Poll, Waker};

use crate::transport::{AuthenticatedPeerValidator, RaftTransport};

use super::super::*;
use super::TransportDriverOptions;

pub(super) struct WriteWaiter<R> {
    options: WriteOptions,
    saw_local_append: bool,
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

    pub(super) fn step(
        &mut self,
        input: GroupInput<G, A::Command>,
    ) -> Result<(), ManagedDriverError> {
        let report = self
            .group_mut()?
            .step_with_options(input, StepReportOptions::without_metrics())
            .map_err(|error| ManagedDriverError::Group {
                cause: ErrorCause::new(error),
            })?;
        self.route_report(report);
        self.publish_metrics();
        Ok(())
    }

    pub(super) fn apply_recovery_outputs(
        &mut self,
        outputs: Vec<RaftOutput>,
    ) -> Result<(), ManagedDriverError> {
        let report = self
            .group_mut()?
            .apply_raft_outputs(outputs)
            .map_err(|error| ManagedDriverError::Group {
                cause: ErrorCause::new(error),
            })?;
        self.route_report(report);
        self.publish_metrics();
        Ok(())
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
            ProposalEvent::Appended {
                local_proposal_id, ..
            } => {
                if let Some(waiter) = self.write_waiters.get_mut(local_proposal_id) {
                    waiter.saw_local_append = true;
                }
                return;
            }
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
    pub(super) fn attempt_read(&mut self, read_id: ReadId) -> Result<(), ManagedDriverError> {
        let Some(waiter) = self.read_waiters.get(&read_id) else {
            return Ok(());
        };
        if waiter.outcome.is_some() {
            return Ok(());
        }
        let request = waiter.request.clone();
        let read = match self.group_mut()?.read(request) {
            Ok(read) => read,
            Err(error) => return self.fail_read(read_id, error),
        };
        self.route_report(read.report);
        self.handle_read_outcome(read_id, read.outcome);
        Ok(())
    }

    /// Attributes one read call's failure to the barrier that caused it.
    ///
    /// The group refuses a spent `ReadId` and a retry whose parameters moved,
    /// and both mean one thing here: this driver still holds a waiter for a
    /// barrier the group no longer tracks. Routing terminal read events is what
    /// makes that unreachable, so reaching it is a driver invariant violation —
    /// and the client is told so. Propagating instead would leave that waiter
    /// unresolved forever while every later call raised the same error, and
    /// would deny service to every other barrier in the same pass. Anything else
    /// is not attributable to one barrier and reaches the caller.
    fn fail_read(
        &mut self,
        read_id: ReadId,
        error: GroupError<A::Error, R::Error>,
    ) -> Result<(), ManagedDriverError> {
        if !matches!(
            error,
            GroupError::NonMonotonicReadId { .. } | GroupError::DuplicateReadId { .. }
        ) {
            return Err(ManagedDriverError::Group {
                cause: ErrorCause::new(error),
            });
        }
        self.resolve_read(
            read_id,
            Err(ReadError::ManagedInvariantViolation {
                message: format!(
                    "managed driver still holds a waiter for {read_id}, which its group no \
                     longer tracks: a terminal read event was not routed"
                ),
            }),
        );
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
            return Err(WriteError::UnknownOutcome {
                local_proposal_id: LocalProposalId(0),
                client_request_id: options.client_request_id,
                reason: UnknownOutcomeReason::DriverReleased,
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
                saw_local_append: false,
                outcome: None,
                waker: None,
            },
        );
        let proposal = Proposal {
            local_proposal_id,
            client_request_id: options.client_request_id,
            command,
        };
        if let Err(error) = self.step(GroupInput::Proposal { proposal }) {
            let fate = if self
                .write_waiters
                .get(&local_proposal_id)
                .is_some_and(|waiter| waiter.saw_local_append)
            {
                WriteFate::Unresolved
            } else {
                WriteFate::NotAppended
            };
            self.resolve_write(
                local_proposal_id,
                Err(WriteError::Transport {
                    fate,
                    cause: ErrorCause::new(error),
                }),
            );
        }
        Ok(local_proposal_id)
    }

    pub(super) fn begin_read(
        &mut self,
        group_id: &G,
        query: A::Query,
        consistency: ReadConsistency,
    ) -> Result<ReadId, ReadError> {
        if self.shutting_down {
            return Err(ReadError::ShuttingDown);
        }
        if group_id != &self.group_id {
            return Err(ReadError::WrongGroup);
        }
        if self.group.is_none() {
            return Err(ReadError::Abandoned {
                read_id: ReadId(0),
                reason: ReadAbandonReason::DriverReleased,
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
        // barrier must find a waiter listening.
        self.read_waiters.insert(
            read_id,
            ReadWaiter {
                request: ReadRequest::Linearizable {
                    group_id: self.group_id.clone(),
                    read_id,
                    query,
                    min_applied_index: None,
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
