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
    pub(super) fn route_report(&mut self, report: DriverStepReport<G, A>) {
        for envelope in report.peer_messages {
            if self.transport.send(envelope).is_err() {
                self.refused_sends = self.refused_sends.saturating_add(1);
            }
        }
        for event in &report.proposal_events {
            self.observe_proposal_event(event);
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

    pub(super) fn drive_pending_reads(&mut self) -> Result<(), ManagedDriverError> {
        if self.group.is_none() {
            return Err(ManagedDriverError::NoGroup);
        }
        let pending = self
            .read_waiters
            .iter()
            .filter(|(_, waiter)| waiter.outcome.is_none())
            .map(|(read_id, _)| *read_id)
            .collect::<Vec<_>>();
        for read_id in pending {
            for _ in 0..self.options.max_read_retries {
                if !self.retry_read(read_id)? {
                    break;
                }
            }
        }
        self.publish_metrics();
        Ok(())
    }

    /// Retries one barrier once. Returns whether another retry could help.
    pub(super) fn retry_read(&mut self, read_id: ReadId) -> Result<bool, ManagedDriverError> {
        let Some(waiter) = self.read_waiters.get(&read_id) else {
            return Ok(false);
        };
        if waiter.outcome.is_some() {
            return Ok(false);
        }
        let request = waiter.request.clone();
        let read = self
            .group_mut()?
            .read(request)
            .map_err(|error| ManagedDriverError::Group {
                cause: ErrorCause::new(error),
            })?;
        self.route_report(read.report);
        Ok(self.handle_read_outcome(read_id, read.outcome))
    }

    /// Resolves one barrier's outcome. Returns whether another retry could
    /// change the answer.
    pub(super) fn handle_read_outcome(
        &mut self,
        read_id: ReadId,
        outcome: ReadOutcome<G, A::QueryResult>,
    ) -> bool {
        match outcome {
            ReadOutcome::Ready { result, proof } => {
                self.resolve_read(read_id, Ok(QueryReceipt { result, proof }));
                false
            }
            // The quorum round is waiting on an inbound frame, which only
            // `deliver` can bring; retrying here would spin against a group
            // whose state cannot change in between.
            ReadOutcome::Pending { .. } => false,
            // The barrier is granted and the state machine is behind it.
            // Retrying is worth it: a read step also steps the group, so a
            // committed entry can apply between one attempt and the next.
            ReadOutcome::LinearizableFreshnessUnavailable { .. } => true,
            ReadOutcome::Rejected {
                read_id: rejected,
                reason,
                leader_hint,
            } => {
                self.resolve_read(
                    read_id,
                    Err(ReadError::Rejected {
                        read_id: Some(rejected),
                        reason,
                        leader_hint,
                    }),
                );
                false
            }
            ReadOutcome::Canceled {
                read_id: canceled,
                reason,
                leader_hint,
            } => {
                self.resolve_read(
                    read_id,
                    Err(ReadError::Canceled {
                        read_id: canceled,
                        reason,
                        leader_hint,
                    }),
                );
                false
            }
            ReadOutcome::LocalFreshnessUnavailable {
                required_applied_index,
                local_applied_index,
            } => {
                self.resolve_read(
                    read_id,
                    Err(ReadError::FreshnessUnavailable {
                        read_id: None,
                        required_applied_index,
                        local_applied_index,
                    }),
                );
                false
            }
            _ => {
                self.resolve_read(
                    read_id,
                    Err(ReadError::ManagedInvariantViolation {
                        message:
                            "managed driver received unsupported app-layer read outcome variant"
                                .to_owned(),
                    }),
                );
                false
            }
        }
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
                outcome: None,
                waker: None,
            },
        );
        for _ in 0..self.options.max_read_retries {
            match self.retry_read(read_id) {
                Ok(true) => {}
                Ok(false) => break,
                Err(error) => {
                    self.resolve_read(
                        read_id,
                        Err(ReadError::Transport {
                            cause: ErrorCause::new(error),
                        }),
                    );
                    break;
                }
            }
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
