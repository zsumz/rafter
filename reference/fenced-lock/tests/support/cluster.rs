//! Consumer-owned deterministic three-node driver for `rafter-service`.
//!
//! `rafter-service` ships one driver, and it owns every replica behind a
//! private queue with no way to cut a link, so a consumer that needs
//! partitions has to supply its own [`DriverCommandSender`]. This module is
//! that driver. Each replica gets its own sender, its own transport endpoint,
//! and its own [`RaftHandle`]; the cluster below owns only the parts a real
//! deployment would own too — when a node ticks, which frames are delivered,
//! which links are cut, and when a replica retires one incarnation for another
//! over the same durable stores.
//!
//! The split matters for the histories. A client future here resolves when the
//! *driver loop* observes a terminal proposal or read outcome, not when the
//! client asks. That is what lets a test start an acquisition, cut the leader's
//! links while it is in flight, and then watch the outcome window close.
//!
//! No simulator, no internal hooks, no privileged observation: an external user
//! with the published crates can write the same thing.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    task::{Context, Poll, Waker},
};

use rafter::{
    LocalProposalId, LogEntryKind, LogIndex, NodeConfig, NodeId, ProposalRejection, ReadId, Role,
};
use rafter_app::{
    error::GroupError,
    group::{
        GroupInput, GroupStepReport, LeadershipTransferEvent, ProposalBeginReport, RaftGroup,
        ReadReport,
    },
    proposal::{Proposal, ProposalBegin, ProposalEvent, ProposalUnknownOutcomeReason},
    read::{ReadEvent, ReadOutcome, ReadRequest},
    state_machine::ReplicatedStateMachine,
};
use rafter_reference_fenced_lock::{
    ApplyOutcome, Command, HistoryEvent, LockAdapterError, LockClient, LockConfig, LockQuery,
    LockQueryResult, LockStateMachine, LogicalTime, OperationId, QueryOutcome, ResourceName,
    SubmitOutcome,
};
use rafter_runtime::{DurableRaftNode, DurableRaftNodeStorage, RaftRuntimeError};
use rafter_service::{
    driver::DriverFuture, DriverCommandSender, MetricsError, MetricsPublisher, MetricsWatch,
    PeerEnvelope, QueryReceipt, RaftHandle, RaftTransport, ReadConsistency, ReadError,
    ShutdownError, TransferLeadershipError, UnknownOutcomeReason, WriteError, WriteOptions,
    WriteReceipt,
};
use rafter_storage::{
    InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore,
};

use crate::transport::{DeterministicNetwork, NodeTransport, PeerDirectory};

/// Bound on driver rounds spent waiting for one outcome.
///
/// Every wait is bounded so a stalled protocol fails the test instead of
/// hanging, and so a client that runs out of rounds observes the same unknown
/// outcome a real client observes when it stops waiting.
pub const MAX_ROUNDS: usize = 32;

/// Bound on drain passes inside one delivery, so a message storm fails loudly.
const MAX_DELIVERY_PASSES: usize = 64;

/// Caller-defined group identity for the single lock group.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct LockGroupId(pub u64);

/// The one group every node in this driver serves.
pub const GROUP_ID: LockGroupId = LockGroupId(1);

type LockStorage = DurableRaftNodeStorage<
    InMemoryRaftHardStateStore,
    InMemoryRaftLogSegment,
    InMemoryRaftSnapshotStore,
>;
type LockRuntime =
    DurableRaftNode<InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore>;
type LockGroup = RaftGroup<LockGroupId, LockStateMachine, LockRuntime>;
type LockReport = GroupStepReport<LockGroupId, ApplyOutcome>;
type LockGroupError = GroupError<LockAdapterError, RaftRuntimeError>;
type LockWriteResult = Result<WriteReceipt<ApplyOutcome>, WriteError>;
type LockReadResult = Result<QueryReceipt<LockGroupId, LockQueryResult>, ReadError>;

/// What starting a managed operation produced.
///
/// An operation either becomes a waiter the driver loop will resolve, or it
/// settles before the caller ever polls. The settled outcome is boxed because
/// a lock query result carries an inline resource name.
#[derive(Debug)]
enum WriteStart {
    Pending(LocalProposalId),
    Settled(Box<LockWriteResult>),
}

#[derive(Debug)]
enum ReadStart {
    Pending(ReadId),
    Settled(Box<LockReadResult>),
}

/// One replica's managed command sender.
///
/// This is the boundary `rafter-service` documents between a cloneable handle
/// and a driver loop: a write completes only after the proposal has committed
/// and applied, and shutdown resolves outstanding waiters instead of leaving
/// them pending forever.
#[derive(Clone, Debug)]
pub struct NodeDriver {
    inner: Arc<Mutex<NodeState>>,
}

#[derive(Debug)]
struct WriteWaiter {
    options: WriteOptions,
    outcome: Option<Result<WriteReceipt<ApplyOutcome>, WriteError>>,
    waker: Option<Waker>,
}

#[derive(Debug)]
struct ReadWaiter {
    query: LockQuery,
    outcome: Option<Result<QueryReceipt<LockGroupId, LockQueryResult>, ReadError>>,
    waker: Option<Waker>,
}

#[derive(Debug)]
struct NodeState {
    election_timeout_ticks: u64,
    peers: Vec<NodeId>,
    /// The live group, absent only while a restart swaps incarnations.
    ///
    /// `RaftGroup::into_parts` consumes the group it retires, so the slot has
    /// to be movable. This state is shared behind a lock and can never be moved
    /// out of, which leaves taking the group out of an `Option` as the only way
    /// to decompose it in place.
    group: Option<LockGroup>,
    transport: NodeTransport,
    metrics: MetricsPublisher<LockGroupId>,
    write_waiters: BTreeMap<LocalProposalId, WriteWaiter>,
    read_waiters: BTreeMap<ReadId, ReadWaiter>,
    next_local_proposal_id: u64,
    next_read_id: u64,
    refused_sends: u64,
    runtime_unknown_outcomes: usize,
    shutting_down: bool,
}

impl NodeDriver {
    /// Returns the identifiers of writes this driver has not resolved.
    pub fn pending_write_ids(&self) -> BTreeSet<LocalProposalId> {
        lock(&self.inner).write_waiters.keys().copied().collect()
    }

    /// Returns the identifiers of reads this driver has not resolved.
    pub fn pending_read_ids(&self) -> BTreeSet<ReadId> {
        lock(&self.inner).read_waiters.keys().copied().collect()
    }

    /// Resolves one outstanding write as unknown because the caller stopped
    /// waiting.
    ///
    /// A client that abandons its wait learns nothing about the commit, which
    /// is exactly [`UnknownOutcomeReason::DriveBoundReached`].
    pub fn abandon_write(&self, local_proposal_id: LocalProposalId) {
        let mut state = lock(&self.inner);
        let options = state
            .write_waiters
            .get(&local_proposal_id)
            .map_or_else(WriteOptions::default, |waiter| waiter.options);
        state.resolve_write(
            local_proposal_id,
            Err(WriteError::UnknownOutcome {
                local_proposal_id,
                client_request_id: options.client_request_id,
                reason: UnknownOutcomeReason::DriveBoundReached,
            }),
        );
    }

    /// Abandons one outstanding read barrier because the caller stopped
    /// waiting.
    ///
    /// The local waiter is cleared through the documented group API. The read
    /// resolves to a non-answer; there is no typed "the caller stopped waiting"
    /// read error, so this mirrors the shipped driver's own stalled-read
    /// vocabulary.
    pub fn abandon_read(&self, read_id: ReadId) {
        let mut state = lock(&self.inner);
        state.group_mut().cancel_read(read_id);
        state.resolve_read(
            read_id,
            Err(ReadError::Transport {
                message: format!("lock read barrier {read_id} stalled and was abandoned"),
            }),
        );
    }

    /// Returns how many proposals the app layer itself declared unresolvable.
    pub fn runtime_unknown_outcomes(&self) -> usize {
        lock(&self.inner).runtime_unknown_outcomes
    }

    /// Returns how many outbound frames the transport refused.
    pub fn refused_sends(&self) -> u64 {
        lock(&self.inner).refused_sends
    }
}

impl DriverCommandSender<LockGroupId, Command, LockQuery, ApplyOutcome, LockQueryResult>
    for NodeDriver
{
    fn write(
        &self,
        group_id: LockGroupId,
        command: Command,
        options: WriteOptions,
    ) -> DriverFuture<Result<WriteReceipt<ApplyOutcome>, WriteError>> {
        let inner = self.inner.clone();
        let started = lock(&inner).begin_write(group_id, command, options);
        match started {
            WriteStart::Settled(outcome) => Box::pin(std::future::ready(*outcome)),
            WriteStart::Pending(local_proposal_id) => {
                Box::pin(std::future::poll_fn(move |context| {
                    lock(&inner).poll_write(local_proposal_id, context)
                }))
            }
        }
    }

    fn read(
        &self,
        group_id: LockGroupId,
        query: LockQuery,
        consistency: ReadConsistency,
    ) -> DriverFuture<Result<QueryReceipt<LockGroupId, LockQueryResult>, ReadError>> {
        let inner = self.inner.clone();
        let started = lock(&inner).begin_read(group_id, query, consistency);
        match started {
            ReadStart::Settled(outcome) => Box::pin(std::future::ready(*outcome)),
            ReadStart::Pending(read_id) => Box::pin(std::future::poll_fn(move |context| {
                lock(&inner).poll_read(read_id, context)
            })),
        }
    }

    fn transfer_leadership(
        &self,
        group_id: LockGroupId,
        target: NodeId,
    ) -> DriverFuture<Result<(), TransferLeadershipError>> {
        let inner = self.inner.clone();
        let outcome = lock(&inner).transfer_leadership(group_id, target);
        Box::pin(std::future::ready(outcome))
    }

    fn metrics(&self, group_id: LockGroupId) -> Result<MetricsWatch<LockGroupId>, MetricsError> {
        if group_id != GROUP_ID {
            return Err(MetricsError::WrongGroup);
        }
        Ok(lock(&self.inner).metrics.watch())
    }

    fn shutdown(&self, group_id: LockGroupId) -> DriverFuture<Result<(), ShutdownError>> {
        let inner = self.inner.clone();
        let outcome = lock(&inner).shutdown(group_id);
        Box::pin(std::future::ready(outcome))
    }
}

impl NodeState {
    fn group(&self) -> &LockGroup {
        self.group
            .as_ref()
            .expect("a node holds its group except while a restart swaps incarnations")
    }

    fn group_mut(&mut self) -> &mut LockGroup {
        self.group
            .as_mut()
            .expect("a node holds its group except while a restart swaps incarnations")
    }

    fn begin_write(
        &mut self,
        group_id: LockGroupId,
        command: Command,
        options: WriteOptions,
    ) -> WriteStart {
        if let Err(error) = self.admits(group_id) {
            return settled_write(Err(error));
        }
        let Some(local_proposal_id) = self.allocate_local_proposal_id() else {
            return settled_write(Err(WriteError::LocalProposalIdExhausted));
        };

        // The waiter is registered before the proposal starts so that any
        // lifecycle event inside this very step report resolves it rather than
        // arriving before anything is listening.
        self.write_waiters.insert(
            local_proposal_id,
            WriteWaiter {
                options,
                outcome: None,
                waker: None,
            },
        );
        let started = self.group_mut().begin_proposal(Proposal {
            local_proposal_id,
            client_request_id: options.client_request_id,
            command,
        });
        let ProposalBeginReport { begin, report } = match started {
            Ok(started) => started,
            Err(error) => {
                self.write_waiters.remove(&local_proposal_id);
                return settled_write(Err(write_error_from_group(&error)));
            }
        };
        self.record_report(report);

        match begin {
            ProposalBegin::Completed {
                index,
                term,
                result,
                ..
            } => {
                self.write_waiters.remove(&local_proposal_id);
                settled_write(Ok(WriteReceipt {
                    index,
                    term,
                    result,
                }))
            }
            ProposalBegin::Rejected {
                reason,
                leader_hint,
                ..
            } => {
                self.write_waiters.remove(&local_proposal_id);
                settled_write(Err(write_error_from_rejection(reason, leader_hint)))
            }
            ProposalBegin::UnknownOutcome {
                client_request_id,
                reason,
                ..
            } => {
                self.write_waiters.remove(&local_proposal_id);
                self.runtime_unknown_outcomes += 1;
                settled_write(Err(WriteError::UnknownOutcome {
                    local_proposal_id,
                    client_request_id: client_request_id.or(options.client_request_id),
                    reason: unknown_outcome_reason(&reason),
                }))
            }
            // A local append is not write success. The waiter stays until the
            // proposal commits, is rejected, or loses its outcome.
            _ => {
                if self
                    .write_waiters
                    .get(&local_proposal_id)
                    .is_some_and(|waiter| waiter.outcome.is_some())
                {
                    return settled_write(self.take_write(local_proposal_id));
                }
                WriteStart::Pending(local_proposal_id)
            }
        }
    }

    fn poll_write(
        &mut self,
        local_proposal_id: LocalProposalId,
        context: &Context<'_>,
    ) -> Poll<Result<WriteReceipt<ApplyOutcome>, WriteError>> {
        let Some(waiter) = self.write_waiters.get_mut(&local_proposal_id) else {
            return Poll::Ready(Err(WriteError::ManagedInvariantViolation {
                message: format!("no waiter remains for {local_proposal_id}"),
            }));
        };
        if waiter.outcome.is_some() {
            return Poll::Ready(self.take_write(local_proposal_id));
        }
        waiter.waker = Some(context.waker().clone());
        Poll::Pending
    }

    fn take_write(
        &mut self,
        local_proposal_id: LocalProposalId,
    ) -> Result<WriteReceipt<ApplyOutcome>, WriteError> {
        self.write_waiters
            .remove(&local_proposal_id)
            .and_then(|waiter| waiter.outcome)
            .unwrap_or_else(|| {
                Err(WriteError::ManagedInvariantViolation {
                    message: format!("write {local_proposal_id} finished without an outcome"),
                })
            })
    }

    fn begin_read(
        &mut self,
        group_id: LockGroupId,
        query: LockQuery,
        consistency: ReadConsistency,
    ) -> ReadStart {
        if let Err(error) = self.admits(group_id) {
            return settled_read(Err(read_error_from_write(error)));
        }
        // The lock contract forbids this application from claiming a lease
        // read, so the driver refuses every weaker mode outright rather than
        // trusting each caller to ask for the right one.
        if consistency != ReadConsistency::Linearizable {
            return settled_read(Err(ReadError::UnsupportedConsistency { consistency }));
        }
        let Some(read_id) = self.allocate_read_id() else {
            return settled_read(Err(ReadError::ReadIdExhausted));
        };

        self.read_waiters.insert(
            read_id,
            ReadWaiter {
                query,
                outcome: None,
                waker: None,
            },
        );
        self.drive_read(read_id);
        match self.read_waiters.get(&read_id) {
            Some(waiter) if waiter.outcome.is_none() => ReadStart::Pending(read_id),
            _ => settled_read(self.take_read(read_id)),
        }
    }

    fn poll_read(
        &mut self,
        read_id: ReadId,
        context: &Context<'_>,
    ) -> Poll<Result<QueryReceipt<LockGroupId, LockQueryResult>, ReadError>> {
        let Some(waiter) = self.read_waiters.get_mut(&read_id) else {
            return Poll::Ready(Err(ReadError::ManagedInvariantViolation {
                message: format!("no waiter remains for {read_id}"),
            }));
        };
        if waiter.outcome.is_some() {
            return Poll::Ready(self.take_read(read_id));
        }
        waiter.waker = Some(context.waker().clone());
        Poll::Pending
    }

    fn take_read(
        &mut self,
        read_id: ReadId,
    ) -> Result<QueryReceipt<LockGroupId, LockQueryResult>, ReadError> {
        self.read_waiters
            .remove(&read_id)
            .and_then(|waiter| waiter.outcome)
            .unwrap_or_else(|| {
                Err(ReadError::ManagedInvariantViolation {
                    message: format!("read {read_id} finished without an outcome"),
                })
            })
    }

    /// Retries one unresolved barrier through the documented read path.
    ///
    /// The contract for a pending helper read is to retry with the same read
    /// ID, freshness, and context until it resolves, which is what makes this
    /// safe to call once per driver round.
    ///
    /// A read step is a step like any other, so its report goes through
    /// [`NodeState::record_report`] alongside the reports proposals and ticks
    /// produce. That is the only place peer frames are routed and the only
    /// place a terminal read event resolves its waiter, whichever step
    /// happened to observe it.
    fn drive_read(&mut self, read_id: ReadId) {
        let Some(waiter) = self.read_waiters.get(&read_id) else {
            return;
        };
        if waiter.outcome.is_some() {
            return;
        }
        let query = waiter.query;
        let started = self.group_mut().read(ReadRequest::Linearizable {
            group_id: GROUP_ID,
            read_id,
            query,
            min_applied_index: None,
            context: Vec::new(),
        });
        let ReadReport { outcome, report } = match started {
            Ok(started) => started,
            Err(error) => {
                self.group_mut().cancel_read(read_id);
                self.resolve_read(read_id, Err(read_error_from_group(&error)));
                return;
            }
        };
        self.record_report(report);
        match outcome {
            ReadOutcome::Ready { result, proof } => {
                self.resolve_read(read_id, Ok(QueryReceipt { result, proof }));
            }
            // The barrier is in flight, or this replica has not applied through
            // it yet. Keep waiting; the next round applies more.
            ReadOutcome::Pending { .. } | ReadOutcome::LinearizableFreshnessUnavailable { .. } => {}
            // A rejection or cancellation is a read event in the report above,
            // so `record_report` resolved the waiter from it before this match
            // ran. Restating it from the outcome would answer the same question
            // twice; asserting it instead pins the report as the one place a
            // terminal read outcome comes from, whichever step observed it.
            ReadOutcome::Rejected { .. } | ReadOutcome::Canceled { .. } => assert!(
                self.read_waiters
                    .get(&read_id)
                    .is_none_or(|waiter| waiter.outcome.is_some()),
                "a terminal read outcome must reach the driver as a read event \
                 in the report of the step that produced it"
            ),
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
            }
            _ => {
                self.group_mut().cancel_read(read_id);
                self.resolve_read(
                    read_id,
                    Err(ReadError::ManagedInvariantViolation {
                        message: "lock driver saw an unsupported read outcome".to_owned(),
                    }),
                );
            }
        }
    }

    fn transfer_leadership(
        &mut self,
        group_id: LockGroupId,
        target: NodeId,
    ) -> Result<(), TransferLeadershipError> {
        if group_id != GROUP_ID {
            return Err(TransferLeadershipError::Transport {
                message: "wrong group".to_owned(),
            });
        }
        if self.shutting_down {
            return Err(TransferLeadershipError::ShuttingDown);
        }
        let report = self
            .group_mut()
            .step(GroupInput::TransferLeadership { target })
            .map_err(|error| transfer_error_from_group(&error))?;
        let rejection = report
            .leadership_transfer_events
            .iter()
            .find_map(|event| match event {
                LeadershipTransferEvent::Rejected {
                    reason,
                    leader_hint,
                    ..
                } => Some(TransferLeadershipError::Rejected {
                    reason: *reason,
                    leader_hint: *leader_hint,
                }),
                LeadershipTransferEvent::Started { .. } => None,
            });
        self.record_report(report);
        rejection.map_or(Ok(()), Err)
    }

    fn shutdown(&mut self, group_id: LockGroupId) -> Result<(), ShutdownError> {
        if group_id != GROUP_ID {
            return Err(ShutdownError::Transport {
                message: "wrong group".to_owned(),
            });
        }
        if self.shutting_down {
            return Err(ShutdownError::AlreadyShutDown);
        }
        self.shutting_down = true;
        self.abandon_all_waiters(UnknownOutcomeReason::RuntimeDroppedProposal);
        self.metrics.close();
        Ok(())
    }

    fn admits(&self, group_id: LockGroupId) -> Result<(), WriteError> {
        if self.shutting_down {
            return Err(WriteError::ShuttingDown);
        }
        if group_id != GROUP_ID {
            return Err(WriteError::Transport {
                message: "wrong group".to_owned(),
            });
        }
        Ok(())
    }

    fn record_report(&mut self, report: LockReport) {
        let LockReport {
            peer_messages,
            proposal_events,
            read_events,
            metrics,
            ..
        } = report;
        self.send_all(peer_messages);
        for event in &proposal_events {
            self.observe_proposal_event(event);
        }
        for event in &read_events {
            self.observe_read_event(event);
        }
        if let Some(metrics) = metrics {
            let _ = self.metrics.publish(metrics);
        }
    }

    fn send_all(&mut self, envelopes: Vec<PeerEnvelope<LockGroupId>>) {
        let mut refused = 0;
        for envelope in envelopes {
            if self.transport.send(envelope).is_err() {
                refused += 1;
            }
        }
        self.refused_sends += refused;
    }

    fn observe_proposal_event(&mut self, event: &ProposalEvent<ApplyOutcome>) {
        match event {
            ProposalEvent::Applied {
                local_proposal_id,
                index,
                term,
                result,
            } => self.resolve_write(
                *local_proposal_id,
                Ok(WriteReceipt {
                    index: *index,
                    term: *term,
                    result: *result,
                }),
            ),
            ProposalEvent::Rejected {
                local_proposal_id,
                reason,
                leader_hint,
            } => {
                let error = write_error_from_rejection(reason.clone(), *leader_hint);
                self.resolve_write(*local_proposal_id, Err(error));
            }
            ProposalEvent::UnknownOutcome {
                local_proposal_id,
                client_request_id,
                reason,
            } => {
                self.runtime_unknown_outcomes += 1;
                let fallback = self
                    .write_waiters
                    .get(local_proposal_id)
                    .and_then(|waiter| waiter.options.client_request_id);
                self.resolve_write(
                    *local_proposal_id,
                    Err(WriteError::UnknownOutcome {
                        local_proposal_id: *local_proposal_id,
                        client_request_id: client_request_id.or(fallback),
                        reason: unknown_outcome_reason(reason),
                    }),
                );
            }
            _ => {}
        }
    }

    fn observe_read_event(&mut self, event: &ReadEvent<LockGroupId>) {
        match event {
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
            // A grant is picked up by the next `drive_read`, which reads the
            // state machine under the proof the group cached.
            _ => {}
        }
    }

    fn resolve_write(
        &mut self,
        local_proposal_id: LocalProposalId,
        outcome: Result<WriteReceipt<ApplyOutcome>, WriteError>,
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

    fn resolve_read(
        &mut self,
        read_id: ReadId,
        outcome: Result<QueryReceipt<LockGroupId, LockQueryResult>, ReadError>,
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

    fn abandon_all_waiters(&mut self, reason: UnknownOutcomeReason) {
        for local_proposal_id in self.write_waiters.keys().copied().collect::<Vec<_>>() {
            let options = self.write_waiters[&local_proposal_id].options;
            self.resolve_write(
                local_proposal_id,
                Err(WriteError::UnknownOutcome {
                    local_proposal_id,
                    client_request_id: options.client_request_id,
                    reason,
                }),
            );
        }
        for read_id in self.read_waiters.keys().copied().collect::<Vec<_>>() {
            self.resolve_read(
                read_id,
                Err(ReadError::Transport {
                    message: format!("lock read barrier {read_id} lost its driver"),
                }),
            );
        }
    }

    fn allocate_local_proposal_id(&mut self) -> Option<LocalProposalId> {
        let id = self.next_local_proposal_id;
        self.next_local_proposal_id = id.checked_add(1)?;
        Some(LocalProposalId(id))
    }

    fn allocate_read_id(&mut self) -> Option<ReadId> {
        let id = self.next_read_id;
        self.next_read_id = id.checked_add(1)?;
        Some(ReadId(id))
    }
}

/// Terminal client outcomes are carried by the application's own vocabulary,
/// so the cluster records history against these rather than raw receipts.
pub struct PendingSubmit {
    operation_id: OperationId,
    node_id: NodeId,
    local_proposal_id: Option<LocalProposalId>,
    future: Pin<Box<dyn Future<Output = SubmitOutcome>>>,
}

impl fmt::Debug for PendingSubmit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingSubmit")
            .field("operation_id", &self.operation_id)
            .field("node_id", &self.node_id)
            .field("local_proposal_id", &self.local_proposal_id)
            .finish_non_exhaustive()
    }
}

/// One linearizable query in flight against a chosen replica.
pub struct PendingQuery {
    node_id: NodeId,
    read_id: Option<ReadId>,
    future: Pin<Box<dyn Future<Output = QueryOutcome<LockGroupId>>>>,
}

impl fmt::Debug for PendingQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingQuery")
            .field("node_id", &self.node_id)
            .field("read_id", &self.read_id)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct ClusterNode {
    node_id: NodeId,
    driver: NodeDriver,
    client: LockClient<LockGroupId, NodeDriver>,
}

/// Deterministic three-node lock cluster with explicit delivery control.
#[derive(Debug)]
pub struct LockCluster {
    config: LockConfig,
    network: DeterministicNetwork,
    nodes: Vec<ClusterNode>,
    history: Vec<HistoryEvent>,
    next_operation_id: u64,
}

impl LockCluster {
    /// Builds three replicas whose election timeouts make every uncontested
    /// election deterministic: the lowest-numbered reachable node wins.
    pub fn new(config: LockConfig) -> Self {
        let network = DeterministicNetwork::new();
        let all_nodes = [NodeId(1), NodeId(2), NodeId(3)];
        let nodes = [(NodeId(1), 4), (NodeId(2), 6), (NodeId(3), 8)]
            .into_iter()
            .map(|(node_id, election_timeout_ticks)| {
                let peers = all_nodes
                    .iter()
                    .copied()
                    .filter(|peer| *peer != node_id)
                    .collect::<Vec<_>>();
                let directory = PeerDirectory::new(&all_nodes, &peers);
                let transport = network.endpoint(node_id, directory);
                let (group, report) = open_group(
                    node_id,
                    &peers,
                    election_timeout_ticks,
                    empty_storage(),
                    LockStateMachine::new(config),
                );
                let metrics = MetricsPublisher::new(group.metrics());
                let mut state = NodeState {
                    election_timeout_ticks,
                    peers,
                    group: Some(group),
                    transport,
                    metrics,
                    write_waiters: BTreeMap::new(),
                    read_waiters: BTreeMap::new(),
                    next_local_proposal_id: 1,
                    next_read_id: 1,
                    refused_sends: 0,
                    runtime_unknown_outcomes: 0,
                    shutting_down: false,
                };
                state.record_report(report);
                let driver = NodeDriver {
                    inner: Arc::new(Mutex::new(state)),
                };
                let client = LockClient::new(RaftHandle::new(GROUP_ID, driver.clone()));
                ClusterNode {
                    node_id,
                    driver,
                    client,
                }
            })
            .collect();

        Self {
            config,
            network,
            nodes,
            history: Vec::new(),
            next_operation_id: 1,
        }
    }

    /// Returns the configured lock bounds shared by every replica.
    pub fn config(&self) -> LockConfig {
        self.config
    }

    /// Returns the recorded client history.
    pub fn history(&self) -> &[HistoryEvent] {
        &self.history
    }

    /// Returns every node ID in the cluster.
    pub fn node_ids(&self) -> Vec<NodeId> {
        self.nodes.iter().map(|node| node.node_id).collect()
    }

    /// Returns one replica's managed lock client.
    pub fn client(&self, node_id: NodeId) -> &LockClient<LockGroupId, NodeDriver> {
        &self.node(node_id).client
    }

    /// Returns one replica's managed command sender.
    pub fn driver(&self, node_id: NodeId) -> &NodeDriver {
        &self.node(node_id).driver
    }

    /// Returns the reachable leader with the highest term, if one exists.
    pub fn leader(&self) -> Option<NodeId> {
        self.nodes
            .iter()
            .filter(|node| self.network.reaches(node.node_id))
            .filter_map(|node| {
                let metrics = lock(&node.driver.inner).group().metrics();
                (metrics.role == Role::Leader).then_some((metrics.term, node.node_id))
            })
            .max()
            .map(|(_, node_id)| node_id)
    }

    /// Returns a replica's role as its own metrics report it.
    ///
    /// A node with cut links keeps reporting whatever its last round told it,
    /// which is how an isolated former leader still believes it leads.
    pub fn believes_it_leads(&self, node_id: NodeId) -> bool {
        lock(&self.node(node_id).driver.inner)
            .group()
            .metrics()
            .role
            == Role::Leader
    }

    /// Returns a copy of one replica's state machine.
    pub fn state_machine(&self, node_id: NodeId) -> LockStateMachine {
        lock(&self.node(node_id).driver.inner)
            .group()
            .state_machine()
            .clone()
    }

    /// Returns the replica's applied index.
    pub fn applied_index(&self, node_id: NodeId) -> LogIndex {
        self.state_machine(node_id)
            .applied_index()
            .expect("lock state machines always report an applied index")
    }

    /// Returns the index a replica's state machine must reach to have applied
    /// every application command that replica knows to be committed.
    ///
    /// This is the readiness half of [`LockCluster::applied_index`]: the two
    /// together say whether a replica has consumed everything it knows about.
    pub fn committed_application_index(&self, node_id: NodeId) -> LogIndex {
        lock(&self.node(node_id).driver.inner)
            .group()
            .committed_application_index()
    }

    /// Returns the commands committed on one replica, in log order.
    ///
    /// This reads the durable log through the public runtime accessor and
    /// decodes it with the same adapter the replica applies with, so the
    /// checker can replay a real replicated history through the oracle.
    pub fn committed_commands(&self, node_id: NodeId) -> Vec<Command> {
        let state = lock(&self.node(node_id).driver.inner);
        committed_application_entries(&state)
            .into_iter()
            .map(|(_, payload)| {
                state
                    .group()
                    .state_machine()
                    .decode_command(&payload)
                    .expect("replicas only append frames this adapter encoded")
            })
            .collect()
    }

    /// Drives ticks until a leader exists among the reachable nodes.
    pub fn elect_leader(&mut self) -> NodeId {
        for _ in 0..MAX_ROUNDS {
            if let Some(leader) = self.leader() {
                return leader;
            }
            self.run_rounds(1);
        }
        panic!("no leader was elected within {MAX_ROUNDS} rounds");
    }

    /// Drives the cluster until every reachable replica has applied every
    /// application command it knows to be committed.
    ///
    /// Elections and membership changes commit entries the state machine never
    /// sees, so the applied index legitimately trails the commit index and the
    /// gate is the group's committed application index instead. Convergence is
    /// a precondition for comparing replicas to each other and to the oracle,
    /// never something a test may assume.
    pub fn settle(&mut self) {
        for _ in 0..MAX_ROUNDS {
            let converged = self.node_ids().into_iter().all(|node_id| {
                !self.network.reaches(node_id)
                    || self.applied_index(node_id) >= self.committed_application_index(node_id)
            });
            if converged && self.network.is_idle() {
                return;
            }
            self.run_rounds(1);
        }
        panic!("replicas did not apply their committed commands within {MAX_ROUNDS} rounds");
    }

    /// Ticks every node, delivers every accepted frame, and retries every
    /// outstanding read barrier, `rounds` times.
    pub fn run_rounds(&mut self, rounds: usize) {
        for _ in 0..rounds {
            self.deliver_all();
            self.tick_all();
            self.deliver_all();
            self.drive_reads();
        }
    }

    /// Ticks every running node once, cut off or not.
    ///
    /// A partitioned replica is not a stopped replica. It keeps ticking, keeps
    /// offering frames its transport refuses, and keeps believing whatever its
    /// last successful round told it.
    pub fn tick_all(&mut self) {
        for node_id in self.node_ids() {
            let mut state = lock(&self.node(node_id).driver.inner);
            if state.shutting_down {
                continue;
            }
            let report = state
                .group_mut()
                .step(GroupInput::Tick)
                .expect("a healthy group accepts ticks");
            state.record_report(report);
        }
    }

    /// Delivers every accepted frame, including frames delivery produces.
    pub fn deliver_all(&mut self) {
        for _ in 0..MAX_DELIVERY_PASSES {
            let batch = self.network.take_deliverable();
            if batch.is_empty() {
                return;
            }
            for envelope in batch {
                let to = envelope.raft_to;
                let mut state = lock(&self.node(to).driver.inner);
                if state.shutting_down {
                    continue;
                }
                // A frame that fails this replica's own authentication policy
                // is refused here, exactly where a production embedder refuses
                // it, and never reaches the group.
                let Ok(envelope) = state.transport.accept_inbound(envelope) else {
                    continue;
                };
                let report = state
                    .group_mut()
                    .step(GroupInput::PeerMessage { envelope })
                    .expect("a healthy group accepts validated peer messages");
                state.record_report(report);
            }
        }
        panic!("peer delivery did not quiesce within {MAX_DELIVERY_PASSES} passes");
    }

    /// Retries every outstanding read barrier on every reachable replica.
    pub fn drive_reads(&mut self) {
        for node_id in self.node_ids() {
            let mut state = lock(&self.node(node_id).driver.inner);
            for read_id in state.read_waiters.keys().copied().collect::<Vec<_>>() {
                state.drive_read(read_id);
            }
        }
    }

    /// Cuts every link to and from `node_id`.
    pub fn isolate(&mut self, node_id: NodeId) {
        self.network.isolate(node_id);
    }

    /// Cuts only the links into `node_id`, leaving it able to send.
    pub fn isolate_inbound(&mut self, node_id: NodeId) {
        self.network.isolate_inbound(node_id);
    }

    /// Restores every cut link.
    pub fn heal(&mut self) {
        self.network.heal();
    }

    /// Restarts one replica over its retained durable media.
    ///
    /// Decomposition is the in-process restart path. The retiring group hands
    /// back its state machine, and its runtime hands back all three durable
    /// stores, so nothing is cloned and no store is silently replaced by an
    /// empty one. From there this follows the documented recipe: read the
    /// application's durable applied floor, recover through the same floor, then
    /// hand the recovery outputs to the new group before using it. The managed
    /// handle survives, because a handle names a service rather than a node
    /// incarnation.
    pub fn restart(&mut self, node_id: NodeId) {
        let mut state = lock(&self.node(node_id).driver.inner);
        state.abandon_all_waiters(UnknownOutcomeReason::RuntimeDroppedProposal);
        let peers = state.peers.clone();
        let election_timeout_ticks = state.election_timeout_ticks;
        let parts = state
            .group
            .take()
            .expect("a running node holds its group")
            .into_parts();
        // The returned ID watermarks are deliberately unused. They are
        // load-bearing only when the same runtime is carried into the new
        // group; this driver drops that runtime and rebuilds one from the
        // durable storage it returns, and a rebuilt runtime carries no local
        // proposal tracking, so a group over it may restart its IDs at zero.
        // This replica's own counters never restart anyway, which is stricter
        // than the contract requires.
        let storage = parts.runtime.into_storage();

        let (group, report) = open_group(
            node_id,
            &peers,
            election_timeout_ticks,
            storage,
            parts.state_machine,
        );
        state.group = Some(group);
        state.record_report(report);
    }

    /// Invokes one command against `node_id` without waiting for it.
    ///
    /// The invocation is recorded immediately, so a command whose outcome is
    /// later lost still appears in the history with the exact bytes a retry
    /// must repeat.
    pub fn begin_submit(&mut self, node_id: NodeId, command: Command) -> PendingSubmit {
        let operation_id = self.record_invocation(command);
        let driver = self.node(node_id).driver.clone();
        let before = driver.pending_write_ids();
        let client = self.node(node_id).client.clone();
        let mut future: Pin<Box<dyn Future<Output = SubmitOutcome>>> =
            Box::pin(async move { client.submit_command(command).await });
        let first = poll_once(&mut future);
        let local_proposal_id = driver
            .pending_write_ids()
            .difference(&before)
            .copied()
            .next();
        let mut pending = PendingSubmit {
            operation_id,
            node_id,
            local_proposal_id,
            future,
        };
        if let Poll::Ready(outcome) = first {
            pending.future = Box::pin(std::future::ready(outcome));
        }
        pending
    }

    /// Waits for a pending submission for at most `rounds` driver rounds.
    ///
    /// A caller that runs out of rounds stops waiting, and the driver closes
    /// the outcome window as unknown. That is a real client's situation, not a
    /// test shortcut.
    pub fn resolve(&mut self, mut pending: PendingSubmit, rounds: usize) -> SubmitOutcome {
        for _ in 0..rounds {
            if let Poll::Ready(outcome) = poll_once(&mut pending.future) {
                return self.record_completion(pending.operation_id, outcome);
            }
            self.run_rounds(1);
        }
        if let Poll::Ready(outcome) = poll_once(&mut pending.future) {
            return self.record_completion(pending.operation_id, outcome);
        }
        if let Some(local_proposal_id) = pending.local_proposal_id {
            self.node(pending.node_id)
                .driver
                .abandon_write(local_proposal_id);
        }
        let Poll::Ready(outcome) = poll_once(&mut pending.future) else {
            panic!("an abandoned write must resolve immediately");
        };
        self.record_completion(pending.operation_id, outcome)
    }

    /// Submits one command and waits for it under the default round budget.
    pub fn submit(&mut self, node_id: NodeId, command: Command) -> SubmitOutcome {
        let pending = self.begin_submit(node_id, command);
        self.resolve(pending, MAX_ROUNDS)
    }

    /// Starts one linearizable `GetLock` against `node_id` without waiting.
    pub fn begin_query(&mut self, node_id: NodeId, resource: ResourceName) -> PendingQuery {
        let driver = self.node(node_id).driver.clone();
        let before = driver.pending_read_ids();
        let client = self.node(node_id).client.clone();
        let mut future: Pin<Box<dyn Future<Output = QueryOutcome<LockGroupId>>>> =
            Box::pin(async move { client.get_lock(resource).await });
        let first = poll_once(&mut future);
        let read_id = driver
            .pending_read_ids()
            .difference(&before)
            .copied()
            .next();
        let mut pending = PendingQuery {
            node_id,
            read_id,
            future,
        };
        if let Poll::Ready(outcome) = first {
            pending.future = Box::pin(std::future::ready(outcome));
        }
        pending
    }

    /// Waits for a pending query for at most `rounds` driver rounds.
    pub fn resolve_query(
        &mut self,
        mut pending: PendingQuery,
        rounds: usize,
    ) -> QueryOutcome<LockGroupId> {
        for _ in 0..rounds {
            if let Poll::Ready(outcome) = poll_once(&mut pending.future) {
                return outcome;
            }
            self.run_rounds(1);
        }
        if let Poll::Ready(outcome) = poll_once(&mut pending.future) {
            return outcome;
        }
        if let Some(read_id) = pending.read_id {
            self.node(pending.node_id).driver.abandon_read(read_id);
        }
        let Poll::Ready(outcome) = poll_once(&mut pending.future) else {
            panic!("an abandoned read must resolve immediately");
        };
        outcome
    }

    /// Runs one linearizable `GetLock` under the default round budget.
    pub fn get_lock(
        &mut self,
        node_id: NodeId,
        resource: ResourceName,
    ) -> QueryOutcome<LockGroupId> {
        let pending = self.begin_query(node_id, resource);
        self.resolve_query(pending, MAX_ROUNDS)
    }

    /// Returns how many accepted frames the network dropped before delivery.
    pub fn dropped_inbound(&self) -> u64 {
        self.network.dropped_inbound()
    }

    /// Returns replicated logical time as one replica has applied it.
    pub fn logical_time(&self, node_id: NodeId) -> LogicalTime {
        self.state_machine(node_id).service().logical_time()
    }

    fn record_invocation(&mut self, command: Command) -> OperationId {
        let operation_id = OperationId::new(self.next_operation_id);
        self.next_operation_id += 1;
        self.history.push(HistoryEvent::Invoked {
            operation_id,
            command,
        });
        operation_id
    }

    fn record_completion(
        &mut self,
        operation_id: OperationId,
        outcome: SubmitOutcome,
    ) -> SubmitOutcome {
        match &outcome {
            SubmitOutcome::Completed { outcome, .. } => {
                self.history.push(HistoryEvent::Completed {
                    operation_id,
                    response: outcome.response,
                });
            }
            // A refusal provably did not commit, but the contract's history
            // vocabulary has no weaker terminal event than `Unknown`, and
            // `Unknown` is the sound over-approximation.
            SubmitOutcome::Refused { .. } | SubmitOutcome::Unknown { .. } => {
                self.history.push(HistoryEvent::Unknown { operation_id });
            }
        }
        outcome
    }

    fn node(&self, node_id: NodeId) -> &ClusterNode {
        self.nodes
            .iter()
            .find(|node| node.node_id == node_id)
            .expect("the driver only addresses its own nodes")
    }
}

/// Returns the log entries whose payloads the checker replays.
///
/// This walks the log because it needs the encoded commands themselves. The
/// convergence predicate does not: the group reports the committed application
/// index directly.
fn committed_application_entries(state: &NodeState) -> Vec<(LogIndex, Vec<u8>)> {
    let runtime = state.group().runtime();
    let commit_index = runtime.commit_index();
    let first_index = runtime.snapshot_index().0 + 1;
    runtime
        .log_entries_from(LogIndex(first_index))
        .into_iter()
        .zip(first_index..)
        .take_while(|(_, index)| LogIndex(*index) <= commit_index)
        .filter_map(|(entry, index)| match entry.kind {
            LogEntryKind::Application(payload) => {
                Some((LogIndex(index), payload.as_ref().to_vec()))
            }
            LogEntryKind::Configuration(_) | LogEntryKind::Noop => None,
        })
        .collect()
}

/// Returns empty durable storage for a replica that has never started.
///
/// Only a replica that has never started gets this. Every later incarnation
/// recovers from the stores `DurableRaftNode::into_storage` returned, including
/// the snapshot store — which this slice's state machine never writes, because
/// it refuses to build a durable application snapshot, but which is still its
/// own medium rather than something a restart may quietly replace.
fn empty_storage() -> LockStorage {
    LockStorage {
        hard_state_store: InMemoryRaftHardStateStore::default(),
        log_segment: InMemoryRaftLogSegment::default(),
        snapshot_store: InMemoryRaftSnapshotStore::new(),
    }
}

fn open_group(
    node_id: NodeId,
    peers: &[NodeId],
    election_timeout_ticks: u64,
    storage: LockStorage,
    app: LockStateMachine,
) -> (LockGroup, LockReport) {
    let config = NodeConfig::new(node_id, peers.to_vec(), election_timeout_ticks)
        .expect("three-node static configuration is valid");
    let applied_index = app
        .applied_index()
        .expect("lock state machines always report an applied index");
    let recovered = DurableRaftNode::recover_with_storage_and_snapshot_store_applied_through(
        config,
        storage.hard_state_store,
        storage.log_segment,
        storage.snapshot_store,
        applied_index,
    )
    .expect("retained durable state reopens");
    let (raft, recovery_outputs) = recovered.into_parts();
    let mut group = RaftGroup::with_applied_index(GROUP_ID, node_id, raft, app, applied_index);
    let report = group
        .apply_raft_outputs(recovery_outputs)
        .expect("recovered outputs apply");
    (group, report)
}

fn write_error_from_rejection(
    reason: ProposalRejection,
    leader_hint: Option<NodeId>,
) -> WriteError {
    match reason {
        ProposalRejection::NotLeader { term, .. } => WriteError::NotLeader { leader_hint, term },
        ProposalRejection::PayloadTooLarge {
            payload_len,
            max_payload_len,
        } => WriteError::PayloadTooLarge {
            max: max_payload_len,
            actual: payload_len,
        },
        reason => WriteError::Rejected { reason },
    }
}

/// Maps a group error into the service layer's write vocabulary.
///
/// `GroupError::StateMachine` carries this consumer's own typed
/// [`rafter_reference_fenced_lock::LockAdapterError`], but every `WriteError`
/// variant that could hold it takes a `String`, so the type is lost here. A
/// caller downstream of this driver can only read the rendered message.
fn write_error_from_group(error: &LockGroupError) -> WriteError {
    match error {
        GroupError::Poisoned { reason } => WriteError::Poisoned {
            reason: reason.clone(),
        },
        GroupError::StateMachine { source, .. } => WriteError::ApplyFailed {
            message: source.to_string(),
        },
        error => WriteError::Transport {
            message: format!("{error:?}"),
        },
    }
}

fn read_error_from_group(error: &LockGroupError) -> ReadError {
    match error {
        GroupError::Poisoned { reason } => ReadError::Poisoned {
            reason: reason.clone(),
        },
        GroupError::StateMachine { source, .. } => ReadError::ApplyFailed {
            message: source.to_string(),
        },
        GroupError::UnsupportedReadConsistency { consistency } => {
            ReadError::UnsupportedConsistency {
                consistency: *consistency,
            }
        }
        error => ReadError::Transport {
            message: format!("{error:?}"),
        },
    }
}

fn transfer_error_from_group(error: &LockGroupError) -> TransferLeadershipError {
    match error {
        GroupError::Poisoned { reason } => TransferLeadershipError::Poisoned {
            reason: reason.clone(),
        },
        error => TransferLeadershipError::Transport {
            message: format!("{error:?}"),
        },
    }
}

/// Reports a write refused before replication on the read path.
///
/// Admission is one decision, but the service layer splits its errors by
/// operation, so a shared check has to be translated for each surface.
fn read_error_from_write(error: WriteError) -> ReadError {
    match error {
        WriteError::ShuttingDown => ReadError::ShuttingDown,
        error => ReadError::Transport {
            message: error.to_string(),
        },
    }
}

fn unknown_outcome_reason(reason: &ProposalUnknownOutcomeReason) -> UnknownOutcomeReason {
    match reason {
        ProposalUnknownOutcomeReason::GroupPoisoned => UnknownOutcomeReason::GroupPoisoned,
        _ => UnknownOutcomeReason::RuntimeDroppedProposal,
    }
}

fn settled_write(outcome: LockWriteResult) -> WriteStart {
    WriteStart::Settled(Box::new(outcome))
}

fn settled_read(outcome: LockReadResult) -> ReadStart {
    ReadStart::Settled(Box::new(outcome))
}

fn poll_once<T>(future: &mut Pin<Box<dyn Future<Output = T>>>) -> Poll<T> {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    future.as_mut().poll(&mut context)
}

fn lock<T>(shared: &Mutex<T>) -> MutexGuard<'_, T> {
    shared.lock().unwrap_or_else(PoisonError::into_inner)
}
