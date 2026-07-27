#![allow(clippy::wildcard_imports)]

//! The step/route loop behind one transport driver.
//!
//! Everything here is private to the driver. It is a separate file because
//! the public surface and the mechanism behind it are read for different
//! reasons: one is a contract, the other is a loop. The waiter tables the loop
//! resolves live beside it in [`super::waiters`], split the same way and for
//! the same reason: this file answers "what does a step do", that one answers
//! "what happens to the client". The third answer — who may send a step's
//! input at all — is [`super::control_plane`], which owns every derivation over
//! the membership, peer, and fence fields declared below.

use std::{collections::BTreeSet, sync::TryLockError};

use crate::transport::{AuthenticatedPeerValidator, RaftTransport, SnapshotChunkEnvelope};

use super::super::*;
use super::waiters::{ReadWaiter, WriteWaiter};
use super::TransportDriverOptions;

/// Why one group step did not run, or did not finish.
///
/// The group error travels unwrapped so each caller can report it in its own
/// vocabulary: a driver hears `ManagedDriverError`, and a client hears the
/// typed write or read category the same mapping gives `InMemoryRaftDriver`.
pub(super) enum StepFailure<E, RE> {
    NoGroup,
    Group(GroupError<E, RE>),
}

/// Which waiter a dropped client future reclaims.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WaiterId {
    Write(LocalProposalId),
    Read(ReadId),
}

/// What starting a read produced.
///
/// Two shapes because the driver serves two consistency levels and only one of
/// them waits. A linearizable read reserves a barrier that some later step
/// resolves, so starting it yields a name; a local read is answered by the call
/// that starts it, so starting it yields the answer. Collapsing the two into an
/// `Option<ReadId>` would leave the caller of a local read holding a `None` and
/// still needing somewhere to put the receipt.
pub(super) enum StartedRead<G, QR> {
    /// A barrier was reserved under this ID and its waiter is registered.
    Barrier(ReadId),
    /// The read was answered inside the call that started it. No waiter exists,
    /// no [`ReadId`] was allocated, and there is nothing to abandon.
    Answered(Result<QueryReceipt<G, QR>, ReadError>),
}

/// The driver's state, shared by every clone and every client future.
pub(super) type SharedState<G, A, R, T, V> = Arc<DriverShared<G, A, R, T, V>>;

/// One held driver lock.
pub(super) type StateGuard<'a, G, A, R, T, V> = MutexGuard<'a, TransportDriverState<G, A, R, T, V>>;

/// One driver's state, and the reclamations that could not run yet.
///
/// Two locks rather than one, because the second exists to be takeable when the
/// first is not. A client future's `Drop` reclaims its own waiter, and it may
/// run on a thread that already holds `state`: inside a
/// [`TransportRaftDriver::with_group`] closure, inside a
/// [`RaftTransport`] call this driver makes while routing a report, inside a
/// [`ReplicatedStateMachine`] apply, or inside a `Waker` this driver woke while
/// resolving a waiter. `std::sync::Mutex` is not reentrant, so a `Drop` that
/// waited for `state` in any of those would stop that thread forever.
///
/// So reclamation never waits. It asks for the lock, and a guard that cannot
/// have it leaves its waiter here for the next acquisition to take.
pub(super) struct DriverShared<G, A, R, T, V>
where
    A: ReplicatedStateMachine,
{
    state: Mutex<TransportDriverState<G, A, R, T, V>>,
    /// Waiters whose client futures were dropped while `state` was held.
    ///
    /// Locked only across a push and a take. No group method, no transport
    /// call, and no embedder code of any kind runs under it, so it is a leaf
    /// and cannot become the next version of the defect it exists to fix.
    deferred: Mutex<Vec<WaiterId>>,
}

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
    pub(super) refused_peer_updates: u64,
    pub(super) refused_non_member_frames: u64,
    /// What the group currently requires, which is the *desired* half of the
    /// peer control plane.
    ///
    /// The driver's record of what the cluster says, and only that. It advances
    /// whether or not the link layer took the statements derived from it,
    /// because a later committed removal has to be computed against the
    /// membership the cluster had rather than against the last one a transport
    /// happened to accept.
    ///
    /// It is committed ∪ effective by construction — every publisher either
    /// widens it or narrows it no further than the committed configuration — so
    /// it names every replica that may legitimately speak to this driver,
    /// including one joining under a change still in flight. That is what makes
    /// it usable, less `retired`, as the inbound admission check in
    /// [`TransportDriverState::is_member`] and not merely as a diff source.
    pub(super) known_members: BTreeSet<NodeId>,
    /// The peer set this driver's transport last *accepted*, or `None` before it
    /// has accepted one.
    ///
    /// Tracking what the group says and tracking what the link layer took are
    /// two different facts, and a driver that keeps only the first cannot tell a
    /// published peer set from a refused one. This is the second, and the
    /// difference between it and the set derived from `known_members` is the
    /// whole of the peer-set half's outstanding work.
    ///
    /// `None` rather than an empty set for "nothing accepted yet", because an
    /// empty peer set is a real and publishable statement — a single-voter group
    /// authorizes no peers — and a driver that could not tell the two apart
    /// would skip the first publication of exactly that group.
    pub(super) published_peers: Option<BTreeSet<NodeId>>,
    /// Committed removals whose fence the transport has not accepted yet.
    ///
    /// An obligation rather than a derived value, and that is why it is stored.
    /// `known_members` advances past a removal the moment the cluster commits
    /// it, so no later membership event re-derives the fence: once the diff has
    /// been taken, the only record that it was owed is this one. A fence leaves
    /// the set when [`RaftTransport::fence_peer`] returns `Ok` for it, and for
    /// no other reason.
    ///
    /// Never contains the local node. Fencing self is excluded where the
    /// obligation is recorded *and* where it is discharged, because a driver's
    /// node ID can change across an adoption while an obligation is outstanding.
    pub(super) pending_fences: BTreeSet<NodeId>,
    /// Every replica this driver has seen a committed removal for, forever.
    ///
    /// A `(group_id, NodeId)` pair is single-use: a committed removal consumes
    /// the identity, and a replica that returns returns under a fresh one. See
    /// [`rafter::NodeId`], which states the contract, and
    /// [`crate::RaftTransport::fence_peer`], which is why it has to hold — a
    /// fence is permanent for the principal it names and there is no unfence, so
    /// an ID whose fence has been accepted can never speak again whatever a
    /// later membership says.
    ///
    /// Recorded by the same event that records the fence obligation, and never
    /// removed for this driver's lifetime. Enforcement across restarts is a
    /// deployment's own allocation discipline and not something a driver can
    /// see; enforcement within one is this set.
    ///
    /// Excludes the local node, exactly as `pending_fences` does and by the same
    /// filter. Both are statements this driver holds about its *peers*, and a
    /// driver that has since become the removed replica holds neither about
    /// itself.
    pub(super) retired: BTreeSet<NodeId>,
    pub(super) shutting_down: bool,
}

impl<G, A, R, T, V> DriverShared<G, A, R, T, V>
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
    pub(super) fn new(state: TransportDriverState<G, A, R, T, V>) -> Self {
        Self {
            state: Mutex::new(state),
            deferred: Mutex::new(Vec::new()),
        }
    }

    /// Locks the driver, reclaiming anything a drop had to defer first.
    ///
    /// This is the only way into the state, so "the next acquisition" in
    /// [`DriverShared::reclaim`]'s contract is every public method of the
    /// driver, every poll of a client future, and every guard drop that finds
    /// the lock free.
    pub(super) fn lock(&self) -> StateGuard<'_, G, A, R, T, V> {
        let mut state = lock_state(&self.state);
        self.reclaim_deferred(&mut state);
        state
    }

    /// Reclaims one dropped future's waiter now, or leaves it for the next
    /// acquisition.
    ///
    /// `try_lock` decides which, and it cannot decide wrongly. It hands out a
    /// `MutexGuard`, so it cannot succeed on a thread that already holds one —
    /// the two guards would alias the same `&mut`. A re-entrant drop therefore
    /// always takes the deferred path by the type system rather than by a
    /// platform detail, and the converse mistake costs nothing: deferring
    /// because another thread happened to hold the lock reclaims at that
    /// thread's next acquisition, which is at most one call away.
    pub(super) fn reclaim(&self, waiter: WaiterId) {
        let Some(mut state) = self.try_lock() else {
            lock_state(&self.deferred).push(waiter);
            return;
        };
        state.discard(waiter);
        self.reclaim_deferred(&mut state);
    }

    fn try_lock(&self) -> Option<StateGuard<'_, G, A, R, T, V>> {
        match self.state.try_lock() {
            Ok(state) => Some(state),
            // A poisoned lock is still this driver's state, and the driver's
            // own `lock` recovers from one rather than propagating it.
            Err(TryLockError::Poisoned(poisoned)) => Some(poisoned.into_inner()),
            Err(TryLockError::WouldBlock) => None,
        }
    }

    /// Takes deferred reclamations until there are none left.
    ///
    /// It loops rather than draining once because reclaiming a read publishes
    /// metrics, publishing wakes watchers under this lock, and a woken task may
    /// drop another client future — which defers onto the queue being drained.
    /// It terminates for the reason the tables are bounded at all: each entry
    /// comes from one guard, and a guard reclaims once.
    fn reclaim_deferred(&self, state: &mut TransportDriverState<G, A, R, T, V>) {
        loop {
            let batch = std::mem::take(&mut *lock_state(&self.deferred));
            if batch.is_empty() {
                return;
            }
            for waiter in batch {
                state.discard(waiter);
            }
        }
    }
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

    /// Steps the group with a leadership transfer, reporting the rejection it
    /// saw.
    ///
    /// Everything else is [`TransportDriverState::step_group`], the poison drain
    /// on both paths included. The only difference is the return value: a
    /// transfer has no waiter table, because it is created and resolved inside
    /// one call, so the one fact its caller needs has to be read out of the
    /// report before `route_report` consumes it.
    ///
    /// That need is why the transfer used to step the group itself, and stepping
    /// it directly is what left this — a call site the poison drain's own design
    /// listed by name — undrained on both paths. A proposal a transfer step
    /// poisoned over stayed unresolved until some unrelated later call rescued
    /// it, and a supervisor that reacted to the failed transfer by releasing
    /// told its client `DriverReleased` for a group that had poisoned under it.
    pub(super) fn step_transfer(
        &mut self,
        target: NodeId,
    ) -> Result<Option<TransferLeadershipError>, StepFailure<A::Error, R::Error>> {
        let Some(group) = self.group.as_mut() else {
            return Err(StepFailure::NoGroup);
        };
        let stepped = group.step_with_options(
            GroupInput::TransferLeadership { target },
            StepReportOptions::without_metrics(),
        );
        let result = match stepped {
            Ok(report) => {
                let rejection =
                    report
                        .leadership_transfer_events
                        .iter()
                        .find_map(|event| match event {
                            LeadershipTransferEvent::Rejected {
                                target: event_target,
                                reason,
                                leader_hint,
                            } if *event_target == target => {
                                Some(TransferLeadershipError::Rejected {
                                    reason: *reason,
                                    leader_hint: *leader_hint,
                                })
                            }
                            _ => None,
                        });
                self.route_report(report);
                Ok(rejection)
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
        for event in report.snapshot_events {
            self.route_snapshot_event(event);
        }
        for event in &report.membership_events {
            self.route_membership_event(event);
        }
        for event in &report.proposal_events {
            self.observe_proposal_event(event);
        }
        for event in &report.read_events {
            self.observe_read_event(event);
        }
    }

    /// Hands a leader chunk directive to the transport, and lets the other two
    /// snapshot events alone.
    ///
    /// `SendChunk` is the only snapshot effect a driver owns.
    /// [`crate::RaftTransport::send_snapshot_chunk`] resolves the directive
    /// against the embedder's snapshot store and frames it; a refusal is counted
    /// like any other, because the protocol re-sends.
    ///
    /// `StageChunk` is already durable. The runtime contract forbids releasing
    /// an output whose snapshot obligation has not completed, and staging the
    /// chunk *is* that obligation — `DurableRaftNode` discharges it before it
    /// returns anything. A second staging area in the driver could only diverge
    /// from the one recovery actually reads. `Apply` is likewise done: the group
    /// installed the snapshot into the state machine and moved its own applied
    /// floor before emitting the event.
    fn route_snapshot_event(&mut self, event: SnapshotEvent<G>) {
        let SnapshotEvent::SendChunk {
            group_id,
            to,
            chunk,
        } = event
        else {
            return;
        };
        let envelope = SnapshotChunkEnvelope {
            group_id,
            from: self.node_id,
            to,
            chunk,
        };
        if self.transport.send_snapshot_chunk(envelope).is_err() {
            self.refused_sends = self.refused_sends.saturating_add(1);
        }
    }

    /// Publishes the group's metrics, and discards the one thing publishing can
    /// report.
    ///
    /// This driver owns the publisher and is the only thing that closes it, in
    /// `shutdown`. A refusal here therefore means the driver is already down,
    /// and a metrics snapshot from a driver that is already down is exactly the
    /// one nobody is waiting for.
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
}
