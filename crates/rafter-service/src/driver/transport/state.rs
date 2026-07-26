#![allow(clippy::wildcard_imports)]

//! The step/route loop behind one transport driver.
//!
//! Everything here is private to the driver. It is a separate file because
//! the public surface and the mechanism behind it are read for different
//! reasons: one is a contract, the other is a loop. The waiter tables the loop
//! resolves live beside it in [`super::waiters`], split the same way and for
//! the same reason: this file answers "what does a step do", that one answers
//! "what happens to the client".

use std::{collections::BTreeSet, sync::TryLockError};

use crate::transport::{AuthenticatedPeerValidator, PeerSet, RaftTransport, SnapshotChunkEnvelope};

use super::super::*;
use super::waiters::{ReadWaiter, WriteWaiter};
use super::TransportDriverOptions;

/// The membership fact one publication is derived from.
///
/// A fact rather than a set plus a decision, and that is the whole point of the
/// type. Publishing answers two questions — which principals the link layer may
/// authorize, and which it must fence — and both are licensed by the same one
/// fact: what the cluster has *committed*. A caller that supplied a set and a
/// fencing flag as separate arguments could answer the two inconsistently, and
/// one did: adoption published a narrowed peer set for a committed removal and
/// withheld the fence for it, because the two travelled apart. Here they cannot.
///
/// So every publisher names what it knows, and
/// [`TransportDriverState::publish_membership`] derives both answers from it.
pub(super) enum MembershipFact {
    /// A configuration that is effective and may still be uncommitted.
    ///
    /// It may only widen. A replica joining under joint consensus has to be able
    /// to speak before the change commits, or it can never catch up and the
    /// change can never commit; and an uncommitted change can still be reverted,
    /// so nothing may be taken away for it.
    Effective(BTreeSet<NodeId>),
    /// A committed configuration, and the effective one beside it.
    ///
    /// Both halves are load-bearing and neither stands alone. `committed` is the
    /// only fact that licenses narrowing the set and fencing what left it.
    /// `effective` is what keeps an in-flight change's joiner able to speak
    /// across the same publication — a replica that rebuilt its runtime from
    /// durable storage can hold an appended-but-uncommitted addition in its log,
    /// and publishing the committed set alone would take the joiner's
    /// authorization away and stall the change that needs it.
    Committed {
        committed: BTreeSet<NodeId>,
        effective: BTreeSet<NodeId>,
    },
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

/// Which waiter a dropped client future reclaims.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WaiterId {
    Write(LocalProposalId),
    Read(ReadId),
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
    /// The membership this driver last published to its transport, or tried to.
    ///
    /// Kept even when publishing was refused, because it is the driver's record
    /// of what the group says rather than of what the link layer accepted: a
    /// later committed removal has to be computed against the membership the
    /// cluster had, not against the last one a transport happened to take.
    pub(super) known_members: BTreeSet<NodeId>,
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

    /// Keeps the transport's peer set level with the group's membership.
    ///
    /// `Appended` carries the effective configuration and `Applied` carries the
    /// committed one — that is how `rafter-app` builds them — so each arm names
    /// the fact it has and [`TransportDriverState::publish_membership`] decides
    /// what the fact licenses. The `Applied` arm reads the effective membership
    /// beside it for the reason [`MembershipFact::Committed`] gives: a change
    /// committing does not retract a *later* change already appended over it.
    fn route_membership_event(&mut self, event: &MembershipEvent<G>) {
        match event {
            MembershipEvent::Appended { membership, .. } => {
                let effective = membership.replica_ids().into_iter().collect();
                self.publish_membership(MembershipFact::Effective(effective));
            }
            MembershipEvent::Applied { membership, .. } => {
                let committed = membership.replica_ids().into_iter().collect();
                // A driver holding no group contributes no widening and still
                // honors the committed fact: an absent effective membership must
                // not turn a fence into a silence.
                let effective = self.effective_members().unwrap_or_default();
                self.publish_membership(MembershipFact::Committed {
                    committed,
                    effective,
                });
            }
            // A rejected change never entered the log, and a variant this driver
            // does not know is not a membership fact it can act on.
            _ => {}
        }
    }

    /// Publishes one membership to the transport, and fences what the committed
    /// half of it no longer names.
    ///
    /// Two statements, derived from one fact, installed in order and
    /// independently. Publishing the peer set is all or nothing; fencing is per
    /// replica. Neither may be skipped because the other could not be made,
    /// which is half the shape of this method: a membership event that both
    /// narrows the set and licenses a fence installs two admission controls, and
    /// a driver that dropped one of them because the other failed would leave a
    /// committed-removed replica able to speak with nothing reporting why.
    ///
    /// The other half is that no caller chooses between them. The set published
    /// is a superset of the committed membership, and the replicas fenced are
    /// exactly those the driver had published before and this superset no longer
    /// names — so a fenced replica is always absent from the committed
    /// membership, which is the only thing that licenses fencing it. Both
    /// properties are consequences of the derivation below rather than
    /// obligations on a caller, and every publisher therefore reaches them by
    /// construction: a caller that supplies
    /// [`MembershipFact::Effective`] cannot narrow or fence, and one that
    /// supplies [`MembershipFact::Committed`] cannot narrow past what committed.
    fn publish_membership(&mut self, fact: MembershipFact) {
        let members = match fact {
            // Union with what is already published, never a replacement: an
            // uncommitted change may be reverted, so it may add authorization
            // and may not take any away.
            MembershipFact::Effective(effective) => {
                let mut widened = self.known_members.clone();
                widened.extend(effective);
                widened
            }
            // Union of the two, so the committed half sets the floor the set may
            // not narrow past and the effective half adds whatever a change in
            // flight needs on top of it.
            MembershipFact::Committed {
                mut committed,
                effective,
            } => {
                committed.extend(effective);
                committed
            }
        };
        let removed = self
            .known_members
            .difference(&members)
            .copied()
            .filter(|node_id| *node_id != self.node_id)
            .collect::<Vec<_>>();
        // Advanced before either half runs, and kept even when publishing is
        // refused: this is the driver's record of what the group says rather
        // than of what the link layer accepted, so the next committed removal is
        // computed against the membership the cluster had.
        self.known_members = members;
        self.update_transport_peers();
        self.fence_removed_peers(removed);
    }

    /// Publishes the current membership as a peer set, or publishes nothing.
    ///
    /// All or nothing. A membership the validator cannot fully name is not
    /// published at all: a partial peer set authorizes fewer replicas than the
    /// cluster has, which is a quorum-splitting configuration change made by
    /// accident, while leaving the previous set in place is merely stale. That
    /// last clause is true of the peer set and only of the peer set — a fence
    /// the same event licensed is not stale when it is withheld, it is absent,
    /// which is why fencing is not part of this decision.
    ///
    /// Both a principal that cannot be named and a transport refusal are
    /// counted, because a peer set that never updated does not repair itself the
    /// way a dropped frame does.
    ///
    /// The local replica is not in its own peer set: a `PeerSet` names who may
    /// speak *to* this node, and a node is not a peer of itself.
    fn update_transport_peers(&mut self) {
        let mut principals = Vec::new();
        for node_id in self
            .known_members
            .iter()
            .copied()
            .filter(|node_id| *node_id != self.node_id)
        {
            let Some(principal) = self.validator.principal_for_node(&self.group_id, node_id) else {
                self.refused_peer_updates = self.refused_peer_updates.saturating_add(1);
                return;
            };
            principals.push(principal);
        }
        if self
            .transport
            .update_peers(&self.group_id, PeerSet::new(principals))
            .is_err()
        {
            self.refused_peer_updates = self.refused_peer_updates.saturating_add(1);
        }
    }

    /// Fences every principal a committed removal took out of the peer set.
    ///
    /// Per replica rather than all or nothing, because the two statements have
    /// different shapes. A peer set is one statement about a whole cluster, and
    /// a partial one authorizes a quorum-splitting subset of it. A fence is one
    /// statement about one replica, and fencing three of four removed replicas
    /// is strictly better than fencing none of them. A replica this deployment
    /// cannot name is counted like any other peer-set fault: the link layer is
    /// behind the group, and the count is what says so.
    fn fence_removed_peers(&mut self, removed: Vec<NodeId>) {
        for node_id in removed {
            let Some(principal) = self.validator.principal_for_node(&self.group_id, node_id) else {
                self.refused_peer_updates = self.refused_peer_updates.saturating_add(1);
                continue;
            };
            if self
                .transport
                .fence_peer(&self.group_id, principal)
                .is_err()
            {
                self.refused_peer_updates = self.refused_peer_updates.saturating_add(1);
            }
        }
    }

    /// Reads the group's effective membership, or `None` if it holds no group.
    fn effective_members(&self) -> Option<BTreeSet<NodeId>> {
        self.group.as_ref().map(|group| {
            group
                .runtime()
                .membership()
                .replica_ids()
                .into_iter()
                .collect()
        })
    }

    /// Publishes the adopted group's membership, so the transport's peer set is
    /// defined from adoption rather than from the first change this incarnation
    /// happens to observe.
    ///
    /// A committed fact, because an adoption is where a *change* can be observed
    /// without any event announcing it. The supervisor pattern this driver
    /// documents is release, rebuild the runtime from durable storage, adopt:
    /// the committed membership a rebuilt runtime reports can have advanced past
    /// a removal while the driver held no group, and no `Applied` will ever be
    /// emitted for it because the change committed elsewhere. The driver still
    /// holds its own `known_members` from before the release, so the difference
    /// is there to be taken — and taking it is the only thing that makes one
    /// committed removal mean the same at adoption as it does on a routed event.
    ///
    /// The effective membership travels with it rather than instead of it. A
    /// runtime rebuilt from durable storage can hold an appended-but-uncommitted
    /// change, which makes its effective membership *narrower* than its
    /// committed one for a removal in flight; publishing that would take
    /// authorization away for a change that may still revert, with nothing left
    /// to repair it — no `Applied` fires, because committed never moved, and no
    /// `Appended` fires, because this driver has no input that carries a
    /// membership request.
    pub(super) fn publish_adopted_membership(&mut self) {
        let Some(group) = self.group.as_ref() else {
            return;
        };
        let runtime = group.runtime();
        let committed = runtime
            .committed_membership()
            .replica_ids()
            .into_iter()
            .collect();
        let effective = runtime.membership().replica_ids().into_iter().collect();
        self.publish_membership(MembershipFact::Committed {
            committed,
            effective,
        });
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

#[cfg(test)]
#[path = "state/tests.rs"]
mod tests;
