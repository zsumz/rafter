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
    /// The configuration this replica is operating under, as last reported.
    ///
    /// **Assigned, never merged.** One of the two membership facts this driver
    /// tracks, and it is the one that can move in both directions: a
    /// configuration that appended and did not commit can be truncated back off
    /// the log by a new leader, and a driver holding a set that only ever grew
    /// could not express that at all — the replica an overwritten configuration
    /// named would stay authorized for the life of the incarnation, because
    /// nothing would ever commit its removal.
    ///
    /// It is a widening input and never a narrowing one: the peer set and the
    /// inbound check take it in union with `committed_members`, so this alone
    /// can add authorization and never take any away.
    pub(super) effective_members: BTreeSet<NodeId>,
    /// The configuration the cluster has committed, as last reported.
    ///
    /// The other fact, assigned from its own stream for the same reason. It is
    /// the only one that licenses narrowing the peer set or fencing what left,
    /// and it is the only one retirement reads.
    ///
    /// Raw, exactly as the cluster reported it, including an identity a
    /// committed removal already spent — see `live_committed_members`, which is
    /// the part of it this driver can still honor. Keeping the raw fact is what
    /// makes a contract violation *nameable*: `readmitted_retired_peers` counts
    /// spent identities the group's membership names again, and a set that had
    /// quietly filtered them out would have nothing to count.
    pub(super) committed_members: BTreeSet<NodeId>,
    /// The part of the committed configuration whose identities are unspent.
    ///
    /// Equal to `committed_members` on every cluster that keeps the single-use
    /// contract, which is what makes the difference worth storing. A committed
    /// fact naming an identity a committed removal already spent is not a fact
    /// about who may speak — `RaftTransport::fence_peer` has no inverse, so the
    /// principal is gone — and admitting it here would un-spend the ID and
    /// re-authorize a replica the link layer will refuse forever.
    ///
    /// This is what `committed_id_high_water` is compared against, and the two
    /// together are the whole of retirement: a bounded set the size of the
    /// cluster, and one scalar.
    pub(super) live_committed_members: BTreeSet<NodeId>,
    /// The greatest `NodeId` this driver has ever seen in a committed
    /// configuration.
    ///
    /// The whole of the retirement record, in one word. A `(group, NodeId)` is
    /// spent by a committed removal, and enumerating spent IDs needs a set that
    /// grows with every removal the group ever makes — unbounded state under a
    /// retention policy nobody wrote. Under monotonic allocation it is also
    /// unnecessary: every ID ever committed is at or below this mark, so an ID
    /// at or below it that the live committed configuration does not name is
    /// exactly an ID that has been spent.
    ///
    /// `None` before the first committed fact, and that is not the same as
    /// zero: with no committed configuration observed, nothing has been spent
    /// and no ID is refusable. `NodeId(0)` is a legal identity like any other,
    /// so it cannot stand in for "no mark".
    ///
    /// The consequence a deployment must hear is that allocation gaps below the
    /// mark are unallocatable: "fresh" means *greater than anything this group
    /// has ever committed*, not merely unused. See [`rafter::NodeId`], which
    /// states the contract this reads.
    pub(super) committed_id_high_water: Option<NodeId>,
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
    /// The committed membership advances past a removal the moment the cluster
    /// commits it, so no later membership event re-derives the fence: once the
    /// diff has been taken, the only record that it was owed is this one. A
    /// fence leaves the set when [`RaftTransport::fence_peer`] returns `Ok` for
    /// it, and for no other reason.
    ///
    /// **It may contain the local node, and that entry is deferred rather than
    /// dropped.** A committed removal of this replica owes its own link layer
    /// the same statement every other replica is making, and the only thing this
    /// driver cannot do is make it while it *is* that replica — fencing itself
    /// would cut off its own inbound frames. So the flush skips an entry equal
    /// to the current `node_id` without removing it, and the first adoption
    /// under a different identity discharges it like any other.
    ///
    /// The one structure here that still holds exact identities, and therefore
    /// the one whose growth an embedder is told about:
    /// [`TransportDriverOptions::fence_backlog_service_threshold`] decides when
    /// the driver stops serving, and
    /// [`super::PeerControlPlaneCheckpoint`] is how the set survives a process
    /// restart.
    pub(super) pending_fences: BTreeSet<NodeId>,
    /// How many times the checkpointable control-plane state has changed.
    ///
    /// The change signal an embedder persists against. Eq over the checkpoint
    /// itself would work and costs a set comparison per poll; this costs a
    /// `u64` load and cannot report equal for two different states. Monotone and
    /// **instance-local**: a fresh driver starts at zero whatever it restored,
    /// so a caller records the epoch it last persisted *for this driver* and
    /// compares against that.
    pub(super) checkpoint_epoch: u64,
    pub(super) shutting_down: bool,
}

/// Why a driver is refusing new client work, if it is.
///
/// **A total answer to that question**, which it did not used to be: a released
/// driver and a shut-down one both reported `Serving` while refusing everything,
/// so a supervisor polling this could not tell "ready" from "gone". Every state
/// in which this driver refuses a client operation for a reason of its own is
/// named here.
///
/// One shape, because a client asking "may I write" needs one answer and an
/// operator asking "why not" needs the reason beside it. These are states rather
/// than counts, like [`TransportRaftDriver::pending_peer_fences`] and unlike the
/// refusal counters: they say what is true now.
///
/// **Three of them end and two do not.** [`DriverServiceState::FenceBacklog`]
/// drains, [`DriverServiceState::NotMember`] clears when the cluster names this
/// replica again, and [`DriverServiceState::Released`] ends at the next
/// adoption. [`DriverServiceState::Decommissioned`] and
/// [`DriverServiceState::ShuttingDown`] are terminal.
///
/// None of the first four stops the protocol. A driver in any of them still
/// ticks, still delivers, still flushes its peer control plane, and still
/// applies what commits — what stops is admitting *new* client operations. A
/// replica that stopped stepping could not finish the catch-up that ends one of
/// these conditions, and could not stay a useful follower through the others.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum DriverServiceState {
    /// The driver is serving clients.
    ///
    /// Requires that a committed removal has not spent this replica's identity
    /// **and** that some configuration this driver knows still names it. The
    /// second clause is not implied by the first: an addition that appended and
    /// was then truncated back off the log leaves this replica in no
    /// configuration at all with nothing spent, which is
    /// [`DriverServiceState::NotMember`].
    Serving,
    /// A committed configuration change removed this driver's own replica.
    ///
    /// Terminal for this incarnation and not for the driver. The group stays
    /// until [`TransportRaftDriver::release_group`] — the durable log is still
    /// there and the runtime is still live, so a replica that is stepping down
    /// can still be read from and can still help others catch up — and the
    /// supervisor's move is release, then adopt a *fresh* identity. Adopting the
    /// same one back is refused, because the cluster spent it.
    ///
    /// Outranks every non-terminal state when several hold: a backlog drains and
    /// a rollback can be re-proposed, and a removal can be neither.
    Decommissioned { node_id: NodeId },
    /// No configuration this driver knows names this replica, and no committed
    /// removal spent it either.
    ///
    /// Distinct from [`DriverServiceState::Decommissioned`] in both direction and
    /// permanence, and the difference is the point. A local replica that joined
    /// effectively and was then rolled back — a new leader truncating the
    /// uncommitted addition back off the log — is in no configuration, has an
    /// unspent ID, and is receiving no replication. Reporting it as serving let
    /// it answer local reads from a replica the cluster is not replicating to,
    /// which is an unboundedly stale view with nothing to bound it: exactly the
    /// hazard [`crate::ReadConsistency::Local`] cannot detect on its own. It
    /// covers construction around an unnamed ID too, which is a legitimate
    /// starting point for a fresh joiner whose addition has not committed.
    ///
    /// **Writes and both read levels are refused; ticks and deliveries are
    /// not.** The replica must be able to catch up if the change is re-proposed,
    /// or if it is a joiner whose addition is still in flight, and it cannot do
    /// that without stepping.
    ///
    /// It clears by itself the moment a configuration names the ID again.
    NotMember { node_id: NodeId },
    /// The link layer has left more fence obligations outstanding than
    /// [`TransportDriverOptions::fence_backlog_service_threshold`] allows.
    ///
    /// The threshold does not cap the queue, and could not: a committed fact is
    /// not a request and cannot be refused, so discarding an obligation on
    /// overflow would be exactly the forgotten fence this control plane exists to
    /// prevent, with a capacity limit attached as an excuse. What it decides is
    /// when a driver whose link layer has stopped taking admission controls
    /// should stop taking client work: past it, this driver is authorizing
    /// replicas the cluster has removed, and adding writes to that is making the
    /// problem larger.
    ///
    /// It ends by itself. The driver keeps flushing while degraded, and returns
    /// to [`DriverServiceState::Serving`] as soon as the backlog is back under
    /// the threshold.
    FenceBacklog {
        pending_fences: usize,
        service_threshold: usize,
    },
    /// The driver released its group and has not adopted another.
    ///
    /// Reported rather than folded into `Serving`, which is what it used to be:
    /// every client operation was already refused in this state, and the one
    /// surface a supervisor polls to decide whether to route here said the
    /// replica was fine. Ends at [`TransportRaftDriver::adopt_group`].
    Released,
    /// [`crate::DriverCommandSender::shutdown`] has run, which is terminal.
    ///
    /// The driver refuses every operation including adoption; a supervisor that
    /// wants to serve again builds a driver. Outranks every other state, because
    /// nothing this driver could otherwise report changes what happens next.
    ShuttingDown,
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
        self.reconcile_membership();
        self.drain_poisoned_waiters();
        self.publish_metrics();
        result
    }

    /// Routes every membership fact the group has moved through and not yet
    /// handed back.
    ///
    /// **Runs after every stepping outcome, `Err` included, and that is the
    /// point.** A step that fails returns no report while the runtime has
    /// already appended, truncated, committed, or installed the configuration
    /// that moved, so a driver that routed only successful reports left the loss
    /// window open until some later successful step happened to arrive — and for
    /// a removal that later step is exactly what a stale peer set and an unmade
    /// fence prevent. `RaftGroup::drain_membership_events` is the app layer's
    /// error-path companion for that, and this is the driver's use of it.
    ///
    /// Empty after a successful step, because the report already carried the
    /// delta and the group advanced its own mark handing it over. So this costs
    /// one comparison on the path that works and closes the path that does not.
    ///
    /// The events are drained before any of them is routed, because routing
    /// reaches the transport and the group is borrowed to drain.
    pub(super) fn reconcile_membership(&mut self) {
        let events = {
            let Some(group) = self.group.as_mut() else {
                return;
            };
            group.drain_membership_events()
        };
        for event in &events {
            self.route_membership_event(event);
        }
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
        self.reconcile_membership();
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
        self.reconcile_membership();
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
                self.reconcile_membership();
                self.drain_poisoned_waiters();
                self.handle_read_outcome(read_id, read.outcome);
            }
            Err(error) => {
                // A read that starts a barrier steps the runtime, so it reaches
                // this driver's membership reconciliation like any other step.
                self.reconcile_membership();
                // The drain runs before the resolution so a barrier the group
                // handed over keeps the poison's own answer; `resolve_read`
                // keeps the first outcome either way, and both arms say
                // `Poisoned`.
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
