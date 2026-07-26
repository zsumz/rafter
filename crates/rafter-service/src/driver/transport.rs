#![allow(clippy::wildcard_imports)]

//! Managed driver for one local Raft group over an attached transport.

use std::{
    error::Error,
    future::{poll_fn, ready},
};

use crate::transport::{
    validate_inbound_peer_envelope, AuthenticatedPeerEnvelope, AuthenticatedPeerEnvelopeError,
    AuthenticatedPeerValidator, RaftTransport,
};

use super::*;

/// Bounds on one driver's local work.
///
/// Every bound is a refusal rather than an unbounded wait, so a stalled
/// protocol surfaces as a typed error instead of a hang.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct TransportDriverOptions {
    /// Refuses to enqueue more than this many unresolved client waiters of
    /// each kind, so a driver whose transport is down fails closed rather than
    /// growing.
    ///
    /// Defaults to 1024. The transport contract already requires bounded
    /// queues of a transport; a driver that buffered without a bound would
    /// move the unbounded growth one layer up.
    ///
    /// A waiter stops counting the moment it resolves, including when
    /// [`TransportRaftDriver::abandon_write`] or
    /// [`TransportRaftDriver::abandon_read`] resolves it, so a caller that stops
    /// waiting gets its slot back without waiting for the client to poll.
    pub max_pending_waiters: usize,
}

impl TransportDriverOptions {
    /// Returns the shipped defaults.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_pending_waiters: 1024,
        }
    }

    /// Sets [`TransportDriverOptions::max_pending_waiters`].
    ///
    /// A setter rather than struct-update syntax, because the type is
    /// `#[non_exhaustive]`: an embedder outside this crate cannot name every
    /// field, and a later field must not break their construction.
    #[must_use]
    pub const fn with_max_pending_waiters(mut self, max_pending_waiters: usize) -> Self {
        self.max_pending_waiters = max_pending_waiters;
        self
    }

    /// Fails closed on a bound that would make an operation impossible.
    ///
    /// Zero is meaningless rather than merely small: a driver that admits no
    /// waiters refuses every write, which is a driver that cannot serve
    /// anything, discovered at the first request.
    fn validate(self) -> Result<Self, ManagedDriverError> {
        if self.max_pending_waiters == 0 {
            return Err(ManagedDriverError::InvalidOptions {
                field: "max_pending_waiters",
                reason: "a driver that admits no waiters cannot serve any operation",
            });
        }
        Ok(self)
    }
}

impl Default for TransportDriverOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// One unresolved write a driver is still holding.
///
/// Named both ways a caller can name it, because the two IDs answer different
/// questions: the driver's own ID is what
/// [`TransportRaftDriver::abandon_write`] takes, and the caller's is how a
/// caller with several writes in flight tells them apart.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct PendingWrite {
    /// The ID this driver allocated for the proposal.
    pub local_proposal_id: LocalProposalId,
    /// The ID the caller supplied in [`WriteOptions`], if any.
    pub client_request_id: Option<ClientRequestId>,
}

/// Why an inbound peer envelope did not reach a group.
#[derive(Debug)]
#[non_exhaustive]
pub enum InboundEnvelopeError {
    /// The envelope failed inbound validation and was dropped. The group was
    /// not stepped and no state changed.
    Rejected {
        source: AuthenticatedPeerEnvelopeError,
    },
    /// The group step failed after the envelope was accepted.
    Driver { source: ManagedDriverError },
}

impl fmt::Display for InboundEnvelopeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected { .. } => {
                formatter.write_str("inbound peer envelope failed validation and was dropped")
            }
            Self::Driver { .. } => {
                formatter.write_str("the group step failed after the envelope was accepted")
            }
        }
    }
}

impl Error for InboundEnvelopeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Rejected { source } => Some(source),
            Self::Driver { source } => Some(source),
        }
    }
}

mod state;
mod waiters;

use state::{DriverShared, SharedState, StepFailure, TransportDriverState, WaiterId};

/// Managed driver for one local Raft group over an attached transport.
///
/// This is the driver an embedder writes when frames leave the process.
/// [`InMemoryRaftDriver`] owns every replica of a group and moves frames
/// between them itself, which makes it a complete cluster and an unusable
/// node; this driver owns exactly one replica, hands its outbound frames to a
/// [`RaftTransport`], and receives inbound frames from whatever loop the
/// embedder runs. Rafter opens no sockets and spawns no tasks: the embedder
/// calls [`TransportRaftDriver::tick`] and [`TransportRaftDriver::deliver`],
/// and this type owns everything between those calls and a resolved client
/// future.
///
/// Cloning shares the driver. Handles obtained from
/// [`TransportRaftDriver::handle`] stay valid across a group release and
/// re-adoption, because a handle names a service rather than a node
/// incarnation.
pub struct TransportRaftDriver<G, A, R, T, V>
where
    A: ReplicatedStateMachine,
{
    inner: SharedState<G, A, R, T, V>,
}

impl<G, A, R, T, V> Clone for TransportRaftDriver<G, A, R, T, V>
where
    A: ReplicatedStateMachine,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<G, A, R, T, V> Debug for TransportRaftDriver<G, A, R, T, V>
where
    A: ReplicatedStateMachine,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TransportRaftDriver")
            .finish_non_exhaustive()
    }
}

impl<G, A, R, T, V> TransportRaftDriver<G, A, R, T, V>
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
    /// Builds a driver over one already-configured group and routes its
    /// recovery outputs.
    ///
    /// The group must be quiescent — no pending proposals and no reserved
    /// reads — for the same reason [`InMemoryRaftDriver::new`] requires it: the
    /// driver correlates outcomes to waiters it created, and a waiter it did
    /// not create can never be resolved. Generated IDs start above the group's
    /// adopted watermarks.
    ///
    /// `recovery_outputs` are the outputs the recovered runtime released, taken
    /// here for the reason [`TransportRaftDriver::adopt_group`] takes them: a
    /// recovery report carries peer messages and snapshot directives that must
    /// be routed, and a caller that applied them outside the driver would drop
    /// exactly the effects a restart depends on. A first incarnation over empty
    /// storage recovers nothing and passes an empty vector.
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError`] when the group is poisoned, holds
    /// undrained poisoned waiters, is not quiescent, has exhausted a local
    /// ID space, the options are out of range, or the recovery outputs fail to
    /// apply.
    pub fn new(
        group: RaftGroup<G, A, R>,
        recovery_outputs: Vec<RaftOutput>,
        transport: T,
        validator: V,
        options: TransportDriverOptions,
    ) -> Result<Self, ManagedDriverError> {
        let options = options.validate()?;
        let group_id = group.group_id().clone();
        let node_id = group.node_id();
        let (next_proposal_id, next_read_id) =
            adopted_watermarks(&group, PendingProposals::Refuse)?;
        let metrics = MetricsPublisher::new(group.metrics());
        let driver = Self {
            inner: Arc::new(DriverShared::new(TransportDriverState {
                group_id,
                node_id,
                group: Some(group),
                transport,
                validator,
                options,
                metrics,
                next_proposal_id,
                next_read_id,
                write_waiters: BTreeMap::new(),
                read_waiters: BTreeMap::new(),
                refused_sends: 0,
                refused_peer_updates: 0,
                known_members: BTreeSet::new(),
                shutting_down: false,
            })),
        };
        // Published before the driver serves anything, so the transport's peer
        // set is the group's membership from construction onward rather than
        // undefined until the first membership change. A group that never
        // changes membership would otherwise never tell its link layer anything,
        // and a recovery report carries no membership event to stand in.
        driver.inner.lock().publish_adopted_membership();
        if !recovery_outputs.is_empty() {
            driver
                .inner
                .lock()
                .apply_recovery_outputs(recovery_outputs)?;
        }
        Ok(driver)
    }

    /// Reads the adopted group under this driver's own lock.
    ///
    /// The closure receives a shared borrow for its own duration and nothing
    /// outlives the call: no guard, no owned escape, no way to keep the group
    /// after the lock is released. `&RaftGroup` rather than `&mut` is the whole
    /// policy — this driver correlates outcomes to waiters it created, and a
    /// group stepped, read, or cancelled from outside would break that
    /// correspondence silently.
    ///
    /// This is how an embedder observes a *running* replica.
    /// [`TransportRaftDriver::release_group`] also hands the group back, but it
    /// resolves every outstanding waiter to do so, which makes it a way to
    /// retire a replica rather than a way to look at one.
    ///
    /// The closure runs with the driver locked, so it must not *call* back into
    /// this driver: the lock is not reentrant, and a second acquisition on the
    /// same thread stops it. That includes polling a client future, which is a
    /// call like any other.
    ///
    /// **Dropping a value is not calling in.** A client future of either kind,
    /// resolved or not, may be dropped inside the closure. Dropping one
    /// reclaims its waiter, and reclamation never waits for this lock — it
    /// leaves the waiter for the next acquisition instead. The same guarantee
    /// holds wherever else this driver runs an embedder's code under its lock: a
    /// [`crate::RaftTransport`] call, a
    /// [`rafter_app::state_machine::ReplicatedStateMachine`] apply or read, and
    /// a `Waker` woken while a waiter resolves.
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError::NoGroup`] when the driver has released its
    /// group.
    pub fn with_group<U>(
        &self,
        read: impl FnOnce(&RaftGroup<G, A, R>) -> U,
    ) -> Result<U, ManagedDriverError> {
        let state = self.inner.lock();
        let group = state.group.as_ref().ok_or(ManagedDriverError::NoGroup)?;
        Ok(read(group))
    }

    /// Returns the index this replica's state machine must reach to have
    /// applied every application command it knows to be committed.
    ///
    /// A direct forwarder because this one is the readiness gate the
    /// decomposition recipe is written around, and because it takes no argument
    /// and returns a scalar, so [`TransportRaftDriver::with_group`] would be
    /// pure ceremony around it. Reads that project out of the state machine keep
    /// using the closure, which is what they need anyway.
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError::NoGroup`] when the driver has released its
    /// group.
    pub fn committed_application_index(&self) -> Result<LogIndex, ManagedDriverError> {
        self.with_group(RaftGroup::committed_application_index)
    }

    /// Stops waiting for one write and resolves its client.
    ///
    /// This is the caller's own decision, not the cluster's, so the client
    /// resolves as [`WriteError::UnknownOutcome`] with
    /// [`UnknownOutcomeReason::DriveBoundReached`]: the proposal may already be
    /// in the durable log and may still commit, and there is no
    /// `cancel_proposal` that could make it otherwise. The waiter stops counting
    /// against [`TransportDriverOptions::max_pending_waiters`] immediately.
    ///
    /// Abandonment is terminal for the client. A later `Applied`, `Rejected`, or
    /// `UnknownOutcome` event for this proposal resolves nothing and changes
    /// nothing, which is the correct direction: the client already holds a
    /// terminal answer, and on this side that answer is *unknown*, which is
    /// exactly the statement that the proposal may still commit. A caller that
    /// wants the eventual fact keeps its future and does not call this.
    ///
    /// Returns whether a waiter was retired. Abandoning a write this driver no
    /// longer holds, one that has already resolved, or one whose client future
    /// was dropped — which reclaims the waiter — is a no-op rather than an
    /// error: a caller racing its own completion is not a fault, and abandonment
    /// resolves a client, so there is nothing to do for one that left.
    #[must_use]
    pub fn abandon_write(&self, local_proposal_id: LocalProposalId) -> bool {
        self.inner.lock().abandon_write(local_proposal_id)
    }

    /// Stops waiting for one read and resolves its client.
    ///
    /// The barrier is cancelled through
    /// [`rafter_app::group::RaftGroup::cancel_read`] first, so the group's
    /// `reserved_reads` returns to its previous value, and the client resolves
    /// as [`ReadError::Abandoned`] with
    /// [`ReadAbandonReason::DriveBoundReached`]. The `ReadId` is spent: a retry
    /// issues a new read.
    ///
    /// Returns whether a waiter was retired. A read whose client future was
    /// already dropped has none: dropping the future cancels the barrier and
    /// reclaims the waiter on its own.
    #[must_use]
    pub fn abandon_read(&self, read_id: ReadId) -> bool {
        self.inner.lock().abandon_read(read_id)
    }

    /// Returns every write this driver has not resolved.
    ///
    /// [`DriverCommandSender::write`] returns a future and nothing else, so the
    /// ID it allocated is otherwise unreachable until the future resolves —
    /// which is too late for a caller that wants to stop waiting. This answers
    /// "what is this driver still holding", which is also the question a
    /// supervisor draining one asks.
    #[must_use]
    pub fn pending_writes(&self) -> Vec<PendingWrite> {
        self.inner.lock().pending_writes()
    }

    /// Returns the read IDs of every barrier this driver has not resolved.
    #[must_use]
    pub fn pending_reads(&self) -> Vec<ReadId> {
        self.inner.lock().pending_reads()
    }

    /// Returns a cloneable handle connected to this driver.
    #[must_use]
    pub fn handle(
        &self,
    ) -> RaftHandle<G, A::Command, A::Query, A::CommandResult, A::QueryResult, Self> {
        let group_id = self.inner.lock().group_id.clone();
        RaftHandle::new(group_id, self.clone())
    }

    /// Steps the group with a tick and routes everything the step produced.
    ///
    /// This is one of the two entry points that advance the protocol. Call it
    /// on the embedder's own timer; the app layer's election and heartbeat
    /// timing is measured in ticks, not in wall time, so the tick interval is
    /// the embedder's policy and Rafter does not choose it.
    ///
    /// The step's report is routed before this returns: peer messages go to
    /// the transport, proposal and read events resolve waiters, and the metrics
    /// snapshot is published. A terminal event resolves its waiter whichever
    /// step observed it, which is why a client future can complete inside a
    /// tick it has no other relationship to.
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError`] when the driver has released its group,
    /// is shutting down, or the group step fails.
    pub fn tick(&self) -> Result<(), ManagedDriverError> {
        let mut state = self.inner.lock();
        state.reject_if_shutting_down()?;
        state.step(GroupInput::Tick)
    }

    /// Validates one inbound authenticated envelope and steps the group with
    /// it.
    ///
    /// Validation is [`validate_inbound_peer_envelope`] against this driver's
    /// validator, and it happens before the group is touched. A frame that
    /// fails it is refused here, exactly where a production embedder refuses
    /// it, and the group never sees it. Rejection is not a driver failure: an
    /// unauthorized or fenced peer sending frames is an expected condition, and
    /// the caller decides whether to log it, count it, or drop the connection.
    ///
    /// # Errors
    ///
    /// Returns [`InboundEnvelopeError::Rejected`] when validation refuses the
    /// frame, leaving the group untouched, and [`InboundEnvelopeError::Driver`]
    /// when the group step itself fails.
    pub fn deliver(
        &self,
        envelope: AuthenticatedPeerEnvelope<G, T::PeerPrincipal>,
    ) -> Result<(), InboundEnvelopeError> {
        let mut state = self.inner.lock();
        state
            .reject_if_shutting_down()
            .map_err(|source| InboundEnvelopeError::Driver { source })?;
        let node_id = state.node_id;
        let envelope = validate_inbound_peer_envelope(envelope, node_id, &state.validator)
            .map_err(|source| InboundEnvelopeError::Rejected { source })?;
        state
            .step(GroupInput::PeerMessage { envelope })
            .map_err(|source| InboundEnvelopeError::Driver { source })
    }

    /// Retries every unresolved read barrier.
    ///
    /// A granted barrier is consumed by a later read call rather than
    /// announced by an event, so a driver that only ticks and delivers leaves
    /// granted proofs uncollected. Call this after each batch of deliveries and
    /// after each tick. It is a no-op when no barrier is outstanding, and it is
    /// safe to call at any time: the app layer's contract for a pending helper
    /// read is to retry with the same read ID, freshness requirement, and
    /// context until it resolves.
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError`] when the driver has released its group or
    /// a read step fails for a reason that is not attributable to one barrier.
    pub fn drive_pending_reads(&self) -> Result<(), ManagedDriverError> {
        let mut state = self.inner.lock();
        state.drive_pending_reads()
    }

    /// Returns how many outbound frames the attached transport refused.
    ///
    /// A refused frame is not a failure. Raft tolerates drops and the protocol
    /// re-sends, so the driver counts refusals rather than propagating them —
    /// a write must not fail because one heartbeat could not be delivered. The
    /// count is how an operator tells a cut link from an idle cluster, and a
    /// driver that discarded it would leave nothing to tell them apart.
    #[must_use]
    pub fn refused_sends(&self) -> u64 {
        self.inner.lock().refused_sends
    }

    /// Returns how many membership updates this driver could not publish.
    ///
    /// Counted rather than propagated, for the reason a refused send is: a peer
    /// set that could not be updated is a link-layer condition, and a client's
    /// write must not fail for one. It is a separate count from
    /// [`TransportRaftDriver::refused_sends`] because the two do not repair the
    /// same way — Raft re-sends a dropped frame, and nothing re-publishes a peer
    /// set until the membership changes again.
    ///
    /// A non-zero value means either the transport refused the update or this
    /// driver's validator could not name a principal for some replica in the
    /// membership. Both leave the link layer's peer set behind the group's.
    #[must_use]
    pub fn refused_peer_updates(&self) -> u64 {
        self.inner.lock().refused_peer_updates
    }

    /// Retires the running incarnation and returns its group.
    ///
    /// This is the driver-level half of decomposition.
    /// [`rafter_app::group::RaftGroup::into_parts`] consumes the group it
    /// retires, and a driver's group lives behind the lock its cloned handles
    /// share, which nothing can move out of. The driver owns the movable slot
    /// so an embedder does not have to build one.
    ///
    /// Every outstanding waiter resolves before this returns. Writes resolve as
    /// [`WriteError::UnknownOutcome`] with
    /// [`UnknownOutcomeReason::DriverReleased`], because a proposal already
    /// appended may still commit and apply under the next incarnation. Reads
    /// resolve as [`ReadError::Abandoned`] with
    /// [`ReadAbandonReason::DriverReleased`], and their barriers are cancelled
    /// through the group first so the retired group is quiescent.
    ///
    /// The driver refuses every operation until
    /// [`TransportRaftDriver::adopt_group`] installs a new incarnation. It does
    /// not close the transport: the same link serves the next incarnation, and
    /// closing it is the embedder's decision.
    ///
    /// The metrics watch stays open across the gap, because a handle names a
    /// service rather than an incarnation — but its last snapshot describes the
    /// retired one and nothing refreshes it until `adopt_group` publishes the
    /// next. Closing the watch would break re-adoption, and there is no metrics
    /// snapshot to publish for a group that does not exist, so the surface that
    /// tells a released driver from an idle one is
    /// [`TransportRaftDriver::with_group`] and
    /// [`TransportRaftDriver::committed_application_index`], which answer
    /// [`ManagedDriverError::NoGroup`].
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError::NoGroup`] when the driver has already
    /// released its group.
    pub fn release_group(&self) -> Result<RaftGroup<G, A, R>, ManagedDriverError> {
        let mut state = self.inner.lock();
        if state.group.is_none() {
            return Err(ManagedDriverError::NoGroup);
        }
        state.release_waiters();
        state.group.take().ok_or(ManagedDriverError::NoGroup)
    }

    /// Installs a new incarnation and routes its recovery outputs.
    ///
    /// `recovery_outputs` are the outputs the recovered runtime released, and
    /// the driver applies them itself rather than accepting an already-applied
    /// group. That is deliberate: the recovery report carries peer messages and
    /// snapshot directives that must be routed, and a caller that applied them
    /// outside the driver would drop exactly the effects a restart depends on.
    ///
    /// The new group must hold no reserved reads, and its local ID watermarks
    /// must be at or above the retired incarnation's when the two share a
    /// runtime; see [`rafter_app::group::RaftGroupParts`]. A driver that
    /// rebuilt its runtime from durable storage may restart its IDs at zero.
    ///
    /// Unlike [`TransportRaftDriver::new`], this accepts a group that still
    /// tracks appended proposals, because that is precisely what a released
    /// group carries: the entry is in the durable log and may commit under this
    /// incarnation. Its client already received
    /// [`UnknownOutcomeReason::DriverReleased`], so the later `Applied` event
    /// resolves no waiter — which is the correct outcome and not a lost one.
    /// A group whose waiters were never resolved must not be adopted here; use
    /// [`TransportRaftDriver::new`], which refuses it.
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError::ShuttingDown`] when the driver has shut
    /// down — that is terminal, and a supervisor that wants to serve again
    /// builds a driver — [`ManagedDriverError::GroupAlreadyAdopted`] when the
    /// driver still holds a group, the same validation errors as
    /// [`TransportRaftDriver::new`], and a group error when the recovery
    /// outputs fail to apply.
    pub fn adopt_group(
        &self,
        group: RaftGroup<G, A, R>,
        recovery_outputs: Vec<RaftOutput>,
    ) -> Result<(), ManagedDriverError> {
        let mut state = self.inner.lock();
        // Shutdown is terminal, and `shutdown` itself says so by refusing a
        // second call. A driver that could be re-armed by adopting a group would
        // make the entry's own distinction — a supervisor restarting a replica
        // releases, a supervisor stopping one shuts down and then releases —
        // a distinction with no consequence.
        state.reject_if_shutting_down()?;
        if state.group.is_some() {
            return Err(ManagedDriverError::GroupAlreadyAdopted);
        }
        let (next_proposal_id, next_read_id) = adopted_watermarks(&group, PendingProposals::Carry)?;
        state.node_id = group.node_id();
        state.next_proposal_id = highest(state.next_proposal_id, next_proposal_id);
        state.next_read_id = highest(state.next_read_id, next_read_id);
        state.group = Some(group);
        state.publish_adopted_membership();
        if recovery_outputs.is_empty() {
            state.publish_metrics();
            return Ok(());
        }
        state.apply_recovery_outputs(recovery_outputs)
    }
}

/// Reclaims one waiter when its client future is dropped.
///
/// Every client future owns one of these. A future polled to completion
/// releases it, because `poll_write` and `poll_read` already removed the entry
/// they answered from; a future dropped before that reclaims the entry itself,
/// which is what keeps the tables bounded for a driver whose clients time out.
///
/// The guard is the only remover other than a completing poll. Abandonment
/// deliberately resolves without removing, so an abandoned waiter still answers
/// a late poll from a future its caller kept.
///
/// Reclamation goes through [`DriverShared::reclaim`] rather than taking the
/// driver's lock here, and that indirection is the whole point of it: a future
/// may be dropped by code this driver is running under its own lock — an
/// embedder's transport, state machine, or waker, or a
/// [`TransportRaftDriver::with_group`] closure — and a `Drop` that waited for
/// that lock would stop the thread it ran on.
struct WaiterGuard<G, A, R, T, V>
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
    inner: SharedState<G, A, R, T, V>,
    waiter: Option<WaiterId>,
}

impl<G, A, R, T, V> WaiterGuard<G, A, R, T, V>
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
    fn new(inner: SharedState<G, A, R, T, V>, waiter: WaiterId) -> Self {
        Self {
            inner,
            waiter: Some(waiter),
        }
    }

    fn state(&self) -> &SharedState<G, A, R, T, V> {
        &self.inner
    }

    /// Marks the waiter as already consumed by a completed poll.
    fn release(&mut self) {
        self.waiter = None;
    }
}

impl<G, A, R, T, V> Drop for WaiterGuard<G, A, R, T, V>
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
    fn drop(&mut self) {
        let Some(waiter) = self.waiter.take() else {
            return;
        };
        self.inner.reclaim(waiter);
    }
}

fn highest(current: Option<u64>, adopted: Option<u64>) -> Option<u64> {
    match (current, adopted) {
        (Some(current), Some(adopted)) => Some(current.max(adopted)),
        (value, None) | (None, value) => value,
    }
}

/// Whether appended proposals may travel into a driver with the group.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingProposals {
    /// A group from outside: a waiter this driver did not create can never be
    /// resolved, so pending proposals are refused.
    Refuse,
    /// A group this driver released: it already resolved those waiters as
    /// unknown outcomes, and the entries themselves are durable.
    Carry,
}

/// Validates one group for adoption and returns the ID floors above it.
fn adopted_watermarks<G, A, R>(
    group: &RaftGroup<G, A, R>,
    pending_proposals: PendingProposals,
) -> Result<(Option<u64>, Option<u64>), ManagedDriverError>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine,
    A::CommandResult: Clone,
    R: PersistedRaftRuntime,
{
    let node_id = group.node_id();
    match group.fatal_state() {
        GroupFatalState::Poisoned { reason } => {
            return Err(ManagedDriverError::PoisonedGroup {
                node_id,
                reason: reason.clone(),
            });
        }
        GroupFatalState::Healthy if !group.poisoned_waiters().is_empty() => {
            return Err(ManagedDriverError::PoisonedGroup {
                node_id,
                reason: "group has undrained poisoned waiters".to_owned(),
            });
        }
        GroupFatalState::Healthy => {}
    }
    let metrics = group.metrics();
    let refused_proposals =
        pending_proposals == PendingProposals::Refuse && metrics.pending_proposals != 0;
    if refused_proposals || metrics.reserved_reads != 0 {
        return Err(ManagedDriverError::NonQuiescentGroup {
            node_id,
            pending_proposals: metrics.pending_proposals,
            reserved_reads: metrics.reserved_reads,
        });
    }
    let next_proposal_id = match group.local_proposal_id_watermark() {
        Some(last_seen_local_proposal_id) => {
            Some(last_seen_local_proposal_id.0.checked_add(1).ok_or(
                ManagedDriverError::LocalProposalIdExhausted {
                    node_id,
                    last_seen_local_proposal_id,
                },
            )?)
        }
        None => Some(1),
    };
    let next_read_id = match group.read_id_watermark() {
        Some(last_seen_read_id) => Some(last_seen_read_id.0.checked_add(1).ok_or(
            ManagedDriverError::ReadIdExhausted {
                node_id,
                last_seen_read_id,
            },
        )?),
        None => Some(1),
    };
    Ok((next_proposal_id, next_read_id))
}

impl<G, A, R, T, V> DriverCommandSender<G, A::Command, A::Query, A::CommandResult, A::QueryResult>
    for TransportRaftDriver<G, A, R, T, V>
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
    fn write(
        &self,
        group_id: G,
        command: A::Command,
        options: WriteOptions,
    ) -> DriverFuture<Result<WriteReceipt<A::CommandResult>, WriteError>> {
        let inner = self.inner.clone();
        // Registered synchronously, polled later: the waiter exists before the
        // group is stepped, so a terminal event emitted inside that very step
        // resolves it rather than arriving before anything is listening.
        let started = inner.lock().begin_write(&group_id, command, options);
        match started {
            Ok(local_proposal_id) => {
                let mut guard = WaiterGuard::new(inner, WaiterId::Write(local_proposal_id));
                Box::pin(poll_fn(move |context| {
                    let polled = guard.state().lock().poll_write(local_proposal_id, context);
                    if polled.is_ready() {
                        guard.release();
                    }
                    polled
                }))
            }
            Err(error) => Box::pin(ready(Err(error))),
        }
    }

    fn read(
        &self,
        group_id: G,
        query: A::Query,
        consistency: ReadConsistency,
        options: ReadOptions,
    ) -> DriverFuture<Result<QueryReceipt<G, A::QueryResult>, ReadError>> {
        let inner = self.inner.clone();
        let started = inner
            .lock()
            .begin_read(&group_id, query, consistency, options);
        match started {
            Ok(read_id) => {
                let mut guard = WaiterGuard::new(inner, WaiterId::Read(read_id));
                Box::pin(poll_fn(move |context| {
                    let polled = guard.state().lock().poll_read(read_id, context);
                    if polled.is_ready() {
                        guard.release();
                    }
                    polled
                }))
            }
            Err(error) => Box::pin(ready(Err(error))),
        }
    }

    fn transfer_leadership(
        &self,
        group_id: G,
        target: NodeId,
    ) -> DriverFuture<Result<(), TransferLeadershipError>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            let mut state = inner.lock();
            if state.shutting_down {
                return Err(TransferLeadershipError::ShuttingDown);
            }
            if group_id != state.group_id {
                return Err(TransferLeadershipError::WrongGroup);
            }
            // Through the state's own stepping path, not around it: a transfer
            // step can commit, apply, and poison, and the drain that resolves
            // what a poison captured runs there on both paths.
            let rejection = state
                .step_transfer(target)
                .map_err(|failure| match failure {
                    StepFailure::NoGroup => TransferLeadershipError::Transport {
                        cause: ErrorCause::new(ManagedDriverError::NoGroup),
                    },
                    StepFailure::Group(error) => transfer_error_from_group(error),
                })?;
            rejection.map_or(Ok(()), Err)
        })
    }

    fn metrics(&self, group_id: G) -> Result<MetricsWatch<G>, MetricsError> {
        let state = self.inner.lock();
        if group_id != state.group_id {
            return Err(MetricsError::WrongGroup);
        }
        Ok(state.metrics.watch())
    }

    fn shutdown(&self, group_id: G) -> DriverFuture<Result<(), ShutdownError>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            let mut state = inner.lock();
            if group_id != state.group_id {
                return Err(ShutdownError::WrongGroup);
            }
            if state.shutting_down {
                return Err(ShutdownError::AlreadyShutDown);
            }
            state.shutting_down = true;
            state.release_waiters();
            state.metrics.close();
            Ok(())
        })
    }
}
