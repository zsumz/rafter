#![allow(clippy::wildcard_imports)]

//! Managed driver for one local Raft group over an attached transport.

use std::future::{poll_fn, ready};

use crate::transport::{
    validate_inbound_peer_envelope, AuthenticatedPeerEnvelope, AuthenticatedPeerValidator,
    RaftTransport,
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

/// A write this driver admitted, named and awaited separately.
///
/// Returned by [`TransportRaftDriver::begin_write`]. The ID is what
/// [`TransportRaftDriver::abandon_write`] takes; the future is what
/// [`DriverCommandSender::write`] would have returned alone.
pub type AddressedWrite<R> = (
    LocalProposalId,
    DriverFuture<Result<WriteReceipt<R>, WriteError>>,
);

/// A read barrier this driver reserved, named and awaited separately.
///
/// The read counterpart of [`AddressedWrite`], returned by
/// [`TransportRaftDriver::begin_read`].
pub type AddressedRead<G, QR> = (ReadId, DriverFuture<Result<QueryReceipt<G, QR>, ReadError>>);

mod adoption;
mod control_plane;
mod error;
mod state;
mod waiters;

pub use error::InboundEnvelopeError;

use adoption::{adopted_watermarks, highest, PendingProposals, WaiterGuard};
use state::{DriverShared, SharedState, StartedRead, StepFailure, TransportDriverState, WaiterId};

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
///
/// Reads are [`ReadConsistency::Linearizable`] and [`ReadConsistency::Local`],
/// which are the two levels [`rafter_app::group::RaftGroup::read`] implements.
/// Any other level is refused with [`ReadError::UnsupportedConsistency`] rather
/// than served at a weaker one — including [`ReadConsistency::LeaseRead`], which
/// is refused because the app layer refuses it and not because this driver chose
/// to.
///
/// A local read answers from this replica's own applied state. It submits no
/// read-index round, reserves no barrier, allocates no [`ReadId`], and its
/// [`QueryReceipt::proof`] is `None`, which is the honest report that it proved
/// nothing about any other replica. What bounds it is
/// [`ReadOptions::min_applied_index`], honored verbatim: a floor this replica
/// has not reached is reported as [`ReadError::FreshnessUnavailable`] carrying
/// both the required and the local applied index, rather than answered from
/// behind it. A caller that states no floor is asking for this replica's applied
/// state and gets it.
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
                refused_non_member_frames: 0,
                known_members: BTreeSet::new(),
                published_peers: None,
                pending_fences: BTreeSet::new(),
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
    /// This answers "what is this driver still holding", which is the question a
    /// supervisor draining one asks. It is not how a caller finds its *own*
    /// write: use [`TransportRaftDriver::begin_write`], which returns the ID it
    /// allocated. A caller that answered the second question with this one was
    /// taking the highest unresolved ID and relying on no other write being
    /// admitted in between, which holds only while nothing else uses a driver
    /// that is [`Sync`].
    #[must_use]
    pub fn pending_writes(&self) -> Vec<PendingWrite> {
        self.inner.lock().pending_writes()
    }

    /// Returns the read IDs of every barrier this driver has not resolved.
    ///
    /// The read counterpart of [`TransportRaftDriver::pending_writes`], and the
    /// same distinction applies: a caller looking for the barrier it just
    /// started wants [`TransportRaftDriver::begin_read`].
    #[must_use]
    pub fn pending_reads(&self) -> Vec<ReadId> {
        self.inner.lock().pending_reads()
    }

    /// Proposes `command` and returns the ID this driver allocated for it
    /// beside the future that resolves it.
    ///
    /// [`DriverCommandSender::write`] returns the future alone, so the only name
    /// for the waiter it created arrives when that future resolves — which is
    /// after the point a caller would have used it, because the one thing the
    /// name is for is [`TransportRaftDriver::abandon_write`].
    ///
    /// No group ID: this driver names one group for its whole life, so it
    /// supplies its own and cannot be handed the wrong one. The future is the
    /// one `write` returns, built by the same call, so the two cannot answer
    /// differently.
    ///
    /// # Errors
    ///
    /// As [`DriverCommandSender::write`], except that a refusal which allocated
    /// no ID is returned here rather than delivered through the future: there is
    /// no waiter to name, so there is no pair to return.
    pub fn begin_write(
        &self,
        command: A::Command,
        options: WriteOptions,
    ) -> Result<AddressedWrite<A::CommandResult>, WriteError> {
        // One acquisition: the shared body takes the group ID this driver was
        // built with, and reading it under the same lock that registers the
        // waiter leaves nothing to argue about. `write_future` takes no lock —
        // the guard is a handle and the poll closure is lazy — so the lock is
        // released before the pair is built.
        let local_proposal_id = {
            let mut state = self.inner.lock();
            let group_id = state.group_id.clone();
            state.begin_write(&group_id, command, options)?
        };
        Ok((local_proposal_id, self.write_future(local_proposal_id)))
    }

    /// Begins a linearizable read and returns the ID of the barrier it reserved
    /// beside the future that resolves it.
    ///
    /// The read counterpart of [`TransportRaftDriver::begin_write`], and
    /// linearizable-only for a reason a consistency parameter would hide: this
    /// exists to name a waiter so that [`TransportRaftDriver::abandon_read`] can
    /// retire it, and a [`ReadConsistency::Local`] read reserves no barrier,
    /// registers no waiter, and is answered inside the call that starts it.
    /// There would be no ID to return and nothing to abandon. Run one through
    /// [`DriverCommandSender::read`], which serves both levels.
    ///
    /// # Errors
    ///
    /// As [`DriverCommandSender::read`], except that a refusal which reserved no
    /// barrier is returned here rather than delivered through the future.
    pub fn begin_read(
        &self,
        query: A::Query,
        options: ReadOptions,
    ) -> Result<AddressedRead<G, A::QueryResult>, ReadError> {
        // One acquisition, for the reason [`TransportRaftDriver::begin_write`]
        // gives.
        let read_id = {
            let mut state = self.inner.lock();
            let group_id = state.group_id.clone();
            state.begin_linearizable_read(&group_id, query, options)?
        };
        Ok((read_id, self.barrier_future(read_id)))
    }

    /// Builds the client future for one registered write waiter.
    ///
    /// Shared by [`TransportRaftDriver::begin_write`] and
    /// [`DriverCommandSender::write`] so there is one polling path rather than
    /// two that could drift.
    fn write_future(
        &self,
        local_proposal_id: LocalProposalId,
    ) -> DriverFuture<Result<WriteReceipt<A::CommandResult>, WriteError>> {
        let mut guard = WaiterGuard::new(self.inner.clone(), WaiterId::Write(local_proposal_id));
        Box::pin(poll_fn(move |context| {
            let polled = guard.state().lock().poll_write(local_proposal_id, context);
            if polled.is_ready() {
                guard.release();
            }
            polled
        }))
    }

    /// Builds the client future for one reserved barrier, shared the same way
    /// [`TransportRaftDriver::write_future`] is.
    fn barrier_future(
        &self,
        read_id: ReadId,
    ) -> DriverFuture<Result<QueryReceipt<G, A::QueryResult>, ReadError>> {
        let mut guard = WaiterGuard::new(self.inner.clone(), WaiterId::Read(read_id));
        Box::pin(poll_fn(move |context| {
            let polled = guard.state().lock().poll_read(read_id, context);
            if polled.is_ready() {
                guard.release();
            }
            polled
        }))
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
        // Before the step, so a peer set or a fence the link layer refused
        // earlier is retried on the embedder's own timer rather than waiting for
        // the cluster's next configuration change — which may never come.
        state.flush_peer_control_plane();
        state.step(GroupInput::Tick)
    }

    /// Validates one inbound authenticated envelope and steps the group with
    /// it.
    ///
    /// Validation happens in two stages, and they answer different questions.
    /// [`validate_inbound_peer_envelope`] asks the *validator* whether the link
    /// layer authenticated this principal as this replica and whether that
    /// replica is authorized and unfenced. Then this driver asks itself whether
    /// its own group's membership names the sender at all. Both run before the
    /// group is touched, exactly where a production embedder refuses a frame,
    /// and the group never sees one that fails either.
    ///
    /// The second stage is the fail-closed half, and it exists because the first
    /// one can be out of date. [`crate::RaftTransport::fence_peer`] is the
    /// operation that stops later frames from a removed replica, and it is
    /// allowed to fail — so between the moment the cluster commits a removal and
    /// the moment the transport accepts the fence, the validator still
    /// authorizes a replica the cluster has retired. The driver knows better
    /// than its own link layer in that window: its membership is committed ∪
    /// effective, so it can refuse the frame itself rather than let a transient
    /// control-plane failure become an authorization.
    ///
    /// It cannot refuse a legitimate joiner. The membership it checks includes
    /// the effective configuration, so a replica added by a change that has
    /// appended and not committed is present and its frames are accepted — which
    /// it must be, or it can never catch up and the change can never commit.
    ///
    /// Rejection is not a driver failure: an unauthorized, fenced, or retired
    /// peer sending frames is an expected condition, and the caller decides
    /// whether to log it, count it, or drop the connection.
    ///
    /// # Errors
    ///
    /// Returns [`InboundEnvelopeError::Rejected`] when the validator refuses the
    /// frame, [`InboundEnvelopeError::NotInMembership`] when this driver's own
    /// membership does not name the sender — both leaving the group untouched —
    /// and [`InboundEnvelopeError::Driver`] when the group step itself fails.
    pub fn deliver(
        &self,
        envelope: AuthenticatedPeerEnvelope<G, T::PeerPrincipal>,
    ) -> Result<(), InboundEnvelopeError> {
        let mut state = self.inner.lock();
        state
            .reject_if_shutting_down()
            .map_err(|source| InboundEnvelopeError::Driver { source })?;
        // A delivery is an entry point like a tick, and a frame from a replica
        // whose fence is owed is the likeliest moment for the retry to matter.
        state.flush_peer_control_plane();
        let node_id = state.node_id;
        let envelope = validate_inbound_peer_envelope(envelope, node_id, &state.validator)
            .map_err(|source| InboundEnvelopeError::Rejected { source })?;
        if !state.is_member(envelope.from) {
            state.refused_non_member_frames = state.refused_non_member_frames.saturating_add(1);
            return Err(InboundEnvelopeError::NotInMembership {
                node_id: envelope.from,
            });
        }
        state
            .step(GroupInput::PeerMessage { envelope })
            .map_err(|source| InboundEnvelopeError::Driver { source })
    }

    /// Collects every barrier whose proof this driver has been told is ready.
    ///
    /// A grant is announced — `tick` and `deliver` route the group's read
    /// events — but the proof it announces is *consumed* by a read call, and
    /// that call runs the state machine, which this driver will not do inside a
    /// tick the embedder asked for on its own timer. So the third entry point
    /// stays: call it after each batch of deliveries and after each tick.
    ///
    /// It attempts exactly the barriers a routed `ReadEvent::Granted` named and
    /// leaves the rest alone. A barrier still waiting on its quorum round, or
    /// granted at an index this replica has not applied through, cannot answer
    /// differently until the group says so, and a read against a barrier the
    /// group already tracks returns an unstepped report — so attempting one
    /// anyway would spin. With nothing granted this is a no-op, and it is safe
    /// to call at any time.
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError::NoGroup`] when the driver has released its
    /// group, and nothing else. Every group error a read call can raise names
    /// the one barrier that call was for, so it resolves that barrier's client
    /// and the pass continues: one barrier's fault does not deny service to the
    /// rest.
    pub fn drive_pending_reads(&self) -> Result<(), ManagedDriverError> {
        let mut state = self.inner.lock();
        // The third entry point flushes like the other two. This is the one an
        // embedder calls after each batch of deliveries, so it is the supervisory
        // surface a driver whose link layer just recovered is most likely to
        // reach first.
        state.flush_peer_control_plane();
        state.drive_pending_reads()
    }

    /// Returns how many outbound sends the attached transport refused.
    ///
    /// Two producers, counted together: a peer frame [`crate::RaftTransport`]
    /// would not take, and a leader snapshot chunk directive it could not
    /// resolve and send. Neither is a failure. Raft tolerates drops and the
    /// protocol re-sends, and the kernel says the same of a chunk directive its
    /// source cannot serve — the transfer resumes from the follower's
    /// acknowledged offset — so the driver counts refusals rather than
    /// propagating them: a write must not fail because one heartbeat could not
    /// be delivered.
    ///
    /// That shared property is why they share a counter, and it is what
    /// separates both from [`TransportRaftDriver::refused_peer_updates`], which
    /// counts the one link-layer statement that does *not* repair itself.
    ///
    /// The count is how an operator tells a cut link from an idle cluster, and a
    /// driver that discarded it would leave nothing to tell them apart.
    #[must_use]
    pub fn refused_sends(&self) -> u64 {
        self.inner.lock().refused_sends
    }

    /// Returns how many peer-control-plane statements this driver could not
    /// install, cumulatively.
    ///
    /// Counted rather than propagated, for the reason a refused send is: a peer
    /// set that could not be updated is a link-layer condition, and a client's
    /// write must not fail for one. It is a separate count from
    /// [`TransportRaftDriver::refused_sends`] because the two do not repair the
    /// same way — Raft re-sends a dropped frame on its own, and a peer set or a
    /// fence is re-published only because this driver retries it.
    ///
    /// A non-zero value means either the transport refused a publication or a
    /// fence, or this driver's validator could not name a principal for some
    /// replica. It counts *attempts* and therefore rises on every retry, which
    /// makes it a history rather than a health check: a driver that has refused
    /// nine times and succeeded on the tenth reads the same as one still
    /// failing. Read [`TransportRaftDriver::pending_peer_fences`] and
    /// [`TransportRaftDriver::peer_set_is_stale`] for the current state; read
    /// this to tell a link that has always worked from one that recovered.
    #[must_use]
    pub fn refused_peer_updates(&self) -> u64 {
        self.inner.lock().refused_peer_updates
    }

    /// Returns how many committed removals this driver has not managed to fence
    /// yet.
    ///
    /// Current state rather than history, and the distinction is the point.
    /// [`crate::RaftTransport::fence_peer`] is what stops later frames from a
    /// replica the cluster has removed, and it is allowed to fail — so a
    /// non-zero value here means that, right now, this driver owes the link
    /// layer an admission control it has not accepted. It falls to zero when
    /// every outstanding fence has been accepted, and it is retried at every
    /// entry point of the driver.
    ///
    /// This is the number to alert on. Inbound frames from an unfenced removed
    /// replica are refused locally in the meantime — see
    /// [`InboundEnvelopeError::NotInMembership`] — so the window is degraded
    /// rather than unsafe, but it is a window that does not close by itself if
    /// the link layer stays refusing.
    #[must_use]
    pub fn pending_peer_fences(&self) -> usize {
        self.inner.lock().pending_fences.len()
    }

    /// Returns whether the transport's peer set is behind the group's
    /// membership.
    ///
    /// True when the set this driver would publish differs from the last one the
    /// transport accepted — because a publication was refused, or because the
    /// validator could not name every replica in it, and no retry has succeeded
    /// since. It is `true` before the first accepted publication for the same
    /// reason: nothing has been accepted, so nothing is level.
    ///
    /// The peer-set counterpart of
    /// [`TransportRaftDriver::pending_peer_fences`], and the milder of the two.
    /// A stale peer set means the link layer authorizes a set the cluster has
    /// moved on from, which is a liveness and least-privilege concern; an
    /// unfenced removal is an authorization the cluster explicitly retracted.
    #[must_use]
    pub fn peer_set_is_stale(&self) -> bool {
        self.inner.lock().peer_set_is_stale()
    }

    /// Returns how many inbound frames this driver refused because its group's
    /// membership does not name the sender.
    ///
    /// The observable half of the fail-closed inbound check
    /// ([`InboundEnvelopeError::NotInMembership`]). A non-zero value means the
    /// link layer and the group disagree about who may speak: the validator
    /// authorized a replica this driver's membership has retired. Read beside
    /// [`TransportRaftDriver::pending_peer_fences`], it separates the two causes
    /// — a fence this driver still owes, or a validator that authorizes a
    /// replica no fence was ever licensed for.
    #[must_use]
    pub fn refused_non_member_frames(&self) -> u64 {
        self.inner.lock().refused_non_member_frames
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
    /// through the group first so the retired group is quiescent *in reads*.
    /// It is deliberately not quiescent in proposals — the appended entry stays
    /// in the group's table, which is what
    /// [`TransportRaftDriver::adopt_group`] accepts and
    /// [`TransportRaftDriver::new`] does not.
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
    /// The new group must serve the group ID this driver was built with. A
    /// driver names one group for its whole life — its handles and metrics
    /// watch were issued against that ID and adoption does not reissue them —
    /// so a group with a different ID is refused rather than adopted under the
    /// wrong name. The node ID may change, because a replacement incarnation is
    /// still a replica of the same group.
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
    /// driver still holds a group, [`ManagedDriverError::MixedGroups`] when the
    /// group serves a different group ID than this driver, the same validation
    /// errors as [`TransportRaftDriver::new`], and a group error when the
    /// recovery outputs fail to apply.
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
        // The driver's group ID is fixed at construction and adoption does not
        // republish it: handles, the metrics watch, and every client-facing
        // group check keep comparing against the original. A group serving a
        // different ID would be driven under this driver's ID, and nothing
        // downstream would catch it — `GroupInput::Proposal` carries no group
        // ID, so a client write addressed to this driver would be proposed into
        // the foreign group's log and answered with a real index and term.
        // `InMemoryRaftDriver::new` refuses the same mismatch with the same
        // error.
        if group.group_id() != &state.group_id {
            return Err(ManagedDriverError::MixedGroups);
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
        // Registered synchronously, polled later: the waiter exists before the
        // group is stepped, so a terminal event emitted inside that very step
        // resolves it rather than arriving before anything is listening.
        let started = self.inner.lock().begin_write(&group_id, command, options);
        match started {
            Ok(local_proposal_id) => self.write_future(local_proposal_id),
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
        let started = self
            .inner
            .lock()
            .begin_read(&group_id, query, consistency, options);
        match started {
            Ok(StartedRead::Barrier(read_id)) => self.barrier_future(read_id),
            // A local read is already finished. No waiter was registered, so
            // there is no guard to hold and nothing to poll.
            Ok(StartedRead::Answered(answered)) => Box::pin(ready(answered)),
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
