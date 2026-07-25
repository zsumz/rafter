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
    /// Retries a pending read barrier at most this many times within one
    /// [`TransportRaftDriver::drive_pending_reads`] call before leaving it
    /// pending for the next one.
    ///
    /// Defaults to 1024, matching [`InMemoryRaftDriver`]'s own drive bound:
    /// both count local steps taken on behalf of one operation, and a driver
    /// that allowed more of them would spin rather than hand control back.
    pub max_read_retries: usize,
    /// Refuses to enqueue more than this many unresolved client waiters of
    /// each kind, so a driver whose transport is down fails closed rather than
    /// growing.
    ///
    /// Defaults to 1024. The transport contract already requires bounded
    /// queues of a transport; a driver that buffered without a bound would
    /// move the unbounded growth one layer up.
    pub max_pending_waiters: usize,
}

impl TransportDriverOptions {
    /// Returns the shipped defaults.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_read_retries: 1024,
            max_pending_waiters: 1024,
        }
    }

    /// Sets [`TransportDriverOptions::max_read_retries`].
    ///
    /// A setter rather than struct-update syntax, because the type is
    /// `#[non_exhaustive]`: an embedder outside this crate cannot name every
    /// field, and a later field must not break their construction.
    #[must_use]
    pub const fn with_max_read_retries(mut self, max_read_retries: usize) -> Self {
        self.max_read_retries = max_read_retries;
        self
    }

    /// Sets [`TransportDriverOptions::max_pending_waiters`].
    #[must_use]
    pub const fn with_max_pending_waiters(mut self, max_pending_waiters: usize) -> Self {
        self.max_pending_waiters = max_pending_waiters;
        self
    }

    /// Fails closed on a bound that would make an operation impossible.
    ///
    /// Zero is meaningless for both fields rather than merely small: zero
    /// retries never collects a granted barrier, and zero pending waiters
    /// refuses every write. A driver that accepted either would be a driver
    /// that cannot serve anything, discovered at the first request.
    fn validate(self) -> Result<Self, ManagedDriverError> {
        if self.max_read_retries == 0 {
            return Err(ManagedDriverError::InvalidOptions {
                field: "max_read_retries",
                reason: "a driver that never retries a barrier never collects a granted proof",
            });
        }
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

use state::{SharedState, TransportDriverState};

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
    /// Builds a driver over one already-configured group.
    ///
    /// The group must be quiescent — no pending proposals and no reserved
    /// reads — for the same reason [`InMemoryRaftDriver::new`] requires it: the
    /// driver correlates outcomes to waiters it created, and a waiter it did
    /// not create can never be resolved. Generated IDs start above the group's
    /// adopted watermarks.
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError`] when the group is poisoned, holds
    /// undrained poisoned waiters, is not quiescent, has exhausted a local
    /// ID space, or the options are out of range.
    pub fn new(
        group: RaftGroup<G, A, R>,
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
        Ok(Self {
            inner: Arc::new(Mutex::new(TransportDriverState {
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
                shutting_down: false,
            })),
        })
    }

    /// Returns a cloneable handle connected to this driver.
    #[must_use]
    pub fn handle(
        &self,
    ) -> RaftHandle<G, A::Command, A::Query, A::CommandResult, A::QueryResult, Self> {
        let group_id = lock_state(&self.inner).group_id.clone();
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
        let mut state = lock_state(&self.inner);
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
        let mut state = lock_state(&self.inner);
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
        let mut state = lock_state(&self.inner);
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
        lock_state(&self.inner).refused_sends
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
    /// # Errors
    ///
    /// Returns [`ManagedDriverError::NoGroup`] when the driver has already
    /// released its group.
    pub fn release_group(&self) -> Result<RaftGroup<G, A, R>, ManagedDriverError> {
        let mut state = lock_state(&self.inner);
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
    /// Returns [`ManagedDriverError::GroupAlreadyAdopted`] when the driver
    /// still holds a group, the same validation errors as
    /// [`TransportRaftDriver::new`], and a group error when the recovery
    /// outputs fail to apply.
    pub fn adopt_group(
        &self,
        group: RaftGroup<G, A, R>,
        recovery_outputs: Vec<RaftOutput>,
    ) -> Result<(), ManagedDriverError> {
        let mut state = lock_state(&self.inner);
        if state.group.is_some() {
            return Err(ManagedDriverError::GroupAlreadyAdopted);
        }
        let (next_proposal_id, next_read_id) = adopted_watermarks(&group, PendingProposals::Carry)?;
        state.node_id = group.node_id();
        state.next_proposal_id = highest(state.next_proposal_id, next_proposal_id);
        state.next_read_id = highest(state.next_read_id, next_read_id);
        state.group = Some(group);
        state.shutting_down = false;
        if recovery_outputs.is_empty() {
            state.publish_metrics();
            return Ok(());
        }
        state.apply_recovery_outputs(recovery_outputs)
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
        let started = lock_state(&inner).begin_write(&group_id, command, options);
        match started {
            Ok(local_proposal_id) => Box::pin(poll_fn(move |context| {
                lock_state(&inner).poll_write(local_proposal_id, context)
            })),
            Err(error) => Box::pin(ready(Err(error))),
        }
    }

    fn read(
        &self,
        group_id: G,
        query: A::Query,
        consistency: ReadConsistency,
    ) -> DriverFuture<Result<QueryReceipt<G, A::QueryResult>, ReadError>> {
        let inner = self.inner.clone();
        let started = lock_state(&inner).begin_read(&group_id, query, consistency);
        match started {
            Ok(read_id) => Box::pin(poll_fn(move |context| {
                lock_state(&inner).poll_read(read_id, context)
            })),
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
            let mut state = lock_state(&inner);
            if state.shutting_down {
                return Err(TransferLeadershipError::ShuttingDown);
            }
            if group_id != state.group_id {
                return Err(TransferLeadershipError::WrongGroup);
            }
            let report = state
                .group_mut()
                .map_err(|error| TransferLeadershipError::Transport {
                    cause: ErrorCause::new(error),
                })?
                .step_with_options(
                    GroupInput::TransferLeadership { target },
                    StepReportOptions::without_metrics(),
                )
                .map_err(transfer_error_from_group)?;
            let rejection =
                report
                    .leadership_transfer_events
                    .iter()
                    .find_map(|event| match event {
                        LeadershipTransferEvent::Rejected {
                            target: event_target,
                            reason,
                            leader_hint,
                        } if *event_target == target => Some(TransferLeadershipError::Rejected {
                            reason: *reason,
                            leader_hint: *leader_hint,
                        }),
                        _ => None,
                    });
            state.route_report(report);
            state.publish_metrics();
            rejection.map_or(Ok(()), Err)
        })
    }

    fn metrics(&self, group_id: G) -> Result<MetricsWatch<G>, MetricsError> {
        let state = lock_state(&self.inner);
        if group_id != state.group_id {
            return Err(MetricsError::WrongGroup);
        }
        Ok(state.metrics.watch())
    }

    fn shutdown(&self, group_id: G) -> DriverFuture<Result<(), ShutdownError>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            let mut state = lock_state(&inner);
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
