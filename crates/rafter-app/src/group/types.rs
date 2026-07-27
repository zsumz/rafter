use super::{
    ApplyEntry, ApplyResult, BTreeMap, ClientRequestId, Debug, ErrorCause, GroupError,
    LeadershipTransferRejection, LocalProposalId, LogIndex, MembershipChange, MembershipConfig,
    MembershipEvent, NodeId, PeerEnvelope, PersistedRaftRuntime, Proposal, ProposalBegin,
    ProposalEvent, RaftGroupMetrics, ReadBarrierRequest, ReadEvent, ReadId, ReadOutcome, ReadProof,
    ReadProofOutcome, ReplicatedStateMachine, SnapshotEvent, Term,
};

/// Fatal health state for a Raft group.
///
/// This enum is exhaustive: a group is either healthy or permanently poisoned
/// until the caller replaces it through an explicit recovery path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GroupFatalState {
    Healthy,
    Poisoned { reason: String },
}

/// Proposal and read waiters drained when a group enters a fatal poison state.
///
/// A poison is not an event stream: the group moves every pending waiter here
/// and emits nothing further for them. A driver that routes reports and does
/// not drain this leaves those clients waiting forever, which is why every
/// stepping path drains it.
///
/// Writes here must be reported as an unknown outcome, not a refusal: the entry
/// may already be in the durable log and may commit under a later incarnation.
/// Reads are terminal — a barrier the group dropped will never produce an
/// answer.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PoisonedWaiters {
    /// Each dropped proposal's local ID, paired with the caller's own request
    /// ID when one was supplied, so a driver can name the write to its client.
    pub proposals: Vec<(LocalProposalId, Option<ClientRequestId>)>,
    /// Each dropped barrier's read ID. Those IDs are spent; a retry issues a
    /// new read.
    pub reads: Vec<ReadId>,
}

impl PoisonedWaiters {
    /// Returns `true` when no proposal or read waiters were drained by poison.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.proposals.is_empty() && self.reads.is_empty()
    }
}

/// Synchronous driver for one local Raft node and one replicated state machine.
///
/// A `RaftGroup` is the live mutable owner of its Raft node, application state
/// machine, pending local waiters, and local ID watermarks. It is intentionally
/// not `Clone`; drive exactly one instance for a given local node.
#[derive(Debug)]
pub struct RaftGroup<G, A, R> {
    pub(super) group_id: G,
    pub(super) node_id: NodeId,
    pub(super) raft: R,
    pub(super) app: A,
    pub(super) pending_proposals: BTreeMap<LocalProposalId, Option<ClientRequestId>>,
    pub(super) last_seen_local_proposal_id: Option<LocalProposalId>,
    pub(super) pending_reads: BTreeMap<ReadId, PendingRead>,
    pub(super) pending_query_reads: BTreeMap<ReadId, PendingQueryRead>,
    pub(super) completed_query_reads: BTreeMap<ReadId, CompletedQueryRead<G>>,
    pub(super) last_seen_read_id: Option<ReadId>,
    pub(super) last_applied_index: LogIndex,
    pub(super) fatal_state: GroupFatalState,
    /// The error that poisoned this group, when the poison came from a typed
    /// failure. Held beside [`RaftGroup::fatal_state`] rather than inside it,
    /// because the health state is published in every metrics snapshot and a
    /// snapshot must stay a plain comparable value.
    pub(super) poison_cause: Option<ErrorCause>,
    pub(super) poisoned_waiters: PoisonedWaiters,
    /// The two memberships as of the last report this group handed back.
    ///
    /// **Durable comparison state, not a per-step snapshot, and the difference
    /// is the whole contract.** A membership event is derived by comparing the
    /// runtime's current configuration against this mark, and the mark advances
    /// only when the report carrying the difference is returned to a caller. A
    /// step that fails after the runtime moved therefore leaves the delta
    /// *owed*: the next report — or [`RaftGroup::drain_membership_events`] —
    /// still carries it.
    ///
    /// The previous model compared against a snapshot taken at the top of each
    /// step, which made every failing step lose whatever it had moved through.
    /// The runtime appends, truncates, commits, and installs before the group
    /// finishes the step, and applying entries, completing granted barriers, and
    /// deciding whether a proposal started can all fail afterwards. On any of
    /// those the group returned `Err` with no report while the configuration had
    /// already moved, and the *next* step's snapshot was taken from the moved
    /// configuration — so the transition was unreportable from then on, and a
    /// consumer's peer set and fences stayed on the old membership for the life
    /// of the incarnation.
    ///
    /// Initialized at construction from the runtime, because pre-existing state
    /// is not an event: a replica reopened over a log that already holds a
    /// three-node configuration has moved through nothing.
    pub(super) reported_membership: MembershipReportMark,
}

/// One committed configuration this group crossed and has not yet reported.
///
/// Queued rather than compared, because a comparison cannot see it. The commit
/// index can cross several configuration entries in one step, and the two the
/// comparison reads — the memberships before and after — can be *equal* across
/// a pair that added a replica and removed it again. The kernel names each
/// crossing as it happens; this is where the group holds them until a report
/// carries them out.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CommittedConfigurationCrossing {
    /// The configuration entry's own index, not the commit index the step
    /// reached.
    pub(super) index: LogIndex,
    pub(super) term: Term,
    pub(super) membership: MembershipConfig,
}

/// What a group's membership reporting is owed against.
///
/// Two memberships and a queue. The memberships are the state the effective and
/// final-committed comparisons are taken against; the queue is the committed
/// configurations the kernel named that no report has carried yet. Both are
/// durable across steps and both advance in exactly one place — where a report's
/// membership events are built.
///
/// Cloned before a report is built and put back when that report is discarded,
/// which is what keeps a delta owed rather than reported into a value the caller
/// never received. Every site that builds a report and then decides it cannot
/// return it takes one of these first; there is no other way to discard a report
/// without losing what it carried.
///
/// **Opaque on purpose.** The fields are private and there is no public
/// constructor: the only ways to obtain one are
/// [`RaftGroup::into_parts`] and the only thing to do with one is hand it to
/// [`RaftGroup::from_parts`]. A caller that could forge one could tell a group it
/// had already reported a configuration it never reported, which is the same
/// silent loss this whole mechanism exists to prevent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipReportMark {
    pub(super) effective: MembershipConfig,
    pub(super) committed: MembershipConfig,
    /// Committed configurations named by the kernel and not yet reported, in
    /// the order the commit index crossed them.
    pub(super) crossed: Vec<CommittedConfigurationCrossing>,
}

pub(super) type RuntimeGroupError<A, R> =
    GroupError<<A as ReplicatedStateMachine>::Error, <R as PersistedRaftRuntime>::Error>;
pub(super) type GroupResult<A, R, T> = Result<T, RuntimeGroupError<A, R>>;
pub(super) type StepReportResult<G, A, R> =
    GroupResult<A, R, GroupStepReport<G, <A as ReplicatedStateMachine>::CommandResult>>;
pub(super) type ProposalBeginResult<G, A, R> =
    GroupResult<A, R, ProposalBegin<G, <A as ReplicatedStateMachine>::CommandResult>>;
pub(super) type ProposalBeginReportResult<G, A, R> =
    GroupResult<A, R, ProposalBeginReport<G, <A as ReplicatedStateMachine>::CommandResult>>;
pub(super) type ProposalBatchBeginReportResult<G, A, R> =
    GroupResult<A, R, ProposalBatchBeginReport<G, <A as ReplicatedStateMachine>::CommandResult>>;
pub(super) type ReadBarrierBeginReportResult<G, A, R> =
    GroupResult<A, R, ReadBarrierBeginReport<G, <A as ReplicatedStateMachine>::CommandResult>>;
pub(super) type ReadOutcomeResult<G, A, R> =
    GroupResult<A, R, ReadOutcome<G, <A as ReplicatedStateMachine>::QueryResult>>;
pub(super) type ReadReportResult<G, A, R> = GroupResult<
    A,
    R,
    ReadReport<
        G,
        <A as ReplicatedStateMachine>::QueryResult,
        <A as ReplicatedStateMachine>::CommandResult,
    >,
>;
pub(super) type ApplyEntryResult<A, R> =
    GroupResult<A, R, ApplyEntry<<A as ReplicatedStateMachine>::Command>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct GrantedReadIndex {
    /// What the quorum round certified.
    pub(super) read_index: LogIndex,
    /// The highest committed application entry at or below `read_index`, and
    /// therefore the applied index a state machine can actually reach.
    pub(super) application_floor: LogIndex,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PendingRead {
    pub(super) min_applied_index: Option<LogIndex>,
    pub(super) granted: Option<GrantedReadIndex>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PendingQueryRead {
    pub(super) min_applied_index: Option<LogIndex>,
    pub(super) context: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CompletedQueryRead<G> {
    pub(super) proof: ReadProof<G>,
    pub(super) min_applied_index: Option<LogIndex>,
    pub(super) context: Vec<u8>,
}

/// Inputs accepted by the synchronous group driver.
///
/// This enum is exhaustive for the public input kinds currently accepted by
/// `RaftGroup`; new driver operations may add variants before 1.0.
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum GroupInput<G, C> {
    Tick,
    PeerMessage { envelope: PeerEnvelope<G> },
    Proposal { proposal: Proposal<C> },
    ProposalBatch { proposals: Vec<Proposal<C>> },
    ReadBarrier { request: ReadBarrierRequest<G> },
    TransferLeadership { target: NodeId },
    Membership { change: MembershipChange },
}

/// Controls which observability fields are materialized in a group step report.
///
/// Metrics are an observation snapshot, not protocol state. Disabling them
/// keeps every protocol output, waiter outcome, apply result, and lifecycle
/// event intact while avoiding a metrics walk on hot paths that publish their
/// own metrics snapshot at a coarser boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StepReportOptions {
    pub include_metrics: bool,
}

impl StepReportOptions {
    /// Build a full-fidelity report, including a fresh metrics snapshot.
    #[must_use]
    pub const fn full() -> Self {
        Self {
            include_metrics: true,
        }
    }

    /// Build a protocol/lifecycle report without the metrics snapshot.
    #[must_use]
    pub const fn without_metrics() -> Self {
        Self {
            include_metrics: false,
        }
    }
}

impl Default for StepReportOptions {
    fn default() -> Self {
        Self::full()
    }
}

/// Explicit side effects from one group step.
///
/// This is the sans-IO boundary made concrete: the group performs no IO, so
/// everything a step needs the outside world to do arrives here, and a caller
/// that drops a report drops protocol progress. Everything in it is already
/// durable — the runtime discharges its persistence obligation before releasing
/// any output, so a report exists only for effects that are safe to release.
///
/// One list is *stateful* and the rest are per-step: see `membership_events`,
/// which carries whatever the group has moved through and not yet handed back,
/// so a failed step's transitions arrive on the next report rather than being
/// lost with it.
///
/// The lists are in the order the step produced them, and that order is
/// load-bearing across `snapshot_events` and `peer_messages` in particular; do
/// not reorder them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupStepReport<G, R> {
    /// The group these effects belong to.
    pub group_id: G,
    /// Frames to hand to the transport.
    ///
    /// Dropping one is safe — Raft re-sends — but dropping all of them stalls
    /// the protocol: elections, replication, and read-index rounds all travel
    /// here. Send them; do not answer a client with them.
    pub peer_messages: Vec<PeerEnvelope<G>>,
    /// Results the state machine returned from applying committed entries.
    ///
    /// An entry reaching this list has committed and applied. This is the only
    /// list that proves a write took effect.
    pub applied: Vec<ApplyResult<R>>,
    /// Lifecycle transitions for locally submitted proposals.
    ///
    /// A caller correlating client futures matches on `local_proposal_id`.
    /// `Appended` is not success: it says the entry reached the local log, not
    /// that it committed.
    pub proposal_events: Vec<ProposalEvent<R>>,
    /// Lifecycle transitions for read barriers.
    ///
    /// A barrier ends in whichever step observes its cause, which is often a
    /// tick or a peer message rather than the read call that started it — so a
    /// caller that watches only its own read calls will wait forever.
    pub read_events: Vec<ReadEvent<G>>,
    /// Lifecycle transitions for leadership transfer.
    pub leadership_transfer_events: Vec<LeadershipTransferEvent>,
    /// Snapshot work, of which only `SendChunk` is the caller's to perform.
    ///
    /// `StageChunk` and `Apply` are already discharged when the event is
    /// emitted; see [`crate::snapshot::SnapshotEvent`].
    pub snapshot_events: Vec<SnapshotEvent<G>>,
    /// Membership transitions, which a transport must track to keep its peer
    /// set current.
    ///
    /// An `EffectiveChanged` configuration may still be uncommitted and may
    /// still be taken back, so it may only *widen* a peer set; only an `Applied`
    /// change licenses narrowing it or fencing what left. Both are reported
    /// whatever moved them — a local request, replication, a truncation, or a
    /// snapshot install — so a consumer that follows this list is level with the
    /// group on every step, and one that follows only its own membership calls
    /// is not.
    ///
    /// **The committed half is a history, not a difference.** One step can
    /// commit several configurations, and this list carries one `Applied` per
    /// configuration in index order rather than one for where the step ended. A
    /// consumer that retires identities has to see the ones in the middle: a
    /// pair that added a replica and removed it again leaves the endpoints equal,
    /// so a difference would report nothing while an identity was spent. The
    /// effective half stays a difference, because an intermediate effective
    /// configuration inside one step never authorized anything — no frame was
    /// checked against it — and what a peer set needs is the configuration in
    /// force now.
    ///
    /// **This list carries what the group has moved through and not yet
    /// reported, which is not the same as what *this* step moved.** The
    /// comparison is against the memberships as of the last report the group
    /// handed back, and that mark advances only when a report reaches a caller.
    /// So a step that fails after the runtime moved leaves the transition owed,
    /// and the next report carries it — including a report from
    /// [`RaftGroup::apply_raft_outputs`], which used to be unable to report
    /// membership at all. A caller with no next step to make takes the owed
    /// delta straight out of [`RaftGroup::drain_membership_events`], which is
    /// what a driver runs after every step outcome so its loss window is zero.
    pub membership_events: Vec<MembershipEvent<G>>,
    /// A metrics snapshot, present only when the step was asked for one.
    ///
    /// `None` means [`StepReportOptions::without_metrics`] was used, never that
    /// metrics were unavailable — a caller publishing at a coarser boundary
    /// takes its own snapshot from [`RaftGroup::metrics`].
    pub metrics: Option<RaftGroupMetrics<G>>,
}

/// Full-fidelity result of beginning a local proposal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProposalBeginReport<G, R> {
    pub begin: ProposalBegin<G, R>,
    pub report: GroupStepReport<G, R>,
}

/// Full-fidelity result of beginning a local proposal batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProposalBatchBeginReport<G, R> {
    pub begins: Vec<ProposalBegin<G, R>>,
    pub report: GroupStepReport<G, R>,
}

/// Full-fidelity result of beginning a read-index barrier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadBarrierBeginReport<G, R> {
    pub outcome: ReadProofOutcome<G>,
    pub report: GroupStepReport<G, R>,
}

/// The reusable pieces of a decomposed [`RaftGroup`].
#[derive(Debug)]
pub struct RaftGroupParts<G, A, R> {
    /// The group ID the retired group served.
    pub group_id: G,
    /// The local node ID the retired group owned.
    pub node_id: NodeId,
    /// The persisted runtime, still live: nothing was closed or flushed.
    pub runtime: R,
    /// The application state machine, at whatever applied index it reached.
    pub state_machine: A,
    /// The highest local proposal ID the retired group consumed, if any.
    ///
    /// Load-bearing when `runtime` is carried into a new group: the new group
    /// must be given IDs strictly above this, or a reused ID silently completes
    /// the new waiter with the older proposal's result. `None` when the group
    /// never proposed.
    pub local_proposal_id_watermark: Option<LocalProposalId>,
    /// The highest read ID the retired group consumed, if any, with the same
    /// obligation [`RaftGroupParts::local_proposal_id_watermark`] carries.
    pub read_id_watermark: Option<ReadId>,
    /// Whether the retired group was healthy or poisoned.
    pub fatal_state: GroupFatalState,
    /// The error that poisoned the group, when the poison came from a typed
    /// failure. Carried so decomposition stays lossless.
    pub poison_cause: Option<ErrorCause>,
    /// Waiters the poison captured, so decomposition can resolve clients the
    /// retired group would never have answered. Empty for a healthy group.
    pub poisoned_waiters: PoisonedWaiters,
    /// What the retired group's membership reporting was owed against.
    ///
    /// **Load-bearing whenever the parts are rebuilt into a group, and the one
    /// piece of decomposition state a caller cannot reconstruct.** A group
    /// derives its membership events against what it last *reported*, not
    /// against what its runtime currently holds, so a group that moved through a
    /// configuration and failed the step that would have reported it leaves the
    /// transition owed. Seeding a fresh mark from the rebuilt runtime instead
    /// would read the moved configuration as the starting point and answer
    /// "nothing has changed" forever after — the same silent loss a failing step
    /// used to cause, arriving through decomposition.
    ///
    /// Pass it to [`RaftGroup::from_parts`], which is the only thing it is for.
    /// A caller that reopens the durable stores into a *different* runtime and
    /// wants the fresh-start behaviour uses [`RaftGroup::with_applied_index`]
    /// and drops this.
    pub membership_report_mark: MembershipReportMark,
}

/// Full-fidelity result of a state-machine read.
///
/// This carries three type parameters where its siblings carry two, because a
/// query read is the only group operation whose outcome type differs from the
/// report's result type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadReport<G, Q, R> {
    pub outcome: ReadOutcome<G, Q>,
    pub report: GroupStepReport<G, R>,
}

/// Leadership-transfer lifecycle events surfaced by the app layer.
///
/// This enum is exhaustive for the leadership-transfer outcomes emitted by
/// the current app layer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LeadershipTransferEvent {
    Started {
        target: NodeId,
    },
    Rejected {
        target: NodeId,
        reason: LeadershipTransferRejection,
        leader_hint: Option<NodeId>,
    },
}

impl<G, R> GroupStepReport<G, R> {
    pub(super) fn new(group_id: G) -> Self {
        Self {
            group_id,
            peer_messages: Vec::new(),
            applied: Vec::new(),
            proposal_events: Vec::new(),
            read_events: Vec::new(),
            leadership_transfer_events: Vec::new(),
            snapshot_events: Vec::new(),
            membership_events: Vec::new(),
            metrics: None,
        }
    }
}

pub(super) fn report_has_proposal_lifecycle<G, R>(
    local_proposal_id: LocalProposalId,
    report: &GroupStepReport<G, R>,
) -> bool {
    report.proposal_events.iter().any(|event| {
        matches!(
            event,
            ProposalEvent::Appended {
                local_proposal_id: id,
                ..
            } | ProposalEvent::Applied {
                local_proposal_id: id,
                ..
            } | ProposalEvent::Rejected {
                local_proposal_id: id,
                ..
            } | ProposalEvent::UnknownOutcome {
                local_proposal_id: id,
                ..
            } if *id == local_proposal_id
        )
    })
}

impl<G, A, R> RaftGroup<G, A, R> {
    /// Creates a group with an empty local applied floor.
    ///
    /// Restart paths should prefer [`RaftGroup::with_applied_index`] and pass
    /// the state machine's durable applied index alongside a runtime recovered
    /// with the same applied-through floor.
    #[must_use]
    pub fn new(group_id: G, node_id: NodeId, raft: R, app: A) -> Self
    where
        R: PersistedRaftRuntime,
    {
        Self::with_applied_index(group_id, node_id, raft, app, LogIndex::ZERO)
    }

    /// Creates a group whose local applied floor is known at construction.
    ///
    /// Use this constructor on restart after reading
    /// [`ReplicatedStateMachine::applied_index`] from durable application
    /// state. The group still validates the state machine's reported applied
    /// index before every apply batch and poisons itself rather than replaying
    /// entries that the application already says are durable.
    ///
    /// The runtime's two memberships are read here to seed the group's
    /// membership comparison. A group reports transitions it *moves through*, so
    /// the configuration it opens over is never one of them; see
    /// [`RaftGroup::drain_membership_events`].
    #[must_use]
    pub fn with_applied_index(
        group_id: G,
        node_id: NodeId,
        raft: R,
        app: A,
        applied_index: LogIndex,
    ) -> Self
    where
        R: PersistedRaftRuntime,
    {
        let reported_membership = MembershipReportMark {
            effective: raft.membership(),
            committed: raft.committed_membership(),
            crossed: Vec::new(),
        };
        Self {
            group_id,
            node_id,
            raft,
            app,
            pending_proposals: BTreeMap::new(),
            last_seen_local_proposal_id: None,
            pending_reads: BTreeMap::new(),
            pending_query_reads: BTreeMap::new(),
            completed_query_reads: BTreeMap::new(),
            last_seen_read_id: None,
            last_applied_index: applied_index,
            fatal_state: GroupFatalState::Healthy,
            poison_cause: None,
            poisoned_waiters: PoisonedWaiters::default(),
            reported_membership,
        }
    }

    /// Returns this group's caller-defined group ID.
    #[must_use]
    pub fn group_id(&self) -> &G {
        &self.group_id
    }

    /// Returns the local Raft node ID owned by this group.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Returns the latest leader hint known by the underlying Raft runtime.
    #[must_use]
    pub fn leader_hint(&self) -> Option<NodeId>
    where
        R: PersistedRaftRuntime,
    {
        self.raft.leader_hint()
    }

    /// Highest local proposal ID consumed by this group, if any.
    ///
    /// `rafter-app` requires strictly increasing `LocalProposalId`s for the
    /// lifetime of a group. IDs less than or equal to this watermark will be
    /// rejected.
    #[must_use]
    pub fn local_proposal_id_watermark(&self) -> Option<LocalProposalId> {
        self.last_seen_local_proposal_id
    }

    /// Highest read-index ID consumed by this group, if any.
    ///
    /// `rafter-app` requires strictly increasing `ReadId`s for read-index
    /// operations over the lifetime of a group. IDs less than or equal to this
    /// watermark will be rejected.
    #[must_use]
    pub fn read_id_watermark(&self) -> Option<ReadId> {
        self.last_seen_read_id
    }

    /// Returns the group's fatal health state.
    #[must_use]
    pub fn fatal_state(&self) -> &GroupFatalState {
        &self.fatal_state
    }

    /// Returns the error that poisoned this group, if it is poisoned and the
    /// poison came from a typed failure.
    ///
    /// [`GroupFatalState`] says *that* a group is poisoned and is published in
    /// every metrics snapshot, so it stays a plain comparable value. The cause
    /// is a diagnostic held beside it and is never published: a metrics
    /// snapshot is cloned and compared on every step and must not carry a
    /// `dyn Error`.
    #[must_use]
    pub fn poison_cause(&self) -> Option<&ErrorCause> {
        self.poison_cause.as_ref()
    }

    /// Returns shared access to the owned replicated state machine.
    #[must_use]
    pub fn state_machine(&self) -> &A {
        &self.app
    }

    /// Returns mutable access to the owned state machine.
    ///
    /// This is intended for caller-owned maintenance hooks and test fixtures.
    /// Do not mutate the durable applied floor behind the group; restart paths
    /// should use [`RaftGroup::with_applied_index`] to seed that boundary.
    pub fn state_machine_mut(&mut self) -> &mut A {
        &mut self.app
    }

    /// Returns a shared reference to the owned persisted runtime.
    ///
    /// This is useful for inspection and for fake runtimes in integration
    /// tests. Protocol progress should still flow through [`RaftGroup::step`]
    /// and the other group APIs.
    #[must_use]
    pub fn runtime(&self) -> &R {
        &self.raft
    }

    /// Returns waiters that were drained when the group entered poison.
    #[must_use]
    pub fn poisoned_waiters(&self) -> &PoisonedWaiters {
        &self.poisoned_waiters
    }

    /// Drains and returns waiters that were resolved by poison handling.
    #[must_use]
    pub fn drain_poisoned_waiters(&mut self) -> PoisonedWaiters {
        std::mem::take(&mut self.poisoned_waiters)
    }

    /// Consumes the group and returns the parts a caller can reuse.
    ///
    /// This is the in-process teardown path. An embedder replacing a group —
    /// after poison, on a supervised restart, or when a host closes one group of
    /// many — reclaims the state machine and the runtime instead of dropping
    /// them. It is the group-level half of decomposition;
    /// `DurableRaftNode::into_storage` in `rafter-runtime` is the runtime-level
    /// half that reaches the durable stores. Decomposition takes the group by
    /// value, so a group kept in shared or lock-guarded state needs a movable
    /// slot — an `Option` holding the group is the usual shape.
    ///
    /// Decomposition never steps the runtime, never applies, and never emits
    /// outputs, so no protocol effect can be lost by calling it — and the
    /// membership delta the group had not yet reported travels with the parts in
    /// [`RaftGroupParts::membership_report_mark`], so that statement holds for a
    /// caller that *rebuilds* as well as for one that walks away. It did not:
    /// the mark used to be dropped here, and a group rebuilt over the same
    /// runtime seeded a fresh comparison from the configuration that had already
    /// moved, which made the owed transition unreportable for the life of the new
    /// incarnation. Rebuild through [`RaftGroup::from_parts`].
    ///
    /// What ends is
    /// local waiter tracking: every pending proposal and every reserved read
    /// disappears with the group. A proposal already appended may still commit
    /// and apply under a later incarnation, so a caller that has acknowledged
    /// nothing must treat each dropped waiter exactly as
    /// [`crate::proposal::ProposalEvent::UnknownOutcome`] — the write may or may
    /// not have taken effect.
    ///
    /// Decomposition is allowed on a poisoned group, because poison is the state
    /// a caller most needs to leave. `fatal_state`, `poison_cause`, and
    /// `poisoned_waiters` travel with the parts, so a caller that decomposes
    /// without inspecting the group first can still resolve its clients and
    /// still report what broke.
    ///
    /// The returned watermarks are load-bearing when `runtime` is carried into a
    /// new group. A live runtime still tracks local proposal IDs for entries it
    /// has not yet committed, and a new group starts with no watermark of its
    /// own, so it must be given IDs strictly above both returned watermarks.
    /// Reusing an ID completes the new group's waiter with the older proposal's
    /// result, at the older proposal's index — silently, because both the
    /// runtime and the new group are behaving exactly as documented. A runtime
    /// rebuilt from durable storage carries no local proposal tracking, and a
    /// group over it may restart its IDs at zero.
    ///
    /// The applied floor is not returned: the state machine reports it through
    /// [`crate::state_machine::ReplicatedStateMachine::applied_index`], and a
    /// group never advances its own floor past what the state machine reported.
    ///
    /// Nothing is closed and nothing is flushed. The runtime and its stores stay
    /// live until the caller drops them, so a caller reopening the same durable
    /// medium must drop the returned runtime first when the store requires
    /// exclusive access.
    #[must_use]
    pub fn into_parts(self) -> RaftGroupParts<G, A, R> {
        RaftGroupParts {
            group_id: self.group_id,
            node_id: self.node_id,
            runtime: self.raft,
            state_machine: self.app,
            local_proposal_id_watermark: self.last_seen_local_proposal_id,
            read_id_watermark: self.last_seen_read_id,
            fatal_state: self.fatal_state,
            poison_cause: self.poison_cause,
            poisoned_waiters: self.poisoned_waiters,
            membership_report_mark: self.reported_membership,
        }
    }

    /// Rebuilds a group over parts a previous incarnation handed back.
    ///
    /// **The lossless half of decomposition, and the only constructor that is.**
    /// [`RaftGroup::with_applied_index`] seeds its membership comparison from the
    /// runtime it is given, which is right for a group opening over durable state
    /// — pre-existing configuration is not an event — and wrong for a rebuild,
    /// because the runtime it is handed has already *moved* through whatever the
    /// retired group had not yet reported. This takes
    /// [`RaftGroupParts::membership_report_mark`] instead, so a transition a
    /// failed step left owed is still owed to the new group and arrives on its
    /// first report or straight out of [`RaftGroup::drain_membership_events`].
    ///
    /// `applied_index` is the state machine's durable applied floor, exactly as
    /// [`RaftGroup::with_applied_index`] takes it. It is not carried in the parts
    /// because the state machine reports it through
    /// [`crate::state_machine::ReplicatedStateMachine::applied_index`], which is
    /// the authority a group never overrides.
    ///
    /// The poison state travels with the parts, so a group rebuilt from a
    /// poisoned one is poisoned. That is deliberate: decomposition is how a
    /// caller *leaves* poison, and it does so by replacing the runtime or the
    /// state machine, not by rebuilding the same parts and hoping.
    ///
    /// The local ID watermarks travel too, which removes the obligation
    /// [`RaftGroup::into_parts`] documents for a caller that rebuilds here: the
    /// new group starts above the retired group's IDs by construction rather
    /// than by the caller remembering to.
    #[must_use]
    pub fn from_parts(parts: RaftGroupParts<G, A, R>, applied_index: LogIndex) -> Self {
        Self {
            group_id: parts.group_id,
            node_id: parts.node_id,
            raft: parts.runtime,
            app: parts.state_machine,
            pending_proposals: BTreeMap::new(),
            last_seen_local_proposal_id: parts.local_proposal_id_watermark,
            pending_reads: BTreeMap::new(),
            pending_query_reads: BTreeMap::new(),
            completed_query_reads: BTreeMap::new(),
            last_seen_read_id: parts.read_id_watermark,
            last_applied_index: applied_index,
            fatal_state: parts.fatal_state,
            poison_cause: parts.poison_cause,
            poisoned_waiters: parts.poisoned_waiters,
            reported_membership: parts.membership_report_mark,
        }
    }
}
