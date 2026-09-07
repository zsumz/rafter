//! The group's live state, and the report one step hands back.
//!
//! `RaftGroup` is the mutable owner of a node, a state machine, and every
//! local waiter. `GroupStepReport` is the sans-IO boundary made concrete,
//! and `StepReportOptions` decides how much of it a step materializes. The
//! impls that drive them live in sibling modules.

use super::{
    ApplyResult, BTreeMap, ClientRequestId, CompletedQueryRead, ErrorCause, GroupFatalState,
    LeadershipTransferEvent, LocalProposalId, LogIndex, MembershipEvent, MembershipReportMark,
    NodeId, PeerEnvelope, PendingQueryRead, PendingRead, PoisonedWaiters, ProposalEvent,
    RaftGroupMetrics, ReadEvent, ReadId, SnapshotEvent,
};

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

/// Controls which observability fields are materialized in a group step report.
///
/// Metrics are an observation snapshot, not protocol state. Disabling them
/// keeps every protocol output, waiter outcome, apply result, and lifecycle
/// event intact while avoiding a metrics walk on hot paths that publish their
/// own metrics snapshot at a coarser boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StepReportOptions {
    /// Whether the step walks and includes a fresh metrics snapshot.
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
