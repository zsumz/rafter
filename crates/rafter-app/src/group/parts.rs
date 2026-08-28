//! Decomposition: the parts a retired group hands back, and the rebuild.
//!
//! Nothing is stepped, applied, closed, or flushed here. The membership
//! delta the group had not yet reported travels with the parts, which is
//! what makes a rebuild lossless where a fresh constructor over the same
//! runtime is not.

use super::{
    BTreeMap, ErrorCause, GroupFatalState, LocalProposalId, LogIndex, MembershipReportMark, NodeId,
    PoisonedWaiters, RaftGroup, ReadId,
};

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

impl<G, A, R> RaftGroup<G, A, R> {
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
