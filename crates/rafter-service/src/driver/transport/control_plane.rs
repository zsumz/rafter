#![allow(clippy::wildcard_imports)]

//! What this driver tells its link layer about who may speak.
//!
//! Split from [`super::state`] along the line that file's own header draws:
//! that one answers "what does a step do", and this one answers "who is allowed
//! to send one". Everything between a committed configuration and the two
//! statements the transport is owed for it — a peer set and a fence — is here,
//! and the step loop reaches it through one call.
//!
//! The state these derivations read still lives on
//! [`super::state::TransportDriverState`], because the step loop's fields and
//! these are one struct behind one lock. What moved is every rule that reads
//! them.
//!
//! Every rule here is pinned from outside the crate, through `deliver`, `tick`,
//! and adoption — `tests/transport_membership.rs` for what the event stream
//! does to the peer set, `tests/transport_identity.rs` for what a committed
//! removal does to an identity. This module used to carry its own test file
//! whose header explained that the widening branch had no public entry point,
//! and that was true and was the defect: the app layer reported an effective
//! change only for a step carrying a local membership request, so no follower
//! could reach the branch its tests were passing. Scripting the router directly
//! also let those fixtures state membership sequences no correct cluster
//! produces, and two of them did.
//!
//! The durable half of the same concern lives in [`super::checkpoint`]: the
//! record a restart reads back, what makes one valid, how two join, and how far
//! through the committed configuration stream one has been consumed. The rule
//! that a replayed configuration is not news is stated there and consulted here.

use std::collections::BTreeSet;

use crate::transport::{AuthenticatedPeerValidator, PeerSet, RaftTransport};

use super::super::*;
use super::state::TransportDriverState;

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
///
/// Each variant *assigns* the fact it carries rather than merging it into what
/// was there before. Two facts are tracked separately for exactly this reason: a
/// single merged set could only ever grow, and a configuration that appended and
/// was then truncated back off the log would leave the replica it named
/// authorized forever, because no committed removal would ever arrive to take it
/// out.
pub(super) enum MembershipFact {
    /// A configuration that is effective and may still be uncommitted.
    ///
    /// It replaces the effective half and nothing else, so what it can do to the
    /// published peer set depends on which direction it moved. A replica joining
    /// under joint consensus has to be able to speak before the change commits,
    /// or it can never catch up and the change can never commit; a replica the
    /// change *drops* keeps speaking, because the committed configuration is
    /// still the floor and this fact cannot narrow past it.
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
        /// The log index this committed fact stands at.
        ///
        /// It travels *in* the fact rather than beside it for the reason the two
        /// membership sets do: a committed configuration only means anything
        /// together with where it sits in the stream, and a publisher that
        /// supplied the two separately could supply a configuration from one
        /// point and a position from another. The retirement diff is licensed by
        /// the pair or by neither.
        through: LogIndex,
        /// Whether this fact is a point in the stream or the end of it.
        source: CommittedMembershipSource,
    },
}

/// Where a committed membership fact came from, which decides **which position
/// answers for it** and what that position is evidence *of*.
///
/// Both variants carry a position and both are gated on one for the retirement
/// diff, because re-folding a fact this driver has already consumed computes a
/// difference between a historical membership and a present one and calls
/// everything the present added a removal. That much is common.
///
/// What is not common is the *reach* of the position. A crossing's index is a
/// configuration entry's own, so consuming it covers that index; an endpoint's
/// index is a commit index observed for a move with no entry behind it, and
/// covers nothing beneath itself. The driver therefore keeps a position per
/// variant — `committed_crossings_through` and `committed_endpoint_through` —
/// and neither ever gates the other. One shared position let a
/// snapshot-recovered record's endpoint at commit 10 suppress real crossings at
/// 6 and 7, so an identity a committed removal spent was never spent here and
/// its fence was never owed.
///
/// **Neither variant governs the raw committed membership, and both used to.**
/// That value is read — by [`TransportDriverState::is_member`], by
/// [`TransportDriverState::named_members`], by
/// [`TransportDriverState::readmitted_retired_peers`] — only as a statement
/// about the cluster **now**, which no position has an opinion about. It is
/// assigned on every committed fact whatever the gate says; see
/// [`TransportDriverState::publish_membership`] for the two lifecycle cells that
/// tying it to the fold left stale.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CommittedMembershipSource {
    /// A configuration the commit index crossed, at that entry's own index.
    ///
    /// History, and *checkable* history: a restart replays every crossing above
    /// the application's applied floor, oldest first, and a driver that folded
    /// the entry at index *n* has genuinely covered *n*. That is what makes
    /// `committed_crossings_through` a position a later replay may be skipped
    /// against.
    ///
    /// Carried by [`MembershipEvent::Applied`], and by nothing else.
    Crossing,
    /// The committed membership a runtime holds, at its commit index.
    ///
    /// The end of the stream, and by construction the cluster's committed
    /// configuration as that runtime has it. Produced by the two moves with no
    /// configuration entry to name — a snapshot install, and a group opened over
    /// a runtime whose commit index had already moved — and by adoption, which
    /// asks the runtime directly.
    ///
    /// **Its position covers nothing beneath itself**, which is the whole reason
    /// it is kept apart. It still needs gating, because an ungated endpoint fold
    /// computes a retirement diff against a live set that may have moved past a
    /// rebuilt runtime's volatile commit index — but it may only gate other
    /// endpoints.
    ///
    /// Carried by [`MembershipEvent::CommittedEndpoint`], and by
    /// [`TransportDriverState::publish_adopted_membership`].
    Endpoint,
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
    /// Keeps the transport's peer set level with the group's membership.
    ///
    /// `EffectiveChanged` carries the configuration this replica is operating
    /// under and `Applied` carries the committed one — that is how `rafter-app`
    /// builds them — so each arm names the fact it has and
    /// [`TransportDriverState::publish_membership`] decides what the fact
    /// licenses. The `Applied` arm reads the effective membership beside it for
    /// the reason [`MembershipFact::Committed`] gives: a change committing does
    /// not retract a *later* change already appended over it.
    ///
    /// The stream both arms read is complete: `rafter-app` reports an effective
    /// change whatever moved it — a local request, replication, a truncation, or
    /// a snapshot install — so this router hears about a follower's joint
    /// transition and about a new leader taking one back. It did not always, and
    /// the widening arm was for a while live code no public entry point of this
    /// driver could reach.
    #[allow(
        clippy::match_same_arms,
        reason = "`Rejected` and the non-exhaustive wildcard do nothing for \
                  different reasons, and naming the known variant is the audit"
    )]
    pub(super) fn route_membership_event(&mut self, event: &MembershipEvent<G>) {
        match event {
            MembershipEvent::EffectiveChanged { membership, .. } => {
                let effective = membership.replica_ids().into_iter().collect();
                self.publish_membership(MembershipFact::Effective(effective));
            }
            // The two committed facts, routed apart because their indices are
            // evidence of different things. `Applied` names the configuration
            // entry the commit index crossed, so its index really does cover
            // that point in the stream; `CommittedEndpoint` names this replica's
            // commit index for a move with no entry behind it — a snapshot
            // install, or a group opened over a runtime that had already moved —
            // and covers nothing beneath itself. Routing both under one position
            // let an endpoint suppress the crossings a later recovery replayed
            // below it, which spends no identity and owes no fence.
            MembershipEvent::Applied {
                membership, index, ..
            } => self.publish_committed(membership, *index, CommittedMembershipSource::Crossing),
            MembershipEvent::CommittedEndpoint {
                membership, index, ..
            } => self.publish_committed(membership, *index, CommittedMembershipSource::Endpoint),
            // A rejected change never entered the log, so there is no membership
            // fact in it to act on.
            MembershipEvent::Rejected { .. } => {}
            // `MembershipEvent` is `#[non_exhaustive]`, so this arm is required
            // and is the one place a new membership fact can be missed. It is
            // deliberately not a silent skip in spirit: a variant this build
            // does not know cannot be classified as a crossing or an endpoint
            // without guessing which, and guessing wrong either manufactures a
            // retirement or suppresses one. The honest local answer is to change
            // nothing, and the real defence is that `rafter-app` and this driver
            // ship together — the app-layer match has no wildcard, so a fourth
            // variant stops that build first.
            _ => {}
        }
    }

    /// Routes one committed membership fact under the provenance its event
    /// carried.
    ///
    /// Shared by the two committed arms above because everything except the
    /// provenance is identical, and the one thing that differs is the one thing
    /// that must not be decided here.
    fn publish_committed(
        &mut self,
        membership: &MembershipConfig,
        index: LogIndex,
        source: CommittedMembershipSource,
    ) {
        let committed = membership.replica_ids().into_iter().collect();
        // The runtime is the authority on what is in effect, and it agrees with
        // the effective event that preceded this one in the same report. A
        // driver holding no group keeps what it had rather than assigning an
        // empty set: an absent effective membership must not turn a fence into a
        // silence, and must not narrow anything either.
        let effective = self
            .runtime_effective_members()
            .unwrap_or_else(|| self.effective_members.clone());
        self.publish_membership(MembershipFact::Committed {
            committed,
            effective,
            through: index,
            source,
        });
    }

    /// Records what one membership fact requires of the link layer, then tries
    /// to install it.
    ///
    /// Two statements, derived from one fact: which principals the transport may
    /// authorize, and which it must fence. Neither may be skipped because the
    /// other could not be made — a membership event that both narrows the set
    /// and licenses a fence installs two admission controls, and a driver that
    /// dropped one because the other failed would leave a committed-removed
    /// replica able to speak.
    ///
    /// No caller chooses between them. Everything the link layer is told is
    /// derived from the union of the two membership facts and the spent test
    /// over it, so a caller that supplies [`MembershipFact::Effective`] cannot
    /// narrow past what committed and cannot fence anything, and one that
    /// supplies [`MembershipFact::Committed`] fences exactly the identities that
    /// left the live committed configuration. Both are consequences of the
    /// derivation below rather than obligations on a caller.
    ///
    /// **Recording is separate from installing, and that separation is the
    /// contract.** This method derives obligations; it does not decide that they
    /// were met. The membership facts advance here unconditionally, because they
    /// are the record of what the *cluster* says and the next committed removal
    /// has to be computed against the membership the cluster had. The record of
    /// what the *link layer* took lives in `published_peers` and
    /// `pending_fences`, and only
    /// [`TransportDriverState::flush_peer_control_plane`] moves those — on an
    /// `Ok` from the transport and on nothing else. A driver with one piece of
    /// state for both forgets a refused fence the instant it advances, because
    /// there is no later event to re-derive it from: the removal is already
    /// behind the committed membership.
    ///
    /// **Retirement reads the committed stream and only the committed stream.**
    /// The diff is `previous live committed − new live committed`, with no
    /// exclusion of any kind — the local node included, which is the point. A
    /// committed removal of this replica spends this replica's identity exactly
    /// as it spends a peer's; a driver that filtered itself out of the diff
    /// observed the cluster remove it and recorded nothing, and could then adopt
    /// a peer's spent ID as its own with no backstop left anywhere.
    ///
    /// Taking the diff from the committed fact and not from the union is what
    /// closes the opposite window: an addition that appended and was then
    /// truncated back off the log was never in a committed configuration, so its
    /// disappearance retires nothing and licenses no fence. Its ID is still
    /// allocatable, because a reverted change may legitimately be proposed
    /// again.
    ///
    /// **Nothing un-spends an identity.** A committed configuration naming an
    /// already-spent ID is filtered out of the live set rather than obeyed, so
    /// the ID stays spent and stays refused. A fence is permanent for the
    /// principal it names — [`RaftTransport::fence_peer`] has no inverse,
    /// deliberately — so obeying such a fact would promise an authorization the
    /// link layer cannot give back. The raw fact is kept beside the live one so
    /// the violation is countable rather than silently absorbed; see
    /// [`TransportDriverState::readmitted_retired_peers`].
    fn publish_membership(&mut self, fact: MembershipFact) {
        match fact {
            // Assigned, not merged. The effective configuration moves in both
            // directions — a new leader can truncate an uncommitted one back off
            // the log — and it still cannot narrow the peer set, because every
            // derivation takes it in union with the committed floor.
            MembershipFact::Effective(effective) => self.effective_members = effective,
            MembershipFact::Committed {
                committed,
                effective,
                through,
                source,
            } => {
                self.effective_members = effective;
                // **Two operations, and the position governs one of them.** The
                // retirement fold is gated in both directions and for both
                // sources, because a fact already folded in must not be folded
                // again from either end of the stream — each against the
                // position kept for its own source.
                self.observe_committed_members(&committed, through, source);
                // The raw floor is a different question, and the answer is the
                // same for both sources: **assign it always**. It is read only
                // as "what does the cluster have committed now", and a position
                // answers "have I folded this retirement in" and never that.
                //
                // Tying it to the fold was the defect, twice. Gating it for an
                // endpoint left the floor empty on every second recovery from
                // one checkpoint; gating it for a crossing left the floor at the
                // pre-catch-up configuration whenever the restored position ran
                // ahead of the runtime, which a supervisor handing over a
                // checkpoint produces routinely. In both cases the union that
                // keeps an uncommitted narrowing from de-authorizing a committed
                // replica had one half missing, and a union with an empty half
                // is the other half.
                self.committed_members = committed;
            }
        }
        self.flush_peer_control_plane();
    }

    /// Takes one committed configuration: the retirement diff, the high-water
    /// mark, and the live set the spent test reads.
    ///
    /// Split from [`TransportDriverState::publish_membership`] because it is the
    /// only place identity is *consumed*, and everything about consumption is
    /// here: which IDs a removal spends, which a violating fact is refused for,
    /// how far allocation has got, and how far the stream has been read.
    ///
    /// **A fact at or below the cursor is not news, and returning early for one
    /// is the whole of the replay fix.** The retirement diff below is taken
    /// against the live set *as it stands now*, so re-taking it for a
    /// configuration this driver already folded in does not repeat an
    /// observation — it computes a difference between a historical membership
    /// and a present one, and calls everything the present added a removal. That
    /// is what a restart hands this method: the runtime replays every
    /// configuration entry above the application's applied floor, oldest first.
    ///
    /// **Retirement only.** The raw committed membership is the caller's, and
    /// deliberately: it is not a fold and no position answers for it. A gate
    /// that returned early from both left the committed floor stale in two
    /// different lifecycle cells — an endpoint standing at or below its position,
    /// which is an ordinary second recovery, and a genuinely new crossing
    /// standing beneath a restored position that had run ahead of the runtime,
    /// which is an ordinary supervisor handover.
    /// [`TransportDriverState::publish_membership`] now assigns it
    /// unconditionally and this method answers only for the fold.
    ///
    /// **`source` picks which position gates and which position advances**, and
    /// nothing here reads the other one. An endpoint covers nothing beneath
    /// itself, so an endpoint position must never make a crossing look like
    /// history.
    fn observe_committed_members(
        &mut self,
        committed: &BTreeSet<NodeId>,
        through: LogIndex,
        source: CommittedMembershipSource,
    ) {
        if self.committed_configuration_is_replayed(through, source) {
            return;
        }
        // Anything the fact names that this driver has already watched leave a
        // committed configuration. Computed against the state *before* the
        // assignment, which is what makes it stick: once an ID is spent, every
        // later fact naming it is filtered the same way.
        let live = committed
            .iter()
            .copied()
            .filter(|node_id| !self.is_spent(*node_id))
            .collect::<BTreeSet<_>>();
        let newly_retired = self
            .live_committed_members
            .difference(&live)
            .copied()
            .collect::<Vec<_>>();
        // Read before the checkpointable fields move, so the epoch below is
        // advanced for exactly the observations an embedder must persist. The
        // fence set only grows here, so its length is a faithful witness and
        // costs no clone.
        let previous_mark = self.committed_id_high_water;
        let previous_fences = self.pending_fences.len();
        self.pending_fences.extend(newly_retired);
        // Over the raw fact rather than the live one: an ID the cluster
        // committed is allocated whether or not this driver will honor it, and a
        // mark that ignored the violation would leave the ID allocatable again.
        if let Some(highest) = committed.iter().copied().max() {
            self.committed_id_high_water = Some(
                self.committed_id_high_water
                    .map_or(highest, |mark| mark.max(highest)),
            );
        }
        // The position is part of the same decision rather than a separate one.
        // A fact that moved nothing else still moved this, and an embedder that
        // persisted the retirement record without it would replay from the older
        // position on the next restart — which is the failure this closes,
        // arriving one crash later.
        let position_moved = self.advance_committed_position(through, source);
        let checkpoint_moved = position_moved
            || previous_mark != self.committed_id_high_water
            || previous_fences != self.pending_fences.len()
            || live != self.live_committed_members;
        self.live_committed_members = live;
        if checkpoint_moved {
            self.advance_checkpoint_epoch();
        }
    }

    /// Whether a committed removal has already consumed this `(group, NodeId)`.
    ///
    /// Two reads and no set. Under the allocation contract [`rafter::NodeId`]
    /// states — every newly admitted ID exceeds every ID the group has ever
    /// committed — the IDs that have ever been committed are exactly those at or
    /// below `committed_id_high_water`, so one of them that the live committed
    /// configuration no longer names is one a committed removal spent.
    ///
    /// That replaces a set of every removal the driver ever saw, which grew
    /// without bound over the life of a long-running group and had no retention
    /// policy — the same unbounded-tombstone structure the kernel declined to
    /// keep, moved one layer up and no more affordable there.
    ///
    /// Before any committed configuration has been observed there is no mark and
    /// nothing is spent, which is why the mark is an `Option` rather than a
    /// zero: `NodeId(0)` is a legal identity and cannot double as "none".
    ///
    /// The deployment-visible consequence is that an allocation *gap* below the
    /// mark is unallocatable. That is not a rounding error in the derivation, it
    /// is the contract said out loud: fresh means greater than anything ever
    /// committed. A deployment that allocates non-monotonically has its "fresh"
    /// IDs refused here, which is the fail-closed direction — the alternative
    /// reads a violated precondition as permission.
    pub(super) fn is_spent(&self, node_id: NodeId) -> bool {
        self.committed_id_high_water
            .is_some_and(|mark| node_id <= mark)
            && !self.live_committed_members.contains(&node_id)
    }

    /// Every replica either membership fact names, which is the set the peer set
    /// and the inbound check are both derived from.
    ///
    /// The union is what keeps a joiner able to speak: a replica added by a
    /// change that has appended and not committed is in the effective half only,
    /// and it has to be able to catch up or the change can never commit. The
    /// committed half is the floor the effective one cannot narrow past.
    fn named_members(&self) -> BTreeSet<NodeId> {
        self.effective_members
            .union(&self.committed_members)
            .copied()
            .collect()
    }

    /// Installs everything the link layer still owes the group, and leaves owed
    /// whatever it refuses.
    ///
    /// The retry the transport boundary requires. Both
    /// [`RaftTransport::update_peers`] and [`RaftTransport::fence_peer`] are
    /// documented as fallible, which makes retry the caller's obligation — and
    /// this driver is the caller. Neither statement repairs itself the way a
    /// dropped frame does: Raft re-sends a lost heartbeat, and nothing
    /// re-derives a peer set or a fence, because the membership fact that
    /// licensed them is already behind `known_members`.
    ///
    /// Idempotent and cheap when there is nothing owed: a peer set that matches
    /// what the transport accepted makes no call, and an empty obligation set
    /// makes no call. That is what lets it run from every entry point rather
    /// than from a schedule.
    ///
    /// **Where it may run.** Every named entry point of the driver — both
    /// constructors, `tick`, `deliver`, and `drive_pending_reads` — and nowhere
    /// implicit. It is deliberately not called from `DriverShared::lock`, which
    /// would put an embedder's transport calls inside every poll of a client
    /// future, and deliberately not reachable from any `Drop`: reclamation is a
    /// leaf that never waits for this lock, and a flush is neither. Like every
    /// other embedder call this driver makes, it runs with the state lock held,
    /// so a transport that drops a client future inside it reclaims through the
    /// deferred queue exactly as [`super::state::DriverShared::reclaim`]
    /// describes.
    pub(super) fn flush_peer_control_plane(&mut self) {
        self.flush_peer_set();
        self.flush_pending_fences();
    }

    /// Publishes the peer set the group requires, or publishes nothing.
    ///
    /// All or nothing. A membership the validator cannot fully name is not
    /// published at all: a partial peer set authorizes fewer replicas than the
    /// cluster has, which is a quorum-splitting configuration change made by
    /// accident, while leaving the previous set in place is merely stale. That
    /// last clause is true of the peer set and only of the peer set — a fence
    /// the same fact licensed is not stale when it is withheld, it is absent,
    /// which is why fencing is not part of this decision.
    ///
    /// Staleness is now bounded rather than merely tolerable, and that is what
    /// changed. The previous set stays in place until the *next* attempt, and
    /// the next attempt is the driver's next entry point rather than the
    /// cluster's next configuration change. `published_peers` is what makes the
    /// difference observable: it advances only on an accepted publication, so
    /// "the link layer is behind the group" is a state this driver can see and
    /// report rather than an event it counted once.
    ///
    /// Both a principal that cannot be named and a transport refusal are
    /// counted, for the reason they always were, and both now leave the work
    /// outstanding as well as counted.
    ///
    /// The local replica is not in its own peer set: a `PeerSet` names who may
    /// speak *to* this node, and a node is not a peer of itself. Derived on each
    /// attempt rather than stored, so an incarnation adopted under a different
    /// node ID excludes the right replica without anything having to notice.
    fn flush_peer_set(&mut self) {
        let desired = self.desired_peers();
        // `NodeId` sets on both sides, comparing a set of replicas against a
        // record of the principals the link layer accepted for them, and it is
        // sound for exactly one reason: a `PeerPrincipal` is stable for the
        // lifetime of its `NodeId`. `AuthenticatedPeerValidator::principal_for_node`
        // states that, so a directory cannot move a live ID to a different
        // principal underneath a published set and leave this comparison
        // reporting level while the transport authorizes the wrong subject.
        // Credential rotation happens *beneath* a principal and is invisible
        // here, which is what makes the stability requirement affordable.
        if self.published_peers.as_ref() == Some(&desired) {
            return;
        }
        let mut principals = Vec::new();
        for node_id in desired.iter().copied() {
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
            return;
        }
        self.published_peers = Some(desired);
    }

    /// Fences every replica a committed removal left owed, and keeps owing the
    /// rest.
    ///
    /// Per replica rather than all or nothing, because the two statements have
    /// different shapes. A peer set is one statement about a whole cluster, and
    /// a partial one authorizes a quorum-splitting subset of it. A fence is one
    /// statement about one replica, and fencing three of four removed replicas
    /// is strictly better than fencing none of them. The set is drained one
    /// entry at a time for the same reason: one replica's refusal is not
    /// another's.
    ///
    /// A replica this deployment cannot name **stays owed** rather than being
    /// discarded as counted. `principal_for_node` answering `None` is a
    /// statement about the directory, not about the cluster: a deployment that
    /// cannot name a replica today can name it once its directory catches up,
    /// and the removal it could not act on is exactly as committed either way.
    /// Dropping the obligation there would make an unnameable removal
    /// permanently unfenced — the same forgetting as a refused fence, arriving
    /// through the validator instead of the transport.
    ///
    /// **The local node's own fence is deferred, not dropped.** A committed
    /// removal of this replica owes the link layer the same statement every
    /// other replica is making about it, and the one thing this driver cannot do
    /// is make it while it *is* that replica: fencing itself would refuse its
    /// own inbound frames, and a replica stepping down still has to receive
    /// enough of the log to be useful until the supervisor lets go of it. So the
    /// entry is skipped and left in place, matched against the *current*
    /// `node_id` rather than the one the obligation was recorded under. The
    /// first adoption of a different identity makes it an ordinary peer
    /// obligation, and the next flush discharges it.
    ///
    /// Dropping it instead — which is what this did — was the local half of
    /// forgetting a fence: the driver became something else and never told its
    /// link layer to stop trusting what it used to be.
    fn flush_pending_fences(&mut self) {
        for node_id in self.pending_fences.iter().copied().collect::<Vec<_>>() {
            if node_id == self.node_id {
                continue;
            }
            // The obligation holds a `NodeId` and the principal is resolved
            // *now*, at each retry, rather than captured when the removal
            // committed — and that is correct by contract rather than by luck.
            // `AuthenticatedPeerValidator::principal_for_node` requires a
            // directory to keep a removed replica's mapping resolvable until its
            // fence has been accepted, and the mapping it must keep is stable
            // for the lifetime of that ID. So the principal this resolves is the
            // one the removal named; there is no later principal for a retired
            // ID to be moved to, because the ID is retired.
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
                continue;
            }
            self.pending_fences.remove(&node_id);
            self.advance_checkpoint_epoch();
        }
    }

    /// The peer set the group currently requires, which is its membership less
    /// the local node and less every identity a committed removal has spent.
    ///
    /// The spent exclusion is the fail-closed reading of a violated single-use
    /// contract. A `(group_id, NodeId)` pair is retired by a committed removal
    /// and is never validly re-added, but the kernel keeps no tombstones and
    /// cannot refuse the re-addition after compaction has erased the history it
    /// would need — so this driver can be handed a committed membership naming a
    /// replica whose fence it has already installed. Leaving it out of the peer
    /// set is the only answer a transport with no unfence can carry out:
    /// publishing the ID would ask the link layer to authorize a principal it
    /// has permanently fenced, which is a set no transport can honor and a
    /// driver that believed it had would report itself level while the replica
    /// stayed silent.
    ///
    /// The local node is excluded here rather than stored excluded: a `PeerSet`
    /// names who may speak *to* this node, and a node is not a peer of itself.
    /// Derived on each attempt, so an incarnation adopted under a different node
    /// ID excludes the right replica without anything having to notice.
    fn desired_peers(&self) -> BTreeSet<NodeId> {
        self.named_members()
            .into_iter()
            .filter(|node_id| *node_id != self.node_id && !self.is_spent(*node_id))
            .collect()
    }

    /// Whether the transport's peer set is behind the one the group requires.
    pub(super) fn peer_set_is_stale(&self) -> bool {
        self.published_peers.as_ref() != Some(&self.desired_peers())
    }

    /// How many spent identities the group's membership names again.
    ///
    /// Zero for every cluster that keeps the single-use contract, which is what
    /// makes it worth reading. A non-zero value is one specific violation and
    /// not a link-layer condition: some replica was named again under a `NodeId`
    /// a committed removal had already spent, and this driver is refusing it —
    /// out of the peer set, out of the inbound check, and with its fence still
    /// owed if the link never took it.
    ///
    /// Counted over the *raw* membership facts rather than the live ones, which
    /// is why the raw committed configuration is stored at all: the live set has
    /// the violating ID filtered out, so counting there would report zero for
    /// exactly the case this exists to name.
    ///
    /// Current state rather than history, like
    /// [`super::TransportRaftDriver::pending_peer_fences`] and unlike
    /// `refused_peer_updates`: it falls back to zero only if a later membership
    /// stops naming the spent replica, which no correct deployment needs and no
    /// incorrect one is helped by.
    pub(super) fn readmitted_retired_peers(&self) -> usize {
        self.named_members()
            .into_iter()
            .filter(|node_id| self.is_spent(*node_id))
            .count()
    }

    /// Whether `node_id` may speak to this driver at all.
    ///
    /// The inbound admission check, and it is deliberately the same set the peer
    /// set is derived from. The union of the two membership facts names every
    /// replica that may legitimately speak — including one added by a change
    /// that has appended and not committed, which has to be able to speak before
    /// the change commits or the change can never commit.
    ///
    /// Spent-ness outranks it, and the order is the whole point. A committed
    /// removal spends the `(group_id, NodeId)` pair, so a later fact naming that
    /// ID is not evidence that the replica may speak again — it is evidence that
    /// the contract was broken, and the frame is refused whatever the fact says.
    /// The alternative reads a violated precondition as permission, and would
    /// admit exactly the replica whose principal the transport has permanently
    /// fenced.
    ///
    /// Asks each fact rather than building their union, because this runs on
    /// every inbound frame and the union is the same answer with an allocation
    /// in front of it. [`TransportDriverState::named_members`] is for the
    /// derivations that need the set itself.
    pub(super) fn is_member(&self, node_id: NodeId) -> bool {
        !self.is_spent(node_id)
            && (self.effective_members.contains(&node_id)
                || self.committed_members.contains(&node_id))
    }

    /// Whether a committed removal has spent this driver's own identity.
    ///
    /// The local replica is retired by the same fact and the same diff as any
    /// peer, so this needs no separate record: `node_id` simply stops being in
    /// the live committed configuration, and the spent test answers.
    pub(super) fn is_decommissioned(&self) -> bool {
        self.is_spent(self.node_id)
    }

    /// Whether the link layer has left more fences owed than the service
    /// threshold allows.
    fn fence_backlog_is_over_threshold(&self) -> bool {
        self.pending_fences.len() > self.options.fence_backlog_service_threshold
    }

    /// Why this driver is refusing new client work, if it is.
    ///
    /// **Ordered by what a supervisor can still do about it**, most terminal
    /// first. Shutdown outranks everything because nothing else changes what
    /// happens next; a released driver is reported before anything derived from
    /// a group it does not hold; decommissioning outranks the two conditions
    /// that end, because a backlog drains and a rollback can be re-proposed and
    /// a spent identity can be neither.
    pub(super) fn service_state(&self) -> DriverServiceState {
        if self.shutting_down {
            return DriverServiceState::ShuttingDown;
        }
        if self.group.is_none() {
            return DriverServiceState::Released;
        }
        if self.is_decommissioned() {
            return DriverServiceState::Decommissioned {
                node_id: self.node_id,
            };
        }
        // Not the negation of decommissioning: `is_member` is the union of the
        // two membership facts *and* the spent test, so a replica that was never
        // named and one that was removed both fail it, and only the second is a
        // retirement.
        if !self.is_member(self.node_id) {
            return DriverServiceState::NotMember {
                node_id: self.node_id,
            };
        }
        if self.fence_backlog_is_over_threshold() {
            return DriverServiceState::FenceBacklog {
                pending_fences: self.pending_fences.len(),
                service_threshold: self.options.fence_backlog_service_threshold,
            };
        }
        DriverServiceState::Serving
    }

    /// Refuses a client operation this driver's own state says it must not
    /// start.
    ///
    /// Every refusal is a `NotAppended`-shaped fact: nothing was proposed, so
    /// nothing is in flight to be uncertain about. None is reported as a group
    /// failure, because the group is fine — what is wrong is this replica's
    /// standing in the cluster, its link layer's, or this driver's own.
    pub(super) fn reject_if_not_serving(&self) -> Result<(), DriverUnavailableReason> {
        match self.service_state() {
            DriverServiceState::Serving => Ok(()),
            DriverServiceState::Decommissioned { .. } => {
                Err(DriverUnavailableReason::Decommissioned)
            }
            DriverServiceState::NotMember { .. } => Err(DriverUnavailableReason::NotMember),
            DriverServiceState::FenceBacklog { .. } => Err(DriverUnavailableReason::FenceBacklog),
            DriverServiceState::Released => Err(DriverUnavailableReason::Released),
            // Every client surface refuses shutdown ahead of this call with its
            // own older variant, so this arm is the projection staying total
            // rather than a path a client reaches; see
            // [`DriverUnavailableReason::ShuttingDown`].
            DriverServiceState::ShuttingDown => Err(DriverUnavailableReason::ShuttingDown),
        }
    }

    /// Reads the group's effective membership, or `None` if it holds no group.
    fn runtime_effective_members(&self) -> Option<BTreeSet<NodeId>> {
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
    /// holds its own committed membership from before the release, so the
    /// difference is there to be taken — and taking it is the only thing that
    /// makes one committed removal mean the same at adoption as it does on a
    /// routed event.
    ///
    /// The effective membership travels with it rather than instead of it. A
    /// runtime rebuilt from durable storage can hold an appended-but-uncommitted
    /// change, which makes its effective membership *narrower* than its
    /// committed one for a removal in flight; publishing that alone would take
    /// authorization away for a change that may still revert. The union is what
    /// keeps both readings correct at once.
    ///
    /// Adoption also discharges whatever the previous incarnation left owed, and
    /// gets that for free rather than by arrangement: publishing runs the flush,
    /// and the obligations are the driver's rather than the group's, so a
    /// release does not cancel them. That is the half a re-derivation cannot
    /// cover — by the time the driver holds no group, its committed membership
    /// has already moved past any removal it observed, so the difference this
    /// method takes is empty and the fence is owed rather than derivable. It is
    /// also where a deferred self-fence is finally made: the entry stops
    /// matching `node_id` the moment a fresh identity is installed, and the
    /// flush this runs is the next one.
    ///
    /// A driver holding no group publishes nothing and still owes what it owed.
    /// The early return skips the *derivation*, which needs a runtime; it does
    /// not discard obligations, which need only the next entry point.
    ///
    /// **This is the endpoint of the stream, so it stands at the commit index
    /// and its retirement fold is gated against the endpoint position.** The
    /// runtime does not publish the index of the entry its committed
    /// configuration came from, and it does not need to: `committed_membership`
    /// is by definition the latest configuration at or below `commit_index`, so
    /// the commit index is a sound and monotone position *for an endpoint*. A
    /// driver whose endpoint position already covers it has folded this
    /// observation in and takes no diff.
    ///
    /// The gate is not decoration here. A commit index is *volatile* — a
    /// recovered runtime can legitimately report a lower one than the
    /// incarnation that wrote the checkpoint had reached — so an ungated fold
    /// would compute a fresh retirement diff between a rebuilt runtime's older
    /// committed configuration and a restored live set that had already moved
    /// past it, and retire everything the newer configurations had added. That
    /// is the same manufactured removal the replay produces, arriving through
    /// the endpoint instead of through the history.
    ///
    /// **And it gates endpoints only.** This position covers nothing beneath
    /// itself: the runtime reports the configuration it holds and says nothing
    /// about what committed and was superseded below it, which is exactly the
    /// case a snapshot install produces. Letting it gate crossings skipped
    /// history that had genuinely never been folded — see
    /// [`CommittedMembershipSource`].
    ///
    /// **The raw committed membership is not gated at all**, because it answers
    /// a question no position has an opinion about. This call is where a driver
    /// gets it from the runtime directly rather than from an event, and it runs
    /// last in both construction and adoption so the floor ends those sequences
    /// level with the group the driver now holds.
    ///
    /// **It is not, however, the only publisher of an endpoint, and it is not
    /// sampled per step.** `rafter-app` emits
    /// [`MembershipEvent::CommittedEndpoint`] whenever a step moves the
    /// committed membership with no crossing to carry it, and the driver
    /// reconciles the event stream after every step outcome including errors. So
    /// the floor tracks the runtime without this method being called again; this
    /// one exists for the moment *before* any step, when the driver has just
    /// been handed a group and no event has announced anything.
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
        let through = runtime.commit_index();
        self.publish_membership(MembershipFact::Committed {
            committed,
            effective,
            through,
            source: CommittedMembershipSource::Endpoint,
        });
    }
}
