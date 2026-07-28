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
//! record a restart reads back, what makes one valid, and how two join. The join
//! and the fold below are the same algebra reached from two directions — one
//! merges two records, the other merges a record and a fact — so the rules that
//! make either safe are stated once, there, and read here.

use std::collections::BTreeSet;

use crate::transport::{AuthenticatedPeerValidator, PeerSet, RaftTransport};

use super::super::*;
use super::checkpoint::CurrentCommittedState;
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
        committed: CommittedObservation,
        effective: BTreeSet<NodeId>,
    },
}

/// One committed membership fact: where it stands, what it names, and what it
/// proves was removed.
///
/// **A position and a removal set, and the removal set is what a position could
/// not replace.** This driver used to keep a consumer offset per provenance and
/// skip any fact at or below it, because folding a historical membership against
/// a present one reads as a removal of everything the present added. That was
/// the wrong repair for a real hazard. An offset claims a *prefix* has been
/// consumed, and nothing a driver observes is a prefix: a snapshot-recovered
/// process that then folds a crossing at index 8 has consumed neither 6 nor 7,
/// and an offset reading 8 says it has.
///
/// The repair is at the source instead. A crossing arrives as the *transition*
/// the kernel computed where the chronology is known — see
/// [`rafter::Output::ConfigurationCommitted`] — so its removal set is exact
/// wherever, whenever and however often it is folded, and there is nothing left
/// for an offset to protect.
///
/// The position still travels, and it now decides one thing: which of two
/// observations of the *current* committed membership is later, and therefore
/// which one this driver believes. That is
/// [`super::checkpoint::CurrentCommittedState`], and the comparison is the same
/// one the checkpoint join makes between two records.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CommittedObservation {
    /// The log position this fact stands at.
    ///
    /// A configuration entry's own index for a crossing, this replica's commit
    /// index for an endpoint observation. Both name a point at which the
    /// committed membership is exactly `membership`, which is the only property
    /// the comparison needs — so the two provenances no longer need separate
    /// positions, and this is where the second cursor went.
    pub(super) through: LogIndex,
    /// The committed membership at `through`, raw as the cluster reported it.
    pub(super) membership: BTreeSet<NodeId>,
    /// The identities this fact *proves* a committed removal consumed.
    ///
    /// Non-empty only for a crossing, where it is the kernel's own
    /// `previous − configuration`. An endpoint carries no transition and proves
    /// nothing by itself; what it can still contribute is inferred by comparing
    /// its position against the one this driver holds, which is where the
    /// removals *between* two observations come from.
    pub(super) removed: BTreeSet<NodeId>,
    /// Every identity either end of this fact named, which is what the mark is
    /// raised over.
    ///
    /// A removed identity is in here, and that is the point: an ID the cluster
    /// committed is allocated whether or not it survives the transition, and a
    /// mark taken over the surviving membership alone would leave a removed ID
    /// above the mark and therefore allocatable again.
    pub(super) named: BTreeSet<NodeId>,
}

impl CommittedObservation {
    /// A configuration entry the commit index crossed, as the transition it is.
    fn crossing(
        through: LogIndex,
        previous: &MembershipConfig,
        committed: &MembershipConfig,
    ) -> Self {
        let previous: BTreeSet<NodeId> = previous.replica_ids().into_iter().collect();
        let membership: BTreeSet<NodeId> = committed.replica_ids().into_iter().collect();
        Self {
            through,
            removed: previous.difference(&membership).copied().collect(),
            named: previous.union(&membership).copied().collect(),
            membership,
        }
    }

    /// The committed membership a runtime holds, at its commit index.
    ///
    /// It proves no removal on its own — the moves that produce one are exactly
    /// the moves with no history to carry — so `removed` is empty and `named` is
    /// the membership itself.
    fn endpoint(through: LogIndex, committed: &MembershipConfig) -> Self {
        let membership: BTreeSet<NodeId> = committed.replica_ids().into_iter().collect();
        Self {
            through,
            named: membership.clone(),
            membership,
            removed: BTreeSet::new(),
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
            // The two committed facts, routed apart because they carry different
            // evidence. `Applied` is a transition — the configuration entry the
            // commit index crossed, with the membership that stood before it —
            // so it proves exactly which identities that entry removed, whatever
            // state it is folded into. `CommittedEndpoint` is an observation of
            // the current membership at this replica's commit index, for a move
            // with no entry behind it: a snapshot install, or a group opened
            // over a runtime that had already moved. It proves no removal by
            // itself, and treating it as one is what retired the replicas a
            // catching-up replica had most recently admitted.
            MembershipEvent::Applied {
                membership,
                index,
                previous,
                ..
            } => {
                self.publish_committed(CommittedObservation::crossing(
                    *index, previous, membership,
                ));
            }
            MembershipEvent::CommittedEndpoint {
                membership, index, ..
            } => self.publish_committed(CommittedObservation::endpoint(*index, membership)),
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

    /// Routes one committed membership fact, whatever produced it.
    ///
    /// Shared by the two committed arms above because everything except the
    /// evidence is identical, and the evidence now travels inside the
    /// observation rather than as a provenance tag the reducer has to interpret.
    fn publish_committed(&mut self, committed: CommittedObservation) {
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
    /// **Retirement reads the committed stream and only the committed stream**,
    /// with no exclusion of any kind — the local node included, which is the
    /// point. A committed removal of this replica spends this replica's identity
    /// exactly as it spends a peer's; a driver that filtered itself out observed
    /// the cluster remove it and recorded nothing, and could then adopt a peer's
    /// spent ID as its own with no backstop left anywhere.
    ///
    /// Reading removals from committed facts and not from the union is what
    /// closes the opposite window: an addition that appended and was then
    /// truncated back off the log was never in a committed configuration, so its
    /// disappearance retires nothing and licenses no fence. Its ID is still
    /// allocatable, because a reverted change may legitimately be proposed
    /// again.
    ///
    /// **Nothing un-spends an identity.** A committed configuration naming an
    /// already-spent ID is filtered out of the current state rather than obeyed,
    /// so the ID stays spent and stays refused. A fence is permanent for the
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
            } => {
                self.effective_members = effective;
                self.observe_committed_members(committed);
            }
        }
        self.flush_peer_control_plane();
    }

    /// Takes one committed membership fact: the removals it proves, the
    /// high-water mark, and the current state the spent test reads.
    ///
    /// Split from [`TransportDriverState::publish_membership`] because it is the
    /// only place identity is *consumed*, and everything about consumption is
    /// here: which IDs a removal spends, which a violating fact is refused for,
    /// and how far allocation has got.
    ///
    /// # Why this needs no cursor
    ///
    /// A restart hands this method the same facts twice: the runtime replays
    /// every configuration entry above the application's applied floor, oldest
    /// first, beneath a current state that has already moved past them. What
    /// made that dangerous was deriving removals by subtraction from the
    /// driver's own membership, which is right only when that membership stands
    /// exactly where the fact does. Two cursors were kept so a fact standing
    /// anywhere else could be skipped.
    ///
    /// Every operation below is now monotone evidence instead, so re-folding one
    /// changes nothing and there is nothing left to skip:
    ///
    /// * **Removals come from the fact.** A crossing carries its own transition,
    ///   computed by the kernel where the chronology is known, so
    ///   `previous − configuration` is the same set at every replay. An
    ///   endpoint carries none and asserts none.
    /// * **The mark is a maximum**, taken over both ends of the fact.
    /// * **Obligations are a union**, and one removal contributes at most once —
    ///   the `∖ spent` below is what makes a re-fold of an already-absorbed
    ///   removal add nothing.
    /// * **The current state is a versioned register.** An older observation
    ///   never displaces a later one; it only contributes what the pair proves,
    ///   which is the identities it named that the later one does not.
    ///
    /// # The removal a later register still names
    ///
    /// A crossing beneath the register proves a removal whose ID the register
    /// may still name, and `spent(id) = id ≤ mark ∧ id ∉ membership` cannot see
    /// it while the membership does. That gap is closed here without a holding
    /// set, and the argument is short:
    ///
    /// 1. A fact that proves `id` removed named `id`, so it raised the mark to
    ///    at least `id`.
    /// 2. `id` is subtracted from the register's membership *whatever the fact's
    ///    position* — a removal is not an observation of the present, it is a
    ///    permanent fact about an identity, so the later-wins rule does not
    ///    apply to it.
    /// 3. So `id ≤ mark ∧ id ∉ membership` holds the moment the fold returns.
    /// 4. Every later assignment to the register filters its incoming membership
    ///    through the spent test, so `id` can never re-enter.
    ///
    /// A contract-violating configuration that names `id` again is therefore
    /// refused at step 4 rather than parked in a set that has to be bounded, and
    /// the violation stays countable through the raw floor beside the register —
    /// see [`TransportDriverState::readmitted_retired_peers`].
    fn observe_committed_members(&mut self, fact: CommittedObservation) {
        // Read before anything moves, so the epoch below is advanced for exactly
        // the observations an embedder must persist.
        let before = self.control_plane_checkpoint();

        // Everything this fact proves was removed: its own transition, plus the
        // identities the older of the two observations names and the newer does
        // not. The second half is what one record and one runtime jointly prove
        // and neither states — the same inference the checkpoint join makes.
        let mut removed = fact.removed;
        let held = self.current_committed.as_ref();
        let is_later = held.is_none_or(|current| fact.through >= current.through);
        if let Some(current) = held {
            let (older, newer) = if is_later {
                (&current.membership, &fact.membership)
            } else {
                (&fact.membership, &current.membership)
            };
            removed.extend(older.difference(newer).copied());
        }

        // **Every derivation below reads the spent test as it stood before this
        // fact**, and that order is load-bearing rather than tidy. The mark
        // moves first only in wall-clock terms; read against the raised mark,
        // an identity this driver has simply not observed yet — every identity
        // at all, on the very first fact — would test as spent, and the
        // membership the fact names would filter down to nothing.
        let was_spent = {
            let mark = self.committed_id_high_water;
            let live = self.live_committed_members().clone();
            move |node_id: NodeId| {
                mark.is_some_and(|mark| node_id <= mark) && !live.contains(&node_id)
            }
        };

        // Only what this driver did not already know. A removal it had absorbed
        // is either still in the obligations or was discharged by the link
        // layer, and re-deriving it would owe a second fence for one fact.
        self.pending_fences
            .extend(removed.iter().copied().filter(|id| !was_spent(*id)));
        // Over every identity the fact named rather than the survivors: an ID
        // the cluster committed is allocated whether or not it survived the
        // transition, and a mark that ignored a removed one would leave it
        // allocatable again.
        if let Some(highest) = fact.named.iter().copied().max() {
            self.committed_id_high_water = Some(
                self.committed_id_high_water
                    .map_or(highest, |mark| mark.max(highest)),
            );
        }

        if let Some(current) = self.current_committed.as_mut() {
            // Step 2 above: absorbed at any position, because a removal is not
            // an observation of the present.
            current.membership.retain(|id| !removed.contains(id));
        }
        if is_later {
            // Step 4 above: the filter is what keeps a spent identity out
            // forever, so a violating readmission cannot un-spend one.
            let membership = fact
                .membership
                .iter()
                .copied()
                .filter(|id| !was_spent(*id))
                .collect();
            self.current_committed = Some(CurrentCommittedState::new(fact.through, membership));
        }
        // **The raw floor is not part of the register, and assigning it always
        // is round 8's rule kept rather than re-litigated.** It answers "what
        // does this replica's own stream say the cluster has committed", which
        // no position has an opinion about, and tying it to a gate left it stale
        // in two lifecycle cells — a second recovery from one checkpoint, and a
        // supervisor handing a record to a runtime that rebuilt behind it. The
        // only facts that can leave it historical are recovery outputs, and both
        // entry points publish the runtime's endpoint after them.
        //
        // Raw rather than filtered, which is what makes a readmission countable.
        self.committed_members = fact.membership;

        if before != self.control_plane_checkpoint() {
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
            && !self.live_committed_members().contains(&node_id)
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
        let committed =
            CommittedObservation::endpoint(runtime.commit_index(), &runtime.committed_membership());
        let effective = runtime.membership().replica_ids().into_iter().collect();
        self.publish_membership(MembershipFact::Committed {
            committed,
            effective,
        });
    }
}
