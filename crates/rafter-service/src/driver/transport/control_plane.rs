#![allow(clippy::wildcard_imports)]

//! What this driver tells its link layer about who may speak.
//!
//! Split from [`super::state`] along the line that file's own header draws:
//! that one answers "what does a step do", and this one answers "who is allowed
//! to send one". Everything between a committed configuration and the one
//! statement the transport is owed for it — a [`PeerPolicy`], which is the
//! authorized principals beside the retirement floor — is here, and the step loop
//! reaches it through one call.
//!
//! It used to be two statements: a peer set, plus a permanent per-principal
//! fence per committed removal, owed until the link layer accepted it. That
//! second one was an *operation* rather than a derivation, so the driver had to
//! remember which removals it had already acted on — and it answered "has this
//! fence been made" with the same bit that answers "may this identity be admitted
//! again". Publishing a floor makes retirement a function of state the driver
//! still holds, which is what deletes the ledger and everything that could go
//! wrong inside it.
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

use crate::transport::{AuthenticatedPeerValidator, PeerPolicy, RaftTransport};

use super::super::*;
use super::checkpoint::{merge_current_state, IncomingObservation};
use super::state::{DesiredPeerPolicy, TransportDriverState};

/// The membership fact one publication is derived from.
///
/// A fact rather than a set plus a decision, and that is the whole point of the
/// type. Publishing answers two questions — which principals the link layer may
/// authorize, and how far retirement reaches — and both are licensed by the same
/// one fact: what the cluster has *committed*. A caller that supplied a set and a
/// retirement flag as separate arguments could answer the two inconsistently, and
/// one did: adoption published a narrowed peer set for a committed removal and
/// withheld the retirement for it, because the two travelled apart. Here they
/// cannot.
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
    /// only fact that licenses narrowing the set and retiring what left it.
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
    ///
    /// **A contradiction discovered here is recorded rather than returned**, and
    /// there is nowhere else for it to go: this runs from `route_report` and
    /// `reconcile_membership`, which are reached from every step outcome
    /// including a failing one. What it must never do is publish anyway —
    /// [`TransportDriverState::publish_membership`] leaves the driver's own state
    /// untouched and issues nothing, and
    /// [`DriverServiceState::ContradictoryCurrentState`] is how a supervisor
    /// hears about it.
    pub(super) fn route_membership_event(&mut self, event: &MembershipEvent<G>) {
        // The refusal is already recorded on the state by the time this returns;
        // there is no caller here that could act on a second copy of it.
        let _ = self.route_membership_fact(event);
    }

    #[allow(
        clippy::match_same_arms,
        reason = "`Rejected` and the non-exhaustive wildcard do nothing for \
                  different reasons, and naming the known variant is the audit"
    )]
    fn route_membership_fact(
        &mut self,
        event: &MembershipEvent<G>,
    ) -> Result<(), ControlPlaneCheckpointError> {
        match event {
            MembershipEvent::EffectiveChanged { membership, .. } => {
                let effective = membership.replica_ids().into_iter().collect();
                self.publish_membership(MembershipFact::Effective(effective))
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
                self.publish_committed(CommittedObservation::crossing(*index, previous, membership))
            }
            MembershipEvent::CommittedEndpoint {
                membership, index, ..
            } => self.publish_committed(CommittedObservation::endpoint(*index, membership)),
            // A rejected change never entered the log, so there is no membership
            // fact in it to act on.
            MembershipEvent::Rejected { .. } => Ok(()),
            // `MembershipEvent` is `#[non_exhaustive]`, so this arm is required
            // and is the one place a new membership fact can be missed. It is
            // deliberately not a silent skip in spirit: a variant this build
            // does not know cannot be classified as a crossing or an endpoint
            // without guessing which, and guessing wrong either manufactures a
            // retirement or suppresses one. The honest local answer is to change
            // nothing, and the real defence is that `rafter-app` and this driver
            // ship together — the app-layer match has no wildcard, so a fourth
            // variant stops that build first.
            _ => Ok(()),
        }
    }

    /// Routes one committed membership fact, whatever produced it.
    ///
    /// Shared by the two committed arms above because everything except the
    /// evidence is identical, and the evidence now travels inside the
    /// observation rather than as a provenance tag the reducer has to interpret.
    fn publish_committed(
        &mut self,
        committed: CommittedObservation,
    ) -> Result<(), ControlPlaneCheckpointError> {
        // The runtime is the authority on what is in effect, and it agrees with
        // the effective event that preceded this one in the same report. A
        // driver holding no group keeps what it had rather than assigning an
        // empty set: an absent effective membership must not turn a retirement
        // into a silence, and must not narrow anything either.
        let effective = self
            .runtime_effective_members()
            .unwrap_or_else(|| self.effective_members.clone());
        self.publish_membership(MembershipFact::Committed {
            committed,
            effective,
        })
    }

    /// Records what one membership fact requires of the link layer, then tries
    /// to install it.
    ///
    /// One statement, derived from one fact: the principals the transport may
    /// authorize, beside the floor at or below which an unauthorized identity is
    /// retired. The two halves cannot be published apart and cannot be published
    /// inconsistently, because they are one value — which is what a membership
    /// event that narrows the set and retires what left it needs.
    ///
    /// No caller chooses between them. Everything the link layer is told is
    /// derived from the union of the two membership facts, the spent test over
    /// it, and the mark, so a caller that supplies [`MembershipFact::Effective`]
    /// cannot narrow past what committed and cannot retire anything, and one that
    /// supplies [`MembershipFact::Committed`] retires exactly the identities that
    /// left the live committed configuration. Both are consequences of the
    /// derivation below rather than obligations on a caller.
    ///
    /// **Recording is separate from installing, and that separation is what is
    /// left of the contract.** This method derives what the link layer should
    /// hold; it does not decide that the link layer took it. The membership facts
    /// advance here unconditionally, because they are the record of what the
    /// *cluster* says. The record of what the *link layer* took lives in
    /// `published_policy`, and only
    /// [`TransportDriverState::flush_peer_policy`] moves it — on an `Ok` from
    /// the transport and on nothing else. What no longer needs recording is the
    /// *work outstanding*: a refused publication is re-derived from state this
    /// driver still holds, so there is nothing to forget.
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
    /// disappearance retires nothing. Its ID is still allocatable, because a
    /// reverted change may legitimately be proposed again.
    ///
    /// **Nothing un-spends an identity.** A committed configuration naming an
    /// already-spent ID is filtered out of the current state rather than obeyed,
    /// so the ID stays spent and stays refused: a `(group_id, NodeId)` pair a
    /// committed removal consumed is not a pair the cluster can hand back. The
    /// raw fact is kept beside the live one so the violation is countable rather
    /// than silently absorbed; see
    /// [`TransportDriverState::readmitted_retired_peers`].
    ///
    /// # Errors
    ///
    /// Returns [`ControlPlaneCheckpointError::ContradictoryCurrentState`] when
    /// the fact and the state this driver holds stand at one position and
    /// disagree about the membership there. **Nothing moves and nothing is
    /// published on that path**, and the refusal is recorded on the driver so a
    /// supervisor polling [`TransportDriverState::service_state`] sees it: a
    /// retirement floor is permanent, and a permanent statement must never be
    /// issued while the facts licensing it contradict each other.
    fn publish_membership(
        &mut self,
        fact: MembershipFact,
    ) -> Result<(), ControlPlaneCheckpointError> {
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
                // The committed half first, because it is the one that can
                // refuse. An effective membership assigned ahead of a refusal
                // would be half a fact applied.
                if let Err(reason) = self.observe_committed_members(committed) {
                    let ControlPlaneCheckpointError::ContradictoryCurrentState { through } = reason
                    else {
                        return Err(reason);
                    };
                    self.contradicted_at = Some(through);
                    return Err(reason);
                }
                self.effective_members = effective;
            }
        }
        self.flush_peer_policy();
        Ok(())
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
    ///
    /// # What the spent test is no longer asked
    ///
    /// It used to gate a *side effect*: a proven removal owed the link layer a
    /// fence unless the identity already tested as spent. That read one bit —
    /// "may this ID ever be admitted again" — as the answer to a different
    /// question — "has this identity's link layer been told" — and the two come
    /// apart exactly where it matters. A record recovered from a snapshot can
    /// make an identity test as spent without this process's link layer having
    /// heard anything about it, and an exact removal transition arriving beneath
    /// that record was then discarded as already-handled.
    ///
    /// Retirement is a floor now, published beside the peer set, so this method
    /// has no side effect to gate. `spent` answers only what it can: whether an
    /// identity may be admitted, adopted, or published again.
    ///
    /// # Errors
    ///
    /// Returns [`ControlPlaneCheckpointError::ContradictoryCurrentState`] when
    /// this fact and the register stand at one position and still disagree about
    /// the membership there after normalization. **Nothing is mutated on that
    /// path**: every value is computed into a local and assigned only once the
    /// merge has answered.
    fn observe_committed_members(
        &mut self,
        fact: CommittedObservation,
    ) -> Result<(), ControlPlaneCheckpointError> {
        // Read before anything moves, so the epoch below is advanced for exactly
        // the observations an embedder must persist.
        let before = self.control_plane_checkpoint();

        // **The merge reads the spent test as it stood before this fact**, and
        // that order is load-bearing rather than tidy. Read against a mark this
        // fact had already raised, an identity this driver has simply not
        // observed yet — every identity at all, on the very first fact — would
        // test as spent, and the membership the fact names would filter down to
        // nothing.
        let was_spent = {
            let mark = self.committed_id_high_water;
            let live = self.live_committed_members().clone();
            move |node_id: NodeId| {
                mark.is_some_and(|mark| node_id <= mark) && !live.contains(&node_id)
            }
        };
        let current = merge_current_state(
            self.current_committed.as_ref(),
            &IncomingObservation {
                through: fact.through,
                membership: &fact.membership,
                proven_removed: &fact.removed,
            },
            &was_spent,
        )?;

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
        self.current_committed = Some(current);
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
        Ok(())
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

    /// Installs the authorization policy the group requires, or installs
    /// nothing.
    ///
    /// The retry the transport boundary requires. [`RaftTransport::update_peers`]
    /// is documented as fallible, which makes retry the caller's obligation — and
    /// this driver is the caller. It does not repair itself the way a dropped
    /// frame does: Raft re-sends a lost heartbeat, and nothing re-derives a
    /// policy, because the membership fact that licensed it is already behind
    /// `committed_members`.
    ///
    /// **All or nothing, and now that is the whole of it.** A membership the
    /// validator cannot fully name is not published: a partial peer set
    /// authorizes fewer replicas than the cluster has, which is a quorum-splitting
    /// configuration change made by accident, while leaving the previous policy
    /// in place is merely stale — the floor is monotone, so an older policy
    /// retires fewer identities and never more, and the driver's own inbound
    /// check refuses the retired replica meanwhile.
    ///
    /// This used to be two flushes, and the second one had to be per replica: a
    /// fence was one statement about one replica, so fencing three of four
    /// removed replicas was strictly better than fencing none. A floor makes that
    /// distinction disappear — one statement retires every identity beneath it at
    /// once, and a directory that cannot name some *live* replica no longer
    /// withholds anything about the *removed* ones, because there is nothing
    /// per-removal to withhold.
    ///
    /// Idempotent and cheap when nothing moved: a policy that matches what the
    /// transport accepted makes no call. That is what lets it run from every
    /// entry point rather than from a schedule.
    ///
    /// **Nothing is published while this driver's own inputs contradict each
    /// other.** A retirement floor is permanent, and a permanent statement issued
    /// from facts that disagree is the one mistake no later publication corrects.
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
    pub(super) fn flush_peer_policy(&mut self) {
        if self.contradicted_at.is_some() {
            return;
        }
        let desired = self.desired_policy();
        // `NodeId` sets on both sides, comparing a set of replicas against a
        // record of the principals the link layer accepted for them, and it is
        // sound for exactly one reason: a `PeerPrincipal` is stable for the
        // lifetime of its `NodeId`. `AuthenticatedPeerValidator::principal_for_node`
        // states that, so a directory cannot move a live ID to a different
        // principal underneath a published set and leave this comparison
        // reporting level while the transport authorizes the wrong subject.
        // Credential rotation happens *beneath* a principal and is invisible
        // here, which is what makes the stability requirement affordable.
        if self.published_policy.as_ref() == Some(&desired) {
            return;
        }
        let mut principals = Vec::new();
        for node_id in desired.peers.iter().copied() {
            let Some(principal) = self.validator.principal_for_node(&self.group_id, node_id) else {
                self.refused_peer_updates = self.refused_peer_updates.saturating_add(1);
                return;
            };
            principals.push(principal);
        }
        if self
            .transport
            .update_peers(
                &self.group_id,
                PeerPolicy::new(principals, desired.retirement_floor),
            )
            .is_err()
        {
            self.refused_peer_updates = self.refused_peer_updates.saturating_add(1);
            return;
        }
        self.published_policy = Some(desired);
    }

    /// The policy the group currently requires: who may speak, and how far
    /// retirement reaches.
    ///
    /// **The floor is the mark, unchanged and uninterpreted.** Every identity
    /// this group has ever committed is at or below `committed_id_high_water`, so
    /// an identity at or below it that this policy does not authorize is one a
    /// committed removal spent — or one an allocator produced out of order, which
    /// [`TransportDriverState::is_spent`] already refuses for the same reason and
    /// in the same direction.
    fn desired_policy(&self) -> DesiredPeerPolicy {
        DesiredPeerPolicy {
            peers: self.desired_peers(),
            retirement_floor: self.committed_id_high_water,
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

    /// Whether the transport's policy is behind the one the group requires.
    pub(super) fn peer_policy_is_stale(&self) -> bool {
        self.published_policy.as_ref() != Some(&self.desired_policy())
    }

    /// How many spent identities the group's membership names again.
    ///
    /// Zero for every cluster that keeps the single-use contract, which is what
    /// makes it worth reading. A non-zero value is one specific violation and
    /// not a link-layer condition: some replica was named again under a `NodeId`
    /// a committed removal had already spent, and this driver is refusing it —
    /// out of the published peer set, out of the inbound check, and beneath the
    /// retirement floor its own policy states.
    ///
    /// Counted over the *raw* membership facts rather than the live ones, which
    /// is why the raw committed configuration is stored at all: the live set has
    /// the violating ID filtered out, so counting there would report zero for
    /// exactly the case this exists to name.
    ///
    /// Current state rather than history, like
    /// [`super::TransportRaftDriver::peer_policy_is_stale`] and unlike
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
    /// admit exactly the replica this driver's own published policy retires.
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

    /// Why this driver is refusing new client work, if it is.
    ///
    /// **Ordered by what a supervisor can still do about it**, most terminal
    /// first. Shutdown outranks everything because nothing else changes what
    /// happens next; a released driver is reported before anything derived from
    /// a group it does not hold; a contradiction outranks both conclusions drawn
    /// from the membership facts, because it says those facts cannot be trusted;
    /// and decommissioning outranks the condition that ends, because a rollback
    /// can be re-proposed and a spent identity cannot.
    pub(super) fn service_state(&self) -> DriverServiceState {
        if self.shutting_down {
            return DriverServiceState::ShuttingDown;
        }
        if self.group.is_none() {
            return DriverServiceState::Released;
        }
        if let Some(through) = self.contradicted_at {
            return DriverServiceState::ContradictoryCurrentState { through };
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
            DriverServiceState::ContradictoryCurrentState { .. } => {
                Err(DriverUnavailableReason::ContradictoryCurrentState)
            }
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
    /// Adoption also republishes whatever the previous incarnation could not, and
    /// gets that for free rather than by arrangement: publishing runs the flush,
    /// and the policy is the driver's rather than the group's, so a release does
    /// not cancel it. It is also where a fresh incarnation retires the identity
    /// it used to be — the old `node_id` is at or below the floor and absent from
    /// the peer set the moment a different identity is installed, which is the
    /// whole of what a deferred self-fence used to arrange by hand.
    ///
    /// A driver holding no group publishes nothing. The early return skips the
    /// *derivation*, which needs a runtime; the policy it already published stays
    /// installed, because a release retracts nothing.
    ///
    /// **This is the endpoint of the stream, so it stands at the commit index.**
    /// The runtime does not publish the index of the entry its committed
    /// configuration came from, and it does not need to: `committed_membership`
    /// is by definition the latest configuration at or below `commit_index`, so
    /// the commit index is a sound position *for an endpoint*.
    ///
    /// **It carries no removal evidence, and that is the whole of what makes it
    /// safe to fold from anywhere.** A commit index is volatile — a recovered
    /// runtime can legitimately report a lower one than the incarnation that
    /// wrote the checkpoint had reached — and an endpoint standing beneath the
    /// register is simply an older observation of the same register, which
    /// [`merge_current_state`] answers by keeping the later one. The gate this
    /// used to need, and the two-cursor apparatus behind it, went with the
    /// provenance tag: nothing here reads a position to decide whether a fact has
    /// been consumed, because every fact is monotone evidence that can be folded
    /// again.
    ///
    /// **What the position still decides is the tie.** A runtime and a restored
    /// record that both stand at the same index are two claims about one
    /// committed configuration, so they must agree once every proven removal and
    /// every spent identity is taken out of both — and if they do not, this
    /// refuses rather than letting the runtime overwrite the record. That is the
    /// one direction in which an endpoint can still do permanent damage: silently
    /// retiring a live replica, or silently raising the floor past an identity the
    /// durable record says was never committed.
    ///
    /// **The raw committed membership is not part of the register**, because it
    /// answers a question no position has an opinion about. This call is where a
    /// driver gets it from the runtime directly rather than from an event, and it
    /// runs last in both construction and adoption so the floor ends those
    /// sequences level with the group the driver now holds.
    ///
    /// **It is not, however, the only publisher of an endpoint, and it is not
    /// sampled per step.** `rafter-app` emits
    /// [`MembershipEvent::CommittedEndpoint`] whenever a step moves the
    /// committed membership with no crossing to carry it, and the driver
    /// reconciles the event stream after every step outcome including errors. So
    /// the floor tracks the runtime without this method being called again; this
    /// one exists for the moment *before* any step, when the driver has just
    /// been handed a group and no event has announced anything.
    ///
    /// # Errors
    ///
    /// Returns [`ControlPlaneCheckpointError::ContradictoryCurrentState`] when
    /// the adopted runtime and the record this driver restored stand at one
    /// position and disagree about the committed membership there. Nothing is
    /// published and nothing moves; see
    /// [`TransportDriverState::check_adopted_membership`], which asks the same
    /// question before an adoption installs the group at all.
    pub(super) fn publish_adopted_membership(&mut self) -> Result<(), ControlPlaneCheckpointError> {
        let Some(group) = self.group.as_ref() else {
            return Ok(());
        };
        let (committed, effective) = Self::adopted_observation(group.runtime());
        self.publish_membership(MembershipFact::Committed {
            committed,
            effective,
        })
    }

    /// Asks whether an offered runtime contradicts what this driver holds,
    /// without installing anything.
    ///
    /// **Adoption's refusals are ordered so that everything above the
    /// installation leaves the driver exactly as it was**, and this belongs in
    /// that half: a runtime whose committed membership disagrees with the
    /// restored record at one position is a supervisor handing over a replica
    /// that must not open, not a replica that opens and then reports itself sick.
    /// So the merge is run against a candidate and its answer discarded, and the
    /// group is installed only if it agrees.
    ///
    /// # Errors
    ///
    /// As [`TransportDriverState::publish_adopted_membership`].
    pub(super) fn check_adopted_membership(
        &self,
        runtime: &R,
    ) -> Result<(), ControlPlaneCheckpointError> {
        let (committed, _) = Self::adopted_observation(runtime);
        let was_spent = {
            let mark = self.committed_id_high_water;
            let live = self.live_committed_members().clone();
            move |node_id: NodeId| {
                mark.is_some_and(|mark| node_id <= mark) && !live.contains(&node_id)
            }
        };
        merge_current_state(
            self.current_committed.as_ref(),
            &IncomingObservation {
                through: committed.through,
                membership: &committed.membership,
                proven_removed: &committed.removed,
            },
            &was_spent,
        )
        .map(|_| ())
    }

    /// The endpoint observation and effective membership one runtime reports.
    ///
    /// Shared by the check and the publication so the question asked before an
    /// adoption is the same one answered after it.
    fn adopted_observation(runtime: &R) -> (CommittedObservation, BTreeSet<NodeId>) {
        (
            CommittedObservation::endpoint(runtime.commit_index(), &runtime.committed_membership()),
            runtime.membership().replica_ids().into_iter().collect(),
        )
    }
}
