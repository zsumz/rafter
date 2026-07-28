#![allow(clippy::wildcard_imports)]

//! One membership statement per batch of facts, or none at all.
//!
//! **The staged transaction, and it exists because a retirement floor is
//! permanent.** The facts that license a publication do not arrive one at a time.
//! An adoption offers a durable record *and* a runtime; one step's report can
//! carry an effective change, a crossing, and a committed endpoint together; a
//! recovery replay carries every crossing it produced in one report. Each of
//! those used to be merged into live state and flushed to the link layer the
//! moment it was read — so a contradiction in the second fact arrived after the
//! first had already been written into the checkpoint an embedder persists and
//! stated to the transport, and neither of those is retractable.
//!
//! So every site here does the same three things. Clone the membership fields
//! into a [`MembershipCandidate`]; fold every fact of the batch into it, keeping
//! the first refusal; and install the candidate — with exactly one
//! [`TransportDriverState::flush_peer_policy`] behind it — only if the whole
//! batch survived. A batch that refuses leaves the driver holding the last
//! consistent state it had, not the prefix that happened to parse, and tells the
//! link layer nothing.
//!
//! **What is deliberately outside the transaction is everything loss-tolerant.**
//! A report's peer messages, snapshot directives, and proposal and read
//! resolutions route normally, before and after a refusal alike: Raft re-sends a
//! dropped frame, a waiter resolved is a waiter that stays resolved, and neither
//! is a permanent statement about who may speak. Only the control-plane statement
//! is transactional, because only it cannot be taken back.
//!
//! The candidate holds the same four fields the driver does, and no more. The two
//! checkpointable ones — the mark and the register — are what an embedder
//! persists; the two live ones are what admission and the local service state are
//! read from. Splitting them across two transactions would let a refusal leave a
//! published peer set describing a membership the durable record does not agree
//! with, which is the same defect one layer down.

use std::collections::BTreeSet;

use crate::transport::{AuthenticatedPeerValidator, RaftTransport};

use super::super::*;
use super::checkpoint::{
    merge_current_state, restore_checkpoint, CurrentCommittedState, IncomingObservation,
};
use super::observation::{
    observed_membership, CommittedObservation, MembershipFact, ObservedMembership,
};
use super::state::TransportDriverState;

/// The membership fields of one driver, staged.
///
/// Every field here is one the driver installs as a unit or not at all. The
/// candidate is a plain value with no transport, no group, and no epoch: it can
/// refuse, and refusing costs a drop.
pub(super) struct MembershipCandidate {
    /// The configuration this replica is operating under, as last reported.
    pub(super) effective_members: BTreeSet<NodeId>,
    /// The configuration the cluster has committed, raw as reported.
    pub(super) committed_members: BTreeSet<NodeId>,
    /// The committed membership this candidate believes is current, positioned.
    pub(super) current_committed: Option<CurrentCommittedState>,
    /// The greatest `NodeId` any committed configuration has named.
    pub(super) committed_id_high_water: Option<NodeId>,
}

impl MembershipCandidate {
    /// The membership this candidate's register names, or the empty set.
    fn live_members(&self) -> &BTreeSet<NodeId> {
        static NONE: BTreeSet<NodeId> = BTreeSet::new();
        self.current_committed
            .as_ref()
            .map_or(&NONE, |current| &current.membership)
    }

    /// Whether a committed removal has consumed `node_id`, by this candidate's
    /// own record.
    ///
    /// The same two reads [`TransportDriverState::is_spent`] makes, against the
    /// staged fields rather than the installed ones — which is what lets the
    /// adoption gate ask about an offered node ID before anything is installed.
    pub(super) fn is_spent(&self, node_id: NodeId) -> bool {
        self.committed_id_high_water
            .is_some_and(|mark| node_id <= mark)
            && !self.live_members().contains(&node_id)
    }

    /// The spent test as it stood *before* the fact being folded.
    ///
    /// Read against a mark the incoming fact had already raised, an identity this
    /// driver has simply not observed yet — every identity at all, on the very
    /// first fact — would test as spent, and the membership the fact names would
    /// filter down to nothing. So the closure captures the mark and the live set
    /// first, and the fold reads it.
    fn spent_before(&self) -> impl Fn(NodeId) -> bool {
        let mark = self.committed_id_high_water;
        let live = self.live_members().clone();
        move |node_id: NodeId| mark.is_some_and(|mark| node_id <= mark) && !live.contains(&node_id)
    }

    /// Folds one membership fact into this candidate.
    ///
    /// The committed half goes first, because it is the one that can refuse: an
    /// effective membership assigned ahead of a refusal would be half a fact
    /// applied, and the candidate is dropped whole on a refusal precisely so that
    /// cannot matter to the driver.
    ///
    /// # Errors
    ///
    /// Returns [`ControlPlaneCheckpointError::ContradictoryCurrentState`] when
    /// the fact and this candidate stand at one position and disagree about the
    /// membership there.
    fn apply(&mut self, fact: MembershipFact) -> Result<(), ControlPlaneCheckpointError> {
        match fact {
            // Assigned, not merged. The effective configuration moves in both
            // directions — a new leader can truncate an uncommitted one back off
            // the log — and it still cannot narrow what this driver authorizes,
            // because every derivation takes it in union with the committed floor
            // and the register.
            MembershipFact::Effective(effective) => self.effective_members = effective,
            MembershipFact::Committed {
                committed,
                effective,
            } => {
                self.observe_committed(committed)?;
                self.effective_members = effective;
            }
        }
        Ok(())
    }

    /// Takes one committed membership fact: the removals it proves, the
    /// high-water mark, and the current state the spent test reads.
    ///
    /// The only place identity is *consumed*, and everything about consumption is
    /// here: which IDs a removal spends, which a violating fact is refused for,
    /// and how far allocation has got.
    ///
    /// # Why this needs no cursor
    ///
    /// A restart hands this the same facts twice: the runtime replays every
    /// configuration entry above the application's applied floor, oldest first,
    /// beneath a current state that has already moved past them. What made that
    /// dangerous was deriving removals by subtraction from the driver's own
    /// membership, which is right only when that membership stands exactly where
    /// the fact does. Two cursors were kept so a fact standing anywhere else
    /// could be skipped.
    ///
    /// Every operation below is monotone evidence instead, so re-folding one
    /// changes nothing and there is nothing left to skip:
    ///
    /// * **Removals come from the fact.** A crossing carries its own transition,
    ///   computed by the kernel where the chronology is known, so
    ///   `previous − configuration` is the same set at every replay. An endpoint
    ///   carries none and asserts none.
    /// * **The mark is a maximum**, taken over both ends of the fact.
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
    /// # Errors
    ///
    /// Returns [`ControlPlaneCheckpointError::ContradictoryCurrentState`] when
    /// this fact and the register stand at one position and still disagree about
    /// the membership there after normalization. **Nothing is mutated on that
    /// path**: every value is computed into a local and assigned only once the
    /// merge has answered.
    fn observe_committed(
        &mut self,
        fact: CommittedObservation,
    ) -> Result<(), ControlPlaneCheckpointError> {
        let was_spent = self.spent_before();
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
        Ok(())
    }

    /// Asks whether one committed fact would contradict this candidate, without
    /// folding it.
    ///
    /// The adoption precheck. A runtime whose committed membership disagrees with
    /// the record at a position both observed is a supervisor handing over a
    /// replica that must not open, not a replica that opens and then reports
    /// itself sick — so the merge is run and its answer discarded, and the whole
    /// candidate is installed only if it agrees.
    ///
    /// # Errors
    ///
    /// As [`MembershipCandidate::observe_committed`].
    fn probe(&self, fact: &CommittedObservation) -> Result<(), ControlPlaneCheckpointError> {
        let was_spent = self.spent_before();
        merge_current_state(
            self.current_committed.as_ref(),
            &IncomingObservation {
                through: fact.through,
                membership: &fact.membership,
                proven_removed: &fact.removed,
            },
            &was_spent,
        )
        .map(|_| ())
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
    /// Stages this driver's membership fields.
    pub(super) fn membership_candidate(&self) -> MembershipCandidate {
        MembershipCandidate {
            effective_members: self.effective_members.clone(),
            committed_members: self.committed_members.clone(),
            current_committed: self.current_committed.clone(),
            committed_id_high_water: self.committed_id_high_water,
        }
    }

    /// Installs a candidate that survived its whole batch, and states it once.
    ///
    /// The flush is the single publication the transaction promises — after every
    /// field is in place, so the policy it derives describes the whole batch
    /// rather than a prefix of it.
    pub(super) fn install_membership(&mut self, candidate: MembershipCandidate) {
        self.assign_membership(candidate);
        self.flush_peer_policy();
    }

    /// Installs a candidate a *record* produced, and states nothing yet.
    ///
    /// **A restored record is not on its own a publishable statement**, and both
    /// entry points that restore one reach the runtime's endpoint immediately
    /// afterwards. A record says what this driver has spent; what the link layer
    /// is owed is derived once the record and the runtime have met, and that
    /// meeting can still refuse. Publishing between the two would state a floor
    /// licensed by half the inputs — which is the same defect the transaction
    /// exists for, one level up.
    pub(super) fn install_restored_membership(&mut self, candidate: MembershipCandidate) {
        self.assign_membership(candidate);
    }

    /// Moves the staged fields onto the driver, advancing the epoch if the
    /// checkpointable half moved.
    ///
    /// The epoch moves only if the *checkpointable* half actually changed, which
    /// is the contract an embedder persists against: a batch whose facts the
    /// driver had already absorbed asks nobody to write a file.
    fn assign_membership(&mut self, candidate: MembershipCandidate) {
        let before = self.control_plane_checkpoint();
        self.effective_members = candidate.effective_members;
        self.committed_members = candidate.committed_members;
        self.current_committed = candidate.current_committed;
        self.committed_id_high_water = candidate.committed_id_high_water;
        if before != self.control_plane_checkpoint() {
            self.advance_checkpoint_epoch();
        }
    }

    /// Folds every membership event of one report into one candidate and
    /// installs it, or installs none of them.
    ///
    /// **A contradiction discovered here is recorded rather than returned**, and
    /// there is nowhere else for it to go: this runs from `route_report` and
    /// `reconcile_membership`, which are reached from every step outcome
    /// including a failing one. What it must never do is publish anyway —
    /// [`TransportDriverState::install_membership`] is not reached on that path,
    /// so the driver keeps the state and the policy it had, and
    /// [`DriverServiceState::ContradictoryCurrentState`] is how a supervisor
    /// hears about it.
    pub(super) fn route_membership_events(&mut self, events: &[MembershipEvent<G>]) {
        // The refusal is already recorded on the state by the time this returns;
        // there is no caller here that could act on a second copy of it.
        let _ = self.absorb_membership_events(events);
    }

    fn absorb_membership_events(
        &mut self,
        events: &[MembershipEvent<G>],
    ) -> Result<(), ControlPlaneCheckpointError> {
        // **A batch of nothing installs nothing and states nothing.** Most steps
        // move no membership at all, and the error-path reconciliation is empty
        // after every successful one — so without this the commonest path in the
        // driver would re-derive and re-attempt a policy the link layer has
        // already been offered, turning a refused publication's retry into
        // something that happens per step rather than per entry point. The
        // entry points flush on their own; this is a reconciliation.
        if events.is_empty() {
            return Ok(());
        }
        let mut candidate = self.membership_candidate();
        for event in events {
            let fact = match observed_membership(event) {
                ObservedMembership::Effective(effective) => MembershipFact::Effective(effective),
                // The runtime is the authority on what is in effect, and it
                // agrees with the effective event that preceded this one in the
                // same report. A driver holding no group keeps what the candidate
                // had rather than assigning an empty set: an absent effective
                // membership must not turn a retirement into a silence, and must
                // not narrow anything either.
                ObservedMembership::Committed(committed) => MembershipFact::Committed {
                    committed,
                    effective: self
                        .runtime_effective_members()
                        .unwrap_or_else(|| candidate.effective_members.clone()),
                },
                ObservedMembership::Nothing => continue,
            };
            if let Err(reason) = candidate.apply(fact) {
                self.record_contradiction(reason);
                return Err(reason);
            }
        }
        self.install_membership(candidate);
        Ok(())
    }

    /// Records a contradiction so a supervisor polling
    /// [`TransportDriverState::service_state`] sees it.
    ///
    /// Only the one variant is terminal for the incarnation. The others are
    /// refusals of an *input* — a damaged record, a foreign group, a record older
    /// than what this driver holds — and every one of them is raised where a
    /// caller can still be told, so recording them here would report a driver as
    /// sick for a file it declined to open.
    fn record_contradiction(&mut self, reason: ControlPlaneCheckpointError) {
        if let ControlPlaneCheckpointError::ContradictoryCurrentState { through } = reason {
            self.contradicted_at = Some(through);
        }
    }

    /// The contradiction this driver recorded while routing, if it recorded one.
    ///
    /// **The only way a refusal escapes the routing path.**
    /// [`TransportDriverState::route_membership_events`] has nowhere to return
    /// one — it runs from every step outcome, including a failing one — so it
    /// records instead, and the two entry points that *can* refuse ask here once
    /// their recovery outputs have been routed. A driver that opened over a
    /// durable record its own replayed history contradicts would be serving from
    /// inputs it has already declared untrustworthy.
    pub(super) fn recorded_contradiction(&self) -> Option<ControlPlaneCheckpointError> {
        self.contradicted_at
            .map(|through| ControlPlaneCheckpointError::ContradictoryCurrentState { through })
    }

    /// Joins a recovered checkpoint into a candidate and asks the offered runtime
    /// whether it agrees, installing nothing either way.
    ///
    /// **The adoption transaction, and both halves have to be in it.** The join
    /// moves the mark, the register, and therefore the checkpoint an embedder
    /// persists; the runtime beside it can contradict the result at a position
    /// both stand at. Running the join against live state and the check
    /// afterwards left a refused adoption holding durable state recovered from the
    /// very input it had just declared contradictory — and the epoch move telling
    /// the embedder to write it down.
    ///
    /// The returned candidate carries the *record* and not the runtime's
    /// endpoint. The endpoint is folded afterwards by
    /// [`TransportDriverState::publish_adopted_membership`], once the recovery
    /// outputs have been replayed, because a recovered runtime's endpoint is
    /// newer than its own history and folding it first reads every replayed
    /// crossing as a removal of what the endpoint added.
    ///
    /// # Errors
    ///
    /// Returns [`ControlPlaneCheckpointError`] when the checkpoint names another
    /// group, contradicts the invariants a driver maintains for one, stands
    /// before what this driver already holds, or disagrees with either this
    /// driver's register or the offered runtime at a position they share.
    pub(super) fn adoption_candidate(
        &self,
        checkpoint: PeerControlPlaneCheckpoint<G>,
        runtime: &R,
    ) -> Result<MembershipCandidate, ControlPlaneCheckpointError> {
        let mut candidate = self.membership_candidate();
        restore_checkpoint(&mut candidate, checkpoint, &self.group_id)?;
        let (committed, _) = Self::adopted_observation(runtime);
        candidate.probe(&committed)?;
        Ok(candidate)
    }

    /// Joins a recovered checkpoint into this driver, with no runtime to check it
    /// against yet.
    ///
    /// Construction's half of the same transaction. The group is already this
    /// driver's by the time this runs, so there is nothing to refuse an adoption
    /// of — what the restore protects is the same thing: a record that cannot be
    /// installed whole is installed not at all.
    ///
    /// # Errors
    ///
    /// As [`TransportDriverState::adoption_candidate`], less the runtime probe.
    pub(super) fn restore_control_plane_checkpoint(
        &mut self,
        checkpoint: PeerControlPlaneCheckpoint<G>,
    ) -> Result<(), ControlPlaneCheckpointError> {
        let mut candidate = self.membership_candidate();
        restore_checkpoint(&mut candidate, checkpoint, &self.group_id)?;
        self.install_restored_membership(candidate);
        Ok(())
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
    /// **It is not the only publisher of an endpoint, and it is not sampled per
    /// step.** `rafter-app` emits [`MembershipEvent::CommittedEndpoint`] whenever
    /// a step moves the committed membership with no crossing to carry it, and the
    /// driver reconciles the event stream after every step outcome including
    /// errors. So the floor tracks the runtime without this method being called
    /// again; this one exists for the moment *before* any step, when the driver
    /// has just been handed a group and no event has announced anything.
    ///
    /// # Errors
    ///
    /// Returns [`ControlPlaneCheckpointError::ContradictoryCurrentState`] when
    /// the adopted runtime and the record this driver restored stand at one
    /// position and disagree about the committed membership there. Nothing is
    /// published and nothing moves.
    pub(super) fn publish_adopted_membership(&mut self) -> Result<(), ControlPlaneCheckpointError> {
        let Some(group) = self.group.as_ref() else {
            return Ok(());
        };
        let (committed, effective) = Self::adopted_observation(group.runtime());
        let mut candidate = self.membership_candidate();
        if let Err(reason) = candidate.apply(MembershipFact::Committed {
            committed,
            effective,
        }) {
            self.record_contradiction(reason);
            return Err(reason);
        }
        self.install_membership(candidate);
        Ok(())
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

    /// The endpoint observation and effective membership one runtime reports.
    ///
    /// Shared by the adoption probe and the publication so the question asked
    /// before an adoption is the same one answered after it.
    fn adopted_observation(runtime: &R) -> (CommittedObservation, BTreeSet<NodeId>) {
        (
            CommittedObservation::endpoint(runtime.commit_index(), &runtime.committed_membership()),
            runtime.membership().replica_ids().into_iter().collect(),
        )
    }
}
