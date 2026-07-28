#![allow(clippy::wildcard_imports)]

//! The peer-control-plane record a restarted process reads back.
//!
//! Split from [`super::control_plane`] along the line between a *record* and a
//! *derivation*. That file answers "who is allowed to send a step" — the
//! membership facts, the retirement diff, the peer set, the fences the link
//! layer still owes. This one answers "what does a process that crashed get
//! back, and what may it conclude from it": the type, what makes one valid, and
//! how two of them join.
//!
//! **There is no consumer offset here, and there used to be two.** A record kept
//! a position in the committed configuration stream so a replayed history could
//! be skipped as already-folded, because folding a historical membership against
//! a present one reads as a removal of everything the present added. That is a
//! true hazard and a cursor is the wrong answer to it: a position is a claim
//! about a *prefix*, and nothing a driver observes is a prefix — a
//! snapshot-recovered process that then folds a crossing at index 8 has consumed
//! neither 6 nor 7, and a cursor reading 8 says otherwise. The facts are
//! monotone evidence now, so re-folding one changes nothing and there is nothing
//! to skip. See [`super::control_plane`] for the algebra that makes that true.
//!
//! The state these rules read still lives on
//! [`super::state::TransportDriverState`], like every other field behind the one
//! lock. What lives here is the record's own algebra.

use std::cmp::Ordering;
use std::collections::BTreeSet;

use crate::transport::{AuthenticatedPeerValidator, RaftTransport};

use super::super::*;
use super::state::TransportDriverState;

/// The peer-control-plane state a restarted process cannot re-derive.
///
/// **Caller-owned and durable, like everything else in this repo that has to
/// survive a crash.** Rafter opens no files, so this is a plain value with
/// public fields: read it with
/// [`TransportRaftDriver::control_plane_checkpoint`], write it wherever the
/// embedder keeps its own small metadata, and hand it back at
/// [`TransportRaftDriver::with_control_plane_checkpoint`] or
/// [`TransportRaftDriver::adopt_group_with_checkpoint`].
///
/// **Why Raft cannot reconstruct it.** A driver derives retirement from the
/// *difference* between two committed configurations it observed, and a
/// restarted process observes only the latest one. Compaction then erases the
/// configuration history below the snapshot boundary, so the difference is gone
/// from the log as well. Concretely: committed `{1,2,5}`, node 5 removed, the
/// link layer refuses `fence_peer(5)`, the process crashes. A new process
/// reconstructs committed `{1,2}` and a high-water mark of 2 — the fence is
/// never retried, and node 5 is no longer spent, so the identity the cluster
/// consumed is allocatable again.
///
/// **The three facts.** The mark, the current committed state, and the
/// obligations. Nothing else is here, because everything else about the control
/// plane is re-derived at adoption: the effective membership comes from the
/// runtime, and `published_peers` deliberately does not survive — a new process
/// has a new link layer that has accepted nothing, and starting from "nothing
/// accepted" is what forces the first republication.
///
/// **Bound to one group, and validated against the driver's own at restore.**
/// Retirement is per `(group_id, NodeId)` pair, so a checkpoint's mark and live
/// set describe identities in one group and mean nothing in another. A process
/// that hosts several replicas keeps several of these, and the one thing it must
/// not do is hand a driver the wrong file — which would raise this group's mark
/// past identities it never committed and refuse replicas it has. The group
/// travels *in* the value rather than beside it, so there is no way to persist
/// the checkpoint and forget to persist what it is a checkpoint of.
///
/// **Staleness costs exactly the window it closes.** A crash between a change
/// and its persistence loses that change and no more, which re-opens this
/// window for the removals inside it. The deployment's monotonic `NodeId`
/// allocator remains the cross-process backstop for the identity half — see
/// [`rafter::NodeId`] — and it is the only backstop for a removal committed
/// while no driver was running at all. Persist when
/// [`TransportRaftDriver::control_plane_checkpoint_epoch`] moves, which is on
/// every committed configuration this driver observes and every fence its link
/// layer accepts.
///
/// **A stale checkpoint is a legal input, and joining one can only ever add
/// spent-ness.** That is a property of the join rather than of the caller's
/// discipline: see
/// the crate-internal `restore_control_plane_checkpoint`, which states the
/// three properties — symmetric, order-free, monotone — and proves them. A
/// checkpoint that contradicts the invariants a driver maintains is refused
/// whole with [`ControlPlaneCheckpointError`] and installs nothing, because
/// every way it can be wrong lowers a retirement record.
///
/// # What a snapshot cannot give back
///
/// A replica that catches up by snapshot learns the committed configuration at
/// the snapshot's boundary and **nothing about the configurations that committed
/// and were superseded below it** — they are not in the snapshot and the log
/// that held them is compacted away. So an identity admitted and removed
/// entirely below a boundary this replica installed is one no local reasoning
/// can discover was spent.
///
/// The boundary itself is still worth what it is worth, and the driver already
/// takes it: a snapshot install reaches
/// the crate-internal `observe_committed_members` as an ordinary committed
/// fact, which raises `committed_id_high_water` to the greatest identity the
/// boundary configuration names and retires everything the driver had live that
/// the boundary does not. That is the whole of the cheap improvement available
/// here, and it is why nothing further is attempted: a mark raised past the
/// boundary would be a guess, and a guess in this direction refuses live
/// replicas.
///
/// So the answer is three layers and this type is the middle one. **This
/// checkpoint preserves what this driver itself witnessed**, across a restart,
/// which is the case a snapshot would otherwise erase. The boundary
/// configuration covers what the snapshot still carries. The deployment's
/// monotonic `NodeId` allocator covers what nothing witnessed — a removal that
/// committed while this process was down and was then compacted below a boundary
/// it received. Only the third layer can close that one, which is why
/// [`rafter::NodeId`] states monotonic allocation as a requirement.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct PeerControlPlaneCheckpoint<G> {
    /// The group this checkpoint describes.
    pub group: G,
    /// The greatest `NodeId` this driver has ever seen in a committed
    /// configuration, or `None` before it has seen one.
    ///
    /// Half of the spent test. `None` is not zero: `NodeId(0)` is a legal
    /// identity, and with no committed configuration observed nothing has been
    /// spent.
    pub committed_id_high_water: Option<NodeId>,
    /// The committed membership this record believes is current, and where it
    /// was observed, or `None` before anything was observed.
    ///
    /// The other half of the spent test, and the field the two-fact version of
    /// this checkpoint could not do without. A mark restored beside an empty
    /// live set spends every identity at or below it — the whole cluster — and a
    /// live set re-derived from the adopted group's committed configuration
    /// instead would *un-spend* an identity a violating readmission committed
    /// while this process was down.
    ///
    /// **The position travels inside it**, and that is what the round-8 pair of
    /// a set beside an offset could not express. Two honest records disagreeing
    /// about the current membership are two observations from different
    /// positions, and only the position decides between them; a record that
    /// carried the two apart could be joined by uniting the memberships and
    /// taking the greater position, which answers "who is a member now" with the
    /// union of two different nows. The join that does it correctly is the
    /// crate-internal `restore_control_plane_checkpoint`.
    pub current_committed: Option<CurrentCommittedState>,
    /// Committed removals whose fence the link layer has not accepted.
    ///
    /// One entry per unfenced removal, and nothing here ever discards one: a
    /// committed fact is not a request. Retention across restarts is therefore
    /// the embedder's, stated at
    /// [`TransportDriverOptions::fence_backlog_service_threshold`].
    pub pending_fences: BTreeSet<NodeId>,
}

/// The committed membership a record believes is current, and where it looked.
///
/// **One value rather than a set beside a position, because the two are only
/// meaningful together.** "Who is committed" is not a fact a record accumulates,
/// it is an answer that was true somewhere; a membership without its position
/// cannot be compared against another membership, and a position without its
/// membership licenses nothing. Carrying them apart let a join take the union of
/// the memberships and the maximum of the positions, which is a state neither
/// record ever held and which silently drops every removal that happened between
/// them.
///
/// `membership` is the *live* reading: the observed committed configuration less
/// every identity a committed removal has spent. An already-spent identity that
/// a later configuration names again is filtered out here rather than obeyed —
/// [`RaftTransport::fence_peer`] has no inverse, so re-authorizing it is a
/// promise the link layer cannot keep — and the raw fact is kept beside it on
/// the driver so the violation stays countable.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct CurrentCommittedState {
    /// The log position this observation stands at.
    ///
    /// A configuration entry's own index when a crossing produced it, and this
    /// replica's commit index when an endpoint observation did. The two are
    /// comparable because both name a point in one log at which the committed
    /// membership is exactly what this record holds — which is all the ordering
    /// needs, and is why no separate position per provenance is kept.
    pub through: LogIndex,
    /// The committed membership observed there, less every spent identity.
    ///
    /// Bounded by the size of the cluster.
    pub membership: BTreeSet<NodeId>,
}

impl CurrentCommittedState {
    /// A committed membership observed at `through`.
    #[must_use]
    pub fn new(through: LogIndex, membership: BTreeSet<NodeId>) -> Self {
        Self {
            through,
            membership,
        }
    }
}

impl<G> PeerControlPlaneCheckpoint<G> {
    /// The checkpoint a first incarnation over empty storage would have written.
    ///
    /// Nothing observed, nothing spent, nothing owed, nothing consumed. This is
    /// the honest value for a process whose durable checkpoint file does not
    /// exist yet, and it is what [`TransportRaftDriver::new`] and
    /// [`TransportRaftDriver::adopt_group`] pass on the caller's behalf. It is
    /// *not* the right value for a process whose file is unreadable, nor for one
    /// whose file is merely *missing* beside durable state that proves the
    /// replica has run before: see [`PeerControlPlaneCheckpoint`] for why a
    /// restart that starts from nothing is precisely the failure this type
    /// exists to prevent.
    #[must_use]
    pub fn empty(group: G) -> Self {
        Self {
            group,
            committed_id_high_water: None,
            current_committed: None,
            pending_fences: BTreeSet::new(),
        }
    }

    /// Whether this checkpoint's own record says `node_id` is spent.
    ///
    /// The same two reads [`TransportDriverState::is_spent`] makes, against a
    /// recovered record rather than a live driver, and the primitive the merge
    /// below is written in terms of. An identity *above* this record's mark is
    /// not spent by it and not live in it either — this record simply has no
    /// opinion, which is what lets two records with different marks be joined
    /// without either overruling the other outside what it saw.
    fn spends(&self, node_id: NodeId) -> bool {
        self.committed_id_high_water
            .is_some_and(|mark| node_id <= mark)
            && !self.names(node_id)
    }

    /// Whether this record's current state names `node_id` as live.
    fn names(&self, node_id: NodeId) -> bool {
        self.current_committed
            .as_ref()
            .is_some_and(|current| current.membership.contains(&node_id))
    }

    /// The membership of this record's current state, or the empty set.
    fn membership(&self) -> BTreeSet<NodeId> {
        self.current_committed
            .as_ref()
            .map(|current| current.membership.clone())
            .unwrap_or_default()
    }

    /// Drops a current state that no observation produced.
    ///
    /// The join builds one unconditionally so the arithmetic above has somewhere
    /// to land, and joining two empty records must still yield an empty record
    /// rather than a state at position zero — which the coupling biconditional
    /// would then refuse.
    fn without_empty_state(mut self) -> Self {
        if self.committed_id_high_water.is_none() && self.pending_fences.is_empty() {
            self.current_committed = None;
        }
        self
    }

    /// Refuses a checkpoint that contradicts the invariants a driver maintains.
    ///
    /// Every clause holds by construction for a checkpoint a driver produced, so
    /// each failure means the durable record was damaged, truncated, or belongs
    /// to another replica — and each one lowers a retirement record in the
    /// dangerous direction if it is absorbed instead of refused.
    ///
    /// * A live set needs a mark, and every live identity sits at or below it:
    ///   `observe_committed_members` raises the mark to the greatest identity of
    ///   the configuration it is assigning as live, in the same call, so the two
    ///   can never disagree. A lowered mark is how a corrupted record un-retires
    ///   everything above it.
    /// * **Every pending fence names an identity this record saw spent**, which
    ///   is `pending_fences ⊆ spent`. The fence set is extended with exactly the
    ///   difference the live assignment removes, so a fenced identity is one that
    ///   was live, is no longer, and therefore sits at or below the mark. The two
    ///   ways to break that are separate variants because they fail in opposite
    ///   directions and an operator needs to know which:
    ///   [`ControlPlaneCheckpointError::FenceNamesLiveMember`] for a fence
    ///   naming an identity this record still calls live, and
    ///   [`ControlPlaneCheckpointError::FenceNamesUnspentIdentity`] for one
    ///   naming an identity above the mark, which no committed configuration
    ///   here ever named at all.
    ///
    /// The second is the clause a live-set comparison alone cannot state, and it
    /// is the one that survives the join rather than being caught by it: the
    /// joined mark rises to cover an identity the *other* record calls live, the
    /// obligation travels with it, and the next flush publishes that replica to
    /// the link layer and then permanently fences it. [`RaftTransport::fence_peer`]
    /// has no inverse, so unlike a stale peer set that is not something a later
    /// flush corrects.
    ///
    /// * **The mark and the current state are one record, and the coupling is a
    ///   biconditional.** Deleting the two offsets collapsed three clauses into
    ///   this one, and the collapse is a consequence rather than a
    ///   simplification: what the offsets were coupled *to* is now the only
    ///   other thing here.
    ///
    ///   Every observation of a committed configuration raises the mark — a
    ///   committed configuration always names at least one replica, since
    ///   [`rafter::MembershipSet`] refuses an empty voter set — and assigns the
    ///   current state in the same call, because a first observation is always
    ///   the latest one this record has. So `mark.is_some() ⟺
    ///   current_committed.is_some()`, and the two ways to break it are separate
    ///   variants because they fail in opposite directions:
    ///   [`ControlPlaneCheckpointError::RetirementWithoutCurrentState`] leaves a
    ///   mark and obligations with nothing to compare them against — every
    ///   identity at or below the mark reads as spent, which is the whole
    ///   cluster — and
    ///   [`ControlPlaneCheckpointError::CurrentStateWithoutRetirement`] is a
    ///   membership this record could not have observed, since observing it
    ///   would have raised a mark.
    ///
    ///   An embedder with a format of its own should refuse the same two shapes
    ///   at its own decoder, so the refusal names the file rather than the
    ///   value.
    fn validate(&self, group: &G) -> Result<(), ControlPlaneCheckpointError>
    where
        G: Ord,
    {
        if &self.group != group {
            return Err(ControlPlaneCheckpointError::ForeignGroup);
        }
        // **One record, checked as one.** A mark is raised by an observation and
        // an observation assigns the current state, so neither stands alone;
        // obligations are the residue of a removal, which is an observation too.
        let retired_something =
            self.committed_id_high_water.is_some() || !self.pending_fences.is_empty();
        match (self.current_committed.is_some(), retired_something) {
            (false, true) => {
                return Err(ControlPlaneCheckpointError::RetirementWithoutCurrentState)
            }
            (true, false) => {
                return Err(ControlPlaneCheckpointError::CurrentStateWithoutRetirement)
            }
            (false, false) | (true, true) => {}
        }
        for node_id in self.membership() {
            let Some(mark) = self.committed_id_high_water else {
                // Unreachable behind the biconditional above, and kept because
                // the loop must not read a mark it has not proved is there.
                return Err(ControlPlaneCheckpointError::RetirementWithoutCurrentState);
            };
            if node_id > mark {
                return Err(ControlPlaneCheckpointError::LiveMemberAboveMark { node_id, mark });
            }
        }
        for node_id in self.pending_fences.iter().copied() {
            if self.names(node_id) {
                return Err(ControlPlaneCheckpointError::FenceNamesLiveMember { node_id });
            }
            if !self.spends(node_id) {
                return Err(ControlPlaneCheckpointError::FenceNamesUnspentIdentity { node_id });
            }
        }
        Ok(())
    }
}

/// Chooses between two current states and names what the pair proves.
///
/// The later observation wins and the earlier one is not discarded: the
/// identities it named that the later one does not are the removals that
/// happened between the two positions. That is the only inference here, and it
/// is the one neither record can make alone.
///
/// # Errors
///
/// [`ControlPlaneCheckpointError::ContradictoryCurrentState`] when the two stand
/// at one position and disagree about the membership there.
fn join_current_state<G>(
    held: &PeerControlPlaneCheckpoint<G>,
    incoming: &PeerControlPlaneCheckpoint<G>,
) -> Result<(CurrentCommittedState, BTreeSet<NodeId>), ControlPlaneCheckpointError> {
    match (
        held.current_committed.as_ref(),
        incoming.current_committed.as_ref(),
    ) {
        (None, None) => Ok((
            CurrentCommittedState::new(LogIndex::ZERO, BTreeSet::new()),
            BTreeSet::new(),
        )),
        (Some(only), None) | (None, Some(only)) => Ok((only.clone(), BTreeSet::new())),
        (Some(held), Some(incoming)) => {
            let (older, newer) = match held.through.cmp(&incoming.through) {
                Ordering::Less => (held, incoming),
                Ordering::Greater => (incoming, held),
                Ordering::Equal if held.membership == incoming.membership => (held, incoming),
                Ordering::Equal => {
                    return Err(ControlPlaneCheckpointError::ContradictoryCurrentState {
                        through: held.through,
                    })
                }
            };
            let inferred = older
                .membership
                .difference(&newer.membership)
                .copied()
                .collect();
            Ok((newer.clone(), inferred))
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
    /// Records that the checkpointable control-plane state moved.
    ///
    /// Called wherever one of the checkpoint fields changes and nowhere else, so
    /// an embedder that persists on every epoch move persists exactly the
    /// changes it must not lose. Saturating rather than wrapping: an epoch that
    /// wrapped past a caller's last-persisted value would report "no change" for
    /// a state that had changed, and a driver that reached `u64::MAX`
    /// configuration changes has an embedder that should persist unconditionally
    /// from then on.
    pub(super) fn advance_checkpoint_epoch(&mut self) {
        self.checkpoint_epoch = self.checkpoint_epoch.saturating_add(1);
    }

    /// Returns the peer-control-plane state this driver's embedder must make
    /// durable.
    pub(super) fn control_plane_checkpoint(&self) -> PeerControlPlaneCheckpoint<G> {
        PeerControlPlaneCheckpoint {
            group: self.group_id.clone(),
            committed_id_high_water: self.committed_id_high_water,
            current_committed: self.current_committed.clone(),
            pending_fences: self.pending_fences.clone(),
        }
    }

    /// The membership this driver's current state names, or the empty set.
    pub(super) fn live_committed_members(&self) -> &BTreeSet<NodeId> {
        static NONE: BTreeSet<NodeId> = BTreeSet::new();
        self.current_committed
            .as_ref()
            .map_or(&NONE, |current| &current.membership)
    }

    /// Joins a recovered checkpoint into what this driver holds, before any
    /// membership fact is derived from the adopted group.
    ///
    /// **Order is the whole contract.** The spent test reads the mark and the
    /// live set together, so both must be in place before any committed
    /// configuration is observed — otherwise a recovered mark of 5 meets an empty
    /// live set and spends every identity at or below it. With both installed,
    /// what follows is the ordinary observation it always was: a recovered mark
    /// of 5 beats a reconstructed committed set of `{1,2}`, and an identity a
    /// removal spent stays spent even if the cluster names it again.
    ///
    /// **A lattice join, not three independent merges, and the current state is
    /// where that matters.** Taking the union of the two memberships was wrong
    /// twice over, in the two directions this mechanism exists to prevent.
    ///
    /// It un-spent a witnessed removal: a stale-but-valid record holding
    /// `{mark 5, live {1,2,5}}` joined into a driver holding `{mark 5, live
    /// {1,2}}` produced live `{1,2,5}`, and the identity the cluster consumed
    /// became adoptable again. And it dropped a removal *neither* record
    /// witnessed but the two jointly prove: an older `{through 7, mark 5, live
    /// {1,2,3,5}}` beside a later snapshot-derived `{through 10, mark 3, live
    /// {1,2,3}}` says node 5 was in the committed membership at position 7 and
    /// is not in it at position 10, which is a committed removal — while the
    /// union kept 5 live, unspent and unfenced. The missing fact was not in
    /// either record; it was *between* them, and only a positioned current state
    /// can see it.
    ///
    /// Write `spent_x(n) = n ≤ mark_x ∧ n ∉ live_x`, and let `older` and `newer`
    /// be the two current states ordered by `through`. Then:
    ///
    /// ```text
    /// mark     = max(mark_a, mark_b)
    /// inferred = older.membership \ newer.membership
    /// spent    = S_a ∪ S_b ∪ inferred
    /// current  = { through: newer.through, membership: newer.membership \ spent }
    /// fences   = fences_a ∪ fences_b ∪ (inferred \ (S_a ∪ S_b))
    /// ```
    ///
    /// An identity *above* one side's mark is judged only by the side whose mark
    /// covers it, which is what lets a record that never saw an identity avoid
    /// overruling one that did. The inferred set is the exception and earns it:
    /// it is judged by both, because being named at one position and absent at a
    /// later one is a two-record fact.
    ///
    /// **Equal positions with different memberships are refused rather than
    /// merged** — see [`ControlPlaneCheckpointError::ContradictoryCurrentState`].
    /// The committed membership at one position is one set, so two records
    /// disagreeing there are not two observations to reconcile; picking either
    /// would be choosing which record to believe, and merging them would invent
    /// a third. Unreachable for a cluster that keeps the single-use contract:
    /// each side's filter removes only identities a committed removal spent, and
    /// under the contract no such identity is named again at a later position,
    /// so the filters agree wherever the positions do.
    ///
    /// **The three properties, which are what make it safe to apply in any
    /// order.** Let `S_x` be the spent set of `x` and `L_x` its live set.
    ///
    /// 1. *Symmetric.* Every operator above is symmetric in `a` and `b`: `max`,
    ///    `∪`, and "the later of the two, refusing a tie that disagrees" — which
    ///    is symmetric precisely because the tie is refused rather than broken.
    /// 2. *Order-free.* `S_join ⊇ S_a ∪ S_b` (take `n ∈ S_a`: `n ≤ mark_a ≤
    ///    mark_join`, and `n` is filtered out of the joined membership by
    ///    construction). Position is `max`, which is associative, and the joined
    ///    membership is the newest observation minus an accumulated spent set —
    ///    so a third record joined afterwards sees the same newest observation
    ///    and a spent set that only grew, whichever order the first two arrived
    ///    in. Idempotent: joining `a` with itself leaves the position equal, the
    ///    memberships equal, `inferred` empty, and `L_a \ S_a = L_a`.
    /// 3. *Monotone in spent-ness.* From property 2, every identity either side
    ///    had witnessed spent is spent in the join and in every later join.
    ///    **A witnessed removal cannot be undone by any merge order**, and an
    ///    inferred one cannot either: it leaves the joined membership, and the
    ///    filter above keeps a later observation from putting it back.
    ///
    /// Obligations are still the union, because a driver can hold fences of its
    /// own before a checkpoint is restored and losing either set would be losing
    /// a fence. **An inferred removal contributes a fence only when neither side
    /// already knew the identity was spent**, which is what keeps one committed
    /// removal to exactly one obligation: a side that knew either still owes the
    /// fence, and it is in that side's set, or the link layer already accepted
    /// it and nothing is owed.
    ///
    /// **The joined candidate is validated too, and that is deliberate
    /// redundancy.** The proof above says it cannot fail — `fences ⊆ spent` is
    /// preserved because a fence of `a` is in `S_a` and every element of `S_a`
    /// is excluded from the joined membership while sitting at or below the
    /// joined mark, and an inferred fence is spent by the same construction. The
    /// coupling biconditional is preserved because the mark is a `max` and the
    /// current state is a choice between two: the join has a mark exactly when a
    /// side did, and a current state exactly when a side did.
    ///
    /// The properties are nonetheless *executed* rather than only argued,
    /// because the cost is one pass over a cluster-sized set and the thing being
    /// protected is a permanent, uninvertible fence on a live replica. A proof
    /// that stops holding because someone edited the join is a proof that fails
    /// silently; this one fails loudly.
    ///
    /// # Errors
    ///
    /// Returns [`ControlPlaneCheckpointError`] when the checkpoint names another
    /// group or contradicts the invariants a driver maintains for one, when the
    /// two records contradict each other at one position, and when the joined
    /// candidate would contradict the invariants. **Nothing is mutated on that
    /// path** — the join is computed into a candidate and validated before the
    /// first field moves — so a caller that refuses to open is left with a driver
    /// in exactly the state it was.
    pub(super) fn restore_control_plane_checkpoint(
        &mut self,
        checkpoint: PeerControlPlaneCheckpoint<G>,
    ) -> Result<(), ControlPlaneCheckpointError> {
        checkpoint.validate(&self.group_id)?;

        let held = self.control_plane_checkpoint();
        let committed_id_high_water = match (
            held.committed_id_high_water,
            checkpoint.committed_id_high_water,
        ) {
            (Some(held), Some(restored)) => Some(held.max(restored)),
            (held, None) => held,
            (None, restored) => restored,
        };
        // The two-record fact, and the only thing here that neither record
        // states on its own: an identity named at one position and absent at a
        // later one was removed between them.
        let (current, inferred) = join_current_state(&held, &checkpoint)?;
        let already_spent = |node_id: NodeId| held.spends(node_id) || checkpoint.spends(node_id);
        let membership = current
            .membership
            .iter()
            .copied()
            .filter(|node_id| !already_spent(*node_id))
            .collect();
        // Only what neither side already knew. A removal one side had witnessed
        // is either still owed in that side's obligations or already discharged,
        // and re-deriving it would owe a second fence for one committed fact.
        let owed = inferred
            .into_iter()
            .filter(|id| !already_spent(*id))
            .collect::<Vec<_>>();
        let mut pending_fences = checkpoint.pending_fences;
        pending_fences.extend(held.pending_fences);
        pending_fences.extend(owed);

        let joined = PeerControlPlaneCheckpoint {
            group: self.group_id.clone(),
            committed_id_high_water,
            current_committed: Some(CurrentCommittedState::new(current.through, membership)),
            pending_fences,
        }
        .without_empty_state();
        joined.validate(&self.group_id)?;

        // The raw committed floor is deliberately not restored from here. It
        // answers "what does this replica's own stream say the cluster has
        // committed now", a record says what this driver has *spent*, and both
        // entry points that reach this call publish the runtime's endpoint
        // afterwards — so the floor is assigned from the runtime before the
        // driver serves anything.
        self.committed_id_high_water = joined.committed_id_high_water;
        self.current_committed = joined.current_committed;
        self.pending_fences = joined.pending_fences;
        self.advance_checkpoint_epoch();
        Ok(())
    }
}
