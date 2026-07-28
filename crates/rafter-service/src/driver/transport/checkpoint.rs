#![allow(clippy::wildcard_imports)]

//! The peer-control-plane record a restarted process reads back, and the one
//! merge every pair of observations goes through.
//!
//! Split from [`super::control_plane`] along the line between a *record* and a
//! *derivation*. That file answers "who is allowed to send a step" — the
//! membership facts, the retirement floor, the policy the link layer is handed.
//! This one answers "what does a process that crashed get back, and what may it
//! conclude from it": the type, what makes one valid, and how two observations of
//! the committed membership combine.
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
//! **There is no obligation ledger here either, and there used to be one.** A
//! record carried the committed removals whose per-principal fence the link layer
//! had not accepted, because `fence_peer` was an operation that could be refused
//! and no later event re-derived it. Retirement is published as a *floor* now —
//! see [`crate::transport::PeerPolicy`] — so every statement this driver makes to
//! its link layer is a function of state it still holds, and a refused
//! publication is retried rather than remembered. That deleted the one output of
//! the join that was not monotone, which is what restored order-freedom by
//! construction rather than by proof.
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
/// process crashes. A new process reconstructs committed `{1,2}` and a high-water
/// mark of 2 — so node 5 is no longer spent, the policy this driver publishes
/// stops retiring it, and the identity the cluster consumed is allocatable again.
///
/// **The two facts.** The mark and the current committed state. Nothing else is
/// here, because everything else about the control plane is re-derived at
/// adoption: the effective membership comes from the runtime, and the published
/// policy deliberately does not survive — a new process has a new link layer that
/// has accepted nothing, and starting from "nothing accepted" is what forces the
/// first republication.
///
/// **There is deliberately no record of what the link layer accepted.** It used
/// to carry one — the committed removals whose fence was still owed — because
/// fencing was a per-principal operation that could be refused and that no later
/// fact re-derived. Retirement is a floor now, and the floor is a function of the
/// mark, so every statement this driver owes its link layer is derivable from the
/// two facts above at any moment. An obligation nobody has to remember is an
/// obligation nobody can forget, mis-order, or double-count.
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
/// every committed configuration this driver observes that changes what it
/// holds, and on nothing else.
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
/// a `(group, NodeId)` pair the cluster consumed is not a pair it can hand back
/// — and the raw fact is kept beside it on the driver so the violation stays
/// countable.
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
        spends_under(
            self.committed_id_high_water,
            self.current_committed.as_ref(),
            node_id,
        )
    }

    /// The membership of this record's current state, or the empty set.
    fn membership(&self) -> BTreeSet<NodeId> {
        self.current_committed
            .as_ref()
            .map(|current| current.membership.clone())
            .unwrap_or_default()
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
    ///
    /// * **The mark and the current state are one record, and the coupling is a
    ///   biconditional.** Two clauses about the obligation ledger used to sit
    ///   beside this one, and they left with it: a fence was the residue of a
    ///   committed removal and therefore had to name a spent identity, which is
    ///   a rule about a set that no longer exists. Retirement is the mark and the
    ///   live set now, and the mark and the live set are exactly what this
    ///   couples.
    ///
    ///   Every observation of a committed configuration raises the mark — a
    ///   committed configuration always names at least one replica, since
    ///   [`rafter::MembershipSet`] refuses an empty voter set — and assigns the
    ///   current state in the same call, because a first observation is always
    ///   the latest one this record has. So `mark.is_some() ⟺
    ///   current_committed.is_some()`, and the two ways to break it are separate
    ///   variants because they fail in opposite directions:
    ///   [`ControlPlaneCheckpointError::RetirementWithoutCurrentState`] leaves a
    ///   mark with nothing to compare it against — every identity at or below the
    ///   mark reads as spent, which is the whole cluster — and
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
        // an observation assigns the current state, so neither stands alone.
        match (
            self.current_committed.is_some(),
            self.committed_id_high_water.is_some(),
        ) {
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
        Ok(())
    }
}

/// Whether a record holding this mark and this current state spends `node_id`.
///
/// The two reads [`TransportDriverState::is_spent`] makes, over the halves of a
/// record rather than over the record — which is what lets the join read a
/// checkpoint's spent-ness after moving its fields out.
fn spends_under(
    mark: Option<NodeId>,
    current: Option<&CurrentCommittedState>,
    node_id: NodeId,
) -> bool {
    mark.is_some_and(|mark| node_id <= mark)
        && !current.is_some_and(|current| current.membership.contains(&node_id))
}

/// Everything one merge needs to know about the observation arriving.
///
/// A position, the membership observed there **raw**, and what the fact itself
/// proves a committed removal consumed. The third field is what separates a
/// crossing from every other input: only a transition the kernel computed proves
/// a removal on its own, and it proves it wherever the fact is folded.
pub(super) struct IncomingObservation<'a> {
    pub(super) through: LogIndex,
    pub(super) membership: &'a BTreeSet<NodeId>,
    pub(super) proven_removed: &'a BTreeSet<NodeId>,
}

/// Merges one observation of the committed membership into the one held.
///
/// **The single merge, reached from four directions.** Two checkpoints joining;
/// a checkpoint meeting the adopted runtime's own endpoint; a held state meeting
/// a routed `CommittedEndpoint`; a held state meeting a crossing that advances
/// the register. They were four expressions of one rule and only the first
/// refused a tie, so a runtime that disagreed with a durable record at the very
/// position both had observed silently retired a live replica in one direction
/// and silently authorized a never-committed one in the other.
///
/// # The rule
///
/// * **The later observation wins**, and the earlier is not discarded: the
///   identities it named that the later one does not are the committed removals
///   that happened between the two positions. That is the inference neither side
///   can make alone.
/// * **A proven removal is absorbed whatever the fact's position**, because it is
///   not an observation of the present — it is a permanent fact about an
///   identity. A crossing beneath the register still takes its removal out of it.
/// * **A tie is refused rather than broken**, once normalization has had its say.
///   The committed membership at one log position is one set, so two claims about
///   it that still differ are not two readings to reconcile; picking either
///   would be choosing which side to believe with nothing to decide on, and
///   merging them would invent a third neither side ever held.
///
/// # What normalization is for
///
/// Both sides are filtered by `spent` and by `proven_removed` *before* the tie is
/// judged, and each half of that is load-bearing.
///
/// `spent` is what keeps a **readmission** from reading as corruption. A cluster
/// that names an already-spent identity again has broken the single-use contract,
/// and this driver has an answer for that — refuse the replica, count the
/// violation at `readmitted_retired_peers`. The raw membership a runtime reports
/// contains the readmitted identity and the held register does not, which is a
/// difference with a known cause, so it must not be reported as a damaged file.
///
/// `proven_removed` is what keeps a **crossing at the register's own position**
/// from reading as one. The transition says which identities left; the held state
/// has either already absorbed them or is about to. Either way the two agree once
/// the transition is applied to both.
///
/// The normalization is applied to both sides rather than only to the incoming
/// one, which is what keeps the merge symmetric — and symmetry is a property the
/// checkpoint join needs, since a supervisor has no correct order to read two
/// peers' records in.
///
/// # Errors
///
/// [`ControlPlaneCheckpointError::ContradictoryCurrentState`] when the two stand
/// at one position and still disagree about the membership there.
pub(super) fn merge_current_state(
    held: Option<&CurrentCommittedState>,
    incoming: &IncomingObservation<'_>,
    spent: &dyn Fn(NodeId) -> bool,
) -> Result<CurrentCommittedState, ControlPlaneCheckpointError> {
    let Some(held) = held else {
        return Ok(CurrentCommittedState::new(
            incoming.through,
            live(incoming.membership, incoming.proven_removed, spent),
        ));
    };
    let (older, newer) = match held.through.cmp(&incoming.through) {
        Ordering::Less => (&held.membership, incoming.membership),
        Ordering::Greater => (incoming.membership, &held.membership),
        Ordering::Equal => {
            let held_live = live(&held.membership, incoming.proven_removed, spent);
            let incoming_live = live(incoming.membership, incoming.proven_removed, spent);
            if held_live != incoming_live {
                return Err(ControlPlaneCheckpointError::ContradictoryCurrentState {
                    through: held.through,
                });
            }
            return Ok(CurrentCommittedState::new(held.through, held_live));
        }
    };
    let mut removed: BTreeSet<NodeId> = older.difference(newer).copied().collect();
    removed.extend(incoming.proven_removed.iter().copied());
    Ok(CurrentCommittedState::new(
        held.through.max(incoming.through),
        live(newer, &removed, spent),
    ))
}

/// One membership less everything a removal took and everything already spent.
fn live(
    membership: &BTreeSet<NodeId>,
    removed: &BTreeSet<NodeId>,
    spent: &dyn Fn(NodeId) -> bool,
) -> BTreeSet<NodeId> {
    membership
        .iter()
        .copied()
        .filter(|node_id| !removed.contains(node_id) && !spent(*node_id))
        .collect()
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
    /// union kept 5 live, unspent, and published to the link layer. The missing
    /// fact was not in either record; it was *between* them, and only a
    /// positioned current state can see it.
    ///
    /// Write `spent_x(n) = n ≤ mark_x ∧ n ∉ live_x`, and let `older` and `newer`
    /// be the two current states ordered by `through`. Then:
    ///
    /// ```text
    /// mark     = max(mark_a, mark_b)
    /// spent    = S_a ∪ S_b
    /// inferred = older.membership \ newer.membership
    /// current  = { through: max(through_a, through_b),
    ///              membership: newer.membership \ inferred \ spent }
    /// ```
    ///
    /// An identity *above* one side's mark is judged only by the side whose mark
    /// covers it, which is what lets a record that never saw an identity avoid
    /// overruling one that did. The inferred set is the exception and earns it:
    /// it is judged by both, because being named at one position and absent at a
    /// later one is a two-record fact.
    ///
    /// **Every output is a lattice operation, and that is what changed.** There
    /// used to be a fifth line — the fence obligations, which were the union of
    /// both sides *plus the inferred removals neither side had already spent*.
    /// That last clause read the spent set to decide a side effect, which made
    /// the effect depend on how much spent-ness had accumulated by the time the
    /// inference fired, which made it depend on the order. Concretely, with
    /// records at positions 7, 10 and 12: `(A∨B)∨C` derives node 4's removal from
    /// the A/B pair, while `A∨(B∨C)` raises the mark to 6 first and then reads
    /// node 4 as already-spent, deriving nothing. Publishing retirement as a
    /// floor deletes the line rather than repairing it — see
    /// [`crate::transport::PeerPolicy`] — and what is left is `max`, `∪`, `\` and
    /// "the later of the two", every one of which is order-free.
    ///
    /// **Equal positions are normalized and then refused if they still differ**
    /// — see [`merge_current_state`], which owns that rule for all four callers.
    ///
    /// **The three properties, and they now hold of the whole operation rather
    /// than of the spent set alone.** Let `S_x` be the spent set of `x` and `L_x`
    /// its live set.
    ///
    /// 1. *Symmetric.* Every operator above is symmetric in `a` and `b`: `max`,
    ///    `∪`, and "the later of the two, refusing a tie that disagrees" — which
    ///    is symmetric precisely because the tie is refused rather than broken,
    ///    and because the normalization that decides the tie is applied to both
    ///    sides.
    /// 2. *Order-free.* `S_join ⊇ S_a ∪ S_b` (take `n ∈ S_a`: `n ≤ mark_a ≤
    ///    mark_join`, and `n` is filtered out of the joined membership by
    ///    construction). Position is `max`, which is associative, and the joined
    ///    membership is the newest observation minus an accumulated spent set —
    ///    so a third record joined afterwards sees the same newest observation
    ///    and a spent set that only grew, whichever order the first two arrived
    ///    in. Idempotent: joining `a` with itself leaves the position equal, the
    ///    memberships equal, `inferred` empty, and `L_a \ S_a = L_a`.
    ///
    ///    **And the join has no other output to be order-free in.** That is the
    ///    clause this used to be missing rather than getting wrong: the proof
    ///    covered the spent set, and the fence set was a second output it never
    ///    mentioned. `tests/transport_checkpoint_merge.rs` permutes three records
    ///    at three positions and asserts the settled record *and* the
    ///    identities the published policy retires.
    /// 3. *Monotone in spent-ness.* From property 2, every identity either side
    ///    had witnessed spent is spent in the join and in every later join.
    ///    **A witnessed removal cannot be undone by any merge order**, and an
    ///    inferred one cannot either: it leaves the joined membership, and the
    ///    filter above keeps a later observation from putting it back.
    ///
    /// **The joined candidate is validated too, and that is deliberate
    /// redundancy.** The proof above says it cannot fail: the coupling
    /// biconditional is preserved because the mark is a `max` and the current
    /// state is a choice between two, so the join has a mark exactly when a side
    /// did and a current state exactly when a side did; and every live identity
    /// stays at or below the joined mark because the mark only rose.
    ///
    /// The properties are nonetheless *executed* rather than only argued,
    /// because the cost is one pass over a cluster-sized set and the thing being
    /// protected is a retirement floor that never falls. A proof that stops
    /// holding because someone edited the join is a proof that fails silently;
    /// this one fails loudly.
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
    ///
    pub(super) fn restore_control_plane_checkpoint(
        &mut self,
        checkpoint: PeerControlPlaneCheckpoint<G>,
    ) -> Result<(), ControlPlaneCheckpointError> {
        checkpoint.validate(&self.group_id)?;

        let held = self.control_plane_checkpoint();
        // Taken apart rather than read through, so the restored observation is
        // moved into this driver rather than cloned into it. The two halves are
        // what spent-ness is computed from either way.
        let PeerControlPlaneCheckpoint {
            committed_id_high_water: restored_mark,
            current_committed: restored_state,
            ..
        } = checkpoint;
        let committed_id_high_water = match (held.committed_id_high_water, restored_mark) {
            (Some(held), Some(restored)) => Some(held.max(restored)),
            (held, None) => held,
            (None, restored) => restored,
        };
        let spent = |node_id: NodeId| {
            held.spends(node_id) || spends_under(restored_mark, restored_state.as_ref(), node_id)
        };
        // A record carries no transition, so it proves no removal on its own.
        // What the *pair* proves — an identity named at one position and absent
        // at a later one — is the merge's own inference.
        let proves_nothing = BTreeSet::new();
        let current_committed = match restored_state.as_ref() {
            // The incoming record observed nothing, so it can raise no mark
            // either (the biconditional above), and there is nothing to merge.
            None => held.current_committed.clone(),
            Some(incoming) => Some(merge_current_state(
                held.current_committed.as_ref(),
                &IncomingObservation {
                    through: incoming.through,
                    membership: &incoming.membership,
                    proven_removed: &proves_nothing,
                },
                &spent,
            )?),
        };

        let joined = PeerControlPlaneCheckpoint {
            group: self.group_id.clone(),
            committed_id_high_water,
            current_committed,
        };
        joined.validate(&self.group_id)?;

        // The raw committed floor is deliberately not restored from here. It
        // answers "what does this replica's own stream say the cluster has
        // committed now", a record says what this driver has *spent*, and both
        // entry points that reach this call publish the runtime's endpoint
        // afterwards — so the floor is assigned from the runtime before the
        // driver serves anything.
        self.committed_id_high_water = joined.committed_id_high_water;
        self.current_committed = joined.current_committed;
        self.advance_checkpoint_epoch();
        Ok(())
    }
}
