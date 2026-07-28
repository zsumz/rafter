#![allow(clippy::wildcard_imports)]

//! The peer-control-plane record a restarted process reads back.
//!
//! Split from [`super::control_plane`] along the line between a *record* and a
//! *derivation*. That file answers "who is allowed to send a step" — the
//! membership facts, the retirement diff, the peer set, the fences the link
//! layer still owes. This one answers "what does a process that crashed get
//! back, and what may it conclude from it": the type, what makes one valid, how
//! two of them join, and how far through the committed configuration stream a
//! record has already been consumed.
//!
//! The last of those is the newest and the least obvious. Every other field here
//! is a *state* — a mark, a live set, a set of obligations — and a state can be
//! restored by assignment. The configuration stream is not a state, it is a
//! sequence, and a driver that restores the state without also restoring its
//! position in the sequence will re-consume history against a present that has
//! moved past it. See [`TransportDriverState::committed_configuration_is_replayed`]
//! for why that is not a tidiness argument.
//!
//! The state these rules read still lives on
//! [`super::state::TransportDriverState`], like every other field behind the one
//! lock. What lives here is the record's own algebra.

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
/// **The four facts.** Three of them are the retirement record — the mark, the
/// live set, and the obligations — and the fourth is this driver's position in
/// the stream that produces them. Nothing else is here, because everything else
/// about the control plane is re-derived at adoption: the effective and raw
/// committed memberships come from the runtime, and `published_peers`
/// deliberately does not survive — a new process has a new link layer that has
/// accepted nothing, and starting from "nothing accepted" is what forces the
/// first republication.
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
    /// The part of the committed configuration whose identities are unspent.
    ///
    /// The other half of the spent test, and the field the two-fact version of
    /// this checkpoint could not do without. A mark restored beside an empty
    /// live set spends every identity at or below it — the whole cluster — and a
    /// live set re-derived from the adopted group's committed configuration
    /// instead would *un-spend* an identity a violating readmission committed
    /// while this process was down. Carrying it makes a restore behave exactly
    /// like the in-process release-and-re-adopt it is standing in for. It is
    /// bounded by the size of the cluster.
    pub live_committed_members: BTreeSet<NodeId>,
    /// Committed removals whose fence the link layer has not accepted.
    ///
    /// One entry per unfenced removal, and nothing here ever discards one: a
    /// committed fact is not a request. Retention across restarts is therefore
    /// the embedder's, stated at
    /// [`TransportDriverOptions::fence_backlog_service_threshold`].
    pub pending_fences: BTreeSet<NodeId>,
    /// The log index through which this record has consumed committed
    /// configuration history, or `None` before it has consumed any.
    ///
    /// **The consumer offset, and the field that makes the other three
    /// replayable.** The three above are a *state*, and a state restores by
    /// assignment. Committed configurations are a *stream*, and a restart
    /// re-delivers a suffix of it: the runtime replays every configuration entry
    /// between the application's applied floor and the durable commit index as
    /// an ordinary output. Those are historical facts. Computed against a live
    /// set that has already advanced past them, each one reads as a removal of
    /// everything the configurations above it added.
    ///
    /// So this is not a cache and not an optimization. Without it the replay is
    /// not even *idempotent*: re-running the same recovery over the same durable
    /// state manufactures a removal on the second pass that it did not on the
    /// first, because the first pass moved the live set the second one is
    /// computed against. A crash during recovery is an ordinary crash, so that
    /// second pass is a state a correct embedder reaches.
    ///
    /// It moves with the retirement record and never apart from it: every
    /// committed fact that advances the mark, the live set, or the obligations
    /// advances this in the same call and under the same epoch, so the two
    /// cannot be persisted out of step with one another.
    ///
    /// **That coupling is validated rather than merely documented**, in both
    /// directions: `None` here means nothing has been retired, and anything
    /// retired means this is `Some`. A record that breaks it was not written by
    /// a driver, and each way of breaking it loses a different half of what this
    /// type exists for — see
    /// [`ControlPlaneCheckpointError::CommittedStateWithoutCursor`] and
    /// [`ControlPlaneCheckpointError::CursorWithoutCommittedState`]. An embedder
    /// with a format of its own should refuse the same two shapes at its own
    /// decoder, so the refusal names the file rather than the value.
    ///
    /// `None` rather than zero because `LogIndex(0)` is a real position — the
    /// index before any entry — and "no configuration fact consumed" has to be
    /// distinguishable from "consumed through the bottom of the log".
    pub committed_configuration_through: Option<LogIndex>,
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
            live_committed_members: BTreeSet::new(),
            pending_fences: BTreeSet::new(),
            committed_configuration_through: None,
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
            && !self.live_committed_members.contains(&node_id)
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
    /// * **The cursor and the retirement record are one record**, so
    ///   `through == None ⟺ nothing retired`. The field's own documentation
    ///   already said the two move together and never apart; this is the clause
    ///   that makes the claim checkable rather than merely stated, and both
    ///   directions of breaking it are reachable through a damaged or
    ///   hand-written file.
    ///
    ///   The forward direction is the dangerous one and is not subtle: a record
    ///   holding a final live set with no offset makes recovery replay the whole
    ///   configuration history against a live set that already reflects it,
    ///   which fences the replicas the cluster most recently admitted. That is
    ///   the exact failure the offset was added to close, re-entering through a
    ///   checkpoint shape instead of through a missing gate.
    ///
    ///   The reverse direction is the quiet one — an offset beside no retirement
    ///   record skips that history and keeps nothing from it — and it is worth
    ///   saying why refusing it costs nothing. It is not a producible state: a
    ///   committed configuration always names at least one replica, because
    ///   [`rafter::MembershipSet`] refuses an empty voter set, so a driver whose
    ///   cursor advanced raised its mark in the same call. There is no legitimate
    ///   record this clause turns away.
    fn validate(&self, group: &G) -> Result<(), ControlPlaneCheckpointError>
    where
        G: Ord,
    {
        if &self.group != group {
            return Err(ControlPlaneCheckpointError::ForeignGroup);
        }
        // **One record, checked as one.** `committed_configuration_through`
        // documents that it moves with the retirement record and never apart
        // from it; until this clause existed, nothing enforced it, and both ways
        // of breaking it were accepted as ordinary input.
        let retired_something = self.committed_id_high_water.is_some()
            || !self.live_committed_members.is_empty()
            || !self.pending_fences.is_empty();
        match (self.committed_configuration_through, retired_something) {
            (None, true) => return Err(ControlPlaneCheckpointError::CommittedStateWithoutCursor),
            (Some(_), false) => {
                return Err(ControlPlaneCheckpointError::CursorWithoutCommittedState)
            }
            (None, false) | (Some(_), true) => {}
        }
        for node_id in self.live_committed_members.iter().copied() {
            let Some(mark) = self.committed_id_high_water else {
                return Err(ControlPlaneCheckpointError::LiveMembersWithoutMark { node_id });
            };
            if node_id > mark {
                return Err(ControlPlaneCheckpointError::LiveMemberAboveMark { node_id, mark });
            }
        }
        for node_id in self.pending_fences.iter().copied() {
            if self.live_committed_members.contains(&node_id) {
                return Err(ControlPlaneCheckpointError::FenceNamesLiveMember { node_id });
            }
            if !self.spends(node_id) {
                return Err(ControlPlaneCheckpointError::FenceNamesUnspentIdentity { node_id });
            }
        }
        Ok(())
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
            live_committed_members: self.live_committed_members.clone(),
            pending_fences: self.pending_fences.clone(),
            committed_configuration_through: self.committed_configuration_through,
        }
    }

    /// Whether a committed configuration fact standing at `index` is one this
    /// driver has already taken.
    ///
    /// **The gate that makes a replayed stream safe, and it is required rather
    /// than defensive.** A committed configuration is a permanent fact, so a
    /// driver that has consumed the stream through index *n* has already folded
    /// every configuration at or below *n* into its mark, its live set, and its
    /// obligations. Re-folding one is not a harmless repetition: the fold is a
    /// *difference* against the live set as it stands now, and the live set has
    /// moved on. A configuration from index 10 recomputed against the state
    /// index 11 produced reads as a removal of everything index 11 added.
    ///
    /// That is not a corner case, it is what a restart does. The runtime replays
    /// every configuration entry between the application's applied floor and the
    /// durable commit index, so a recovered driver is handed exactly this
    /// sequence — historical facts, in order, all of them older than the
    /// endpoint the runtime also reports.
    ///
    /// And it cannot be fixed by ordering alone. Replaying before the endpoint
    /// is observed handles the *first* recovery; the second one, from the same
    /// durable state and the checkpoint the first one wrote, replays index 10
    /// against a restored live set that already reflects index 11. Idempotence
    /// under arbitrary re-replay needs a position, not an order.
    ///
    /// Strictly at-or-below, because the cursor names a fact that was taken
    /// rather than the next one expected.
    pub(super) fn committed_configuration_is_replayed(&self, index: LogIndex) -> bool {
        self.committed_configuration_through
            .is_some_and(|cursor| index <= cursor)
    }

    /// Moves the cursor to `index`, and reports whether it moved.
    ///
    /// Monotone, like every other field of the record. The caller folds the
    /// answer into the same epoch decision the mark and the live set feed, so a
    /// fact that advanced only the cursor still reaches the embedder's next
    /// persist — a cursor made durable behind its own retirement record would
    /// replay on the following restart, which is the whole failure this closes.
    pub(super) fn advance_committed_configuration_cursor(&mut self, index: LogIndex) -> bool {
        let advanced = self
            .committed_configuration_through
            .is_none_or(|cursor| index > cursor);
        if advanced {
            self.committed_configuration_through = Some(index);
        }
        advanced
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
    /// **A lattice join, not four independent merges, and the live set is where
    /// that matters.** Taking the union of the two live sets was wrong, and
    /// wrong in the one direction this whole mechanism exists to prevent: a
    /// stale-but-valid checkpoint holding `{mark 5, live {1,2,5}}` joined into a
    /// driver holding `{mark 5, live {1,2}}` produced live `{1,2,5}` and
    /// *un-spent* node 5. Stale checkpoints are explicitly allowed — the public
    /// contract says so and bounds what staleness costs — so this is reachable
    /// by a correct embedder that crashed between a removal and its persistence,
    /// and the identity the cluster consumed became adoptable again.
    ///
    /// Write `spent_x(n) = n ≤ mark_x ∧ n ∉ live_x`. Then:
    ///
    /// ```text
    /// mark   = max(mark_a, mark_b)
    /// fences = fences_a ∪ fences_b
    /// live   = { n ∈ live_a ∪ live_b : ¬spent_a(n) ∧ ¬spent_b(n) }
    /// cursor = max(cursor_a, cursor_b)
    /// ```
    ///
    /// An identity *above* one side's mark is judged only by the side whose mark
    /// covers it, which is what lets a record that never saw an identity avoid
    /// overruling one that did.
    ///
    /// **The cursor is `max` for the same reason the mark is, and it is sound
    /// for a reason worth stating.** Skipping a replayed configuration at or
    /// below the joined cursor is only safe if the joined *state* already
    /// reflects it — and it does: whichever side held the higher cursor had
    /// consumed that configuration, its spent-ness is in `S_a ∪ S_b`, and the
    /// join preserves the whole union. So the side that had not consumed it
    /// cannot lose anything by the join declining to consume it again.
    ///
    /// **The three properties, which are what make it safe to apply in any
    /// order.** Let `S_x` be the spent set of `x` and `L_x` its live set.
    ///
    /// 1. *Symmetric.* Every operator above — `max`, `∪`, and a conjunction —
    ///    is symmetric in `a` and `b`, so `join(a, b) = join(b, a)`.
    /// 2. *Order-free.* The key step is that the join's own spent set is exactly
    ///    the union of the two: `S_join = S_a ∪ S_b`. (⊇ is immediate. For ⊆,
    ///    take `n ≤ max(mark_a, mark_b)` with `n ∉ L_join`. Either `n ∈ S_a ∪
    ///    S_b` and we are done, or `n ∉ L_a ∪ L_b`; then `n ≤ mark_a` gives
    ///    `n ∈ S_a` and `n ≤ mark_b` gives `n ∈ S_b`, and one of the two holds
    ///    because `n ≤ max`.) Substituting, `L_(a∨b)∨c = (L_a ∪ L_b ∪ L_c) \
    ///    (S_a ∪ S_b ∪ S_c)`, which is symmetric in all three, so the join is
    ///    associative as well as commutative — any sequence of restores yields
    ///    the same state. It is idempotent too: `L_a \ S_a = L_a`, because a
    ///    validated checkpoint's live and spent sets are disjoint.
    /// 3. *Monotone in spent-ness.* From `S_join = S_a ∪ S_b`, every identity
    ///    either side had witnessed spent is spent in the join, and by
    ///    associativity in every later join as well. **A witnessed removal
    ///    cannot be undone by any merge order.** That is the property the union
    ///    lacked and the reason this is a join rather than four merges.
    ///
    /// Obligations are still the union, because a driver can hold fences of its
    /// own before a checkpoint is restored and losing either set would be losing
    /// a fence. The fence set is idempotent under union by contract: it holds
    /// committed facts, and nothing but an accepted fence removes one.
    ///
    /// **The joined candidate is validated too, and that is deliberate
    /// redundancy.** The proof above says it cannot fail — `fences ⊆ spent` is
    /// preserved by the join, because a fence of `a` is in `S_a`, is at or below
    /// `mark_a ≤ mark_join`, and is excluded from `live_join` by construction.
    /// The cursor coupling is preserved for the same kind of reason: `through`
    /// is a `max` and the retirement fields are unions or maxima, so a joined
    /// record has an offset exactly when one of its sides did, and has
    /// retirement state exactly when one of its sides did — and each side has
    /// both or neither, the incoming one by validation and the held one by the
    /// invariant this driver maintains.
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
    /// group or contradicts the invariants a driver maintains for one, and when
    /// the joined candidate would contradict them. **Nothing is mutated on that
    /// path** — the join is computed into a candidate and validated before the
    /// first field moves — so a caller that refuses to open is left with a driver
    /// in exactly the state it was.
    pub(super) fn restore_control_plane_checkpoint(
        &mut self,
        checkpoint: PeerControlPlaneCheckpoint<G>,
    ) -> Result<(), ControlPlaneCheckpointError> {
        checkpoint.validate(&self.group_id)?;

        let held = self.control_plane_checkpoint();
        // Computed before the record is taken apart below, because both halves
        // of the spent filter need the whole of it.
        let live_committed_members = held
            .live_committed_members
            .union(&checkpoint.live_committed_members)
            .copied()
            .filter(|node_id| !held.spends(*node_id) && !checkpoint.spends(*node_id))
            .collect();
        let committed_id_high_water = match (
            held.committed_id_high_water,
            checkpoint.committed_id_high_water,
        ) {
            (Some(held), Some(restored)) => Some(held.max(restored)),
            (held, None) => held,
            (None, restored) => restored,
        };
        let committed_configuration_through = held
            .committed_configuration_through
            .max(checkpoint.committed_configuration_through);
        let mut pending_fences = checkpoint.pending_fences;
        pending_fences.extend(held.pending_fences);

        let joined = PeerControlPlaneCheckpoint {
            group: self.group_id.clone(),
            committed_id_high_water,
            live_committed_members,
            pending_fences,
            committed_configuration_through,
        };
        joined.validate(&self.group_id)?;

        self.committed_id_high_water = joined.committed_id_high_water;
        self.live_committed_members = joined.live_committed_members;
        self.pending_fences = joined.pending_fences;
        self.committed_configuration_through = joined.committed_configuration_through;
        self.advance_checkpoint_epoch();
        Ok(())
    }
}
