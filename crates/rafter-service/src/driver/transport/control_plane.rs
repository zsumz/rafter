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

use std::collections::BTreeSet;

use crate::transport::{AuthenticatedPeerValidator, PeerSet, RaftTransport};

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
/// **The three facts.** Nothing else is here, because everything else about the
/// control plane is re-derived at adoption: the effective and raw committed
/// memberships come from the runtime, and `published_peers` deliberately does
/// not survive — a new process has a new link layer that has accepted nothing,
/// and starting from "nothing accepted" is what forces the first republication.
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
}

impl<G> PeerControlPlaneCheckpoint<G> {
    /// The checkpoint a first incarnation over empty storage would have written.
    ///
    /// Nothing observed, nothing spent, nothing owed. This is the honest value
    /// for a process whose durable checkpoint file does not exist yet, and it is
    /// what [`TransportRaftDriver::new`] and [`TransportRaftDriver::adopt_group`]
    /// pass on the caller's behalf. It is *not* the right value for a process
    /// whose file is unreadable: see [`PeerControlPlaneCheckpoint`] for why a
    /// restart that starts from nothing is precisely the failure this type
    /// exists to prevent.
    #[must_use]
    pub fn empty(group: G) -> Self {
        Self {
            group,
            committed_id_high_water: None,
            live_committed_members: BTreeSet::new(),
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
    /// * No pending fence names a live member: the fence set is extended with
    ///   exactly the difference the live assignment removes, so an identity
    ///   cannot be in both. A record that says otherwise would have the driver
    ///   permanently fence a replica its own committed configuration still
    ///   needs, and `fence_peer` has no inverse.
    fn validate(&self, group: &G) -> Result<(), ControlPlaneCheckpointError>
    where
        G: Ord,
    {
        if &self.group != group {
            return Err(ControlPlaneCheckpointError::ForeignGroup);
        }
        for node_id in self.live_committed_members.iter().copied() {
            let Some(mark) = self.committed_id_high_water else {
                return Err(ControlPlaneCheckpointError::LiveMembersWithoutMark { node_id });
            };
            if node_id > mark {
                return Err(ControlPlaneCheckpointError::LiveMemberAboveMark { node_id, mark });
            }
        }
        if let Some(node_id) = self
            .pending_fences
            .intersection(&self.live_committed_members)
            .copied()
            .next()
        {
            return Err(ControlPlaneCheckpointError::FenceNamesLiveMember { node_id });
        }
        Ok(())
    }
}

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
    },
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
    pub(super) fn route_membership_event(&mut self, event: &MembershipEvent<G>) {
        match event {
            MembershipEvent::EffectiveChanged { membership, .. } => {
                let effective = membership.replica_ids().into_iter().collect();
                self.publish_membership(MembershipFact::Effective(effective));
            }
            MembershipEvent::Applied { membership, .. } => {
                let committed = membership.replica_ids().into_iter().collect();
                // The runtime is the authority on what is in effect, and it
                // agrees with the effective event that preceded this one in the
                // same report. A driver holding no group keeps what it had
                // rather than assigning an empty set: an absent effective
                // membership must not turn a fence into a silence, and must not
                // narrow anything either.
                let effective = self
                    .runtime_effective_members()
                    .unwrap_or_else(|| self.effective_members.clone());
                self.publish_membership(MembershipFact::Committed {
                    committed,
                    effective,
                });
            }
            // A rejected change never entered the log, and a variant this driver
            // does not know is not a membership fact it can act on.
            _ => {}
        }
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
            } => {
                self.effective_members = effective;
                self.observe_committed_members(committed);
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
    /// and how far allocation has got.
    fn observe_committed_members(&mut self, committed: BTreeSet<NodeId>) {
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
        // Read before the three checkpointable fields move, so the epoch below
        // is advanced for exactly the observations an embedder must persist.
        // The fence set only grows here, so its length is a faithful witness and
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
        let checkpoint_moved = previous_mark != self.committed_id_high_water
            || previous_fences != self.pending_fences.len()
            || live != self.live_committed_members;
        self.live_committed_members = live;
        self.committed_members = committed;
        if checkpoint_moved {
            self.advance_checkpoint_epoch();
        }
    }

    /// Records that the checkpointable control-plane state moved.
    ///
    /// Called wherever one of the three checkpoint fields changes and nowhere
    /// else, so an embedder that persists on every epoch move persists exactly
    /// the changes it must not lose. Saturating rather than wrapping: an epoch
    /// that wrapped past a caller's last-persisted value would report "no
    /// change" for a state that had changed, and a driver that reached `u64::MAX`
    /// configuration changes has an embedder that should persist unconditionally
    /// from then on.
    fn advance_checkpoint_epoch(&mut self) {
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
        }
    }

    /// Joins a recovered checkpoint into what this driver holds, before any
    /// membership fact is derived from the adopted group.
    ///
    /// **Order is the whole contract.** The spent test reads the mark and the
    /// live set together, so both must be in place before
    /// [`TransportDriverState::publish_adopted_membership`] observes the group's
    /// committed configuration — otherwise a recovered mark of 5 meets an empty
    /// live set and spends every identity at or below it. With both installed,
    /// the adoption that follows is the ordinary observation it always was: a
    /// recovered mark of 5 beats a reconstructed committed set of `{1,2}`, and an
    /// identity a removal spent stays spent even if the cluster names it again.
    ///
    /// **A lattice join, not three independent merges, and the live set is where
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
    /// mark  = max(mark_a, mark_b)
    /// fences = fences_a ∪ fences_b
    /// live  = { n ∈ live_a ∪ live_b : ¬spent_a(n) ∧ ¬spent_b(n) }
    /// ```
    ///
    /// An identity *above* one side's mark is judged only by the side whose mark
    /// covers it, which is what lets a record that never saw an identity avoid
    /// overruling one that did.
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
    ///    lacked and the reason this is a join rather than three merges.
    ///
    /// Obligations are still the union, because a driver can hold fences of its
    /// own before a checkpoint is restored and losing either set would be losing
    /// a fence. The fence set is idempotent under union by contract: it holds
    /// committed facts, and nothing but an accepted fence removes one.
    ///
    /// # Errors
    ///
    /// Returns [`ControlPlaneCheckpointError`] when the checkpoint names another
    /// group or contradicts the invariants a driver maintains for one. **Nothing
    /// is mutated on that path** — the checkpoint is validated whole before the
    /// first field moves — so a caller that refuses to open is left with a driver
    /// in exactly the state it was.
    pub(super) fn restore_control_plane_checkpoint(
        &mut self,
        checkpoint: PeerControlPlaneCheckpoint<G>,
    ) -> Result<(), ControlPlaneCheckpointError> {
        checkpoint.validate(&self.group_id)?;

        let held = self.control_plane_checkpoint();
        self.live_committed_members = held
            .live_committed_members
            .union(&checkpoint.live_committed_members)
            .copied()
            .filter(|node_id| !held.spends(*node_id) && !checkpoint.spends(*node_id))
            .collect();
        self.pending_fences.extend(checkpoint.pending_fences);
        self.committed_id_high_water = match (
            self.committed_id_high_water,
            checkpoint.committed_id_high_water,
        ) {
            (Some(held), Some(restored)) => Some(held.max(restored)),
            (held, None) => held,
            (None, restored) => restored,
        };
        self.advance_checkpoint_epoch();
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
        self.publish_membership(MembershipFact::Committed {
            committed,
            effective,
        });
    }
}
