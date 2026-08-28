#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

//! The record itself, and the observation it carries.
//!
//! Caller-owned durable state with public fields: Rafter opens no files, so this
//! is a plain value an embedder reads, persists, and hands back. What a record
//! *means* is decided elsewhere — this file declares the two facts and the
//! verdict a restarted process cannot re-derive, and constructs the empty one.

use super::*;

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
/// **The two facts, and one verdict on them.** The mark and the current
/// committed state are what a restarted process cannot re-derive; the
/// contradiction marker is what it must not be allowed to re-derive *away*.
/// Nothing else is here, because everything else about the control plane is
/// re-derived at adoption: the effective membership comes from the runtime, and
/// the published policy deliberately does not survive — a new process has a new
/// link layer that has accepted nothing, and starting from "nothing accepted" is
/// what forces the first republication.
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
/// holds, on the moment it records a contradiction, and on nothing else. The
/// second is the one an embedder must not skip: a contradiction installs no
/// membership at all, so the marker is the *only* checkpointable change it
/// makes, and a record written without it starts the next incarnation clean.
///
/// **A stale prefix of the same authoritative chain is a legal input at
/// construction, and joining one can only ever add spent-ness.** Both
/// qualifications are load-bearing and the sentence used to carry neither.
/// *Prefix of the same chain*, because order-freedom is claimed over one
/// replica's own succession of records and is false across a fork — two records
/// that disagree about one position are refused rather than reconciled, and the
/// supervisor owns chain identity. *At construction*, because
/// [`TransportRaftDriver::with_control_plane_checkpoint`] restores into empty
/// held state, while a record offered to
/// [`TransportRaftDriver::adopt_group_with_checkpoint`] must stand at or after
/// what that driver has already observed or it is refused with
/// [`ControlPlaneCheckpointError::StaleCurrentState`].
///
/// Inside those bounds it is a property of the join rather than of the caller's
/// discipline: see the crate-internal `restore_checkpoint`, which states the
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
/// the crate-internal `observe_committed` as an ordinary committed
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
    /// crate-internal `restore_checkpoint`.
    pub current_committed: Option<CurrentCommittedState>,
    /// Where this chain observed two irreconcilable claims about the committed
    /// membership, if it did.
    ///
    /// **A terminal marker, and it is durable because a terminal state that does
    /// not survive a restart is not terminal.** A driver whose licensing inputs
    /// contradict each other stops serving, stops publishing, and freezes the two
    /// facts above — but the process holding it can crash, and a record written
    /// without this field said nothing about the fork. A rebuilt driver started
    /// clean, and where the rebuilt runtime happened to agree at the record's
    /// position it went straight back to serving and to publishing retirement
    /// floors derived from a chain it had already declared broken.
    ///
    /// So a restored record carrying this starts the driver in
    /// [`DriverServiceState::ContradictoryCurrentState`]: refusing clients,
    /// publishing nothing, and stepping Raft — the `NEEDS_REPAIR` posture the lock
    /// store takes for a slot it cannot prove, applied to the control plane. It
    /// clears only when an operator reseeds this replica's control plane
    /// deliberately, with the deployment's own record of what was retired beside
    /// them. There is no fact the cluster can supply that decides it: the
    /// committed membership at one index is one set, and this chain has two.
    ///
    /// **A record carrying it may be resumed and may not be merged.**
    /// [`TransportRaftDriver::with_control_plane_checkpoint`] restores into empty
    /// held state, which is this chain resuming itself, and the marker has to
    /// survive that or the freeze ends at the next crash.
    /// [`TransportRaftDriver::adopt_group_with_checkpoint`] joins a record into a
    /// driver that has been running, and a marked record is evidence of an
    /// unresolved fork — merging one would launder exactly what it refused, so it
    /// is refused with
    /// [`ControlPlaneCheckpointError::ContradictedRecordMerged`].
    ///
    /// **It carries the position and not which comparison found it.** Both
    /// terminal shapes — two observations at one index, and a transition
    /// declaring a predecessor the register is not — mean the same thing to a
    /// restarted process, and the diagnosis that told them apart was emitted by
    /// the process that found it. A restored marker therefore reports the general
    /// variant.
    ///
    /// The position is at or below `current_committed`'s, because the register
    /// stops moving the moment this is set and the contradiction was found at or
    /// ahead of where it stood.
    pub contradicted_at: Option<LogIndex>,
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
            contradicted_at: None,
        }
    }
}
