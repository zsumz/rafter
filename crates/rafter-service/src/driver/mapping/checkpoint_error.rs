#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

//! Why a peer-control-plane checkpoint could not be installed.
//!
//! The closed set of ways a durable record can contradict the invariants a
//! driver maintains for one, and how each reads to an operator. Every variant
//! names the identity or the log position the record disagrees about, because
//! that is what an investigation starts from. Nothing here decides anything.

use super::*;

/// Why a peer-control-plane checkpoint could not be installed.
///
/// This enum is exhaustive because it is the closed set of ways a checkpoint can
/// contradict the invariants a driver maintains for one. Each variant names the
/// identity that failed, because an operator reading this is looking for which
/// replica the durable record disagrees about.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlPlaneCheckpointError {
    /// The checkpoint was written for a different group.
    ///
    /// A driver serves one group for its whole life and retirement is per
    /// `(group_id, NodeId)` pair, so a checkpoint from another group carries a
    /// mark and a live set about identities that mean nothing here — and
    /// installing it would raise this group's mark past identities it has never
    /// committed, refusing replicas it has.
    ForeignGroup,
    /// A live committed member sits above the checkpoint's high-water mark.
    ///
    /// Also a state no driver produces, for the same reason, and the dangerous
    /// direction: an identity above the mark is unjudgeable by the spent test,
    /// so a lowered mark is how a corrupted record un-retires everything above
    /// it.
    LiveMemberAboveMark {
        /// Live member beyond the checkpoint's claimed identity range.
        node_id: NodeId,
        /// Greatest identity the checkpoint claims to cover.
        mark: NodeId,
    },
    /// The checkpoint carries retirement state and no current committed state.
    ///
    /// **The dangerous half of the coupling, and it is not subtle.** The spent
    /// test reads the mark against the current membership, so a mark standing
    /// beside nothing spends every identity at or below it — the whole cluster —
    /// and the driver refuses every replica it has.
    ///
    /// Not producible: every observation of a committed configuration raises the
    /// mark and assigns the current state in the same call, because a first
    /// observation is always the latest one a record has. A committed
    /// configuration always names at least one replica —
    /// [`rafter::MembershipSet`] refuses an empty voter set — so there is no
    /// observation that raises one without the other.
    RetirementWithoutCurrentState,
    /// The checkpoint carries a current committed state and no retirement state.
    ///
    /// The other half, and the quieter loss. A membership this record claims to
    /// have observed is one whose greatest identity would have raised a mark, so
    /// a record holding the observation and no mark has had its retirement half
    /// truncated away. Absorbed, every identity the lost facts spent is
    /// allocatable again, with the retirement floor this driver publishes falling
    /// back to cover none of them and no later fact to re-derive it from.
    CurrentStateWithoutRetirement,
    /// Two observations of the committed membership at one position disagree
    /// about it.
    ///
    /// The merge picks the later of two current states, and a tie it cannot break
    /// is refused rather than broken arbitrarily: the committed membership at
    /// one log position is one set, so this is not two observations to reconcile
    /// but two claims about a single fact. Picking either would be choosing
    /// which side to believe with nothing to decide on, and merging them would
    /// invent a third that neither side ever held.
    ///
    /// **Raised from all four places two such observations meet**, which is what
    /// it was missing: two checkpoints joining, a checkpoint meeting the adopted
    /// runtime's endpoint, a held register meeting a routed committed endpoint,
    /// and a held register meeting a crossing. Only the first refused a tie, so a
    /// runtime that disagreed with a durable record at the very position both had
    /// observed silently retired a live replica in one direction and silently
    /// raised the floor past a never-committed identity in the other.
    ///
    /// **A readmitted spent identity is not this.** Both sides are normalized by
    /// what either has proven spent — and by the removals the incoming fact
    /// itself carries — before the tie is judged, so a cluster that names a
    /// retired identity again is reported at
    /// [`crate::TransportRaftDriver::readmitted_retired_peers`] and refused, which
    /// is a configuration fault with a known answer rather than a record this
    /// process cannot read.
    ///
    /// What survives that normalization is an identity one side calls live at a
    /// position where the other, with a mark too low to have any opinion about
    /// it, calls the membership something else. That is damaged, truncated, or
    /// foreign durable state.
    ContradictoryCurrentState {
        /// Log position at which the observations disagree.
        through: LogIndex,
    },
    /// A committed transition declares a predecessor the driver's own register
    /// is not, at the position they both name.
    ///
    /// **The one-chain contract's most direct evidence.** A crossing carries the
    /// membership the kernel computed as standing immediately before its own
    /// entry, so a crossing at index `n+1` and a register standing at index `n`
    /// are two claims about the committed configuration at `n`. There is no index
    /// between them for the difference to have happened at, so a difference that
    /// survives normalization is proof that the record and the log are not one
    /// chain — which is a stronger statement than
    /// [`ControlPlaneCheckpointError::ContradictoryCurrentState`] makes, because
    /// the kernel computed this side of it where the chronology was known.
    ///
    /// Kept as its own variant rather than folded into that one because the two
    /// point an operator at different artifacts. A contradictory current state is
    /// two *observations* colliding, and either could be the damaged one. This is
    /// a durable record colliding with the log's own account of its own history:
    /// the log is the authority, and what is wrong is the record beside it.
    ///
    /// `through` is the position whose membership is contradicted — the
    /// register's, one below the transition's own index — because that is the
    /// committed configuration the two disagree about.
    ///
    /// **Only raised where the two are adjacent.** A transition separated from
    /// the register by any gap makes no claim about where the register stands:
    /// the entries between them are ordinarily application entries, across which
    /// the committed membership does not move, and may equally be configuration
    /// entries a compaction erased. Comparing across one would manufacture the
    /// contradiction rather than detect it.
    ContradictoryTransitionPredecessor {
        /// Log position whose membership the transition contradicts.
        through: LogIndex,
    },
    /// A checkpoint observed the committed membership *before* the driver it was
    /// offered to did.
    ///
    /// **The chain rule, and it is a narrowing of the contract rather than a
    /// corruption report.** A replica's records form one chain: each incarnation
    /// is handed the previous record before it observes anything, so every record
    /// it writes already carries what the earlier ones spent, and a later record
    /// of one chain never stands before an earlier one. A record that does is
    /// from somewhere else — another replica's file, or another process's record
    /// offered to a driver that has been running — and merging records of
    /// different chains is what this refuses.
    ///
    /// It has to be refused rather than absorbed because the register keeps one
    /// observation. Two records that directly contradict each other are compared
    /// only while the register still stands where they do; once any later record
    /// moves it forward, an older one merges against a position it never saw, and
    /// its own spent-ness can retire a replica the latest record calls live. That
    /// is not detectable after the fact without per-position history, so the input
    /// is refused instead.
    ///
    /// **The supported way to restore a record from before this driver existed is
    /// [`crate::TransportRaftDriver::with_control_plane_checkpoint`]**, which
    /// restores into empty held state and is the documented crash-recovery path.
    /// A supervisor holding another process's record builds a driver around it
    /// rather than joining it into one that has already observed something.
    StaleCurrentState {
        /// Position of the driver's current observation.
        held: LogIndex,
        /// Earlier position carried by the incoming checkpoint.
        incoming: LogIndex,
    },
    /// A record carrying a contradiction marker was offered to an adoption.
    ///
    /// **A marked record is evidence of an unresolved fork, and merging one is
    /// licensing what it refused.** The mark and register such a record carries
    /// were derived from a chain that observed two irreconcilable claims about
    /// one committed configuration; joining them into a driver that has been
    /// running would take that chain's retirement conclusions on trust and lose
    /// the marker in the join, since the joined record describes a driver that
    /// never saw the fork.
    ///
    /// **The constructor is the supported way to read one back.**
    /// [`crate::TransportRaftDriver::with_control_plane_checkpoint`] restores into
    /// empty held state, which is this chain resuming itself: it carries the
    /// marker, starts the driver refusing, and publishes nothing. So refusing
    /// here loses no record — the file is still on the embedder's disk — and what
    /// it costs is exactly the operation that must not happen.
    ///
    /// There is no retry that clears this. The operator's move is to decide what
    /// this replica's control plane should be, with the deployment's own record
    /// of what was retired, and reseed it deliberately.
    ContradictedRecordMerged {
        /// Position named by the incoming contradiction marker.
        through: LogIndex,
    },
    /// A checkpoint carries a contradiction marker and no current committed
    /// state.
    ///
    /// Not producible: a contradiction is a disagreement *between* the register
    /// and something else, so a driver that has observed nothing cannot record
    /// one. Absorbed, it would be a record that is unreadable rather than
    /// terminal — every identity at or below its mark would read as spent, on top
    /// of a driver that refuses everything anyway — so the honest answer is that
    /// the file was damaged.
    ContradictionWithoutCurrentState,
    /// A checkpoint's contradiction marker stands below its current committed
    /// state.
    ///
    /// The register freezes the moment the marker is set, and the candidate that
    /// found the contradiction may already have folded earlier facts of the same
    /// batch — so the marker's position is at or above the register's and never
    /// beneath it. A record that says otherwise has had one of the two damaged,
    /// and the pair no longer describes any state a driver reached.
    ContradictionBeneathCurrentState {
        /// Position named by the contradiction marker.
        contradicted_at: LogIndex,
        /// Later position of the checkpoint's current state.
        through: LogIndex,
    },
}

impl fmt::Display for ControlPlaneCheckpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ForeignGroup => {
                formatter.write_str("the checkpoint was written for a different group")
            }
            Self::LiveMemberAboveMark { node_id, mark } => write!(
                formatter,
                "the checkpoint's live member {node_id} is above its high-water mark {mark}"
            ),
            Self::RetirementWithoutCurrentState => formatter.write_str(
                "the checkpoint records what it retired and no committed membership to \
                 read it against, so every identity at or below its mark would be spent",
            ),
            Self::CurrentStateWithoutRetirement => formatter.write_str(
                "the checkpoint records a committed membership and no high-water mark, \
                 so the observation that produced it has been lost along with what it \
                 spent",
            ),
            Self::ContradictoryCurrentState { through } => write!(
                formatter,
                "two observations disagree about the committed membership at index {through}"
            ),
            Self::ContradictoryTransitionPredecessor { through } => write!(
                formatter,
                "the committed transition after index {through} declares a membership at \
                 index {through} that this driver's own record contradicts"
            ),
            Self::StaleCurrentState { held, incoming } => write!(
                formatter,
                "the checkpoint observed the committed membership at index {incoming}, before \
                 this driver's own observation at index {held}, so the two are not one chain"
            ),
            Self::ContradictedRecordMerged { through } => write!(
                formatter,
                "the checkpoint records an unresolved contradiction at index {through}, so \
                 merging it into a running driver would license the fork it refused; open a \
                 driver over it instead, or reseed this replica's control plane deliberately"
            ),
            Self::ContradictionWithoutCurrentState => formatter.write_str(
                "the checkpoint records a contradiction and no committed membership for it to \
                 have contradicted, so the record no driver could have written it",
            ),
            Self::ContradictionBeneathCurrentState {
                contradicted_at,
                through,
            } => write!(
                formatter,
                "the checkpoint records a contradiction at index {contradicted_at} beneath its \
                 own observation at index {through}, and a record freezes where it contradicts"
            ),
        }
    }
}

impl Error for ControlPlaneCheckpointError {}
