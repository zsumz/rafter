#![allow(clippy::wildcard_imports)]

use std::{error::Error, fmt};

use super::*;

/// Error returned while constructing or manually driving a managed service
/// driver.
///
/// Both shipped drivers report through this type, and they do not reach the
/// same variants. The cluster-shaped ones — [`ManagedDriverError::EmptyCluster`],
/// [`ManagedDriverError::MissingPrimary`], [`ManagedDriverError::MissingNode`],
/// [`ManagedDriverError::DuplicateNode`], and [`ManagedDriverError::Stalled`] —
/// describe a set of replicas and can only come from
/// [`crate::InMemoryRaftDriver`], which owns one. The incarnation-shaped ones —
/// [`ManagedDriverError::NoGroup`], [`ManagedDriverError::GroupAlreadyAdopted`],
/// and [`ManagedDriverError::InvalidOptions`] — describe a single replica's slot
/// and can only come from [`crate::TransportRaftDriver`], which has one. The
/// rest are adoption and stepping faults that either driver reports, including
/// [`ManagedDriverError::MixedGroups`]: each driver serves one group ID, and
/// each refuses a group that does not belong to it.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum ManagedDriverError {
    /// No groups were supplied, so there is no cluster to drive.
    EmptyCluster,
    /// The node named as primary is not among the supplied groups.
    ///
    /// The primary is the replica the in-memory driver proposes through, so a
    /// driver without one cannot serve a write at all.
    MissingPrimary { node_id: NodeId },
    /// A frame was addressed to a node this driver does not own.
    ///
    /// The in-memory network routes by node ID, so this is a routing fault
    /// rather than a cluster-membership one.
    MissingNode { node_id: NodeId },
    /// Two supplied groups claim the same node ID.
    ///
    /// Refused rather than deduplicated: the driver correlates outcomes by node,
    /// and two replicas answering to one ID make that correspondence undefined.
    DuplicateNode { node_id: NodeId },
    /// A group offered for adoption is poisoned, or still holds waiters a poison
    /// captured.
    ///
    /// A poisoned group emits no further events for those waiters, so adopting
    /// one would install clients that can never be resolved.
    PoisonedGroup { node_id: NodeId, reason: String },
    /// A group offered for adoption still tracks proposals or reads.
    ///
    /// A driver resolves only the waiters it created, so a waiter arriving with
    /// the group could never be resolved. [`crate::TransportRaftDriver::adopt_group`]
    /// is the one exception, and only for proposals: a released group's writes
    /// were already answered, and its entries are durable.
    NonQuiescentGroup {
        node_id: NodeId,
        pending_proposals: usize,
        reserved_reads: usize,
    },
    /// The adopted local proposal ID watermark cannot be advanced.
    ///
    /// Generated IDs must stay strictly above every ID the group has seen, and
    /// there is no ID above this one.
    LocalProposalIdExhausted {
        node_id: NodeId,
        last_seen_local_proposal_id: LocalProposalId,
    },
    /// The adopted read ID watermark cannot be advanced, for the reason
    /// [`ManagedDriverError::LocalProposalIdExhausted`] gives.
    ReadIdExhausted {
        node_id: NodeId,
        last_seen_read_id: ReadId,
    },
    /// A group offered to a driver does not belong to the group ID that driver
    /// serves.
    ///
    /// A driver serves exactly one group. For
    /// [`crate::InMemoryRaftDriver::new`] that means the supplied groups must
    /// all share one ID, or its handles would name only some of the replicas.
    /// For [`crate::TransportRaftDriver::adopt_group`] it means the incoming
    /// group must serve the ID the driver was built with, or client commands
    /// addressed to that ID would be proposed into another group's log.
    MixedGroups,
    /// The driver made no progress within its drive bound.
    ///
    /// A refusal rather than an unbounded wait, so a protocol that cannot
    /// advance surfaces as a typed error instead of a hang.
    Stalled { max_steps: usize },
    /// The driver has shut down, which is terminal.
    ///
    /// A supervisor that wants to serve again builds a driver; adopting a group
    /// into a shut-down one is refused.
    ShuttingDown,
    /// The driver released its group and has not adopted a new one.
    ///
    /// Every operation refuses in this state; nothing panics, because a slot
    /// with a typed empty state is the point of having one.
    NoGroup,
    /// The driver still holds a group, so it cannot adopt another.
    GroupAlreadyAdopted,
    /// A [`crate::TransportDriverOptions`] field was outside its valid range.
    InvalidOptions {
        field: &'static str,
        reason: &'static str,
    },
    /// A group was offered for adoption under a node ID a committed removal has
    /// already spent.
    ///
    /// A `(group_id, NodeId)` pair is single-use, and the identity a committed
    /// removal consumes is consumed for every replica of the group at once —
    /// including for the driver that watched the removal commit. Adopting it
    /// would install an identity whose transport principal the rest of the
    /// cluster has permanently fenced, so the replica would appear to join and
    /// then never be heard from.
    ///
    /// Refused before anything is installed, so the driver still holds no group
    /// and the supervisor's next move is to allocate a *fresh* ID — greater than
    /// every ID this group has ever committed — and adopt under that. There is
    /// no retry that clears this: see [`rafter::NodeId`].
    RetiredNodeId { node_id: NodeId },
    /// A recovered peer-control-plane checkpoint was refused, and nothing about
    /// it was installed.
    ///
    /// A checkpoint is durable caller-owned state that a restarted process reads
    /// back off its own disk, so it is exactly the kind of input that arrives
    /// corrupted, truncated, or belonging to a different replica. Every way it
    /// can be wrong lowers a retirement record — a smaller mark, an extra live
    /// identity, a fence against an active member — so it is refused whole
    /// rather than absorbed in part. The driver's own state is untouched.
    InvalidControlPlaneCheckpoint { reason: ControlPlaneCheckpointError },
    /// This driver already carries an unresolved contradiction, which is
    /// terminal for the incarnation — including across a group release.
    ///
    /// Distinct from [`ManagedDriverError::InvalidControlPlaneCheckpoint`]
    /// because nothing is wrong with the incoming record: the refusing state
    /// belongs to the driver already in memory. A supervisor that wants to
    /// recover builds a new driver from deliberately repaired or reseeded
    /// state; it does not rearm this one by handing it another group.
    ControlPlaneContradicted { reason: ControlPlaneCheckpointError },
    /// A group operation failed while the driver was driving it.
    ///
    /// The category is the variant and the detail is the preserved cause; there
    /// is no free-text message field, so nothing downstream can be tempted to
    /// match on rendered text.
    Group { cause: ErrorCause },
}

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
    LiveMemberAboveMark { node_id: NodeId, mark: NodeId },
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
    ContradictoryCurrentState { through: LogIndex },
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
    ContradictoryTransitionPredecessor { through: LogIndex },
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
    StaleCurrentState { held: LogIndex, incoming: LogIndex },
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
    ContradictedRecordMerged { through: LogIndex },
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
        contradicted_at: LogIndex,
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

impl fmt::Display for ManagedDriverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyCluster => formatter.write_str("managed driver requires at least one group"),
            Self::MissingPrimary { node_id } => {
                write!(formatter, "managed driver primary node {node_id} is missing")
            }
            Self::MissingNode { node_id } => {
                write!(formatter, "managed driver node {node_id} is missing")
            }
            Self::DuplicateNode { node_id } => {
                write!(formatter, "managed driver has duplicate node {node_id}")
            }
            Self::PoisonedGroup { node_id, reason } => write!(
                formatter,
                "managed driver group for node {node_id} is poisoned: {reason}"
            ),
            Self::NonQuiescentGroup {
                node_id,
                pending_proposals,
                reserved_reads,
            } => write!(
                formatter,
                "managed driver cannot adopt node {node_id}: {pending_proposals} pending proposals and {reserved_reads} reserved reads remain"
            ),
            Self::LocalProposalIdExhausted {
                node_id,
                last_seen_local_proposal_id,
            } => write!(
                formatter,
                "managed driver node {node_id} exhausted local proposal ids after {last_seen_local_proposal_id}"
            ),
            Self::ReadIdExhausted {
                node_id,
                last_seen_read_id,
            } => write!(
                formatter,
                "managed driver node {node_id} exhausted read ids after {last_seen_read_id}"
            ),
            Self::MixedGroups => formatter.write_str("managed driver cannot adopt mixed group ids"),
            Self::Stalled { max_steps } => write!(
                formatter,
                "managed driver made no progress within {max_steps} drive steps"
            ),
            Self::ShuttingDown => formatter.write_str("managed driver is shutting down"),
            Self::NoGroup => {
                formatter.write_str("managed driver has released its group and holds none")
            }
            Self::GroupAlreadyAdopted => {
                formatter.write_str("managed driver already holds a group")
            }
            Self::InvalidOptions { field, reason } => {
                write!(formatter, "managed driver option {field} is invalid: {reason}")
            }
            Self::RetiredNodeId { node_id } => write!(
                formatter,
                "managed driver cannot adopt {node_id}: a committed removal spent that identity"
            ),
            Self::InvalidControlPlaneCheckpoint { reason } => write!(
                formatter,
                "managed driver refused the peer control plane checkpoint: {reason}"
            ),
            Self::ControlPlaneContradicted { reason } => write!(
                formatter,
                "managed driver is terminally contradicted and adopts nothing: {reason}"
            ),
            Self::Group { .. } => formatter.write_str("managed driver group operation failed"),
        }
    }
}

impl Error for ManagedDriverError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Group { cause } => Some(cause.as_error()),
            Self::InvalidControlPlaneCheckpoint { reason }
            | Self::ControlPlaneContradicted { reason } => Some(reason),
            Self::EmptyCluster
            | Self::MissingPrimary { .. }
            | Self::MissingNode { .. }
            | Self::DuplicateNode { .. }
            | Self::PoisonedGroup { .. }
            | Self::NonQuiescentGroup { .. }
            | Self::LocalProposalIdExhausted { .. }
            | Self::ReadIdExhausted { .. }
            | Self::MixedGroups
            | Self::Stalled { .. }
            | Self::ShuttingDown
            | Self::NoGroup
            | Self::GroupAlreadyAdopted
            | Self::InvalidOptions { .. }
            | Self::RetiredNodeId { .. } => None,
        }
    }
}

/// Internal error carried between driver stages before it reaches a client.
///
/// `WrongGroup` is a driver fact rather than a delivery failure, which is why
/// it is a variant here instead of a synthesized transport error.
#[derive(Debug)]
pub(super) enum ManagedOperationError<E, RE> {
    MissingNode { node_id: NodeId },
    WrongGroup,
    DriveBoundReached { max_steps: usize },
    ShuttingDown,
    Write(WriteError),
    Read(ReadError),
    Transfer(TransferLeadershipError),
    Group(GroupError<E, RE>),
}

/// A driver stage that could not route its own work.
///
/// This is the driver reporting on itself, so it is the one place the service
/// layer authors an error object rather than preserving somebody else's.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DriverRoutingError {
    /// A frame was addressed to a node this driver does not own.
    MissingNode { node_id: NodeId },
    /// The driver stopped routing at its own bound rather than looping forever.
    DriveBoundReached { max_steps: usize },
    /// The driver already holds its configured maximum of unresolved waiters.
    ///
    /// Failing closed rather than growing: the operation was refused before
    /// anything was proposed, so nothing is in flight to be uncertain about.
    PendingWaiterLimit { max_pending_waiters: usize },
}

impl fmt::Display for DriverRoutingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingNode { node_id } => {
                write!(formatter, "managed driver node {node_id} is missing")
            }
            Self::DriveBoundReached { max_steps } => write!(
                formatter,
                "managed driver did not drain within {max_steps} drive steps"
            ),
            Self::PendingWaiterLimit {
                max_pending_waiters,
            } => write!(
                formatter,
                "managed driver already holds {max_pending_waiters} unresolved waiters"
            ),
        }
    }
}

impl Error for DriverRoutingError {}

impl<E, RE> ManagedOperationError<E, RE>
where
    E: Error + Send + Sync + 'static,
    RE: Error + Send + Sync + 'static,
{
    /// Maps a staged error into the write surface.
    ///
    /// `fate` is the fate the driver observed for this write, and it is passed
    /// in rather than inferred: the same fault can occur on either side of the
    /// local append, and only the caller knows which side this one was on.
    pub(super) fn into_write_error(self, fate: WriteFate) -> WriteError {
        match self {
            Self::Write(error) => error,
            Self::Read(error) => WriteError::Transport {
                fate,
                cause: ErrorCause::new(error),
            },
            Self::Transfer(error) => WriteError::Transport {
                fate,
                cause: ErrorCause::new(error),
            },
            Self::MissingNode { node_id } => WriteError::Transport {
                fate,
                cause: ErrorCause::new(DriverRoutingError::MissingNode { node_id }),
            },
            Self::DriveBoundReached { max_steps } => WriteError::Transport {
                fate,
                cause: ErrorCause::new(DriverRoutingError::DriveBoundReached { max_steps }),
            },
            Self::WrongGroup => WriteError::WrongGroup,
            Self::ShuttingDown => WriteError::ShuttingDown,
            Self::Group(error) => write_error_from_group(error, fate),
        }
    }

    pub(super) fn into_read_error(self) -> ReadError {
        match self {
            Self::Read(error) => error,
            Self::Write(error) => ReadError::Transport {
                cause: ErrorCause::new(error),
            },
            Self::Transfer(error) => ReadError::Transport {
                cause: ErrorCause::new(error),
            },
            Self::MissingNode { node_id } => ReadError::Transport {
                cause: ErrorCause::new(DriverRoutingError::MissingNode { node_id }),
            },
            Self::DriveBoundReached { max_steps } => ReadError::Transport {
                cause: ErrorCause::new(DriverRoutingError::DriveBoundReached { max_steps }),
            },
            Self::WrongGroup => ReadError::WrongGroup,
            Self::ShuttingDown => ReadError::ShuttingDown,
            Self::Group(error) => read_error_from_group(error),
        }
    }

    pub(super) fn into_transfer_error(self) -> TransferLeadershipError {
        match self {
            Self::Transfer(error) => error,
            Self::Write(error) => TransferLeadershipError::Transport {
                cause: ErrorCause::new(error),
            },
            Self::Read(error) => TransferLeadershipError::Transport {
                cause: ErrorCause::new(error),
            },
            Self::MissingNode { node_id } => TransferLeadershipError::Transport {
                cause: ErrorCause::new(DriverRoutingError::MissingNode { node_id }),
            },
            Self::DriveBoundReached { max_steps } => TransferLeadershipError::Transport {
                cause: ErrorCause::new(DriverRoutingError::DriveBoundReached { max_steps }),
            },
            Self::WrongGroup => TransferLeadershipError::WrongGroup,
            Self::ShuttingDown => TransferLeadershipError::ShuttingDown,
            Self::Group(error) => transfer_error_from_group(error),
        }
    }
}

impl<E, RE> From<GroupError<E, RE>> for ManagedOperationError<E, RE> {
    fn from(error: GroupError<E, RE>) -> Self {
        Self::Group(error)
    }
}

impl<E, RE> From<ManagedOperationError<E, RE>> for ManagedDriverError
where
    E: Error + Send + Sync + 'static,
    RE: Error + Send + Sync + 'static,
{
    fn from(error: ManagedOperationError<E, RE>) -> Self {
        match error {
            ManagedOperationError::MissingNode { node_id } => Self::MissingNode { node_id },
            ManagedOperationError::DriveBoundReached { max_steps } => Self::Group {
                cause: ErrorCause::new(DriverRoutingError::DriveBoundReached { max_steps }),
            },
            ManagedOperationError::WrongGroup => Self::Group {
                cause: ErrorCause::new(WriteError::WrongGroup),
            },
            ManagedOperationError::ShuttingDown => Self::ShuttingDown,
            ManagedOperationError::Write(error) => Self::Group {
                cause: ErrorCause::new(error),
            },
            ManagedOperationError::Read(error) => Self::Group {
                cause: ErrorCause::new(error),
            },
            ManagedOperationError::Transfer(error) => Self::Group {
                cause: ErrorCause::new(error),
            },
            ManagedOperationError::Group(error) => Self::Group {
                cause: ErrorCause::new(error),
            },
        }
    }
}

pub(super) fn write_error_from_group<E, RE>(error: GroupError<E, RE>, fate: WriteFate) -> WriteError
where
    E: Error + Send + Sync + 'static,
    RE: Error + Send + Sync + 'static,
{
    match error {
        GroupError::Poisoned { reason, cause } => WriteError::Poisoned {
            fate,
            reason,
            cause,
        },
        GroupError::NonMonotonicLocalProposalId {
            local_proposal_id,
            last_seen_local_proposal_id,
        } => {
            // Provably pre-append: the group refuses before it proposes.
            WriteError::ManagedInvariantViolation {
                fate: WriteFate::NotAppended,
                message: format!(
                    "managed driver local-ID invariant violation: generated non-monotonic local proposal id {local_proposal_id} after {last_seen_local_proposal_id}"
                ),
            }
        }
        GroupError::WrongGroup => WriteError::WrongGroup,
        // The operation is load-bearing and is no longer folded away: encoding
        // a command touches no storage, and reporting it as a storage failure
        // pointed an operator at the wrong subsystem.
        GroupError::StateMachine { operation, source } => WriteError::StateMachine {
            operation,
            fate,
            cause: ErrorCause::from_shared(source),
        },
        GroupError::Runtime(error) => WriteError::Storage {
            fate,
            cause: ErrorCause::new(error),
        },
        error => WriteError::Transport {
            fate,
            cause: ErrorCause::new(error),
        },
    }
}

/// The client answer a routed [`ReadEvent`] carries, when it ends the barrier.
///
/// Both shipped drivers route read events, so both need the same reading of
/// one. `Rejected` and `Canceled` are terminal: the app layer cleared the
/// barrier's local waiter state before emitting them, so the event is the whole
/// answer and nothing may ask the group again — a retry against a spent
/// `ReadId` gets [`GroupError::NonMonotonicReadId`], which a driver can only
/// report as an invariant violation of its own.
///
/// The rest are `None` because they are not answers. `Granted` leaves the proof
/// cached for a read call to consume, `FreshnessUnavailable` leaves the barrier
/// reserved until the applied index catches up, and a variant neither driver
/// knows is not something to resolve a client with. In all three the caller
/// keeps waiting.
pub(super) fn terminal_read_error<G>(event: &ReadEvent<G>) -> Option<(ReadId, ReadError)> {
    match event {
        ReadEvent::Rejected {
            read_id,
            reason,
            leader_hint,
        } => Some((
            *read_id,
            ReadError::Rejected {
                read_id: Some(*read_id),
                reason: *reason,
                leader_hint: *leader_hint,
            },
        )),
        ReadEvent::Canceled {
            read_id,
            reason,
            leader_hint,
        } => Some((
            *read_id,
            ReadError::Canceled {
                read_id: *read_id,
                reason: *reason,
                leader_hint: *leader_hint,
            },
        )),
        _ => None,
    }
}

pub(super) fn read_error_from_group<E, RE>(error: GroupError<E, RE>) -> ReadError
where
    E: Error + Send + Sync + 'static,
    RE: Error + Send + Sync + 'static,
{
    match error {
        GroupError::Poisoned { reason, cause } => ReadError::Poisoned { reason, cause },
        GroupError::DuplicateReadId { read_id } => ReadError::ManagedInvariantViolation {
            message: format!(
                "managed driver local-ID invariant violation: generated duplicate read id {read_id}"
            ),
        },
        GroupError::NonMonotonicReadId {
            read_id,
            last_seen_read_id,
        } => ReadError::ManagedInvariantViolation {
            message: format!(
                "managed driver local-ID invariant violation: generated non-monotonic read id {read_id} after {last_seen_read_id}"
            ),
        },
        GroupError::WrongGroup => ReadError::WrongGroup,
        GroupError::StateMachine { operation, source } => ReadError::StateMachine {
            operation,
            cause: ErrorCause::from_shared(source),
        },
        GroupError::Runtime(error) => ReadError::Storage {
            cause: ErrorCause::new(error),
        },
        GroupError::UnsupportedReadConsistency { consistency } => {
            ReadError::UnsupportedConsistency { consistency }
        }
        error => ReadError::Transport {
            cause: ErrorCause::new(error),
        },
    }
}

pub(super) fn transfer_error_from_group<E, RE>(error: GroupError<E, RE>) -> TransferLeadershipError
where
    E: Error + Send + Sync + 'static,
    RE: Error + Send + Sync + 'static,
{
    match error {
        GroupError::Poisoned { reason, cause } => {
            TransferLeadershipError::Poisoned { reason, cause }
        }
        GroupError::WrongGroup => TransferLeadershipError::WrongGroup,
        GroupError::Runtime(error) => TransferLeadershipError::Storage {
            cause: ErrorCause::new(error),
        },
        error => TransferLeadershipError::Transport {
            cause: ErrorCause::new(error),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rafter_app::error::StateMachineOperation;

    use super::*;

    #[derive(Debug)]
    struct MappingRuntimeError;

    impl fmt::Display for MappingRuntimeError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("mapping runtime error")
        }
    }

    impl Error for MappingRuntimeError {}

    #[derive(Debug)]
    struct MappingAppError;

    impl fmt::Display for MappingAppError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("mapping app error")
        }
    }

    impl Error for MappingAppError {}

    #[test]
    fn non_monotonic_local_proposal_id_maps_to_managed_invariant_write_error() {
        let error = write_error_from_group::<MappingAppError, MappingRuntimeError>(
            GroupError::NonMonotonicLocalProposalId {
                local_proposal_id: LocalProposalId(7),
                last_seen_local_proposal_id: LocalProposalId(9),
            },
            WriteFate::Unresolved,
        );

        let WriteError::ManagedInvariantViolation { fate, message } = &error else {
            panic!("expected a managed invariant violation, got {error:?}");
        };
        assert_eq!(
            message,
            "managed driver local-ID invariant violation: generated non-monotonic local proposal id local-proposal-7 after local-proposal-9"
        );
        assert_eq!(
            *fate,
            WriteFate::NotAppended,
            "the group refuses a non-monotonic id before it proposes"
        );
    }

    #[test]
    fn duplicate_read_id_maps_to_managed_invariant_read_error() {
        let error = read_error_from_group::<MappingAppError, MappingRuntimeError>(
            GroupError::DuplicateReadId { read_id: ReadId(8) },
        );

        let ReadError::ManagedInvariantViolation { message } = &error else {
            panic!("expected a managed invariant violation, got {error:?}");
        };
        assert_eq!(
            message,
            "managed driver local-ID invariant violation: generated duplicate read id read-8"
        );
    }

    #[test]
    fn non_monotonic_read_id_maps_to_managed_invariant_read_error() {
        let error = read_error_from_group::<MappingAppError, MappingRuntimeError>(
            GroupError::NonMonotonicReadId {
                read_id: ReadId(8),
                last_seen_read_id: ReadId(10),
            },
        );

        let ReadError::ManagedInvariantViolation { message } = &error else {
            panic!("expected a managed invariant violation, got {error:?}");
        };
        assert_eq!(
            message,
            "managed driver local-ID invariant violation: generated non-monotonic read id read-8 after read-10"
        );
    }

    /// The old mapping folded six operations into two variants and got one
    /// wrong: `EncodeCommand` was reported as a storage failure, and encoding a
    /// command touches no storage.
    #[test]
    fn a_state_machine_error_keeps_the_operation_that_surfaced_it() {
        let error = write_error_from_group::<MappingAppError, MappingRuntimeError>(
            GroupError::StateMachine {
                operation: StateMachineOperation::EncodeCommand,
                source: Arc::new(MappingAppError),
            },
            WriteFate::NotAppended,
        );

        let WriteError::StateMachine {
            operation, cause, ..
        } = &error
        else {
            panic!("expected a state machine error, got {error:?}");
        };
        assert_eq!(*operation, StateMachineOperation::EncodeCommand);
        assert!(cause.downcast_ref::<MappingAppError>().is_some());
    }

    #[test]
    fn managed_driver_error_is_a_standard_error_with_display_message() {
        let error = ManagedDriverError::MissingNode { node_id: NodeId(9) };
        let standard_error: &(dyn Error + 'static) = &error;

        assert_eq!(
            standard_error.to_string(),
            "managed driver node node-9 is missing"
        );
    }

    /// The category is the variant; the detail is the preserved cause. There is
    /// no message field to render into.
    #[test]
    fn a_group_driver_error_preserves_its_cause() {
        let error = ManagedDriverError::from(ManagedOperationError::<
            MappingAppError,
            MappingRuntimeError,
        >::Group(GroupError::Runtime(
            MappingRuntimeError,
        )));

        let source = error.source().expect("the group error is preserved");

        assert!(source
            .downcast_ref::<GroupError<MappingAppError, MappingRuntimeError>>()
            .is_some());
    }
}
