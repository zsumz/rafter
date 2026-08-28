#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

//! What a driver reports about itself.
//!
//! One type for every way constructing or manually driving a managed driver can
//! fail, with its rendering beside it. It carries no free-text message field:
//! the category is the variant and the detail is the preserved cause, so nothing
//! downstream is tempted to match on rendered text.

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
    MissingPrimary {
        /// Node selected as primary.
        node_id: NodeId,
    },
    /// A frame was addressed to a node this driver does not own.
    ///
    /// The in-memory network routes by node ID, so this is a routing fault
    /// rather than a cluster-membership one.
    MissingNode {
        /// Destination absent from the driver's replica set.
        node_id: NodeId,
    },
    /// Two supplied groups claim the same node ID.
    ///
    /// Refused rather than deduplicated: the driver correlates outcomes by node,
    /// and two replicas answering to one ID make that correspondence undefined.
    DuplicateNode {
        /// Node identifier claimed by more than one supplied group.
        node_id: NodeId,
    },
    /// A group offered for adoption is poisoned, or still holds waiters a poison
    /// captured.
    ///
    /// A poisoned group emits no further events for those waiters, so adopting
    /// one would install clients that can never be resolved.
    PoisonedGroup {
        /// Node identifier of the poisoned group.
        node_id: NodeId,
        /// Stable poison explanation retained by the group.
        reason: String,
    },
    /// A group offered for adoption still tracks proposals or reads.
    ///
    /// A driver resolves only the waiters it created, so a waiter arriving with
    /// the group could never be resolved. [`crate::TransportRaftDriver::adopt_group`]
    /// is the one exception, and only for proposals: a released group's writes
    /// were already answered, and its entries are durable.
    NonQuiescentGroup {
        /// Node identifier of the non-quiescent group.
        node_id: NodeId,
        /// Proposals still awaiting terminal outcomes.
        pending_proposals: usize,
        /// Read identifiers still reserved by the group.
        reserved_reads: usize,
    },
    /// The adopted local proposal ID watermark cannot be advanced.
    ///
    /// Generated IDs must stay strictly above every ID the group has seen, and
    /// there is no ID above this one.
    LocalProposalIdExhausted {
        /// Node identifier of the adopted group.
        node_id: NodeId,
        /// Greatest proposal identifier already observed.
        last_seen_local_proposal_id: LocalProposalId,
    },
    /// The adopted read ID watermark cannot be advanced, for the reason
    /// [`ManagedDriverError::LocalProposalIdExhausted`] gives.
    ReadIdExhausted {
        /// Node identifier of the adopted group.
        node_id: NodeId,
        /// Greatest read identifier already observed.
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
    Stalled {
        /// Maximum drive steps attempted without reaching a terminal outcome.
        max_steps: usize,
    },
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
        /// Invalid option name.
        field: &'static str,
        /// Stable explanation of the rejected value.
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
    RetiredNodeId {
        /// Spent node identifier offered for adoption.
        node_id: NodeId,
    },
    /// A recovered peer-control-plane checkpoint was refused, and nothing about
    /// it was installed.
    ///
    /// A checkpoint is durable caller-owned state that a restarted process reads
    /// back off its own disk, so it is exactly the kind of input that arrives
    /// corrupted, truncated, or belonging to a different replica. Every way it
    /// can be wrong lowers a retirement record — a smaller mark, an extra live
    /// identity, a fence against an active member — so it is refused whole
    /// rather than absorbed in part. The driver's own state is untouched.
    InvalidControlPlaneCheckpoint {
        /// Checkpoint invariant that was violated.
        reason: ControlPlaneCheckpointError,
    },
    /// This driver already carries an unresolved contradiction, which is
    /// terminal for the incarnation — including across a group release.
    ///
    /// Distinct from [`ManagedDriverError::InvalidControlPlaneCheckpoint`]
    /// because nothing is wrong with the incoming record: the refusing state
    /// belongs to the driver already in memory. A supervisor that wants to
    /// recover builds a new driver from deliberately repaired or reseeded
    /// state; it does not rearm this one by handing it another group.
    ControlPlaneContradicted {
        /// Contradiction already retained by the driver.
        reason: ControlPlaneCheckpointError,
    },
    /// A group operation failed while the driver was driving it.
    ///
    /// The category is the variant and the detail is the preserved cause; there
    /// is no free-text message field, so nothing downstream can be tempted to
    /// match on rendered text.
    Group {
        /// Preserved typed group failure.
        cause: ErrorCause,
    },
}

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
