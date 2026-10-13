//! Standard error rendering for managed driver failures.

use std::{error::Error, fmt};

use super::ManagedDriverError;

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
            Self::MembershipRejected { reason, .. } => {
                write!(formatter, "managed driver membership change rejected: {reason}")
            }
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
            | Self::RetiredNodeId { .. }
            | Self::MembershipRejected { .. } => None,
        }
    }
}
