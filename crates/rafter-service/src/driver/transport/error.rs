//! Why an inbound peer envelope did not reach a group.
//!
//! A separate file for the same reason the variants are separate: `deliver`'s
//! three refusals say three different things about a deployment, and the
//! distinctions live here, next to each other, rather than interleaved with
//! the driver that raises them.

use std::error::Error;

use crate::transport::AuthenticatedPeerEnvelopeError;

use super::super::*;

/// Why an inbound peer envelope did not reach a group.
#[derive(Debug)]
#[non_exhaustive]
pub enum InboundEnvelopeError {
    /// The envelope failed inbound validation and was dropped. The group was
    /// not stepped and no state changed.
    Rejected {
        source: AuthenticatedPeerEnvelopeError,
    },
    /// The validator authorized the sender and this driver's own membership
    /// does not name it. The group was not stepped and no state changed.
    ///
    /// Separate from [`InboundEnvelopeError::Rejected`] because the two say
    /// different things about the deployment. `Rejected` is the link layer's own
    /// admission control working. This one is the link layer *disagreeing* with
    /// the group — the driver's membership has retired a replica the validator
    /// still authorizes — which for a removed replica means the fence for it has
    /// not been accepted yet. An operator who cannot tell them apart cannot tell
    /// a hostile peer from a control plane that is behind.
    NotInMembership { node_id: NodeId },
    /// The group step failed after the envelope was accepted.
    Driver { source: ManagedDriverError },
}

impl fmt::Display for InboundEnvelopeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected { .. } => {
                formatter.write_str("inbound peer envelope failed validation and was dropped")
            }
            Self::NotInMembership { node_id } => write!(
                formatter,
                "inbound peer envelope came from node {}, which this group's membership does not name",
                node_id.0
            ),
            Self::Driver { .. } => {
                formatter.write_str("the group step failed after the envelope was accepted")
            }
        }
    }
}

impl Error for InboundEnvelopeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Rejected { source } => Some(source),
            // No source: nothing failed underneath. The driver refused the
            // frame on its own authority, and the reason is the variant.
            Self::NotInMembership { .. } => None,
            Self::Driver { source } => Some(source),
        }
    }
}
