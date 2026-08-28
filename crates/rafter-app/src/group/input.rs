//! The closed set of operations a group driver dispatches.
//!
//! Exhaustive on purpose: a new operation must break a driver that would
//! otherwise ignore it through a wildcard.

use super::{MembershipChange, NodeId, PeerEnvelope, Proposal, ReadBarrierRequest};

/// Inputs accepted by the synchronous group driver.
///
/// This enum is exhaustive because it is the closed set a group driver must
/// dispatch. A new operation must break drivers that would otherwise ignore it
/// through a wildcard.
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(
    clippy::large_enum_variant,
    reason = "payloads ride inline; boxing taxes every dispatch"
)]
pub enum GroupInput<G, C> {
    /// Advance logical Raft time by one caller-owned tick.
    Tick,
    /// Deliver one authenticated and admitted peer message.
    PeerMessage {
        /// Group-aware peer envelope.
        envelope: PeerEnvelope<G>,
    },
    /// Submit one application command.
    Proposal {
        /// Command and local correlation metadata.
        proposal: Proposal<C>,
    },
    /// Submit commands in caller order.
    ProposalBatch {
        /// Ordered proposals; each receives its own lifecycle outcome.
        proposals: Vec<Proposal<C>>,
    },
    /// Begin or retry a proof-producing read barrier.
    ReadBarrier {
        /// Group, read ID, freshness floor, and opaque context.
        request: ReadBarrierRequest<G>,
    },
    /// Ask the leader to hand authority to a caught-up voter.
    TransferLeadership {
        /// Desired successor.
        target: NodeId,
    },
    /// Submit one explicit membership operation.
    Membership {
        /// Membership change to validate and propose.
        change: MembershipChange,
    },
}
