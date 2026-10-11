//! One retained membership proposal, distinct from a committed configuration.
use super::super::reply::Reply;
use rafter::{LogIndex, NodeId, ProposalRejection, Term};
use rafter_app::membership::MembershipChange;
use std::{num::NonZeroUsize, sync::Arc};

mod admission;
mod drive;

/// Queue payload bound for opt-in managed membership proposals.
///
/// Membership and leadership transfer share one retained control slot. It
/// includes queued and submitted work, canceled work still owned by the driver,
/// and a completed result awaiting collection. The payload charge covers the
/// normalized target and promotion-barrier vector capacities. It excludes the
/// kernel log, transient dispatch/step allocations, and caller-owned data.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct DurableMembershipLimits {
    /// Maximum normalized configuration payload retained by a control request.
    pub request_bytes: NonZeroUsize,
}

impl DurableMembershipLimits {
    /// Sets the configuration payload budget for the single control slot.
    #[must_use]
    pub const fn new(request_bytes: NonZeroUsize) -> Self {
        Self { request_bytes }
    }
}

/// A membership proposal was durably appended and its immediate effects drained.
///
/// This is not a commit receipt. The entry may still be replaced after a leader
/// change. Observe the matching configuration commit before advancing a plan,
/// finalizing joint consensus, or fencing a removed peer. A later commit index
/// alone does not prove this particular term/index pair committed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct DurableMembershipReceipt {
    /// Index at which this incarnation appended the configuration proposal.
    pub index: LogIndex,
    /// Term of that configuration proposal.
    pub term: Term,
}

/// Refusal or loss of confirmation for a managed membership proposal.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum DurableMembershipError {
    /// This endpoint belongs to another group; nothing was admitted.
    WrongGroup,
    /// Attach with membership limits to enable managed proposal correlation.
    NotEnabled,
    /// The control credit is retained, or the configuration exceeds its budget.
    Backpressure,
    /// The request failed conversion or a kernel membership precondition.
    Rejected {
        /// Exact configuration or protocol refusal.
        reason: ProposalRejection,
        /// Best-known leader when the request was refused.
        leader_hint: Option<NodeId>,
    },
    /// Admission closed before the queued request was submitted.
    ShuttingDown,
    /// The backend was released before the submitted request was confirmed.
    /// The proposal may have been appended or committed; inspect recovered state.
    Unconfirmed,
}

impl std::fmt::Display for DurableMembershipError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "durable membership proposal: {self:?}")
    }
}
impl std::error::Error for DurableMembershipError {}

type ProposalResult = Result<DurableMembershipReceipt, DurableMembershipError>;
type ProposalReply = Arc<Reply<ProposalResult>>;

pub(super) struct Pending {
    // Request data must drop before its reply releases the admission credit.
    pub change: Option<MembershipChange>,
    pub submitted: bool,
    pub receipt: Option<DurableMembershipReceipt>,
    pub rejection: Option<ProposalRejection>,
    pub leader_hint: Option<NodeId>,
    pub reply: ProposalReply,
}
