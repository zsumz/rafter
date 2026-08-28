//! Application-layer membership planning and reporting types.
//!
//! These types represent safe membership flows while leaving transport
//! peer updates and fencing under the caller's runtime policy.

use std::collections::BTreeMap;

use rafter::{LogIndex, MembershipSet, NodeId, PromotionBarrier};

mod event;

pub use event::MembershipEvent;

/// Application/runtime metadata associated with a Raft node.
///
/// Rafter treats this as opaque app-layer information. Transport identity,
/// addresses, and authorization details remain the caller's responsibility.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NodeInfo {
    /// Caller-defined metadata; Rafter does not interpret or persist it.
    pub metadata: BTreeMap<String, String>,
}

/// A requested membership change at the app layer.
///
/// This enum is exhaustive because it is the closed command vocabulary the app
/// layer translates into Raft membership inputs. A new operation must make
/// planners and executors choose its safety flow explicitly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MembershipChange {
    /// Add a non-voting replica without changing the voter quorum.
    AddLearner {
        /// Identity of the new learner.
        node_id: NodeId,
        /// Opaque caller metadata for the learner.
        info: NodeInfo,
    },
    /// Promote a caught-up learner into the voter set.
    PromoteLearner {
        /// Learner identity to promote.
        node_id: NodeId,
        /// Replication floor that must be satisfied before promotion.
        barrier: PromotionBarrier,
    },
    /// Remove a voter or learner from the group.
    RemoveNode {
        /// Replica identity to remove.
        node_id: NodeId,
    },
    /// Enter joint consensus toward a target membership.
    EnterJoint {
        /// Stable membership desired after leaving joint consensus.
        target: MembershipSet,
        /// Catch-up proofs required for learners promoted by the target.
        promotion_barriers: Vec<PromotionBarrier>,
    },
    /// Commit the stable target of the current joint configuration.
    LeaveJoint,
    /// Plan the safe learner/joint-consensus flow to a voter set.
    ChangeVoters {
        /// Desired stable membership.
        target: MembershipSet,
    },
}

/// One explicit step in a safe membership flow.
///
/// This enum is exhaustive because every emitted step is an obligation for a
/// membership executor. A new step must break executors that would otherwise
/// skip it through a wildcard.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MembershipStep {
    /// Submit an add-learner configuration.
    AddLearner(NodeId),
    /// Wait until the named learner reaches its promotion barrier.
    WaitForCatchUp(NodeId),
    /// Make the caught-up learner a voter.
    PromoteLearner(NodeId),
    /// Submit the joint configuration.
    EnterJoint(MembershipSet),
    /// Submit the stable configuration that ends joint consensus.
    LeaveJoint,
    /// Remove the named replica from membership.
    RemoveNode(NodeId),
    /// Fence the removed identity at the caller's transport boundary.
    FencePeer(NodeId),
}

/// A planned membership flow for one group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipPlan<G> {
    /// Group whose membership the plan changes.
    pub group_id: G,
    /// Ordered obligations; later steps must not run before earlier ones.
    pub steps: Vec<MembershipStep>,
}

/// Status for one membership step report.
///
/// This enum is exhaustive because a step is pending, completed, or failed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MembershipStepStatus {
    /// The step has not reached a terminal result.
    Pending,
    /// The step completed, optionally at a committed log index.
    Completed {
        /// Commit or observation index that completed the step.
        at: Option<LogIndex>,
    },
    /// The step reached a terminal failure.
    Failed {
        /// Caller- or protocol-provided diagnostic.
        reason: String,
    },
}

/// Report for one membership step.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipStepReport {
    /// Planned obligation being reported.
    pub step: MembershipStep,
    /// Current or terminal state of the obligation.
    pub status: MembershipStepStatus,
}

/// Progress report for a membership change flow.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipChangeReport<G> {
    /// Group whose membership is changing.
    pub group_id: G,
    /// Log index observed when the flow began.
    pub started_at: LogIndex,
    /// Log index that completed the flow, when complete.
    pub completed_at: Option<LogIndex>,
    /// Ordered status for every planned step.
    pub steps: Vec<MembershipStepReport>,
}

#[cfg(test)]
#[path = "membership_test.rs"]
mod tests;
