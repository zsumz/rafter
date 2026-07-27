//! Application-layer membership planning and reporting types.
//!
//! These types represent safe membership flows while leaving transport
//! peer updates and fencing under the caller's runtime policy.

use std::collections::BTreeMap;

use rafter::{
    LogIndex, MembershipConfig, MembershipSet, NodeId, PromotionBarrier, ProposalRejection, Term,
};

/// Application/runtime metadata associated with a Raft node.
///
/// Rafter treats this as opaque app-layer information. Transport identity,
/// addresses, and authorization details remain the caller's responsibility.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NodeInfo {
    pub metadata: BTreeMap<String, String>,
}

/// A requested membership change at the app layer.
///
/// This enum is exhaustive for the membership operations planned by
/// `rafter-app` today.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MembershipChange {
    AddLearner {
        node_id: NodeId,
        info: NodeInfo,
    },
    PromoteLearner {
        node_id: NodeId,
        barrier: PromotionBarrier,
    },
    RemoveNode {
        node_id: NodeId,
    },
    EnterJoint {
        target: MembershipSet,
        promotion_barriers: Vec<PromotionBarrier>,
    },
    LeaveJoint,
    ChangeVoters {
        target: MembershipSet,
    },
}

/// One explicit step in a safe membership flow.
///
/// This enum is exhaustive for the flow steps currently emitted by
/// `MembershipPlan`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MembershipStep {
    AddLearner(NodeId),
    WaitForCatchUp(NodeId),
    PromoteLearner(NodeId),
    EnterJoint(MembershipSet),
    LeaveJoint,
    RemoveNode(NodeId),
    FencePeer(NodeId),
}

/// A planned membership flow for one group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipPlan<G> {
    pub group_id: G,
    pub steps: Vec<MembershipStep>,
}

/// Status for one membership step report.
///
/// This enum is exhaustive for the step states reported by `rafter-app`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MembershipStepStatus {
    Pending,
    Completed { at: Option<LogIndex> },
    Failed { reason: String },
}

/// Report for one membership step.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipStepReport {
    pub step: MembershipStep,
    pub status: MembershipStepStatus,
}

/// Progress report for a membership change flow.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipChangeReport<G> {
    pub group_id: G,
    pub started_at: LogIndex,
    pub completed_at: Option<LogIndex>,
    pub steps: Vec<MembershipStepReport>,
}

/// Membership side effects observed by a manual group driver.
///
/// Two facts and a refusal. The two facts are reported separately because a
/// consumer may do opposite things with them: an effective configuration may
/// still be uncommitted and may still be taken back, so it may only *widen* a
/// peer set; a committed one is the only fact that licenses narrowing it or
/// fencing what left.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MembershipEvent<G> {
    /// The configuration this replica is now operating under changed.
    ///
    /// Named for the fact rather than for a cause, because it has several and a
    /// consumer must not care which: a local membership request, a
    /// configuration entry that arrived by replication, a new leader truncating
    /// an uncommitted one back off the log, and a snapshot install all reach
    /// here. The previous name said `Appended`, which was true of exactly one
    /// of those and false of the truncation that is the most dangerous to miss.
    ///
    /// `index` and `term` are an **observation point**, not the location of a
    /// configuration entry. They are this replica's last log index and the term
    /// at it when the change was observed, so for an append they do name the
    /// entry that carried the configuration, and for a truncation or a snapshot
    /// install they name where the log now ends. A consumer that needs the
    /// entry itself must read the log; a consumer keeping a peer set current —
    /// which is what this event is for — needs only the membership.
    EffectiveChanged {
        group_id: G,
        index: LogIndex,
        term: Term,
        membership: MembershipConfig,
    },
    /// The configuration the cluster has committed changed.
    ///
    /// `index` and `term` are the commit index and its term, which is the same
    /// kind of observation point: a commit is observed at the index it reached,
    /// and that index names a configuration entry only when the commit stopped
    /// exactly on one.
    Applied {
        group_id: G,
        index: LogIndex,
        term: Term,
        membership: MembershipConfig,
    },
    Rejected {
        group_id: G,
        reason: ProposalRejection,
        leader_hint: Option<NodeId>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn membership(voters: &[u64], learners: &[u64]) -> MembershipSet {
        MembershipSet::new(
            voters.iter().copied().map(NodeId).collect(),
            learners.iter().copied().map(NodeId).collect(),
        )
        .expect("test membership is valid")
    }

    #[test]
    fn membership_change_types_represent_supported_requests() {
        let mut info = NodeInfo::default();
        info.metadata
            .insert("zone".to_owned(), "test-zone".to_owned());

        let changes = [
            MembershipChange::AddLearner {
                node_id: NodeId(4),
                info,
            },
            MembershipChange::PromoteLearner {
                node_id: NodeId(4),
                barrier: PromotionBarrier {
                    learner_id: NodeId(4),
                    required_match_index: LogIndex(9),
                },
            },
            MembershipChange::RemoveNode { node_id: NodeId(2) },
            MembershipChange::EnterJoint {
                target: membership(&[1, 3, 4], &[]),
                promotion_barriers: Vec::new(),
            },
            MembershipChange::LeaveJoint,
            MembershipChange::ChangeVoters {
                target: membership(&[1, 3, 4], &[]),
            },
        ];

        assert_eq!(changes.len(), 6);
    }

    #[test]
    fn membership_plan_can_represent_safe_flow_and_fencing() {
        let target = membership(&[1, 3, 4], &[]);
        let plan = MembershipPlan {
            group_id: "group-a",
            steps: vec![
                MembershipStep::AddLearner(NodeId(4)),
                MembershipStep::WaitForCatchUp(NodeId(4)),
                MembershipStep::PromoteLearner(NodeId(4)),
                MembershipStep::EnterJoint(target.clone()),
                MembershipStep::LeaveJoint,
                MembershipStep::RemoveNode(NodeId(2)),
                MembershipStep::FencePeer(NodeId(2)),
            ],
        };

        assert!(matches!(
            plan.steps[0],
            MembershipStep::AddLearner(NodeId(4))
        ));
        assert!(matches!(
            plan.steps[1],
            MembershipStep::WaitForCatchUp(NodeId(4))
        ));
        assert!(matches!(
            plan.steps[2],
            MembershipStep::PromoteLearner(NodeId(4))
        ));
        assert_eq!(plan.steps[3], MembershipStep::EnterJoint(target));
        assert!(matches!(plan.steps[4], MembershipStep::LeaveJoint));
        assert!(matches!(
            plan.steps[5],
            MembershipStep::RemoveNode(NodeId(2))
        ));
        assert!(matches!(
            plan.steps[6],
            MembershipStep::FencePeer(NodeId(2))
        ));
    }

    #[test]
    fn membership_change_report_tracks_step_progress() {
        let report = MembershipChangeReport {
            group_id: 7_u64,
            started_at: LogIndex(10),
            completed_at: Some(LogIndex(15)),
            steps: vec![
                MembershipStepReport {
                    step: MembershipStep::AddLearner(NodeId(4)),
                    status: MembershipStepStatus::Completed {
                        at: Some(LogIndex(11)),
                    },
                },
                MembershipStepReport {
                    step: MembershipStep::FencePeer(NodeId(2)),
                    status: MembershipStepStatus::Pending,
                },
            ],
        };

        assert_eq!(report.group_id, 7);
        assert_eq!(report.started_at, LogIndex(10));
        assert_eq!(report.completed_at, Some(LogIndex(15)));
        assert_eq!(report.steps.len(), 2);
    }
}
