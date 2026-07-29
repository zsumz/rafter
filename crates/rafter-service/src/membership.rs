//! Managed membership planning and reporting helpers.
//!
//! The service controller plans safe flows over the app-layer membership
//! primitives. It does not send transport messages or apply Raft inputs by
//! itself; drivers and manual runtimes can inspect the returned app-layer
//! plans and perform equivalent routing, fencing, and reporting.

use rafter::{LogIndex, MembershipSet, NodeId, PromotionBarrier};
use rafter_app::membership::{
    MembershipChange, MembershipChangeReport, MembershipPlan, MembershipStep, MembershipStepReport,
    MembershipStepStatus, NodeInfo,
};

/// Service-layer membership controller handle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipController<G> {
    group_id: G,
}

/// A planned membership change and its safe flow.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannedMembershipChange<G> {
    /// Change that was planned.
    pub change: MembershipChange,
    /// Safe transition plan for applying the change.
    pub plan: MembershipPlan<G>,
}

impl<G> MembershipController<G> {
    /// Creates a membership controller for one group.
    #[must_use]
    pub fn new(group_id: G) -> Self {
        Self { group_id }
    }

    /// Returns the group ID this controller plans changes for.
    #[must_use]
    pub fn group_id(&self) -> &G {
        &self.group_id
    }
}

impl<G: Clone> MembershipController<G> {
    /// Plans adding a learner and waiting for it to catch up before any later
    /// promotion.
    #[must_use]
    pub fn add_learner(&self, node_id: NodeId, info: NodeInfo) -> PlannedMembershipChange<G> {
        PlannedMembershipChange {
            change: MembershipChange::AddLearner { node_id, info },
            plan: self.plan(vec![
                MembershipStep::AddLearner(node_id),
                MembershipStep::WaitForCatchUp(node_id),
            ]),
        }
    }

    /// Plans learner promotion using the caller-supplied promotion barrier.
    #[must_use]
    pub fn promote_learner(&self, barrier: PromotionBarrier) -> PlannedMembershipChange<G> {
        let node_id = barrier.learner_id;
        PlannedMembershipChange {
            change: MembershipChange::PromoteLearner { node_id, barrier },
            plan: self.plan(vec![
                MembershipStep::WaitForCatchUp(node_id),
                MembershipStep::PromoteLearner(node_id),
            ]),
        }
    }

    /// Plans removing a node and then fencing its transport identity.
    #[must_use]
    pub fn remove_node(&self, node_id: NodeId) -> PlannedMembershipChange<G> {
        PlannedMembershipChange {
            change: MembershipChange::RemoveNode { node_id },
            plan: self.plan(vec![
                MembershipStep::RemoveNode(node_id),
                MembershipStep::FencePeer(node_id),
            ]),
        }
    }

    /// Plans a voter-set change through joint consensus.
    #[must_use]
    pub fn change_voters(&self, target: MembershipSet) -> PlannedMembershipChange<G> {
        PlannedMembershipChange {
            change: MembershipChange::ChangeVoters {
                target: target.clone(),
            },
            plan: self.plan(vec![
                MembershipStep::EnterJoint(target),
                MembershipStep::LeaveJoint,
            ]),
        }
    }

    /// Creates a pending progress report for a planned flow.
    #[must_use]
    pub fn pending_report(
        &self,
        started_at: LogIndex,
        plan: &MembershipPlan<G>,
    ) -> MembershipChangeReport<G> {
        MembershipChangeReport {
            group_id: plan.group_id.clone(),
            started_at,
            completed_at: None,
            steps: plan
                .steps
                .iter()
                .cloned()
                .map(|step| MembershipStepReport {
                    step,
                    status: MembershipStepStatus::Pending,
                })
                .collect(),
        }
    }

    /// Creates a completed progress report for a planned flow.
    #[must_use]
    pub fn completed_report(
        &self,
        started_at: LogIndex,
        completed_at: LogIndex,
        plan: &MembershipPlan<G>,
    ) -> MembershipChangeReport<G> {
        MembershipChangeReport {
            group_id: plan.group_id.clone(),
            started_at,
            completed_at: Some(completed_at),
            steps: plan
                .steps
                .iter()
                .cloned()
                .map(|step| MembershipStepReport {
                    step,
                    status: MembershipStepStatus::Completed {
                        at: Some(completed_at),
                    },
                })
                .collect(),
        }
    }

    fn plan(&self, steps: Vec<MembershipStep>) -> MembershipPlan<G> {
        MembershipPlan {
            group_id: self.group_id.clone(),
            steps,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn add_learner_plan_waits_for_catchup() {
        let controller = MembershipController::new("group-a");
        let info = node_info("zone", "a");

        let planned = controller.add_learner(NodeId(4), info.clone());

        assert_eq!(
            planned.change,
            MembershipChange::AddLearner {
                node_id: NodeId(4),
                info,
            }
        );
        assert_eq!(
            planned.plan.steps,
            vec![
                MembershipStep::AddLearner(NodeId(4)),
                MembershipStep::WaitForCatchUp(NodeId(4)),
            ]
        );
    }

    #[test]
    fn promote_learner_plan_uses_barrier() {
        let controller = MembershipController::new("group-a");
        let barrier = PromotionBarrier {
            learner_id: NodeId(4),
            required_match_index: LogIndex(99),
        };

        let planned = controller.promote_learner(barrier);

        assert_eq!(
            planned.change,
            MembershipChange::PromoteLearner {
                node_id: NodeId(4),
                barrier,
            }
        );
        assert_eq!(
            planned.plan.steps,
            vec![
                MembershipStep::WaitForCatchUp(NodeId(4)),
                MembershipStep::PromoteLearner(NodeId(4)),
            ]
        );
    }

    #[test]
    fn remove_node_plan_represents_transport_fencing() {
        let controller = MembershipController::new("group-a");

        let planned = controller.remove_node(NodeId(2));

        assert_eq!(
            planned.change,
            MembershipChange::RemoveNode { node_id: NodeId(2) }
        );
        assert_eq!(
            planned.plan.steps,
            vec![
                MembershipStep::RemoveNode(NodeId(2)),
                MembershipStep::FencePeer(NodeId(2)),
            ]
        );
    }

    #[test]
    fn change_voters_plan_enters_and_leaves_joint_consensus() {
        let controller = MembershipController::new("group-a");
        let target = membership(&[1, 3, 4]);

        let planned = controller.change_voters(target.clone());

        assert_eq!(
            planned.change,
            MembershipChange::ChangeVoters {
                target: target.clone(),
            }
        );
        assert_eq!(
            planned.plan.steps,
            vec![
                MembershipStep::EnterJoint(target),
                MembershipStep::LeaveJoint,
            ]
        );
    }

    #[test]
    fn reports_are_produced_from_app_layer_plan_types() {
        let controller = MembershipController::new("group-a");
        let planned = controller.remove_node(NodeId(2));

        let pending = controller.pending_report(LogIndex(10), &planned.plan);
        assert_eq!(pending.group_id, "group-a");
        assert_eq!(pending.started_at, LogIndex(10));
        assert_eq!(pending.completed_at, None);
        assert!(pending
            .steps
            .iter()
            .all(|step| step.status == MembershipStepStatus::Pending));

        let completed = controller.completed_report(LogIndex(10), LogIndex(12), &planned.plan);
        assert_eq!(completed.completed_at, Some(LogIndex(12)));
        assert!(completed.steps.iter().all(|step| {
            step.status
                == MembershipStepStatus::Completed {
                    at: Some(LogIndex(12)),
                }
        }));
    }

    fn node_info(key: &str, value: &str) -> NodeInfo {
        NodeInfo {
            metadata: BTreeMap::from([(key.to_owned(), value.to_owned())]),
        }
    }

    fn membership(voters: &[u64]) -> MembershipSet {
        MembershipSet::new(voters.iter().copied().map(NodeId).collect(), Vec::new())
            .expect("valid membership")
    }
}
