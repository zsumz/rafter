//! Managed membership planning and reporting helpers.
//!
//! The service controller plans safe flows over the app-layer membership
//! primitives. It does not send transport messages or apply Raft inputs by
//! itself; drivers and manual runtimes can inspect the returned app-layer
//! plans and perform equivalent routing, fencing, and reporting.

use rafter::{
    ConfigurationPhase, ConfigurationProposalRejection, LogIndex, MembershipConfig, MembershipSet,
    NodeId, PromotionBarrier,
};
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

    /// Plans learner promotion through joint consensus into stable membership.
    ///
    /// Uses the caller-supplied promotion barrier. The caller must wait for the
    /// joint configuration to commit before executing the final `LeaveJoint`
    /// step, and for the stable configuration to commit before completing the
    /// flow.
    #[must_use]
    pub fn promote_learner(&self, barrier: PromotionBarrier) -> PlannedMembershipChange<G> {
        let node_id = barrier.learner_id;
        PlannedMembershipChange {
            change: MembershipChange::PromoteLearner { node_id, barrier },
            plan: self.plan(vec![
                MembershipStep::WaitForCatchUp(node_id),
                MembershipStep::PromoteLearner(node_id),
                MembershipStep::LeaveJoint,
            ]),
        }
    }

    /// Plans removing a node and then fencing its transport identity.
    ///
    /// `RemoveNode` is a complete removal obligation here: for a voter, the
    /// executor must also leave joint consensus and observe the stable commit
    /// before fencing. Use [`Self::remove_node_from`] to make those proposal
    /// steps explicit from a known membership.
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

    /// Plans explicit removal steps from a known stable membership.
    ///
    /// Voter removal enters joint consensus, leaves it, and then fences the
    /// identity. Learner removal preserves the voter set and needs no joint
    /// phase. Other voters and learners are preserved in the planned target.
    ///
    /// The caller must serialize membership changes and verify that `current`
    /// is still both effective and committed before starting; otherwise replan.
    /// Wait for the joint commit before `LeaveJoint`, and for the stable commit
    /// excluding `node_id` before `FencePeer`. This helper does not execute or
    /// observe those commits.
    ///
    /// # Errors
    ///
    /// Returns the corresponding configuration rejection for a joint starting
    /// membership, an unknown node, removal of the last voter, or an invalid
    /// derived target.
    pub fn remove_node_from(
        &self,
        node_id: NodeId,
        current: &MembershipConfig,
    ) -> Result<PlannedMembershipChange<G>, ConfigurationProposalRejection> {
        let MembershipConfig::Stable(stable) = current else {
            return Err(
                ConfigurationProposalRejection::StableConfigurationRequired {
                    phase: ConfigurationPhase::Joint,
                },
            );
        };
        if stable.learners().contains(&node_id) {
            return Ok(self.remove_node(node_id));
        }
        if !stable.voters().contains(&node_id) {
            return Err(ConfigurationProposalRejection::VoterNotFound { node_id });
        }
        if stable.voters().len() == 1 {
            return Err(ConfigurationProposalRejection::CannotRemoveLastVoter { node_id });
        }
        let target = MembershipSet::new(
            stable
                .voters()
                .iter()
                .copied()
                .filter(|id| *id != node_id)
                .collect(),
            stable.learners().to_vec(),
        )
        .map_err(|error| ConfigurationProposalRejection::InvalidMembership { error })?;
        Ok(PlannedMembershipChange {
            change: MembershipChange::RemoveNode { node_id },
            plan: self.plan(vec![
                MembershipStep::EnterJoint(target),
                MembershipStep::LeaveJoint,
                MembershipStep::FencePeer(node_id),
            ]),
        })
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
#[path = "membership_test.rs"]
mod tests;
