//! Safe membership-transition entry points.
//!
//! Each operation derives one configuration entry from the effective
//! membership, then delegates shared validation and append mechanics.

use crate::{
    ConfigurationEntry, ConfigurationPhase, JointMembership, MembershipConfig, MembershipSet,
    NodeId, PromotionBarrier,
};

use super::super::{ConfigurationProposalRejection, Node, Output};
use super::validate::validated_derived_membership;

impl Node {
    pub(in crate::node) fn add_learner(&mut self, learner_id: NodeId) -> Vec<Output> {
        if let Err(rejection) = self.validate_configuration_proposal_preflight() {
            return Self::reject_proposal(None, rejection);
        }

        let current_membership = match self.stable_effective_membership() {
            Ok(membership) => membership,
            Err(rejection) => return Self::reject_configuration(rejection),
        };
        if current_membership.voters().contains(&learner_id)
            || current_membership.learners().contains(&learner_id)
        {
            return Self::reject_configuration(ConfigurationProposalRejection::NodeAlreadyMember {
                node_id: learner_id,
            });
        }

        let mut learners = current_membership.learners().to_vec();
        learners.push(learner_id);
        let membership = match validated_derived_membership(MembershipSet::new(
            current_membership.voters().to_vec(),
            learners,
        )) {
            Ok(membership) => membership,
            Err(rejection) => return Self::reject_configuration(rejection),
        };
        let configuration = ConfigurationEntry::stable(self.next_configuration_id(), membership);
        self.append_valid_configuration_proposal(configuration, &[])
    }

    pub(in crate::node) fn promote_learner(
        &mut self,
        learner_id: NodeId,
        promotion_barrier: PromotionBarrier,
    ) -> Vec<Output> {
        if let Err(rejection) = self.validate_configuration_proposal_preflight() {
            return Self::reject_proposal(None, rejection);
        }

        let current_membership = match self.stable_effective_membership() {
            Ok(membership) => membership,
            Err(rejection) => return Self::reject_configuration(rejection),
        };
        if !current_membership.learners().contains(&learner_id) {
            return Self::reject_configuration(
                ConfigurationProposalRejection::PromotionTargetNotLearner {
                    node_id: learner_id,
                },
            );
        }

        let mut voters = current_membership.voters().to_vec();
        voters.push(learner_id);
        let learners = current_membership
            .learners()
            .iter()
            .copied()
            .filter(|node_id| *node_id != learner_id)
            .collect();
        let target_membership =
            match validated_derived_membership(MembershipSet::new(voters, learners)) {
                Ok(membership) => membership,
                Err(rejection) => return Self::reject_configuration(rejection),
            };
        let configuration = ConfigurationEntry::joint(
            self.next_configuration_id(),
            JointMembership::new(current_membership, target_membership),
        );
        self.append_valid_configuration_proposal(configuration, &[promotion_barrier])
    }

    pub(in crate::node) fn remove_voter(&mut self, voter_id: NodeId) -> Vec<Output> {
        if let Err(rejection) = self.validate_configuration_proposal_preflight() {
            return Self::reject_proposal(None, rejection);
        }

        let current_membership = match self.stable_effective_membership() {
            Ok(membership) => membership,
            Err(rejection) => return Self::reject_configuration(rejection),
        };
        if !current_membership.voters().contains(&voter_id) {
            return Self::reject_configuration(ConfigurationProposalRejection::VoterNotFound {
                node_id: voter_id,
            });
        }
        if current_membership.voters().len() == 1 {
            return Self::reject_configuration(
                ConfigurationProposalRejection::CannotRemoveLastVoter { node_id: voter_id },
            );
        }

        let voters = current_membership
            .voters()
            .iter()
            .copied()
            .filter(|node_id| *node_id != voter_id)
            .collect();
        let target_membership = match validated_derived_membership(MembershipSet::new(
            voters,
            current_membership.learners().to_vec(),
        )) {
            Ok(membership) => membership,
            Err(rejection) => return Self::reject_configuration(rejection),
        };
        let configuration = ConfigurationEntry::joint(
            self.next_configuration_id(),
            JointMembership::new(current_membership, target_membership),
        );
        self.append_valid_configuration_proposal(configuration, &[])
    }

    pub(in crate::node) fn enter_joint(
        &mut self,
        target_membership: MembershipSet,
        promotion_barriers: &[PromotionBarrier],
    ) -> Vec<Output> {
        if let Err(rejection) = self.validate_configuration_proposal_preflight() {
            return Self::reject_proposal(None, rejection);
        }

        let current_membership = match self.stable_effective_membership() {
            Ok(membership) => membership,
            Err(rejection) => return Self::reject_configuration(rejection),
        };
        if target_membership == current_membership {
            return Self::reject_configuration(
                ConfigurationProposalRejection::TargetMembershipUnchanged,
            );
        }

        let configuration = ConfigurationEntry::joint(
            self.next_configuration_id(),
            JointMembership::new(current_membership, target_membership),
        );
        self.append_valid_configuration_proposal(configuration, promotion_barriers)
    }

    pub(in crate::node) fn leave_joint(&mut self) -> Vec<Output> {
        if let Err(rejection) = self.validate_configuration_proposal_preflight() {
            return Self::reject_proposal(None, rejection);
        }

        let MembershipConfig::Joint(joint) = self.effective_membership() else {
            return Self::reject_configuration(
                ConfigurationProposalRejection::JointConfigurationRequired {
                    phase: ConfigurationPhase::Stable,
                },
            );
        };
        let configuration = ConfigurationEntry::stable(
            self.next_configuration_id(),
            joint.new_membership().clone(),
        );
        self.append_valid_configuration_proposal(configuration, &[])
    }

    pub(in crate::node) fn change_membership(
        &mut self,
        target_membership: MembershipSet,
        promotion_barriers: &[PromotionBarrier],
    ) -> Vec<Output> {
        if let Err(rejection) = self.validate_configuration_proposal_preflight() {
            return Self::reject_proposal(None, rejection);
        }

        let configuration = match self.effective_membership() {
            MembershipConfig::Stable(current_membership)
                if target_membership == current_membership =>
            {
                return Self::reject_configuration(
                    ConfigurationProposalRejection::TargetMembershipUnchanged,
                );
            }
            MembershipConfig::Stable(current_membership)
                if target_membership.voters() == current_membership.voters() =>
            {
                ConfigurationEntry::stable(self.next_configuration_id(), target_membership)
            }
            MembershipConfig::Stable(current_membership) => ConfigurationEntry::joint(
                self.next_configuration_id(),
                JointMembership::new(current_membership, target_membership),
            ),
            MembershipConfig::Joint(joint) if target_membership == *joint.new_membership() => {
                ConfigurationEntry::stable(self.next_configuration_id(), target_membership)
            }
            MembershipConfig::Joint(_) => {
                return Self::reject_configuration(
                    ConfigurationProposalRejection::TargetMembershipDoesNotMatchJointNewSide,
                );
            }
        };

        self.append_valid_configuration_proposal(configuration, promotion_barriers)
    }

    pub(in crate::node) fn dangerous_raw_configuration_proposal(
        &mut self,
        configuration: ConfigurationEntry,
        promotion_barriers: &[PromotionBarrier],
    ) -> Vec<Output> {
        if let Err(rejection) = self.validate_configuration_proposal_preflight() {
            return Self::reject_proposal(None, rejection);
        }
        self.append_valid_configuration_proposal(configuration, promotion_barriers)
    }
}
