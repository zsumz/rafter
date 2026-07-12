//! Shared validation and append mechanics for configuration proposals.
//!
//! Validation is complete before the log mutates, so every rejected change is
//! side-effect free and every accepted change follows one append path.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    ConfigurationEntry, ConfigurationId, ConfigurationPhase, LocalProposalId, MembershipConfig,
    MembershipSet, MembershipValidationError, PromotionBarrier,
};

use super::super::{ConfigurationProposalRejection, Node, Output, ProposalRejection, Role};

impl Node {
    pub(super) fn validate_configuration_proposal_preflight(
        &self,
    ) -> Result<(), ProposalRejection> {
        if self.role() != Role::Leader {
            return Err(ProposalRejection::NotLeader {
                role: self.role(),
                term: self.current_term(),
                payload_len: 0,
            });
        }
        if let Some(transfer) = self.leader.pending_transfer.as_ref() {
            return Err(ProposalRejection::LeadershipTransferInProgress {
                target: transfer.target,
            });
        }
        if let Some(index) = self.uncommitted_configuration_indexes().first().copied() {
            return Err(ProposalRejection::Configuration(
                ConfigurationProposalRejection::UncommittedConfiguration { index },
            ));
        }
        Ok(())
    }

    pub(super) fn stable_effective_membership(
        &self,
    ) -> Result<MembershipSet, ConfigurationProposalRejection> {
        match self.effective_membership() {
            MembershipConfig::Stable(membership) => Ok(membership),
            MembershipConfig::Joint(_) => Err(
                ConfigurationProposalRejection::StableConfigurationRequired {
                    phase: ConfigurationPhase::Joint,
                },
            ),
        }
    }

    pub(super) fn next_configuration_id(&self) -> ConfigurationId {
        self.effective_configuration_entry()
            .map(|entry| entry.config_id())
            .or_else(|| {
                self.committed_configuration_state_at(self.commit_index())
                    .map(|state| state.config_id)
            })
            .unwrap_or(ConfigurationId(0))
            .next()
    }

    pub(super) fn reject_configuration(rejection: ConfigurationProposalRejection) -> Vec<Output> {
        Self::reject_proposal(None, ProposalRejection::Configuration(rejection))
    }

    pub(super) fn reject_proposal(
        proposal_id: Option<LocalProposalId>,
        reason: ProposalRejection,
    ) -> Vec<Output> {
        vec![Output::RejectProposal {
            proposal_id,
            reason,
        }]
    }

    pub(super) fn append_valid_configuration_proposal(
        &mut self,
        configuration: ConfigurationEntry,
        promotion_barriers: &[PromotionBarrier],
    ) -> Vec<Output> {
        if let Err(rejection) =
            self.validate_configuration_promotion_barriers(&configuration, promotion_barriers)
        {
            return Self::reject_configuration(rejection);
        }

        let entry = crate::LogEntry::configuration(self.current_term(), configuration);
        self.append_log_entry(entry);
        self.record_local_progress();

        let mut outputs = Vec::new();
        self.advance_commit_index_into(&mut outputs);
        if self.role() == Role::Leader {
            self.broadcast_append_entries_into(&mut outputs);
        }
        outputs
    }

    fn validate_configuration_promotion_barriers(
        &self,
        configuration: &ConfigurationEntry,
        promotion_barriers: &[PromotionBarrier],
    ) -> Result<(), ConfigurationProposalRejection> {
        let mut barriers = BTreeMap::new();
        for barrier in promotion_barriers {
            if barriers.insert(barrier.learner_id, *barrier).is_some() {
                return Err(ConfigurationProposalRejection::DuplicatePromotionBarrier {
                    learner_id: barrier.learner_id,
                });
            }
        }

        let ConfigurationEntry::Joint { membership, .. } = configuration else {
            if let Some(barrier) = promotion_barriers.first() {
                return Err(ConfigurationProposalRejection::UnusedPromotionBarrier {
                    learner_id: barrier.learner_id,
                });
            }
            return Ok(());
        };

        let current_membership = self.effective_membership();
        let mut used_barriers = BTreeSet::new();

        for promoted_node in membership.new_membership().voters() {
            if current_membership.contains_voter(*promoted_node) {
                continue;
            }
            if !current_membership.contains_learner(*promoted_node) {
                return Err(ConfigurationProposalRejection::PromotionTargetNotLearner {
                    node_id: *promoted_node,
                });
            }

            let Some(barrier) = barriers.get(promoted_node).copied() else {
                return Err(ConfigurationProposalRejection::MissingPromotionBarrier {
                    learner_id: *promoted_node,
                });
            };
            used_barriers.insert(*promoted_node);

            if barrier.required_match_index != self.commit_index() {
                return Err(ConfigurationProposalRejection::StalePromotionBarrier {
                    learner_id: *promoted_node,
                    required_match_index: self.commit_index(),
                    supplied_match_index: barrier.required_match_index,
                });
            }

            let actual_match_index = self
                .leader
                .progress
                .get(*promoted_node)
                .map(|progress| progress.match_index)
                .unwrap_or_default();
            if actual_match_index < barrier.required_match_index {
                return Err(ConfigurationProposalRejection::PromotionBarrierNotReached {
                    learner_id: *promoted_node,
                    required_match_index: barrier.required_match_index,
                    actual_match_index,
                });
            }
        }

        if let Some(barrier) = promotion_barriers
            .iter()
            .find(|barrier| !used_barriers.contains(&barrier.learner_id))
        {
            return Err(ConfigurationProposalRejection::UnusedPromotionBarrier {
                learner_id: barrier.learner_id,
            });
        }

        Ok(())
    }
}

pub(super) fn validated_derived_membership(
    result: Result<MembershipSet, MembershipValidationError>,
) -> Result<MembershipSet, ConfigurationProposalRejection> {
    result.map_err(|error| ConfigurationProposalRejection::InvalidMembership { error })
}
