//! Stable operator-facing rendering for local protocol outcomes.

use std::fmt;

use super::{
    ConfigurationProposalRejection, LeadershipTransferRejection, ProposalRejection,
    ReadIndexRejection,
};

impl fmt::Display for ReadIndexRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLeader { role, term } => write!(
                formatter,
                "read barrier rejected: node is a {role} in term {term}, not the leader"
            ),
            Self::NoCommitInCurrentTerm => formatter.write_str(
                "read barrier rejected: the leader has not committed an entry in its current term",
            ),
            Self::LeadershipTransferInProgress { target } => write!(
                formatter,
                "read barrier rejected: a leadership transfer to {target} is in progress"
            ),
            Self::TooManyPendingReads => formatter.write_str(
                "read barrier rejected: too many barriers are awaiting quorum confirmation",
            ),
        }
    }
}

impl fmt::Display for LeadershipTransferRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NotLeader => "this node is not the leader",
            Self::TargetIsSelf => "the transfer target is the leader itself",
            Self::TargetNotVoter => "the transfer target is not an effective voter",
            Self::TransferAlreadyInProgress => "a leadership transfer is already in progress",
        })
    }
}

// Proposal rejections are protocol outputs a leader reports back to callers,
// not process errors, so they render as messages but do not implement
// `std::error::Error`.
impl fmt::Display for ProposalRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLeader {
                role,
                term,
                payload_len,
            } => write!(
                formatter,
                concat!(
                    "proposal of {payload_len} bytes rejected: node is a {role} ",
                    "in term {term}, not the leader",
                ),
                payload_len = payload_len,
                role = role,
                term = term,
            ),
            Self::PayloadTooLarge {
                payload_len,
                max_payload_len,
            } => write!(
                formatter,
                "proposal payload of {payload_len} bytes exceeds the {max_payload_len} byte maximum"
            ),
            Self::LeadershipTransferInProgress { target } => write!(
                formatter,
                "proposal rejected: a leadership transfer to {target} is in progress"
            ),
            Self::Configuration(rejection) => write!(formatter, "{rejection}"),
        }
    }
}

impl fmt::Display for ConfigurationProposalRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StableConfigurationRequired { phase } => write!(
                formatter,
                concat!(
                    "configuration proposal rejected: current configuration is {phase}, ",
                    "but this operation requires a stable configuration",
                ),
                phase = phase,
            ),
            Self::JointConfigurationRequired { phase } => write!(
                formatter,
                concat!(
                    "configuration proposal rejected: current configuration is {phase}, ",
                    "but this operation requires a joint configuration",
                ),
                phase = phase,
            ),
            Self::UncommittedConfiguration { index } => write!(
                formatter,
                concat!(
                    "configuration proposal rejected: configuration entry at index {index} ",
                    "is still uncommitted",
                ),
                index = index,
            ),
            Self::NodeAlreadyMember { node_id } => write!(
                formatter,
                "configuration proposal rejected: node {node_id} is already a member"
            ),
            Self::VoterNotFound { node_id } => write!(
                formatter,
                "configuration proposal rejected: voter {node_id} is not in the current membership"
            ),
            Self::CannotRemoveLastVoter { node_id } => write!(
                formatter,
                concat!(
                    "configuration proposal rejected: removing voter {node_id} would leave ",
                    "the membership without voters",
                ),
                node_id = node_id,
            ),
            Self::InvalidMembership { error } => write!(
                formatter,
                "configuration proposal rejected: derived membership is invalid: {error}"
            ),
            Self::TargetMembershipUnchanged => formatter.write_str(
                "configuration proposal rejected: target membership matches the current membership",
            ),
            Self::TargetMembershipDoesNotMatchJointNewSide => formatter.write_str(concat!(
                "configuration proposal rejected: target membership does not match ",
                "the joint configuration's new side",
            )),
            Self::PromotionTargetNotLearner { .. }
            | Self::DuplicatePromotionBarrier { .. }
            | Self::UnusedPromotionBarrier { .. }
            | Self::MissingPromotionBarrier { .. }
            | Self::StalePromotionBarrier { .. }
            | Self::PromotionBarrierNotReached { .. } => self.fmt_promotion_error(formatter),
        }
    }
}

impl ConfigurationProposalRejection {
    fn fmt_promotion_error(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PromotionTargetNotLearner { node_id } => write!(
                formatter,
                "configuration proposal rejected: promotion target {node_id} is not a learner"
            ),
            Self::DuplicatePromotionBarrier { learner_id } => write!(
                formatter,
                concat!(
                    "configuration proposal rejected: duplicate promotion barrier supplied ",
                    "for learner {learner_id}",
                ),
                learner_id = learner_id,
            ),
            Self::UnusedPromotionBarrier { learner_id } => write!(
                formatter,
                concat!(
                    "configuration proposal rejected: promotion barrier supplied for learner ",
                    "{learner_id}, but that learner is not being promoted",
                ),
                learner_id = learner_id,
            ),
            Self::MissingPromotionBarrier { learner_id } => write!(
                formatter,
                concat!(
                    "configuration proposal rejected: no promotion barrier supplied for ",
                    "learner {learner_id}",
                ),
                learner_id = learner_id,
            ),
            Self::StalePromotionBarrier {
                learner_id,
                required_match_index,
                supplied_match_index,
            } => write!(
                formatter,
                concat!(
                    "configuration proposal rejected: promotion barrier for learner ",
                    "{learner_id} supplies match index {supplied_match_index} but index ",
                    "{required_match_index} is required",
                ),
                learner_id = learner_id,
                supplied_match_index = supplied_match_index,
                required_match_index = required_match_index,
            ),
            Self::PromotionBarrierNotReached {
                learner_id,
                required_match_index,
                actual_match_index,
            } => write!(
                formatter,
                concat!(
                    "configuration proposal rejected: learner {learner_id} matched index ",
                    "{actual_match_index}, below the required promotion barrier ",
                    "{required_match_index}",
                ),
                learner_id = learner_id,
                actual_match_index = actual_match_index,
                required_match_index = required_match_index,
            ),
            _ => unreachable!("caller filters promotion errors"),
        }
    }
}
