//! Shared translation for app groups and manually driven durable owners.
use super::MembershipChange;
use rafter::{Input, MembershipConfig, MembershipSet, MembershipValidationError};

impl MembershipChange {
    /// Converts one app-layer request into the corresponding Raft input.
    ///
    /// Supply the runtime's current effective membership immediately before
    /// submitting the returned input. In particular, `RemoveNode` uses that
    /// membership to select the voter or learner removal path. If admission
    /// is refused because the runtime is busy, inspect membership again and
    /// translate again before retrying; do not reuse a stale derived target.
    ///
    /// This conversion neither admits nor executes the request. The kernel
    /// still checks leadership, outstanding configurations and promotion
    /// barriers. Observe protocol rejections and committed configurations when
    /// driving the runtime. Voter changes need joint and stable commits; this
    /// method does not submit `LeaveJoint` automatically.
    ///
    /// `NodeInfo` is discarded here: metadata, authorization, peer updates and
    /// fencing remain the embedding's responsibility. Retain any metadata the
    /// embedding needs before consuming the request.
    ///
    /// # Errors
    /// Returns a membership validation error if the target derived for learner
    /// removal is invalid. Other protocol refusals are reported when stepping
    /// the returned input, not during this conversion.
    pub fn into_raft_input(
        self,
        current: &MembershipConfig,
    ) -> Result<Input, MembershipValidationError> {
        Ok(match self {
            MembershipChange::AddLearner { node_id, info: _ } => Input::AddLearner {
                learner_id: node_id,
            },
            MembershipChange::PromoteLearner { node_id, barrier } => Input::PromoteLearner {
                learner_id: node_id,
                promotion_barrier: barrier,
            },
            MembershipChange::RemoveNode { node_id } => match current {
                MembershipConfig::Stable(current) if current.learners().contains(&node_id) => {
                    // Learner removal preserves the voter quorum. The kernel's
                    // safe change path still checks leadership and outstanding
                    // configuration work before appending a stable entry.
                    let target = MembershipSet::new(
                        current.voters().to_vec(),
                        current
                            .learners()
                            .iter()
                            .copied()
                            .filter(|id| *id != node_id)
                            .collect(),
                    )?;
                    Input::ChangeMembership {
                        target,
                        promotion_barriers: Vec::new(),
                    }
                }
                _ => Input::RemoveVoter { voter_id: node_id },
            },
            MembershipChange::EnterJoint {
                target,
                promotion_barriers,
            } => Input::EnterJoint {
                target,
                promotion_barriers,
            },
            MembershipChange::LeaveJoint => Input::LeaveJoint,
            MembershipChange::ChangeVoters { target } => Input::ChangeMembership {
                target,
                promotion_barriers: Vec::new(),
            },
        })
    }
}
