use std::{collections::BTreeSet, error::Error, fmt};

use super::NodeId;

/// Effective membership configuration used for quorum checks.
///
/// This enum is exhaustive because membership is either stable or joint.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum MembershipConfig {
    Stable(MembershipSet),
    Joint(JointMembership),
}

/// One stable Raft membership: voters plus non-voting learners.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct MembershipSet {
    voters: Vec<NodeId>,
    learners: Vec<NodeId>,
}

/// Joint-consensus membership containing the old and new stable sets.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct JointMembership {
    old: MembershipSet,
    new: MembershipSet,
}

/// Validation errors for one stable membership set.
///
/// This enum is exhaustive because membership validation is closed over these
/// structural errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MembershipValidationError {
    EmptyVoters,
    DuplicateVoter { node_id: NodeId },
    DuplicateLearner { node_id: NodeId },
    LearnerVoterOverlap { node_id: NodeId },
}

impl MembershipConfig {
    /// Builds a stable membership configuration.
    #[must_use]
    pub const fn stable(set: MembershipSet) -> Self {
        Self::Stable(set)
    }

    /// Builds a joint-consensus membership configuration.
    #[must_use]
    pub const fn joint(old: MembershipSet, new: MembershipSet) -> Self {
        Self::Joint(JointMembership { old, new })
    }

    /// Returns whether `acknowledgements` satisfy this configuration's quorum.
    #[must_use]
    pub fn has_quorum<I>(&self, acknowledgements: I) -> bool
    where
        I: IntoIterator<Item = NodeId>,
    {
        let acknowledgements = acknowledgements.into_iter().collect::<BTreeSet<_>>();
        match self {
            Self::Stable(stable) => stable.has_quorum_set(&acknowledgements),
            Self::Joint(joint) => joint.has_quorum_set(&acknowledgements),
        }
    }

    /// Returns every voter id that can participate in this configuration.
    #[must_use]
    pub fn voter_ids(&self) -> Vec<NodeId> {
        match self {
            Self::Stable(stable) => stable.voters().to_vec(),
            Self::Joint(joint) => {
                let voters = joint
                    .old()
                    .voters()
                    .iter()
                    .chain(joint.new_membership().voters())
                    .copied()
                    .collect::<BTreeSet<_>>();
                voters.into_iter().collect()
            }
        }
    }

    /// Returns every voter and learner id that belongs to this configuration.
    #[must_use]
    pub fn replica_ids(&self) -> Vec<NodeId> {
        match self {
            Self::Stable(stable) => stable.replica_ids(),
            Self::Joint(joint) => union_node_ids(
                joint.old().replica_ids(),
                joint.new_membership().replica_ids(),
            ),
        }
    }

    /// Returns whether `node_id` is a voter in any active membership half.
    #[must_use]
    pub fn contains_voter(&self, node_id: NodeId) -> bool {
        match self {
            Self::Stable(stable) => stable.voters().contains(&node_id),
            Self::Joint(joint) => {
                joint.old().voters().contains(&node_id)
                    || joint.new_membership().voters().contains(&node_id)
            }
        }
    }

    /// Returns whether `node_id` is a learner in any active membership half.
    #[must_use]
    pub fn contains_learner(&self, node_id: NodeId) -> bool {
        match self {
            Self::Stable(stable) => stable.learners().contains(&node_id),
            Self::Joint(joint) => {
                joint.old().learners().contains(&node_id)
                    || joint.new_membership().learners().contains(&node_id)
            }
        }
    }
}

impl MembershipSet {
    /// Builds one stable Raft membership set.
    ///
    /// Voters and learners are sorted into deterministic protocol order after
    /// validation. Learners are represented for replication/catch-up planning
    /// but never count toward quorum.
    ///
    /// # Errors
    ///
    /// Returns [`MembershipValidationError`] when the voter set is empty, a
    /// voter or learner appears more than once, or a learner is also a voter.
    pub fn new(
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
    ) -> Result<Self, MembershipValidationError> {
        let voters = validate_unique(voters, DuplicateKind::Voter)?;
        if voters.is_empty() {
            return Err(MembershipValidationError::EmptyVoters);
        }

        let learners = validate_unique(learners, DuplicateKind::Learner)?;
        for learner in &learners {
            if voters.contains(learner) {
                return Err(MembershipValidationError::LearnerVoterOverlap { node_id: *learner });
            }
        }

        Ok(Self { voters, learners })
    }

    /// Returns the voters in deterministic protocol order.
    #[must_use]
    pub fn voters(&self) -> &[NodeId] {
        &self.voters
    }

    /// Returns the learners in deterministic protocol order.
    #[must_use]
    pub fn learners(&self) -> &[NodeId] {
        &self.learners
    }

    /// Returns every voter and learner id in deterministic order.
    #[must_use]
    pub fn replica_ids(&self) -> Vec<NodeId> {
        union_node_ids(self.voters.iter().copied(), self.learners.iter().copied())
    }

    /// Returns the majority size required by this voter set.
    #[must_use]
    pub fn quorum_size(&self) -> usize {
        majority(self.voters.len())
    }

    /// Returns whether `acknowledgements` satisfy this set's quorum.
    #[must_use]
    pub fn has_quorum<I>(&self, acknowledgements: I) -> bool
    where
        I: IntoIterator<Item = NodeId>,
    {
        let acknowledgements = acknowledgements.into_iter().collect::<BTreeSet<_>>();
        self.has_quorum_set(&acknowledgements)
    }

    fn has_quorum_set(&self, acknowledgements: &BTreeSet<NodeId>) -> bool {
        self.voters
            .iter()
            .filter(|voter| acknowledgements.contains(voter))
            .count()
            >= self.quorum_size()
    }
}

impl JointMembership {
    /// Builds a joint membership from old and new stable sets.
    #[must_use]
    pub const fn new(old: MembershipSet, new: MembershipSet) -> Self {
        Self { old, new }
    }

    /// Returns the old membership half.
    #[must_use]
    pub const fn old(&self) -> &MembershipSet {
        &self.old
    }

    /// Returns the new membership half.
    #[must_use]
    pub const fn new_membership(&self) -> &MembershipSet {
        &self.new
    }

    /// Returns whether `acknowledgements` satisfy both old and new quorums.
    #[must_use]
    pub fn has_quorum<I>(&self, acknowledgements: I) -> bool
    where
        I: IntoIterator<Item = NodeId>,
    {
        let acknowledgements = acknowledgements.into_iter().collect::<BTreeSet<_>>();
        self.has_quorum_set(&acknowledgements)
    }

    fn has_quorum_set(&self, acknowledgements: &BTreeSet<NodeId>) -> bool {
        self.old.has_quorum_set(acknowledgements) && self.new.has_quorum_set(acknowledgements)
    }
}

impl fmt::Display for MembershipValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyVoters => {
                write!(formatter, "Raft membership must contain at least one voter")
            }
            Self::DuplicateVoter { node_id } => {
                write!(
                    formatter,
                    "Raft membership voter {node_id} appears more than once"
                )
            }
            Self::DuplicateLearner { node_id } => {
                write!(
                    formatter,
                    "Raft membership learner {node_id} appears more than once"
                )
            }
            Self::LearnerVoterOverlap { node_id } => {
                write!(
                    formatter,
                    "Raft membership node {node_id} cannot be both voter and learner"
                )
            }
        }
    }
}

impl Error for MembershipValidationError {}

#[derive(Clone, Copy)]
enum DuplicateKind {
    Voter,
    Learner,
}

fn validate_unique(
    mut nodes: Vec<NodeId>,
    kind: DuplicateKind,
) -> Result<Vec<NodeId>, MembershipValidationError> {
    nodes.sort_unstable();
    let mut previous = None;
    for node in nodes.iter().copied() {
        if previous == Some(node) {
            return Err(match kind {
                DuplicateKind::Voter => MembershipValidationError::DuplicateVoter { node_id: node },
                DuplicateKind::Learner => {
                    MembershipValidationError::DuplicateLearner { node_id: node }
                }
            });
        }
        previous = Some(node);
    }
    Ok(nodes)
}

fn union_node_ids<I, J>(left: I, right: J) -> Vec<NodeId>
where
    I: IntoIterator<Item = NodeId>,
    J: IntoIterator<Item = NodeId>,
{
    left.into_iter()
        .chain(right)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

const fn majority(voter_count: usize) -> usize {
    (voter_count / 2) + 1
}
