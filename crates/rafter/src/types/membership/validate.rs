//! Rendering and shared checks behind membership validation refusals.
//!
//! The refusal taxonomy and the membership types stay in the parent; this
//! module owns how a refusal reads and the duplicate/empty checks every
//! configuration shape runs.

use std::{collections::BTreeSet, error::Error, fmt};

use crate::NodeId;

use super::MembershipValidationError;

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
pub(super) enum DuplicateKind {
    Voter,
    Learner,
}

pub(super) fn validate_unique(
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

pub(super) fn union_node_ids<I, J>(left: I, right: J) -> Vec<NodeId>
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

pub(super) const fn majority(voter_count: usize) -> usize {
    (voter_count / 2) + 1
}
