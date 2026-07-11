use std::fmt;

use rafter::{MembershipSet, NodeId};

use crate::model_check::{MessageKind, ProposalId};

/// Randomized simulator action family.
///
/// This enum is exhaustive because soak scheduling uses this closed set of
/// action families for metrics and replay summaries.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SoakActionKind {
    Tick,
    Propose,
    Deliver,
    Delay,
    Drop,
    Duplicate,
    Restart,
    ReadIndex,
    AddLearner,
    RemoveLearner,
    PromoteLearner,
    RemoveVoter,
    EnterJoint,
    LeaveJoint,
    Transfer,
    Partition,
    Heal,
    LossyRestart,
}

impl SoakActionKind {
    /// Returns the stable machine-readable action-family label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Tick => "tick",
            Self::Propose => "propose",
            Self::Deliver => "deliver",
            Self::Delay => "delay",
            Self::Drop => "drop",
            Self::Duplicate => "duplicate",
            Self::Restart => "restart",
            Self::ReadIndex => "read-index",
            Self::AddLearner => "add-learner",
            Self::RemoveLearner => "remove-learner",
            Self::PromoteLearner => "promote-learner",
            Self::RemoveVoter => "remove-voter",
            Self::EnterJoint => "enter-joint",
            Self::LeaveJoint => "leave-joint",
            Self::Transfer => "transfer",
            Self::Partition => "partition",
            Self::Heal => "heal",
            Self::LossyRestart => "lossy-restart",
        }
    }
}

/// Replayable-enough randomized simulator action.
///
/// This enum is exhaustive because randomized soak traces are recorded using
/// this closed simulator action vocabulary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SoakAction {
    Tick(NodeId),
    Propose {
        to: NodeId,
        proposal_id: ProposalId,
    },
    Deliver {
        from: NodeId,
        to: NodeId,
        message: MessageKind,
    },
    Delay {
        from: NodeId,
        to: NodeId,
        message: MessageKind,
        ticks: u64,
    },
    Drop {
        from: NodeId,
        to: NodeId,
        message: MessageKind,
    },
    Duplicate {
        from: NodeId,
        to: NodeId,
        message: MessageKind,
    },
    Restart(NodeId),
    ReadIndex {
        to: NodeId,
        request_id: u64,
    },
    AddLearner {
        to: NodeId,
        learner_id: NodeId,
    },
    RemoveLearner {
        to: NodeId,
        learner_id: NodeId,
    },
    PromoteLearner {
        to: NodeId,
        learner_id: NodeId,
    },
    RemoveVoter {
        to: NodeId,
        voter_id: NodeId,
    },
    EnterJoint {
        to: NodeId,
        target: MembershipSet,
    },
    LeaveJoint {
        to: NodeId,
    },
    Transfer {
        from: NodeId,
        target: NodeId,
    },
    Partition {
        a: NodeId,
        b: NodeId,
    },
    Heal,
    LossyRestart(NodeId),
}

impl SoakAction {
    pub(in crate::model_check) const fn kind(&self) -> SoakActionKind {
        match self {
            Self::Tick(_) => SoakActionKind::Tick,
            Self::Propose { .. } => SoakActionKind::Propose,
            Self::Deliver { .. } => SoakActionKind::Deliver,
            Self::Delay { .. } => SoakActionKind::Delay,
            Self::Drop { .. } => SoakActionKind::Drop,
            Self::Duplicate { .. } => SoakActionKind::Duplicate,
            Self::Restart(_) => SoakActionKind::Restart,
            Self::ReadIndex { .. } => SoakActionKind::ReadIndex,
            Self::AddLearner { .. } => SoakActionKind::AddLearner,
            Self::RemoveLearner { .. } => SoakActionKind::RemoveLearner,
            Self::PromoteLearner { .. } => SoakActionKind::PromoteLearner,
            Self::RemoveVoter { .. } => SoakActionKind::RemoveVoter,
            Self::EnterJoint { .. } => SoakActionKind::EnterJoint,
            Self::LeaveJoint { .. } => SoakActionKind::LeaveJoint,
            Self::Transfer { .. } => SoakActionKind::Transfer,
            Self::Partition { .. } => SoakActionKind::Partition,
            Self::Heal => SoakActionKind::Heal,
            Self::LossyRestart(_) => SoakActionKind::LossyRestart,
        }
    }
}

impl fmt::Display for SoakAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tick(node_id) => write!(formatter, "tick {node_id}"),
            Self::Propose { to, proposal_id } => {
                write!(formatter, "propose {} to {to}", proposal_id.0)
            }
            Self::Deliver { from, to, message } => {
                write!(formatter, "deliver {message} {from}->{to}")
            }
            Self::Delay {
                from,
                to,
                message,
                ticks,
            } => write!(formatter, "delay {message} {from}->{to} by {ticks} ticks"),
            Self::Drop { from, to, message } => {
                write!(formatter, "drop {message} {from}->{to}")
            }
            Self::Duplicate { from, to, message } => {
                write!(formatter, "duplicate {message} {from}->{to}")
            }
            Self::Restart(node_id) => write!(formatter, "restart {node_id}"),
            Self::ReadIndex { to, request_id } => {
                write!(formatter, "read-index {request_id} to {to}")
            }
            Self::AddLearner { to, learner_id } => {
                write!(formatter, "add learner {learner_id} via {to}")
            }
            Self::RemoveLearner { to, learner_id } => {
                write!(formatter, "remove learner {learner_id} via {to}")
            }
            Self::PromoteLearner { to, learner_id } => {
                write!(formatter, "promote learner {learner_id} via {to}")
            }
            Self::RemoveVoter { to, voter_id } => {
                write!(formatter, "remove voter {voter_id} via {to}")
            }
            Self::EnterJoint { to, target } => {
                write!(formatter, "enter joint via {to} target {target:?}")
            }
            Self::LeaveJoint { to } => write!(formatter, "leave joint via {to}"),
            Self::Transfer { from, target } => {
                write!(formatter, "transfer leadership {from}->{target}")
            }
            Self::Partition { a, b } => write!(formatter, "partition {a}<->{b}"),
            Self::Heal => formatter.write_str("heal partitions"),
            Self::LossyRestart(node_id) => write!(formatter, "lossy restart {node_id}"),
        }
    }
}
