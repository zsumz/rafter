use std::{collections::BTreeSet, error::Error, fmt};

use rafter::{MembershipSet, NodeId};

use crate::SimSeed;

use super::{Failure, MessageKind, ProposalId};

/// Configuration for deterministic randomized Raft soak runs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SoakConfig {
    pub(super) seed: SimSeed,
    pub(super) steps: usize,
    pub(super) max_proposals: usize,
    pub(super) max_restarts: usize,
    pub(super) max_read_indexes: usize,
    pub(super) max_membership_changes: usize,
    pub(super) max_transfers: usize,
    pub(super) max_partitions: usize,
    pub(super) max_lossy_restarts: usize,
    /// Tick-rate skew: `(node, weight)` makes tick actions favour `node`
    /// `weight`-to-one over each other node, modelling one process driving
    /// its kernel faster than its peers.
    pub(super) tick_skew: Option<(NodeId, u32)>,
}

impl SoakConfig {
    /// Constructs a deterministic soak configuration.
    #[must_use]
    pub const fn new(seed: SimSeed, steps: usize) -> Self {
        Self {
            seed,
            steps,
            max_proposals: 0,
            max_restarts: 0,
            max_read_indexes: 0,
            max_membership_changes: 0,
            max_transfers: 0,
            max_partitions: 0,
            max_lossy_restarts: 0,
            tick_skew: None,
        }
    }

    /// Allows the soak to inject up to `max_proposals` client proposals.
    #[must_use]
    pub const fn with_max_proposals(mut self, max_proposals: usize) -> Self {
        self.max_proposals = max_proposals;
        self
    }

    /// Allows the soak to restart up to `max_restarts` nodes.
    #[must_use]
    pub const fn with_max_restarts(mut self, max_restarts: usize) -> Self {
        self.max_restarts = max_restarts;
        self
    }

    /// Allows the soak to register up to `max_read_indexes` read barriers.
    #[must_use]
    pub const fn with_max_read_indexes(mut self, max_read_indexes: usize) -> Self {
        self.max_read_indexes = max_read_indexes;
        self
    }

    /// Allows the soak to inject up to `max_membership_changes` membership
    /// proposals.
    #[must_use]
    pub const fn with_max_membership_changes(mut self, max_membership_changes: usize) -> Self {
        self.max_membership_changes = max_membership_changes;
        self
    }

    /// Allows the soak to request up to `max_transfers` leadership
    /// transfers.
    #[must_use]
    pub const fn with_max_transfers(mut self, max_transfers: usize) -> Self {
        self.max_transfers = max_transfers;
        self
    }

    /// Allows the soak to install up to `max_partitions` sustained
    /// partitions (healing is always enabled once one exists).
    #[must_use]
    pub const fn with_max_partitions(mut self, max_partitions: usize) -> Self {
        self.max_partitions = max_partitions;
        self
    }

    /// Allows up to `max_lossy_restarts` floor-truncating lossy restarts —
    /// legal by construction, so safety invariants stay in force.
    #[must_use]
    pub const fn with_max_lossy_restarts(mut self, max_lossy_restarts: usize) -> Self {
        self.max_lossy_restarts = max_lossy_restarts;
        self
    }

    /// Skews tick selection `weight`-to-one toward `node`.
    #[must_use]
    pub const fn with_tick_skew(mut self, node: NodeId, weight: u32) -> Self {
        self.tick_skew = Some((node, weight));
        self
    }

    /// Returns the deterministic simulator seed.
    #[must_use]
    pub const fn seed(self) -> SimSeed {
        self.seed
    }

    /// Returns the configured step count.
    #[must_use]
    pub const fn steps(self) -> usize {
        self.steps
    }
}

/// Summary returned after a successful randomized soak run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SoakSummary {
    pub(super) seed: SimSeed,
    pub(super) steps_executed: usize,
    pub(super) observed_actions: BTreeSet<SoakActionKind>,
}

impl SoakSummary {
    /// Returns the deterministic simulator seed.
    #[must_use]
    pub const fn seed(&self) -> SimSeed {
        self.seed
    }

    /// Returns the number of steps executed.
    #[must_use]
    pub const fn steps_executed(&self) -> usize {
        self.steps_executed
    }

    /// Returns the action families observed during the run.
    #[must_use]
    pub const fn observed_actions(&self) -> &BTreeSet<SoakActionKind> {
        &self.observed_actions
    }
}

/// Error returned when a randomized soak finds an invariant violation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SoakFailure {
    pub(super) seed: SimSeed,
    pub(super) step: usize,
    pub(super) trace: Vec<SoakAction>,
    pub(super) failure: Box<Failure>,
}

impl SoakFailure {
    /// Returns the deterministic simulator seed.
    #[must_use]
    pub const fn seed(&self) -> SimSeed {
        self.seed
    }

    /// Returns the step that exposed the invariant failure.
    #[must_use]
    pub const fn step(&self) -> usize {
        self.step
    }

    /// Returns the action trace that led to the failure.
    #[must_use]
    pub fn trace(&self) -> &[SoakAction] {
        &self.trace
    }

    /// Returns the underlying invariant failure.
    #[must_use]
    pub fn failure(&self) -> &Failure {
        &self.failure
    }
}

impl fmt::Display for SoakFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "seed {:?} failed at step {}: {}",
            self.seed, self.step, self.failure
        )
    }
}

impl Error for SoakFailure {}

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
    pub(super) const fn kind(&self) -> SoakActionKind {
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
