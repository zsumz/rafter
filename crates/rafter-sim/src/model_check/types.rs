use std::{error::Error, fmt, time::Duration};

use rafter::{LogIndex, MembershipSet, Message, NodeId, Role, Term};

/// Bound settings for an in-repo model-checking run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Bounds {
    pub(super) depth: usize,
    pub(super) proposal_count: usize,
    pub(super) restart_count: usize,
    pub(super) read_index_count: usize,
    pub(super) membership_change_count: usize,
    pub(super) max_unique_states: Option<usize>,
    pub(super) max_wall_clock: Option<Duration>,
}

impl Bounds {
    /// Constructs a bounded exploration configuration.
    #[must_use]
    pub const fn new(max_depth: usize) -> Self {
        Self {
            depth: max_depth,
            proposal_count: 0,
            restart_count: 0,
            read_index_count: 0,
            membership_change_count: 0,
            max_unique_states: None,
            max_wall_clock: None,
        }
    }

    /// Allows the checker to inject up to `max_proposals` client proposals.
    #[must_use]
    pub const fn with_max_proposals(mut self, max_proposals: usize) -> Self {
        self.proposal_count = max_proposals;
        self
    }

    /// Allows the checker to restart up to `max_restarts` nodes.
    #[must_use]
    pub const fn with_max_restarts(mut self, max_restarts: usize) -> Self {
        self.restart_count = max_restarts;
        self
    }

    /// Allows the checker to register up to `max_read_indexes` read barriers.
    #[must_use]
    pub const fn with_max_read_indexes(mut self, max_read_indexes: usize) -> Self {
        self.read_index_count = max_read_indexes;
        self
    }

    /// Allows the checker to inject up to `max_membership_changes`
    /// membership proposals.
    #[must_use]
    pub const fn with_max_membership_changes(mut self, max_membership_changes: usize) -> Self {
        self.membership_change_count = max_membership_changes;
        self
    }

    /// Stops admitting new canonical states after `max_unique_states` have
    /// been reached. Already-seen states are still counted as raw visits, and
    /// may be re-expanded if reached with more depth remaining.
    #[must_use]
    pub const fn with_max_unique_states(mut self, max_unique_states: usize) -> Self {
        self.max_unique_states = Some(max_unique_states);
        self
    }

    /// Stops expanding new canonical states after the wall-clock budget
    /// elapses. The checker returns the partial summary collected so far.
    #[must_use]
    pub const fn with_max_wall_clock(mut self, max_wall_clock: Duration) -> Self {
        self.max_wall_clock = Some(max_wall_clock);
        self
    }

    /// Returns the maximum action depth explored from the initial state.
    #[must_use]
    pub const fn max_depth(self) -> usize {
        self.depth
    }

    /// Returns the maximum number of proposals the checker may inject.
    #[must_use]
    pub const fn max_proposals(self) -> usize {
        self.proposal_count
    }

    /// Returns the maximum number of restarts the checker may inject.
    #[must_use]
    pub const fn max_restarts(self) -> usize {
        self.restart_count
    }

    /// Returns the maximum number of membership changes the checker may
    /// inject.
    #[must_use]
    pub const fn max_membership_changes(self) -> usize {
        self.membership_change_count
    }

    /// Returns the configured unique-state budget, if any.
    #[must_use]
    pub const fn max_unique_states(self) -> Option<usize> {
        self.max_unique_states
    }

    /// Returns the configured wall-clock budget, if any.
    #[must_use]
    pub const fn max_wall_clock(self) -> Option<Duration> {
        self.max_wall_clock
    }
}

/// Summary for a successful bounded model-checking run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Summary {
    pub(super) explored_states: usize,
    pub(super) unique_states: usize,
    pub(super) explored_actions: usize,
    pub(super) max_depth: usize,
}

impl Summary {
    /// Returns the number of recursive state visits, including duplicates
    /// pruned by canonical-state deduplication.
    #[must_use]
    pub const fn explored_states(self) -> usize {
        self.explored_states
    }

    /// Returns the number of distinct canonical states reached by the
    /// deduplicated search.
    #[must_use]
    pub const fn unique_states(self) -> usize {
        self.unique_states
    }

    /// Returns the number of actions applied while exploring the state space.
    #[must_use]
    pub const fn explored_actions(self) -> usize {
        self.explored_actions
    }

    /// Returns the maximum configured action depth.
    #[must_use]
    pub const fn max_depth(self) -> usize {
        self.max_depth
    }

    pub(super) const fn combined(self, other: Self) -> Self {
        Self {
            explored_states: self.explored_states + other.explored_states,
            unique_states: self.unique_states + other.unique_states,
            explored_actions: self.explored_actions + other.explored_actions,
            max_depth: if self.max_depth > other.max_depth {
                self.max_depth
            } else {
                other.max_depth
            },
        }
    }
}

/// A small, replayable action emitted in a model-checking counterexample.
///
/// This enum is exhaustive because counterexample traces are recorded using a
/// closed simulator action vocabulary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Action {
    Tick(NodeId),
    Restart(NodeId),
    Propose {
        to: NodeId,
        proposal_id: ProposalId,
    },
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
    Deliver {
        from: NodeId,
        to: NodeId,
        message: MessageKind,
    },
}

impl fmt::Display for Action {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tick(node_id) => write!(formatter, "tick {node_id}"),
            Self::Restart(node_id) => write!(formatter, "restart {node_id}"),
            Self::ReadIndex { to, request_id } => {
                write!(formatter, "read_index {request_id} to {to}")
            }
            Self::Propose { to, proposal_id } => {
                write!(formatter, "propose {} to {to}", proposal_id.0)
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
            Self::Deliver { from, to, message } => {
                write!(formatter, "deliver {message} {from}->{to}")
            }
        }
    }
}

/// Deterministic client proposal identity used in model-checking traces.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProposalId(pub u64);

/// Message category used in model-checking traces.
///
/// This enum is exhaustive because trace rendering groups messages into this
/// closed set of Raft message categories.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MessageKind {
    AppendEntries,
    AppendEntriesResponse,
    InstallSnapshot,
    InstallSnapshotChunk,
    InstallSnapshotResponse,
    PreVote,
    PreVoteResponse,
    RequestVote,
    RequestVoteResponse,
    TimeoutNow,
}

impl fmt::Display for MessageKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::AppendEntries => "append_entries",
            Self::AppendEntriesResponse => "append_entries_response",
            Self::InstallSnapshot => "install_snapshot",
            Self::InstallSnapshotChunk => "install_snapshot_chunk",
            Self::InstallSnapshotResponse => "install_snapshot_response",
            Self::PreVote => "pre_vote",
            Self::PreVoteResponse => "pre_vote_response",
            Self::TimeoutNow => "timeout_now",
            Self::RequestVote => "request_vote",
            Self::RequestVoteResponse => "request_vote_response",
        };
        formatter.write_str(label)
    }
}

impl From<&Message> for MessageKind {
    fn from(message: &Message) -> Self {
        match message {
            Message::AppendEntries(_) => Self::AppendEntries,
            Message::AppendEntriesResponse(_) => Self::AppendEntriesResponse,
            Message::InstallSnapshot(_) => Self::InstallSnapshot,
            Message::InstallSnapshotChunk(_) => Self::InstallSnapshotChunk,
            Message::InstallSnapshotResponse(_) => Self::InstallSnapshotResponse,
            Message::PreVote(_) => Self::PreVote,
            Message::PreVoteResponse(_) => Self::PreVoteResponse,
            Message::TimeoutNow(_) => Self::TimeoutNow,
            Message::RequestVote(_) => Self::RequestVote,
            Message::RequestVoteResponse(_) => Self::RequestVoteResponse,
        }
    }
}

/// Node state summary captured with a model-checking failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeSummary {
    pub node_id: NodeId,
    pub term: Term,
    pub role: Role,
    pub commit_index: LogIndex,
    pub snapshot_index: LogIndex,
    pub last_log_index: LogIndex,
}

/// Cluster state summary captured with a model-checking failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateSummary {
    pub nodes: Vec<NodeSummary>,
}

/// Failure returned when a bounded exploration finds an invariant violation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Failure {
    pub(super) invariant: &'static str,
    pub(super) message: String,
    pub(super) trace: Vec<Action>,
    pub(super) state: StateSummary,
}

impl Failure {
    /// Returns the invariant that failed.
    #[must_use]
    pub const fn invariant(&self) -> &'static str {
        self.invariant
    }

    /// Returns a human-readable failure message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the bounded action trace that led to the failure.
    #[must_use]
    pub fn trace(&self) -> &[Action] {
        &self.trace
    }

    /// Returns the final cluster summary at the failed state.
    #[must_use]
    pub const fn state(&self) -> &StateSummary {
        &self.state
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.invariant, self.message)
    }
}

impl Error for Failure {}
