use std::fmt;

use rafter::{MembershipSet, Message, NodeId};

use crate::SimTick;

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
        identity: EnvelopeIdentity,
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
            Self::Deliver {
                from,
                to,
                message,
                identity,
            } => {
                write!(formatter, "deliver {message} {from}->{to} {identity}")
            }
        }
    }
}

/// Exact identity of one queued envelope in a deterministic trace state.
///
/// The ordinal is counted among envelopes with the same sender, receiver, and
/// message kind. Pairing it with the scheduled tick distinguishes both
/// different messages in the same protocol family and byte-identical copies.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct EnvelopeIdentity {
    ready_at: SimTick,
    matching_ordinal: u64,
}

impl EnvelopeIdentity {
    /// Builds an identity from a queued envelope's scheduling metadata.
    #[must_use]
    pub const fn new(ready_at: SimTick, matching_ordinal: u64) -> Self {
        Self {
            ready_at,
            matching_ordinal,
        }
    }

    /// Returns the logical tick at which the envelope becomes deliverable.
    #[must_use]
    pub const fn ready_at(self) -> SimTick {
        self.ready_at
    }

    /// Returns the envelope's ordinal among equal routing/message families.
    #[must_use]
    pub const fn matching_ordinal(self) -> u64 {
        self.matching_ordinal
    }
}

impl fmt::Display for EnvelopeIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "[ready={}, ordinal={}]",
            self.ready_at.0, self.matching_ordinal
        )
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
