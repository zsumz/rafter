//! Pre-vote, election, and leadership-transfer authority frames.

use crate::{LogIndex, NodeId, Term};

/// Pre-vote poll preceding a real election (thesis 4.2.3 / 9.6).
///
/// `term` is the PROPOSED term (the sender's current term + 1), not a term
/// the sender holds. Granting a pre-vote never mutates the granter's term or
/// `voted_for`, and pre-vote grants are never persisted, so multiple pre-vote
/// grants in one term are allowed by design: a pre-vote is a non-binding poll
/// of whether a real election could be won.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PreVote {
    /// Proposed next election term.
    pub term: Term,
    /// Node polling whether it could win that term.
    pub candidate_id: NodeId,
    /// Candidate's last log index.
    pub last_log_index: LogIndex,
    /// Term of the candidate's last log entry.
    pub last_log_term: Term,
}

/// Response to a [`PreVote`] poll; `term` echoes the proposed request term.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PreVoteResponse {
    /// Proposed term echoed from the request.
    pub term: Term,
    /// Node answering the poll.
    pub voter_id: NodeId,
    /// Whether this voter would grant a real vote.
    pub vote_granted: bool,
}

/// Instructs the recipient to start an election immediately, bypassing
/// pre-vote and leader stickiness; sent by a leader completing a leadership
/// transfer (thesis 3.10).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TimeoutNow {
    /// Current leader term authorizing the transfer.
    pub term: Term,
    /// Leader requesting the immediate election.
    pub leader_id: NodeId,
}

/// Real `RequestVote` election request.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RequestVote {
    /// Election term.
    pub term: Term,
    /// Candidate requesting the vote.
    pub candidate_id: NodeId,
    /// Candidate's last log index.
    pub last_log_index: LogIndex,
    /// Term of the candidate's last log entry.
    pub last_log_term: Term,
}

/// Response to a [`RequestVote`] election request.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RequestVoteResponse {
    /// Voter's current term.
    pub term: Term,
    /// Node answering the request.
    pub voter_id: NodeId,
    /// Whether the binding vote was granted.
    pub vote_granted: bool,
}
