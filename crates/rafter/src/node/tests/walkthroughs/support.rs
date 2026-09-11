//! Scripted inputs and checkpoints, using the public production transition boundary.

pub(super) use super::steps::{AppendReply, ReplyOutcome, Step};
pub(super) use crate::*;

pub(super) struct Scenario {
    pub name: &'static str,
    pub explanation: &'static str,
    pub node: Node,
    pub steps: Vec<Step>,
    pub expected: (Term, Role, LogIndex, LogIndex, usize),
}

pub(super) fn config(id: u64, peers: &[u64]) -> NodeConfig {
    // Default pre-vote and check-quorum remain enabled; lease reads stay off.
    NodeConfig::new(NodeId(id), peers.iter().copied().map(NodeId).collect(), 10).unwrap()
}

pub(super) fn message(from: u64, message: Message) -> Input {
    Input::Message {
        from: NodeId(from),
        message,
    }
}

pub(super) fn pre_vote_granted(from: u64, term: u64) -> Input {
    message(
        from,
        Message::PreVoteResponse(PreVoteResponse {
            term: Term(term),
            voter_id: NodeId(from),
            vote_granted: true,
        }),
    )
}

pub(super) fn vote_granted(from: u64, term: u64) -> Input {
    message(
        from,
        Message::RequestVoteResponse(RequestVoteResponse {
            term: Term(term),
            voter_id: NodeId(from),
            vote_granted: true,
        }),
    )
}

pub(super) fn campaign(term: u64) -> Vec<Step> {
    vec![
        Step::inputs("Election timeout starts pre-vote",
            "Pre-voting asks whether an election could succeed without changing the durable term or vote.",
            vec![Input::Tick; 10]),
        Step::inputs(
            "Pre-vote quorum starts a binding election",
            "A pre-vote quorum permits a real election: advance the term, vote for self, and request binding votes.",
            vec![pre_vote_granted(2, term)],
        ),
        Step::inputs(
            "Binding quorum elects a leader and appends its no-op",
            "The binding quorum establishes leadership. The new-term no-op provides an entry that can authorize commitment of its prefix.",
            vec![vote_granted(2, term)],
        ),
    ]
}

pub(super) fn bootstrap(term: u64, commit: u64, log: Vec<BootstrapLogEntry>) -> Node {
    Node::from_bootstrap_applied_through(
        config(1, &[2, 3]),
        BootstrapState {
            current_term: Term(term),
            voted_for: None,
            commit_index: LogIndex(commit),
            committed_configuration: None,
            snapshot: None,
            log,
        },
        LogIndex(commit),
    )
    .unwrap()
}
