//! Scripted inputs and checkpoints, using the public production transition boundary.

pub(super) use crate::*;

pub(super) struct Scenario {
    pub name: &'static str,
    pub explanation: &'static str,
    pub node: Node,
    pub steps: Vec<(&'static str, Vec<Input>)>,
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

pub(super) fn vote(from: u64, term: u64, pre_vote: bool) -> Input {
    if pre_vote {
        message(
            from,
            Message::PreVoteResponse(PreVoteResponse {
                term: Term(term),
                voter_id: NodeId(from),
                vote_granted: true,
            }),
        )
    } else {
        message(
            from,
            Message::RequestVoteResponse(RequestVoteResponse {
                term: Term(term),
                voter_id: NodeId(from),
                vote_granted: true,
            }),
        )
    }
}

pub(super) fn ack(from: u64, term: u64, index: u64, sequence: u64, success: bool) -> Input {
    message(
        from,
        Message::AppendEntriesResponse(AppendEntriesResponse {
            term: Term(term),
            follower_id: NodeId(from),
            match_index: LogIndex(index),
            sequence,
            success,
        }),
    )
}

pub(super) fn campaign(term: u64) -> Vec<(&'static str, Vec<Input>)> {
    vec![
        ("Election timeout starts pre-vote", vec![Input::Tick; 10]),
        (
            "Pre-vote quorum starts a binding election",
            vec![vote(2, term, true)],
        ),
        (
            "Binding quorum elects a leader and appends its no-op",
            vec![vote(2, term, false)],
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
