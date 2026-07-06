use super::super::*;
use crate::{PreVoteResponse, RequestVoteResponse};

pub(super) fn node(id: u64, peers: &[u64]) -> Node {
    Node::new(
        NodeConfig::new(NodeId(id), peers.iter().copied().map(NodeId).collect(), 3)
            .expect("test Raft node config is valid"),
    )
}

/// Ticks `node` to its election timeout and leaves it a Candidate, running
/// the pre-vote round with a grant from node 2 first when the configuration
/// (default posture) requires one; minimal-protocol configurations campaign
/// directly. Returns the vote-request outputs.
pub(super) fn campaign(node: &mut Node) -> Vec<Output> {
    assert!(node.step(Input::Tick).is_empty());
    assert!(node.step(Input::Tick).is_empty());
    let mut vote_requests = node.step(Input::Tick);

    if node.role() == Role::PreCandidate {
        let proposed_term = vote_requests
            .iter()
            .find_map(|output| match output {
                Output::Send {
                    message: Message::PreVote(request),
                    ..
                } => Some(request.term),
                _ => None,
            })
            .expect("pre-vote round polls the proposed term");
        vote_requests = node.step(Input::Message {
            from: NodeId(2),
            message: Message::PreVoteResponse(PreVoteResponse {
                term: proposed_term,
                voter_id: NodeId(2),
                vote_granted: true,
            }),
        });
    }
    assert_eq!(node.role(), Role::Candidate);
    vote_requests
}

/// Elects `node` with grants from node 2, pre-vote round included when the
/// configuration runs one.
pub(super) fn elect_leader(node: &mut Node) -> Vec<Output> {
    let vote_requests = campaign(node);

    let heartbeats = node.step(Input::Message {
        from: NodeId(2),
        message: Message::RequestVoteResponse(RequestVoteResponse {
            term: node.current_term(),
            voter_id: NodeId(2),
            vote_granted: true,
        }),
    });

    assert_eq!(node.role(), Role::Leader);
    assert_eq!(heartbeats.len(), 2);
    [vote_requests, heartbeats].concat()
}

pub(super) fn bootstrap_entry(index: u64, term: u64, payload: &[u8]) -> BootstrapLogEntry {
    BootstrapLogEntry::application(LogIndex(index), Term(term), payload.to_vec())
}

pub(super) fn assert_vote_response(outputs: &[Output], to: NodeId, vote_granted: bool) {
    assert_eq!(outputs.len(), 1);
    let Output::Send {
        to: actual_to,
        message,
    } = &outputs[0]
    else {
        panic!("expected vote response");
    };
    assert_eq!(*actual_to, to);
    let Message::RequestVoteResponse(response) = message else {
        panic!("expected vote response");
    };
    assert_eq!(response.vote_granted, vote_granted);
}

pub(super) fn assert_append_entries(output: &Output, to: NodeId, entry_count: usize) {
    let Output::Send {
        to: actual_to,
        message,
    } = output
    else {
        panic!("expected append entries");
    };
    assert_eq!(*actual_to, to);
    let Message::AppendEntries(request) = message else {
        panic!("expected append entries");
    };
    assert_eq!(request.entries.len(), entry_count);
}

pub(super) fn assert_append_entries_response(
    outputs: &[Output],
    to: NodeId,
    success: bool,
    match_index: LogIndex,
) {
    assert_eq!(outputs.len(), 1);
    let Output::Send {
        to: actual_to,
        message,
    } = &outputs[0]
    else {
        panic!("expected append entries response");
    };
    assert_eq!(*actual_to, to);
    let Message::AppendEntriesResponse(response) = message else {
        panic!("expected append entries response");
    };
    assert_eq!(response.success, success);
    assert_eq!(response.match_index, match_index);
}
