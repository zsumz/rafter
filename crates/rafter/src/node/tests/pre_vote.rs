//! Pre-vote non-disruption, freshness, identity, and term behavior.

use std::collections::{BTreeMap, VecDeque};

use super::helpers::node;
use super::*;
use crate::{AppendEntries, PreVote, PreVoteResponse, RequestVote, RequestVoteResponse};

fn tick_to_timeout(node: &mut Node) -> Vec<Output> {
    assert!(node.step(Input::Tick).is_empty());
    assert!(node.step(Input::Tick).is_empty());
    node.step(Input::Tick)
}

fn assert_pre_vote_request(output: &Output, to: NodeId, proposed_term: Term) {
    let Output::Send {
        to: actual_to,
        message: Message::PreVote(request),
    } = output
    else {
        panic!("expected pre-vote request, got {output:?}");
    };
    assert_eq!(*actual_to, to);
    assert_eq!(request.term, proposed_term);
}

fn assert_pre_vote_response(outputs: &[Output], to: NodeId, term: Term, vote_granted: bool) {
    assert_eq!(outputs.len(), 1);
    let Output::Send {
        to: actual_to,
        message: Message::PreVoteResponse(response),
    } = &outputs[0]
    else {
        panic!("expected pre-vote response, got {outputs:?}");
    };
    assert_eq!(*actual_to, to);
    assert_eq!(response.term, term);
    assert_eq!(response.vote_granted, vote_granted);
}

fn heartbeat_from(leader_id: u64, term: u64) -> Input {
    Input::Message {
        from: NodeId(leader_id),
        message: Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: Term(term),
            leader_id: NodeId(leader_id),
            prev_log_index: LogIndex::ZERO,
            prev_log_term: Term::default(),
            entries: Vec::new().into(),
            leader_commit: LogIndex::ZERO,
        }),
    }
}

fn pre_vote_from(candidate_id: u64, proposed_term: u64) -> Input {
    Input::Message {
        from: NodeId(candidate_id),
        message: Message::PreVote(PreVote {
            term: Term(proposed_term),
            candidate_id: NodeId(candidate_id),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::default(),
        }),
    }
}

/// Routes queued sends between the supplied nodes until no messages remain.
fn deliver_until_quiescent(nodes: &mut BTreeMap<NodeId, Node>, from: NodeId, outputs: Vec<Output>) {
    let mut queue: VecDeque<(NodeId, NodeId, Message)> = VecDeque::new();
    enqueue_sends(&mut queue, from, outputs);
    while let Some((from, to, message)) = queue.pop_front() {
        let outputs = nodes
            .get_mut(&to)
            .expect("routed node exists")
            .step(Input::Message { from, message });
        enqueue_sends(&mut queue, to, outputs);
    }
}

fn enqueue_sends(
    queue: &mut VecDeque<(NodeId, NodeId, Message)>,
    from: NodeId,
    outputs: Vec<Output>,
) {
    for output in outputs {
        if let Output::Send { to, message } = output {
            queue.push_back((from, to, message));
        }
    }
}

#[test]
fn pre_vote_round_precedes_election_and_does_not_inflate_term() {
    let mut node = node(1, &[2, 3]);

    let outputs = tick_to_timeout(&mut node);

    // The timeout starts a pre-vote round, not a real election: the term is
    // untouched and the fan-out is PreVote, not RequestVote.
    assert_eq!(node.role(), Role::PreCandidate);
    assert_eq!(node.current_term(), Term(0));
    assert_eq!(node.voted_for(), None);
    assert_eq!(outputs.len(), 2);
    assert_pre_vote_request(&outputs[0], NodeId(2), Term(1));
    assert_pre_vote_request(&outputs[1], NodeId(3), Term(1));

    // One grant plus the self-grant is a quorum of three: the real election
    // starts and only now does the term advance.
    let outputs = node.step(Input::Message {
        from: NodeId(2),
        message: Message::PreVoteResponse(PreVoteResponse {
            term: Term(1),
            voter_id: NodeId(2),
            vote_granted: true,
        }),
    });

    assert_eq!(node.role(), Role::Candidate);
    assert_eq!(node.current_term(), Term(1));
    assert!(outputs.iter().all(|output| matches!(
        output,
        Output::Send {
            message: Message::RequestVote(request),
            ..
        } if request.term == Term(1)
    )));
    assert_eq!(outputs.len(), 2);

    let outputs = node.step(Input::Message {
        from: NodeId(2),
        message: Message::RequestVoteResponse(RequestVoteResponse {
            term: Term(1),
            voter_id: NodeId(2),
            vote_granted: true,
        }),
    });

    assert_eq!(node.role(), Role::Leader);
    assert_eq!(node.current_term(), Term(1));
    assert_eq!(outputs.len(), 2);
}

#[test]
fn rejoining_partitioned_node_does_not_disrupt_leader() {
    let mut nodes = BTreeMap::new();
    nodes.insert(NodeId(1), node(1, &[2, 3]));
    nodes.insert(NodeId(2), node(2, &[1, 3]));
    nodes.insert(NodeId(3), node(3, &[1, 2]));

    // Elect node 1 through a full pre-vote round plus a real election, then
    // let its heartbeats give every follower a fresh leader hint.
    let outputs = tick_to_timeout(nodes.get_mut(&NodeId(1)).expect("node 1 exists"));
    deliver_until_quiescent(&mut nodes, NodeId(1), outputs);
    assert_eq!(nodes[&NodeId(1)].role(), Role::Leader);
    assert_eq!(nodes[&NodeId(1)].current_term(), Term(1));
    assert_eq!(nodes[&NodeId(2)].leader_hint(), Some(NodeId(1)));
    assert_eq!(nodes[&NodeId(3)].leader_hint(), Some(NodeId(1)));

    // Node 3 is partitioned away: it times out repeatedly, but every round
    // proposes the SAME term and never inflates its own.
    let mut isolated_rounds = Vec::new();
    for _ in 0..3 {
        let node = nodes.get_mut(&NodeId(3)).expect("node 3 exists");
        let outputs = tick_to_timeout(node);
        assert_eq!(node.role(), Role::PreCandidate);
        assert_eq!(node.current_term(), Term(1));
        assert_pre_vote_request(&outputs[0], NodeId(1), Term(2));
        assert_pre_vote_request(&outputs[1], NodeId(2), Term(2));
        isolated_rounds.push(outputs);
    }

    // The partition heals: the stranded pre-votes reach the peers, whose
    // fresh leader hints deny them, so no election starts.
    let last_round = isolated_rounds.pop().expect("a pre-vote round happened");
    deliver_until_quiescent(&mut nodes, NodeId(3), last_round);
    assert_eq!(nodes[&NodeId(3)].role(), Role::PreCandidate);
    assert_eq!(nodes[&NodeId(1)].role(), Role::Leader);

    // The leader's next heartbeat returns the rejoined node to Follower with
    // no term change anywhere in the cluster: this is the point of pre-vote.
    let heartbeats = nodes
        .get_mut(&NodeId(1))
        .expect("node 1 exists")
        .step(Input::Tick);
    deliver_until_quiescent(&mut nodes, NodeId(1), heartbeats);

    assert_eq!(nodes[&NodeId(1)].role(), Role::Leader);
    assert_eq!(nodes[&NodeId(3)].role(), Role::Follower);
    for node in nodes.values() {
        assert_eq!(node.current_term(), Term(1));
    }
}

#[test]
fn pre_vote_granted_when_no_leader_is_known() {
    let mut node = node(1, &[2, 3]);

    // Fresh cluster: no leader hint anywhere, so bootstrap pre-votes pass.
    let outputs = node.step(pre_vote_from(2, 1));

    assert_pre_vote_response(&outputs, NodeId(2), Term(1), true);
    assert_eq!(node.current_term(), Term(0));
}

#[test]
fn pre_vote_denied_within_election_timeout_of_leader_contact() {
    let mut node = node(1, &[2, 3]);
    let _ = node.step(heartbeat_from(2, 1));
    assert_eq!(node.leader_hint(), Some(NodeId(2)));

    // Leader stickiness (thesis 4.2.3): a granter that heard from a leader
    // within its election timeout denies the pre-vote.
    let outputs = node.step(pre_vote_from(3, 2));
    assert_pre_vote_response(&outputs, NodeId(3), Term(1), false);

    // Still inside the granter's timeout window: denied again.
    assert!(node.step(Input::Tick).is_empty());
    assert!(node.step(Input::Tick).is_empty());
    let outputs = node.step(pre_vote_from(3, 2));
    assert_pre_vote_response(&outputs, NodeId(3), Term(1), false);

    // Once leader contact goes stale (the granter itself times out), the same
    // pre-vote is granted.
    let _ = node.step(Input::Tick);
    let outputs = node.step(pre_vote_from(3, 2));
    assert_pre_vote_response(&outputs, NodeId(3), Term(2), true);
    assert_eq!(node.current_term(), Term(1));
}

#[test]
fn pre_vote_grant_is_not_persisted_and_does_not_set_voted_for() {
    let mut node = node(1, &[2, 3]);
    assert!(node.step(Input::Tick).is_empty());
    let elapsed_before = node.election.elapsed();

    let outputs = node.step(pre_vote_from(2, 1));
    assert_pre_vote_response(&outputs, NodeId(2), Term(1), true);

    // Hard state {current_term, voted_for} is untouched, so the runtime never
    // persists anything for a pre-vote grant, and the granter's election
    // timer keeps running.
    assert_eq!(node.current_term(), Term(0));
    assert_eq!(node.voted_for(), None);
    assert_eq!(node.election.elapsed(), elapsed_before);

    // A second grant to a DIFFERENT candidate in the same proposed term is
    // allowed by design: pre-votes are non-binding polls.
    let outputs = node.step(pre_vote_from(3, 1));
    assert_pre_vote_response(&outputs, NodeId(3), Term(1), true);
    assert_eq!(node.voted_for(), None);
}

#[test]
fn re_timeout_repeats_pre_vote_at_same_proposed_term() {
    let mut node = node(1, &[2, 3]);

    let first_round = tick_to_timeout(&mut node);
    assert_pre_vote_request(&first_round[0], NodeId(2), Term(1));
    assert_pre_vote_request(&first_round[1], NodeId(3), Term(1));

    // A pre-candidate that times out again re-broadcasts at the SAME proposed
    // term: no inflation is the point of the feature.
    let second_round = tick_to_timeout(&mut node);
    assert_eq!(node.role(), Role::PreCandidate);
    assert_eq!(node.current_term(), Term(0));
    assert_pre_vote_request(&second_round[0], NodeId(2), Term(1));
    assert_pre_vote_request(&second_round[1], NodeId(3), Term(1));
}

#[test]
fn pre_candidate_returns_to_follower_on_leader_append() {
    let mut node = node(1, &[2, 3]);
    let _ = node.step(heartbeat_from(2, 1));
    let _ = tick_to_timeout(&mut node);
    assert_eq!(node.role(), Role::PreCandidate);

    // A valid AppendEntries at the current term ends the pre-vote round and
    // is processed as a follower.
    let outputs = node.step(heartbeat_from(2, 1));

    assert_eq!(node.role(), Role::Follower);
    assert_eq!(node.current_term(), Term(1));
    assert_eq!(node.leader_hint(), Some(NodeId(2)));
    assert!(matches!(
        outputs.as_slice(),
        [Output::Send {
            to: NodeId(2),
            message: Message::AppendEntriesResponse(response),
        }] if response.success
    ));
}

#[test]
fn pre_candidate_handles_request_vote_as_follower_would() {
    let mut node = node(1, &[2, 3]);
    let _ = tick_to_timeout(&mut node);
    assert_eq!(node.role(), Role::PreCandidate);

    let outputs = node.step(Input::Message {
        from: NodeId(2),
        message: Message::RequestVote(RequestVote {
            term: Term(1),
            candidate_id: NodeId(2),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::default(),
        }),
    });

    // The real vote is handled exactly as a follower would handle it: the
    // higher term steps the pre-candidate down and the vote is recorded.
    assert_eq!(node.role(), Role::Follower);
    assert_eq!(node.current_term(), Term(1));
    assert_eq!(node.voted_for(), Some(NodeId(2)));
    assert!(matches!(
        outputs.as_slice(),
        [Output::Send {
            to: NodeId(2),
            message: Message::RequestVoteResponse(response),
        }] if response.vote_granted
    ));
}

#[test]
fn pre_vote_disabled_timeout_still_starts_real_election() {
    let mut node = Node::new(
        NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3)
            .expect("test Raft node config is valid")
            .with_pre_vote(false),
    );

    let outputs = tick_to_timeout(&mut node);

    assert_eq!(node.role(), Role::Candidate);
    assert_eq!(node.current_term(), Term(1));
    assert!(outputs.iter().all(|output| matches!(
        output,
        Output::Send {
            message: Message::RequestVote(_),
            ..
        }
    )));
}

#[test]
fn pre_vote_denial_from_newer_term_converts_poller_to_follower() {
    // The candidate is two terms behind: its poll proposes term 2 while the
    // cluster has moved to term 4. The denial teaches it the newer term so
    // the next round proposes past it instead of polling forever.
    let mut node = node(1, &[2, 3]);
    let _ = node.step(Input::Tick);
    let _ = node.step(Input::Tick);
    let _ = node.step(Input::Tick);
    assert_eq!(node.role(), Role::PreCandidate);

    let _ = node.step(Input::Message {
        from: NodeId(2),
        message: Message::PreVoteResponse(PreVoteResponse {
            term: Term(4),
            voter_id: NodeId(2),
            vote_granted: false,
        }),
    });

    assert_eq!(node.role(), Role::Follower);
    assert_eq!(node.current_term(), Term(4));
}

#[test]
fn pre_vote_denial_by_stale_poll_carries_the_denier_term() {
    // A denier at term 5 receiving a poll proposing term 2 answers with its
    // own term, not an echo of the stale proposal.
    let mut denier = node(2, &[1, 3]);
    for _ in 0..4 {
        denier.persistent.current_term = denier.persistent.current_term.next();
    }
    assert_eq!(denier.current_term(), Term(4));

    let outputs = denier.step(Input::Message {
        from: NodeId(1),
        message: Message::PreVote(PreVote {
            term: Term(2),
            candidate_id: NodeId(1),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::default(),
        }),
    });

    let [Output::Send {
        message: Message::PreVoteResponse(response),
        ..
    }] = outputs.as_slice()
    else {
        panic!("expected a pre-vote response");
    };
    assert!(!response.vote_granted);
    assert_eq!(response.term, Term(4));
}
