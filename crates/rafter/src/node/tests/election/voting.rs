//! Binding vote eligibility, identity, durability, and term-fencing scenarios.

use super::*;

#[test]
fn follower_grants_one_vote_per_term() {
    let mut node = node(1, &[2, 3]);

    let first = node.step(Input::Message {
        from: NodeId(2),
        message: Message::RequestVote(RequestVote {
            term: Term(7),
            candidate_id: NodeId(2),
            last_log_index: LogIndex(0),
            last_log_term: Term(0),
        }),
    });
    let second = node.step(Input::Message {
        from: NodeId(3),
        message: Message::RequestVote(RequestVote {
            term: Term(7),
            candidate_id: NodeId(3),
            last_log_index: LogIndex(0),
            last_log_term: Term(0),
        }),
    });

    assert_vote_response(&first, NodeId(2), true);
    assert_vote_response(&second, NodeId(3), false);
}
#[test]
fn same_term_append_entries_step_down_preserves_recorded_vote() {
    let mut node = node(1, &[2, 3]);

    let _ = campaign(&mut node);
    assert_eq!(node.current_term(), Term(1));
    assert_eq!(node.voted_for(), Some(NodeId(1)));

    let append_outputs = node.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: Term(1),
            leader_id: NodeId(2),
            prev_log_index: LogIndex::ZERO,
            prev_log_term: Term::default(),
            entries: Vec::new().into(),
            leader_commit: LogIndex::ZERO,
        }),
    });

    assert_eq!(node.role(), Role::Follower);
    assert_eq!(node.voted_for(), Some(NodeId(1)));
    assert!(matches!(
        append_outputs.as_slice(),
        [Output::Send {
            to: NodeId(2),
            message: Message::AppendEntriesResponse(response),
        }] if response.success
    ));

    let vote_outputs = node.step(Input::Message {
        from: NodeId(3),
        message: Message::RequestVote(RequestVote {
            term: Term(1),
            candidate_id: NodeId(3),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::default(),
        }),
    });

    assert_vote_response(&vote_outputs, NodeId(3), false);
    assert_eq!(node.voted_for(), Some(NodeId(1)));
}
#[test]
fn candidate_rejects_vote_response_when_sender_disagrees_with_voter_id() {
    let mut node = node(1, &[2, 3]);

    let _ = campaign(&mut node);

    let outputs = node.step(Input::Message {
        from: NodeId(2),
        message: Message::RequestVoteResponse(RequestVoteResponse {
            term: node.current_term(),
            voter_id: NodeId(3),
            vote_granted: true,
        }),
    });

    assert!(outputs.is_empty());
    assert_eq!(node.role(), Role::Candidate);
}
#[test]
fn candidate_rejects_vote_response_from_unknown_voter() {
    let mut node = node(1, &[2, 3]);

    let _ = campaign(&mut node);

    let outputs = node.step(Input::Message {
        from: NodeId(9),
        message: Message::RequestVoteResponse(RequestVoteResponse {
            term: node.current_term(),
            voter_id: NodeId(9),
            vote_granted: true,
        }),
    });

    assert!(outputs.is_empty());
    assert_eq!(node.role(), Role::Candidate);
}
#[test]
fn follower_rejects_vote_request_when_sender_disagrees_with_candidate_id() {
    let mut node = node(1, &[2, 3]);

    let outputs = node.step(Input::Message {
        from: NodeId(2),
        message: Message::RequestVote(RequestVote {
            term: Term(7),
            candidate_id: NodeId(3),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::default(),
        }),
    });

    assert!(outputs.is_empty());
    assert_eq!(node.current_term(), Term::default());
    assert_eq!(node.voted_for(), None);
}
#[test]
fn stale_vote_request_is_rejected() {
    let mut node = node(1, &[2, 3]);
    node.become_follower(Term(4));

    let outputs = node.step(Input::Message {
        from: NodeId(2),
        message: Message::RequestVote(RequestVote {
            term: Term(3),
            candidate_id: NodeId(2),
            last_log_index: LogIndex(0),
            last_log_term: Term(0),
        }),
    });

    assert_vote_response(&outputs, NodeId(2), false);
    assert_eq!(node.current_term(), Term(4));
}
#[test]
fn public_transitions_do_not_decrease_current_term() {
    let mut node = node(1, &[2, 3]);
    let _ = node.step(Input::Message {
        from: NodeId(2),
        message: Message::RequestVote(RequestVote {
            term: Term(5),
            candidate_id: NodeId(2),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::default(),
        }),
    });

    let stale_messages = [
        Message::RequestVote(RequestVote {
            term: Term(4),
            candidate_id: NodeId(3),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::default(),
        }),
        Message::RequestVoteResponse(RequestVoteResponse {
            term: Term(4),
            voter_id: NodeId(3),
            vote_granted: true,
        }),
        Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: Term(4),
            leader_id: NodeId(3),
            prev_log_index: LogIndex::ZERO,
            prev_log_term: Term::default(),
            entries: Vec::new().into(),
            leader_commit: LogIndex::ZERO,
        }),
        Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence: 0,
            term: Term(4),
            follower_id: NodeId(3),
            success: true,
            match_index: LogIndex::ZERO,
        }),
    ];

    for message in stale_messages {
        let _ = node.step(Input::Message {
            from: NodeId(3),
            message,
        });
        assert_eq!(node.current_term(), Term(5));
    }
}
