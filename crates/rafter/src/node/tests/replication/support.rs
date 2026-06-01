use super::*;

pub(super) fn assert_committed_append_entries(
    output: &Output,
    to: NodeId,
    leader_commit: LogIndex,
) {
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
    assert_eq!(request.leader_commit, leader_commit);
}

pub(super) fn elect_five_node_leader(leader: &mut Node) {
    assert!(leader.step(Input::Tick).is_empty());
    assert!(leader.step(Input::Tick).is_empty());
    let _ = leader.step(Input::Tick);
    assert_eq!(leader.role(), Role::PreCandidate);
    let proposed_term = leader.current_term().next();
    assert!(leader
        .step(Input::Message {
            from: NodeId(2),
            message: pre_vote_granted(proposed_term, NodeId(2)),
        })
        .is_empty());
    let _ = leader.step(Input::Message {
        from: NodeId(3),
        message: pre_vote_granted(proposed_term, NodeId(3)),
    });
    assert_eq!(leader.role(), Role::Candidate);
    assert!(leader
        .step(Input::Message {
            from: NodeId(2),
            message: vote_granted(leader.current_term(), NodeId(2)),
        })
        .is_empty());
    let _ = leader.step(Input::Message {
        from: NodeId(3),
        message: vote_granted(leader.current_term(), NodeId(3)),
    });
    assert_eq!(leader.role(), Role::Leader);
}

fn vote_granted(term: Term, voter_id: NodeId) -> Message {
    Message::RequestVoteResponse(crate::RequestVoteResponse {
        term,
        voter_id,
        vote_granted: true,
    })
}

fn pre_vote_granted(term: Term, voter_id: NodeId) -> Message {
    Message::PreVoteResponse(crate::PreVoteResponse {
        term,
        voter_id,
        vote_granted: true,
    })
}

pub(super) fn acknowledge_append(leader: &mut Node, follower_id: NodeId, match_index: LogIndex) {
    let _ = leader.step(Input::Message {
        from: follower_id,
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence: 0,
            term: leader.current_term(),
            follower_id,
            success: true,
            match_index,
        }),
    });
}

pub(super) fn push_log_entry(node: &mut Node, term: Term, payload: &[u8]) {
    node.persistent
        .log
        .push(LogEntry::application(term, payload.to_vec()));
}

pub(super) fn append_entries_response(outputs: &[Output]) -> AppendEntriesResponse {
    outputs
        .iter()
        .find_map(|output| {
            let Output::Send { message, .. } = output else {
                return None;
            };
            let Message::AppendEntriesResponse(response) = message else {
                return None;
            };
            Some(*response)
        })
        .expect("expected append entries response")
}
