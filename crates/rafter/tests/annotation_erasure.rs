//! Tests for the Layer 0 annotation-erasure invariant: local proposal and
//! read IDs may annotate inputs and outputs, but after those annotations are
//! erased the Raft protocol state and wire behavior must match the equivalent
//! untracked trace.

use rafter::{
    AppendEntries, AppendEntriesResponse, Input, LocalProposalDropReason, LocalProposalId,
    LogIndex, Message, Node, NodeConfig, NodeId, Output, ReadId, ReadIndexCancelReason,
    RequestVoteResponse,
};

const TEST_ELECTION_TIMEOUT_TICKS: u64 = 3;

fn node(id: u64, peers: &[u64]) -> Node {
    Node::new(
        NodeConfig::new(
            NodeId(id),
            peers.iter().copied().map(NodeId).collect(),
            TEST_ELECTION_TIMEOUT_TICKS,
        )
        .expect("test node config is valid")
        .with_pre_vote(false),
    )
}

fn leader_with_current_term_commit() -> Node {
    let mut leader = node(1, &[2, 3]);
    for _ in 0..3 {
        let _ = leader.step(Input::Tick);
    }
    let _ = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::RequestVoteResponse(RequestVoteResponse {
            term: leader.current_term(),
            voter_id: NodeId(2),
            vote_granted: true,
        }),
    });
    assert_eq!(leader.role(), rafter::Role::Leader);

    let _ = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: true,
            match_index: LogIndex(1),
            sequence: 0,
        }),
    });
    assert_eq!(leader.commit_index(), LogIndex(1));
    leader
}

fn erase_outputs(outputs: &[Output]) -> Vec<Output> {
    outputs
        .iter()
        .filter_map(|output| match output {
            Output::LocalProposalAppended { .. }
            | Output::LocalProposalDropped { .. }
            | Output::ReadIndexCanceled { .. } => None,
            Output::Apply {
                index,
                term,
                payload,
                ..
            } => Some(Output::Apply {
                index: *index,
                term: *term,
                payload: payload.clone(),
                local_proposal_id: None,
            }),
            Output::RejectProposal { reason, .. } => Some(Output::RejectProposal {
                proposal_id: None,
                reason: reason.clone(),
            }),
            Output::ReadIndexGranted { read_index, .. } => Some(Output::ReadIndexGranted {
                read_id: ReadId(0),
                read_index: *read_index,
            }),
            Output::ReadIndexRejected { reason, .. } => Some(Output::ReadIndexRejected {
                read_id: ReadId(0),
                reason: *reason,
            }),
            other => Some(other.clone()),
        })
        .collect()
}

fn append_sequence(outputs: &[Output]) -> u64 {
    outputs
        .iter()
        .find_map(|output| match output {
            Output::Send {
                message: Message::AppendEntries(AppendEntries { sequence, .. }),
                ..
            } => Some(*sequence),
            _ => None,
        })
        .expect("step broadcasts append entries")
}

fn assert_same_protocol_state(left: &Node, right: &Node) {
    assert_eq!(left.current_term(), right.current_term());
    assert_eq!(left.role(), right.role());
    assert_eq!(left.voted_for(), right.voted_for());
    assert_eq!(left.leader_hint(), right.leader_hint());
    assert_eq!(left.commit_index(), right.commit_index());
    assert_eq!(left.applied_index(), right.applied_index());
    assert_eq!(left.snapshot_index(), right.snapshot_index());
    assert_eq!(left.snapshot(), right.snapshot());
    assert_eq!(left.last_log_index(), right.last_log_index());
    assert_eq!(
        left.log_entries_from(LogIndex(1)),
        right.log_entries_from(LogIndex(1))
    );
    assert_eq!(left.effective_membership(), right.effective_membership());
    assert_eq!(left.committed_membership(), right.committed_membership());
    assert_eq!(
        left.leader_replication_progress(),
        right.leader_replication_progress()
    );
}

#[test]
fn tracked_proposal_ids_erase_to_untracked_protocol_behavior() {
    let mut tracked = leader_with_current_term_commit();
    let mut untracked = leader_with_current_term_commit();
    assert_same_protocol_state(&tracked, &untracked);

    let tracked_outputs = tracked.step(Input::TrackedClientProposal {
        proposal_id: LocalProposalId(10),
        payload: b"set a 1".to_vec(),
    });
    let untracked_outputs = untracked.step(Input::ClientProposal {
        payload: b"set a 1".to_vec(),
    });

    assert_eq!(
        erase_outputs(&tracked_outputs),
        erase_outputs(&untracked_outputs)
    );
    assert_same_protocol_state(&tracked, &untracked);

    let sequence = append_sequence(&tracked_outputs);
    let tracked_outputs = tracked.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            term: tracked.current_term(),
            follower_id: NodeId(2),
            success: true,
            match_index: LogIndex(2),
            sequence,
        }),
    });
    let untracked_outputs = untracked.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            term: untracked.current_term(),
            follower_id: NodeId(2),
            success: true,
            match_index: LogIndex(2),
            sequence,
        }),
    });

    assert_eq!(
        erase_outputs(&tracked_outputs),
        erase_outputs(&untracked_outputs)
    );
    assert_same_protocol_state(&tracked, &untracked);
}

#[test]
fn rejected_proposal_ids_erase_to_untracked_rejection_behavior() {
    let mut tracked = node(2, &[1, 3]);
    let mut untracked = node(2, &[1, 3]);

    let tracked_outputs = tracked.step(Input::TrackedClientProposal {
        proposal_id: LocalProposalId(55),
        payload: b"set b 2".to_vec(),
    });
    let untracked_outputs = untracked.step(Input::ClientProposal {
        payload: b"set b 2".to_vec(),
    });

    assert_eq!(
        erase_outputs(&tracked_outputs),
        erase_outputs(&untracked_outputs)
    );
    assert_same_protocol_state(&tracked, &untracked);
}

#[test]
fn read_ids_erase_to_identical_quorum_behavior() {
    let mut first = leader_with_current_term_commit();
    let mut second = leader_with_current_term_commit();
    assert_same_protocol_state(&first, &second);

    let first_outputs = first.step(Input::ReadIndex { read_id: ReadId(1) });
    let second_outputs = second.step(Input::ReadIndex {
        read_id: ReadId(99),
    });

    assert_eq!(
        erase_outputs(&first_outputs),
        erase_outputs(&second_outputs)
    );

    let sequence = append_sequence(&first_outputs);
    let first_outputs = first.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            term: first.current_term(),
            follower_id: NodeId(2),
            success: true,
            match_index: first.last_log_index(),
            sequence,
        }),
    });
    let second_outputs = second.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            term: second.current_term(),
            follower_id: NodeId(2),
            success: true,
            match_index: second.last_log_index(),
            sequence,
        }),
    });

    assert_eq!(
        erase_outputs(&first_outputs),
        erase_outputs(&second_outputs)
    );
    assert_same_protocol_state(&first, &second);
}

#[test]
fn dropped_local_proposals_erase_to_untracked_stepdown_behavior() {
    let mut tracked = leader_with_current_term_commit();
    let mut untracked = leader_with_current_term_commit();
    assert_same_protocol_state(&tracked, &untracked);

    let tracked_outputs = tracked.step(Input::TrackedClientProposal {
        proposal_id: LocalProposalId(77),
        payload: b"uncommitted".to_vec(),
    });
    let untracked_outputs = untracked.step(Input::ClientProposal {
        payload: b"uncommitted".to_vec(),
    });

    assert_eq!(
        erase_outputs(&tracked_outputs),
        erase_outputs(&untracked_outputs)
    );
    assert_same_protocol_state(&tracked, &untracked);

    let mut saw_drop = false;
    for _ in 0..TEST_ELECTION_TIMEOUT_TICKS * 2 {
        let tracked_outputs = tracked.step(Input::Tick);
        let untracked_outputs = untracked.step(Input::Tick);

        saw_drop |= tracked_outputs.iter().any(|output| {
            matches!(
                output,
                Output::LocalProposalDropped {
                    proposal_id: LocalProposalId(77),
                    index: LogIndex(2),
                    term,
                    reason: LocalProposalDropReason::LeadershipLost,
                } if *term == tracked.current_term()
            )
        });

        assert_eq!(
            erase_outputs(&tracked_outputs),
            erase_outputs(&untracked_outputs)
        );
        assert_same_protocol_state(&tracked, &untracked);

        if tracked.role() == rafter::Role::Follower {
            break;
        }
    }

    assert_eq!(tracked.role(), rafter::Role::Follower);
    assert_eq!(untracked.role(), rafter::Role::Follower);
    assert!(
        saw_drop,
        "tracked stepdown should emit a local-only proposal drop"
    );
}

#[test]
fn canceled_read_ids_erase_to_identical_stepdown_behavior() {
    let mut first = leader_with_current_term_commit();
    let mut second = leader_with_current_term_commit();
    assert_same_protocol_state(&first, &second);

    let first_outputs = first.step(Input::ReadIndex {
        read_id: ReadId(11),
    });
    let second_outputs = second.step(Input::ReadIndex {
        read_id: ReadId(99),
    });

    assert_eq!(
        erase_outputs(&first_outputs),
        erase_outputs(&second_outputs)
    );
    assert_same_protocol_state(&first, &second);

    let mut first_canceled = false;
    let mut second_canceled = false;
    for _ in 0..TEST_ELECTION_TIMEOUT_TICKS * 2 {
        let first_outputs = first.step(Input::Tick);
        let second_outputs = second.step(Input::Tick);

        first_canceled |= first_outputs.iter().any(|output| {
            matches!(
                output,
                Output::ReadIndexCanceled {
                    read_id: ReadId(11),
                    reason: ReadIndexCancelReason::LeadershipLost,
                }
            )
        });
        second_canceled |= second_outputs.iter().any(|output| {
            matches!(
                output,
                Output::ReadIndexCanceled {
                    read_id: ReadId(99),
                    reason: ReadIndexCancelReason::LeadershipLost,
                }
            )
        });

        assert_eq!(
            erase_outputs(&first_outputs),
            erase_outputs(&second_outputs)
        );
        assert_same_protocol_state(&first, &second);

        if first.role() == rafter::Role::Follower {
            break;
        }
    }

    assert_eq!(first.role(), rafter::Role::Follower);
    assert_eq!(second.role(), rafter::Role::Follower);
    assert!(first_canceled, "first read should be canceled on stepdown");
    assert!(
        second_canceled,
        "second read should be canceled on stepdown"
    );
}
