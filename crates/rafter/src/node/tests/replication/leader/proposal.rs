//! Proposal admission, local correlation, batching, commitment, and rejection.

use super::support::*;
use super::*;

#[test]
fn leader_proposal_replication_commits_after_quorum_ack() {
    let mut leader = node(1, &[2, 3]);
    let _ = elect_leader(&mut leader);
    // Both followers acknowledge the leader's initial no-op, confirming their
    // log positions: replication to them runs in Replicate mode.
    acknowledge_append(&mut leader, NodeId(2), LogIndex(1));
    acknowledge_append(&mut leader, NodeId(3), LogIndex(1));

    let outputs = leader.step(Input::ClientProposal {
        payload: b"alert-opened".to_vec(),
    });

    assert_eq!(leader.last_log_index(), LogIndex(2));
    assert_eq!(outputs.len(), 2);
    assert_append_entries(&outputs[0], NodeId(2), 1);
    assert!(!outputs
        .iter()
        .any(|output| matches!(output, Output::Apply { .. })));

    let commit_outputs = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence: 0,
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: true,
            match_index: LogIndex(2),
        }),
    });

    assert_eq!(leader.commit_index(), LogIndex(2));
    assert_eq!(
        commit_outputs,
        vec![Output::Apply {
            index: LogIndex(2),
            term: Term(1),
            payload: b"alert-opened".to_vec().into(),
            local_proposal_id: None,
        }]
    );

    let next_outputs = leader.step(Input::ClientProposal {
        payload: b"alert-closed".to_vec(),
    });
    assert_committed_append_entries(&next_outputs[0], NodeId(2), LogIndex(2));
    assert_committed_append_entries(&next_outputs[1], NodeId(3), LogIndex(2));
}
#[test]
fn tracked_client_proposal_matches_untracked_protocol_behavior() {
    let mut untracked = node(1, &[2, 3]);
    let mut tracked = node(1, &[2, 3]);
    let _ = elect_leader(&mut untracked);
    let _ = elect_leader(&mut tracked);
    acknowledge_append(&mut untracked, NodeId(2), LogIndex(1));
    acknowledge_append(&mut untracked, NodeId(3), LogIndex(1));
    acknowledge_append(&mut tracked, NodeId(2), LogIndex(1));
    acknowledge_append(&mut tracked, NodeId(3), LogIndex(1));

    let untracked_outputs = untracked.step(Input::ClientProposal {
        payload: b"tracked-equivalence".to_vec(),
    });
    let tracked_outputs = tracked.step(Input::TrackedClientProposal {
        proposal_id: LocalProposalId(42),
        payload: b"tracked-equivalence".to_vec(),
    });
    assert_eq!(
        tracked_outputs.first(),
        Some(&Output::LocalProposalAppended {
            proposal_id: LocalProposalId(42),
            index: LogIndex(2),
            term: tracked.current_term(),
        })
    );
    assert_eq!(
        erase_local_annotations(&tracked_outputs),
        erase_local_annotations(&untracked_outputs)
    );
    assert!(!untracked_outputs
        .iter()
        .any(|output| matches!(output, Output::LocalProposalAppended { .. })));
    assert_eq!(tracked.persistent, untracked.persistent);
    assert_eq!(
        tracked.volatile.commit_index,
        untracked.volatile.commit_index
    );
    assert_eq!(tracked.last_log_index(), untracked.last_log_index());

    let untracked_commit_outputs = untracked.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence: 0,
            term: untracked.current_term(),
            follower_id: NodeId(2),
            success: true,
            match_index: LogIndex(2),
        }),
    });
    let tracked_commit_outputs = tracked.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence: 0,
            term: tracked.current_term(),
            follower_id: NodeId(2),
            success: true,
            match_index: LogIndex(2),
        }),
    });

    assert_eq!(
        tracked_commit_outputs,
        vec![Output::Apply {
            index: LogIndex(2),
            term: Term(1),
            payload: b"tracked-equivalence".to_vec().into(),
            local_proposal_id: Some(LocalProposalId(42)),
        }]
    );
    assert!(tracked.volatile.local_proposals.is_empty());
    assert_eq!(
        erase_local_annotations(&tracked_commit_outputs),
        erase_local_annotations(&untracked_commit_outputs)
    );
    assert_eq!(tracked.persistent, untracked.persistent);
    assert_eq!(
        tracked.volatile.commit_index,
        untracked.volatile.commit_index
    );
}
#[test]
fn proposal_batch_preserves_input_order_and_contiguous_indexes() {
    let mut leader = node(1, &[2, 3]);
    let _ = elect_leader(&mut leader);
    acknowledge_append(&mut leader, NodeId(2), LogIndex(1));
    acknowledge_append(&mut leader, NodeId(3), LogIndex(1));

    let max_payload_len =
        LogEntry::max_application_payload_len(leader.config.max_append_entries_bytes());
    let oversized = vec![b'x'; max_payload_len + 1];
    let outputs = leader.step_proposal_batch(vec![
        ClientProposalInput {
            proposal_id: Some(LocalProposalId(7)),
            payload: b"first".to_vec(),
        },
        ClientProposalInput {
            proposal_id: Some(LocalProposalId(8)),
            payload: oversized.clone(),
        },
        ClientProposalInput {
            proposal_id: Some(LocalProposalId(9)),
            payload: b"second".to_vec(),
        },
    ]);

    assert_eq!(leader.last_log_index(), LogIndex(3));
    assert_eq!(
        leader
            .log_entries_from(LogIndex(2))
            .iter()
            .filter_map(LogEntry::application_payload)
            .collect::<Vec<_>>(),
        vec![b"first".as_slice(), b"second".as_slice()]
    );
    assert_eq!(
        &outputs[..3],
        [
            Output::LocalProposalAppended {
                proposal_id: LocalProposalId(7),
                index: LogIndex(2),
                term: leader.current_term(),
            },
            Output::RejectProposal {
                proposal_id: Some(LocalProposalId(8)),
                reason: ProposalRejection::PayloadTooLarge {
                    payload_len: oversized.len(),
                    max_payload_len,
                },
            },
            Output::LocalProposalAppended {
                proposal_id: LocalProposalId(9),
                index: LogIndex(3),
                term: leader.current_term(),
            },
        ]
    );
    assert_eq!(
        leader
            .volatile
            .local_proposals
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![LogIndex(2), LogIndex(3)]
    );
}
#[test]
fn overwritten_tracked_proposal_does_not_apply_stale_local_id() {
    let mut node = node(1, &[2, 3]);
    let _ = elect_leader(&mut node);
    acknowledge_append(&mut node, NodeId(2), LogIndex(1));
    acknowledge_append(&mut node, NodeId(3), LogIndex(1));

    let _ = node.step(Input::TrackedClientProposal {
        proposal_id: LocalProposalId(7),
        payload: b"local-uncommitted".to_vec(),
    });
    assert!(node.volatile.local_proposals.contains_key(LogIndex(2)));

    let replacement_term = node.current_term().next();
    let outputs = node.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: replacement_term,
            leader_id: NodeId(2),
            prev_log_index: LogIndex(1),
            prev_log_term: node.term_at(LogIndex(1)).expect("leadership no-op exists"),
            entries: vec![LogEntry::application(
                replacement_term,
                b"remote-replacement".to_vec(),
            )]
            .into(),
            leader_commit: LogIndex(2),
        }),
    });

    assert!(
        outputs.iter().any(|output| matches!(
            output,
            Output::LocalProposalDropped {
                proposal_id: LocalProposalId(7),
                index: LogIndex(2),
                term: Term(1),
                reason: LocalProposalDropReason::LeadershipLost,
            }
        )),
        "leadership loss reports the tracked local proposal as dropped before overwrite"
    );
    assert_eq!(
        outputs
            .iter()
            .find_map(|output| match output {
                Output::Apply {
                    index,
                    term: _,
                    payload,
                    local_proposal_id,
                } => Some((*index, payload.clone(), *local_proposal_id)),
                _ => None,
            })
            .expect("replacement applies"),
        (LogIndex(2), b"remote-replacement".to_vec().into(), None)
    );
    assert!(node.volatile.local_proposals.is_empty());
}
#[test]
fn leader_rejects_client_payload_too_large_for_replication_budget() {
    let mut leader = node_with_max_append_entries_bytes(1, &[2, 3], 100);
    let _ = elect_leader(&mut leader);

    let outputs = leader.step(Input::ClientProposal {
        payload: vec![b'x'; 40],
    });

    assert_eq!(
        outputs,
        vec![Output::RejectProposal {
            proposal_id: None,
            reason: ProposalRejection::PayloadTooLarge {
                payload_len: 40,
                max_payload_len: 36,
            },
        }]
    );
    assert_eq!(leader.last_log_index(), LogIndex(1));
}
#[test]
fn tracked_rejected_client_proposals_preserve_their_local_id() {
    let mut follower = node(1, &[2, 3]);
    let not_leader_outputs = follower.step(Input::TrackedClientProposal {
        proposal_id: LocalProposalId(7),
        payload: b"not-leader".to_vec(),
    });
    assert!(matches!(
        not_leader_outputs.as_slice(),
        [Output::RejectProposal {
            proposal_id: Some(LocalProposalId(7)),
            reason: ProposalRejection::NotLeader { .. },
        }]
    ));

    let mut leader = node_with_max_append_entries_bytes(1, &[2, 3], 100);
    let _ = elect_leader(&mut leader);
    let oversized_outputs = leader.step(Input::TrackedClientProposal {
        proposal_id: LocalProposalId(8),
        payload: vec![b'x'; 40],
    });
    assert_eq!(
        oversized_outputs,
        vec![Output::RejectProposal {
            proposal_id: Some(LocalProposalId(8)),
            reason: ProposalRejection::PayloadTooLarge {
                payload_len: 40,
                max_payload_len: 36,
            },
        }]
    );
}
#[test]
fn single_voter_leader_proposal_commits_immediately() {
    let mut leader = node(1, &[]);

    assert!(leader.step(Input::Tick).is_empty());
    assert!(leader.step(Input::Tick).is_empty());
    assert!(leader.step(Input::Tick).is_empty());
    assert_eq!(leader.role(), Role::Leader);

    let outputs = leader.step(Input::ClientProposal {
        payload: b"alert-opened".to_vec(),
    });

    assert_eq!(leader.commit_index(), LogIndex(2));
    assert_eq!(
        outputs,
        vec![Output::Apply {
            index: LogIndex(2),
            term: Term(1),
            payload: b"alert-opened".to_vec().into(),
            local_proposal_id: None,
        }]
    );
}
#[test]
fn single_voter_proposal_batch_emits_annotations_before_batch_applies() {
    let mut leader = node(1, &[]);

    assert!(leader.step(Input::Tick).is_empty());
    assert!(leader.step(Input::Tick).is_empty());
    assert!(leader.step(Input::Tick).is_empty());
    assert_eq!(leader.role(), Role::Leader);

    let outputs = leader.step_proposal_batch(vec![
        ClientProposalInput {
            proposal_id: Some(LocalProposalId(11)),
            payload: b"one".to_vec(),
        },
        ClientProposalInput {
            proposal_id: Some(LocalProposalId(12)),
            payload: b"two".to_vec(),
        },
    ]);

    assert_eq!(leader.commit_index(), LogIndex(3));
    assert_eq!(
        outputs,
        vec![
            Output::LocalProposalAppended {
                proposal_id: LocalProposalId(11),
                index: LogIndex(2),
                term: Term(1),
            },
            Output::LocalProposalAppended {
                proposal_id: LocalProposalId(12),
                index: LogIndex(3),
                term: Term(1),
            },
            Output::Apply {
                index: LogIndex(2),
                term: Term(1),
                payload: b"one".to_vec().into(),
                local_proposal_id: Some(LocalProposalId(11)),
            },
            Output::Apply {
                index: LogIndex(3),
                term: Term(1),
                payload: b"two".to_vec().into(),
                local_proposal_id: Some(LocalProposalId(12)),
            },
        ]
    );
}
