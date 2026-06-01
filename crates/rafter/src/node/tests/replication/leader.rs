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
fn overwritten_tracked_proposal_does_not_apply_stale_local_id() {
    let mut node = node(1, &[2, 3]);
    let _ = elect_leader(&mut node);
    acknowledge_append(&mut node, NodeId(2), LogIndex(1));
    acknowledge_append(&mut node, NodeId(3), LogIndex(1));

    let _ = node.step(Input::TrackedClientProposal {
        proposal_id: LocalProposalId(7),
        payload: b"local-uncommitted".to_vec(),
    });
    assert!(node.volatile.local_proposals.contains_key(&LogIndex(2)));

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
            )],
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

fn erase_local_annotations(outputs: &[Output]) -> Vec<Output> {
    outputs
        .iter()
        .filter_map(|output| match output {
            Output::LocalProposalAppended { .. } | Output::LocalProposalDropped { .. } => None,
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
            output => Some(output.clone()),
        })
        .collect()
}

#[test]
fn leader_replication_progress_projects_follower_match_and_next_indexes() {
    let mut leader = node(1, &[2, 3]);
    let _ = elect_leader(&mut leader);
    let follower = node(2, &[1, 3]);

    assert!(follower.leader_replication_progress().is_empty());

    let _ = leader.step(Input::ClientProposal {
        payload: b"alert-opened".to_vec(),
    });
    let _ = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence: 0,
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: true,
            match_index: LogIndex(2),
        }),
    });

    let progress = leader.leader_replication_progress();
    assert_eq!(progress.len(), 2);
    assert_eq!(progress[0].follower_id, NodeId(2));
    assert_eq!(progress[0].match_index, LogIndex(2));
    assert_eq!(progress[0].next_index, LogIndex(3));
    assert_eq!(progress[1].follower_id, NodeId(3));
    assert_eq!(progress[1].match_index, LogIndex::ZERO);
    assert_eq!(progress[1].next_index, LogIndex(1));
}

#[test]
fn leader_batches_lagging_follower_suffix_by_replication_byte_budget() {
    let mut leader = node_with_max_append_entries_bytes(1, &[2, 3], 180);
    let _ = elect_leader(&mut leader);

    // While the leadership probe is unanswered, proposals reach follower 2
    // as empty heartbeats only: the suffix accumulates on the leader.
    for byte in [b'a', b'b', b'c'] {
        let outputs = leader.step(Input::ClientProposal {
            payload: vec![byte; 100],
        });
        let request = append_entries_to(&outputs, NodeId(2));
        assert!(
            request.entries.is_empty(),
            "an unanswered probe defers the suffix to the confirming acknowledgement"
        );
    }

    assert_eq!(leader.last_log_index(), LogIndex(4));

    // The probe acknowledgement confirms the leadership no-op and fills the
    // window with the whole application suffix at once: one budget-bounded
    // batch per message, since a 180-byte budget fits one 100-byte payload but
    // not two.
    let catch_up_outputs = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence: 0,
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: true,
            match_index: LogIndex(1),
        }),
    });

    let batches = append_entries_batches_to(&catch_up_outputs, NodeId(2));
    assert_eq!(
        batches.len(),
        3,
        "the byte budget splits the three-entry application suffix into three batches"
    );
    for (offset, (batch, byte)) in batches.iter().zip([b'a', b'b', b'c']).enumerate() {
        assert_eq!(batch.prev_log_index, LogIndex(offset as u64 + 1));
        assert_eq!(batch.entries.len(), 1);
        assert_eq!(
            batch.entries[0].application_payload(),
            Some(&vec![byte; 100][..])
        );
        assert!(replication_bytes(batch) <= leader.config.max_append_entries_bytes());
    }
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
fn heartbeat_with_divergent_follower_tail_reports_matched_prefix_only() {
    let mut leader = node(1, &[2, 3, 4, 5]);
    elect_five_node_leader(&mut leader);

    let _ = leader.step(Input::ClientProposal {
        payload: b"prefix".to_vec(),
    });
    acknowledge_append(&mut leader, NodeId(2), LogIndex(2));
    acknowledge_append(&mut leader, NodeId(3), LogIndex(2));
    assert_eq!(leader.commit_index(), LogIndex(2));

    let _ = leader.step(Input::ClientProposal {
        payload: b"leader-only".to_vec(),
    });
    assert_eq!(leader.last_log_index(), LogIndex(3));
    assert_eq!(leader.commit_index(), LogIndex(2));
    acknowledge_append(&mut leader, NodeId(3), LogIndex(3));
    assert_eq!(leader.commit_index(), LogIndex(2));

    let mut follower = node(2, &[1, 3]);
    follower
        .persistent
        .log
        .push(LogEntry::noop(leader.current_term()));
    push_log_entry(&mut follower, leader.current_term(), b"prefix");
    push_log_entry(&mut follower, Term(99), b"divergent-tail");

    let heartbeat_outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: leader.current_term(),
            leader_id: NodeId(1),
            prev_log_index: LogIndex(2),
            prev_log_term: leader.current_term(),
            entries: Vec::new(),
            leader_commit: LogIndex(2),
        }),
    });

    let response = append_entries_response(&heartbeat_outputs);
    assert!(response.success);
    assert_eq!(
        response.match_index,
        LogIndex(2),
        "empty heartbeat confirms only prev_log_index, not the follower tail"
    );

    let commit_outputs = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(response),
    });

    assert_eq!(
        leader.commit_index(),
        LogIndex(2),
        "leader must not commit an entry the follower did not acknowledge from this leader"
    );
    assert!(commit_outputs.is_empty());
}

#[test]
fn leader_rejects_append_response_when_sender_disagrees_with_follower_id() {
    let mut leader = node(1, &[2, 3]);
    let _ = elect_leader(&mut leader);

    let _ = leader.step(Input::ClientProposal {
        payload: b"leader-only".to_vec(),
    });

    let outputs = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence: 0,
            term: leader.current_term(),
            follower_id: NodeId(3),
            success: true,
            match_index: LogIndex(1),
        }),
    });

    assert!(outputs.is_empty());
    assert_eq!(leader.commit_index(), LogIndex::ZERO);
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

fn node_with_max_append_entries_bytes(id: u64, peers: &[u64], max_bytes: usize) -> Node {
    Node::new(
        NodeConfig::new(NodeId(id), peers.iter().copied().map(NodeId).collect(), 3)
            .expect("test Raft node config is valid")
            .with_max_append_entries_bytes(max_bytes),
    )
}

fn append_entries_to(outputs: &[Output], to: NodeId) -> &AppendEntries {
    append_entries_batches_to(outputs, to)
        .first()
        .copied()
        .expect("expected append entries for peer")
}

fn append_entries_batches_to(outputs: &[Output], to: NodeId) -> Vec<&AppendEntries> {
    outputs
        .iter()
        .filter_map(|output| {
            let Output::Send {
                to: actual_to,
                message,
            } = output
            else {
                return None;
            };
            if *actual_to != to {
                return None;
            }
            let Message::AppendEntries(request) = message else {
                return None;
            };
            Some(request)
        })
        .collect()
}

fn replication_bytes(request: &AppendEntries) -> usize {
    request
        .entries
        .iter()
        .map(LogEntry::replication_bytes)
        .sum()
}

#[test]
fn oversized_entry_still_replicates_as_single_entry_batch() {
    let mut leader = node(1, &[2, 3]);
    let _ = elect_leader(&mut leader);
    // An entry far beyond any batch budget enters the log (as it can via
    // splice from a leader with a larger budget, or via hydration).
    leader
        .persistent
        .log
        .push(LogEntry::application(Term(1), vec![0xab; 700 * 1024]));

    let batch = leader.log_entries_from_bounded(LogIndex(2), 512);
    assert_eq!(
        batch.len(),
        1,
        "an oversized entry must ship alone, never stall the batch"
    );

    let followup = leader.log_entries_from_bounded(LogIndex(3), 512);
    assert!(followup.is_empty(), "no second entry exists");
}

#[test]
fn budget_bounds_batches_beyond_the_first_entry() {
    let mut leader = node(1, &[2, 3]);
    let _ = elect_leader(&mut leader);
    for payload in [b"one".as_slice(), b"two", b"three"] {
        leader
            .persistent
            .log
            .push(LogEntry::application(Term(1), payload.to_vec()));
    }

    let first = leader.log_entries_from_bounded(LogIndex(2), 1);
    assert_eq!(first.len(), 1, "budget smaller than any entry ships one");

    let all = leader.log_entries_from_bounded(LogIndex(2), 512 * 1024);
    assert_eq!(all.len(), 3, "a generous budget ships every entry");
}
