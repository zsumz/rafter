use super::*;

#[test]
fn tracked_single_node_proposal_returns_append_and_apply_ids_after_persist() {
    let mut runtime = durable_node_with_log(
        1,
        &[],
        InMemoryRaftHardStateStore::new(),
        InMemoryRaftLogSegment::new(),
    );
    assert!(runtime
        .step(RaftInput::Tick)
        .expect("leader elected")
        .is_empty());

    let proposal_id = LocalProposalId(7);
    let outputs = runtime
        .step_tracked_proposal(proposal_id, b"create".to_vec())
        .expect("tracked proposal persists");

    assert_eq!(
        runtime.log_segment.replay_entries(),
        vec![
            PersistedRaftLogEntry::noop(LogIndex(1), Term(1)),
            PersistedRaftLogEntry::application(LogIndex(2), Term(1), b"create".to_vec())
        ],
        "the runtime releases local-ID outputs only after the log suffix is durable"
    );
    assert_eq!(
        outputs,
        vec![
            RaftOutput::LocalProposalAppended {
                proposal_id,
                index: LogIndex(2),
                term: Term(1),
            },
            RaftOutput::Apply {
                index: LogIndex(2),
                term: Term(1),
                payload: b"create".to_vec().into(),
                local_proposal_id: Some(proposal_id),
            },
        ]
    );
}

#[test]
fn local_proposal_drop_is_released_after_stepdown_term_is_durable() {
    let mut runtime = durable_node_with_log(
        1,
        &[2, 3],
        InMemoryRaftHardStateStore::new(),
        InMemoryRaftLogSegment::new(),
    );
    elect_runtime_leader_with_grant(&mut runtime, RaftNodeId(2));

    let proposal_term = runtime.current_term();
    let proposal_id = LocalProposalId(31);
    let outputs = runtime
        .step_tracked_proposal(proposal_id, b"maybe".to_vec())
        .expect("tracked proposal persists before stepdown");
    assert!(outputs.iter().any(|output| matches!(
        output,
        RaftOutput::LocalProposalAppended {
            proposal_id: id,
            index: LogIndex(2),
            term,
        } if *id == proposal_id && *term == proposal_term
    )));

    let higher_term = proposal_term.next();
    let outputs = runtime
        .step(RaftInput::Message {
            from: RaftNodeId(3),
            message: Message::AppendEntries(AppendEntries {
                term: higher_term,
                leader_id: RaftNodeId(3),
                prev_log_index: LogIndex::ZERO,
                prev_log_term: Term::default(),
                entries: Vec::new().into(),
                leader_commit: LogIndex::ZERO,
                sequence: 9,
            }),
        })
        .expect("higher-term append persists stepdown");

    assert_eq!(runtime.hard_state().current_term, higher_term);
    assert!(outputs.iter().any(|output| matches!(
        output,
        RaftOutput::LocalProposalDropped {
            proposal_id: id,
            index: LogIndex(2),
            term,
            reason: rafter::LocalProposalDropReason::LeadershipLost,
        } if *id == proposal_id && *term == proposal_term
    )));
}

#[test]
fn tracked_multi_node_proposal_applies_with_local_id_after_quorum_ack() {
    let mut runtime = durable_node_with_log(
        1,
        &[2, 3],
        InMemoryRaftHardStateStore::new(),
        InMemoryRaftLogSegment::new(),
    );
    elect_runtime_leader_with_grant(&mut runtime, RaftNodeId(2));

    let proposal_id = LocalProposalId(8);
    let outputs = runtime
        .step(RaftInput::TrackedClientProposal {
            proposal_id,
            payload: b"replicated".to_vec(),
        })
        .expect("tracked proposal persists");

    assert!(outputs.iter().any(|output| matches!(
        output,
        RaftOutput::LocalProposalAppended {
            proposal_id: id,
            index: LogIndex(2),
            term,
        } if *id == proposal_id && *term == runtime.current_term()
    )));
    assert!(outputs
        .iter()
        .all(|output| !matches!(output, RaftOutput::Apply { .. })));

    let sequence = append_entries_sequence(&outputs);
    let outputs = runtime
        .step(RaftInput::Message {
            from: RaftNodeId(2),
            message: Message::AppendEntriesResponse(AppendEntriesResponse {
                term: runtime.current_term(),
                follower_id: RaftNodeId(2),
                success: true,
                match_index: LogIndex(2),
                sequence,
            }),
        })
        .expect("quorum acknowledgement commits tracked proposal");

    assert!(outputs.iter().any(|output| matches!(
        output,
        RaftOutput::Apply {
            index: LogIndex(2),
            payload,
            local_proposal_id: Some(id),
            ..
        } if payload.as_ref() == b"replicated" && *id == proposal_id
    )));
}
