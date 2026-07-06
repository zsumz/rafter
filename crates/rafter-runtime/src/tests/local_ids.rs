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
                entries: Vec::new(),
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

#[test]
fn tracked_rejection_preserves_id_without_log_append() {
    let mut runtime = durable_node_with_log(
        2,
        &[1, 3],
        InMemoryRaftHardStateStore::new(),
        InMemoryRaftLogSegment::new(),
    );

    let proposal_id = LocalProposalId(9);
    let outputs = runtime
        .step(RaftInput::TrackedClientProposal {
            proposal_id,
            payload: b"not-leader".to_vec(),
        })
        .expect("rejection does not require log persistence");

    assert!(runtime.log_segment.replay_entries().is_empty());
    assert!(matches!(
        outputs.as_slice(),
        [RaftOutput::RejectProposal {
            proposal_id: Some(id),
            reason: rafter::ProposalRejection::NotLeader { .. },
        }] if *id == proposal_id
    ));
}

#[test]
fn tracked_local_append_is_suppressed_when_log_append_fails() {
    let mut runtime = durable_node_with_log(
        1,
        &[],
        InMemoryRaftHardStateStore::new(),
        FailingAfterElectionNoopLogSegment::new(),
    );
    assert!(runtime
        .step(RaftInput::Tick)
        .expect("leader election noop persists")
        .is_empty());

    let error = runtime
        .step(RaftInput::TrackedClientProposal {
            proposal_id: LocalProposalId(10),
            payload: b"must-not-escape".to_vec(),
        })
        .expect_err("tracked proposal append fails");

    assert!(matches!(error, RaftRuntimeError::LogAppend(_)));
    assert!(matches!(
        runtime
            .step(RaftInput::Tick)
            .expect_err("failed append poisons the runtime"),
        RaftRuntimeError::Poisoned { .. }
    ));
}

#[test]
fn tracked_rejection_does_not_touch_failing_log_segment() {
    let mut runtime = durable_node_with_log(
        2,
        &[1, 3],
        InMemoryRaftHardStateStore::new(),
        FailingAfterElectionNoopLogSegment {
            inner: InMemoryRaftLogSegment::new(),
            allowed: 0,
        },
    );

    let proposal_id = LocalProposalId(11);
    let outputs = runtime
        .step(RaftInput::TrackedClientProposal {
            proposal_id,
            payload: b"rejected-before-append".to_vec(),
        })
        .expect("not-leader rejection does not append");

    assert!(matches!(
        outputs.as_slice(),
        [RaftOutput::RejectProposal {
            proposal_id: Some(id),
            reason: rafter::ProposalRejection::NotLeader { .. },
        }] if *id == proposal_id
    ));
}

#[test]
fn restart_replays_committed_tracked_entry_without_local_id() {
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

    let old_id = LocalProposalId(12);
    let outputs = runtime
        .step(RaftInput::TrackedClientProposal {
            proposal_id: old_id,
            payload: b"before-restart".to_vec(),
        })
        .expect("tracked proposal persists before restart");
    assert!(outputs.iter().any(|output| matches!(
        output,
        RaftOutput::Apply {
            local_proposal_id: Some(id),
            ..
        } if *id == old_id
    )));

    let hard_state_store = runtime.hard_state_store.clone();
    let log_segment = runtime.log_segment.clone();
    let recovered = DurableRaftNode::recover_with_storage_and_snapshot_store_applied_through(
        raft_config(1, &[]),
        hard_state_store,
        log_segment,
        InMemoryRaftSnapshotStore::new(),
        LogIndex(1),
    )
    .expect("runtime recovers with unapplied committed entry");
    let (mut restarted, recovery_outputs) = recovered.into_parts();

    assert_eq!(
        recovery_outputs,
        vec![RaftOutput::Apply {
            index: LogIndex(2),
            term: Term(1),
            payload: b"before-restart".to_vec().into(),
            local_proposal_id: None,
        }],
        "volatile local proposal tracking is not recovered"
    );

    let _ = restarted
        .step(RaftInput::Tick)
        .expect("single voter can campaign again after recovery");
    let new_id = LocalProposalId(13);
    let outputs = restarted
        .step(RaftInput::TrackedClientProposal {
            proposal_id: new_id,
            payload: b"after-restart".to_vec(),
        })
        .expect("new tracked proposal persists after restart");

    assert!(outputs.iter().any(|output| matches!(
        output,
        RaftOutput::Apply {
            payload,
            local_proposal_id: Some(id),
            ..
        } if payload.as_ref() == b"after-restart" && *id == new_id
    )));
    assert!(!outputs.iter().any(|output| matches!(
        output,
        RaftOutput::Apply {
            local_proposal_id: Some(id),
            ..
        } if *id == old_id
    )));
}

#[test]
fn typed_read_id_is_preserved_by_runtime_read_index_outputs() {
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

    let read_id = ReadId(22);
    let outputs = runtime.step_read_index(read_id).expect("read index grants");

    assert_eq!(
        outputs,
        vec![RaftOutput::ReadIndexGranted {
            read_id,
            read_index: LogIndex(1),
        }]
    );
}

#[test]
fn pending_read_cancellations_are_released_after_stepdown_term_is_durable() {
    let mut runtime = durable_node_with_log(
        1,
        &[2, 3],
        InMemoryRaftHardStateStore::new(),
        InMemoryRaftLogSegment::new(),
    );
    elect_runtime_leader_with_grant(&mut runtime, RaftNodeId(2));
    runtime
        .step(RaftInput::Message {
            from: RaftNodeId(2),
            message: Message::AppendEntriesResponse(AppendEntriesResponse {
                term: runtime.current_term(),
                follower_id: RaftNodeId(2),
                success: true,
                match_index: LogIndex(1),
                sequence: 0,
            }),
        })
        .expect("leader noop commits");

    let first_read = ReadId(32);
    let second_read = ReadId(33);
    let _ = runtime
        .step_read_index(first_read)
        .expect("first read waits for quorum");
    let _ = runtime
        .step_read_index(second_read)
        .expect("second read waits for quorum");

    let higher_term = runtime.current_term().next();
    let outputs = runtime
        .step(RaftInput::Message {
            from: RaftNodeId(3),
            message: Message::AppendEntries(AppendEntries {
                term: higher_term,
                leader_id: RaftNodeId(3),
                prev_log_index: LogIndex::ZERO,
                prev_log_term: Term::default(),
                entries: Vec::new(),
                leader_commit: LogIndex::ZERO,
                sequence: 10,
            }),
        })
        .expect("higher-term append persists read cancellation");

    assert_eq!(runtime.hard_state().current_term, higher_term);
    assert_eq!(
        outputs
            .iter()
            .filter_map(|output| match output {
                RaftOutput::ReadIndexCanceled { read_id, reason } => Some((*read_id, *reason)),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![
            (first_read, rafter::ReadIndexCancelReason::LeadershipLost),
            (second_read, rafter::ReadIndexCancelReason::LeadershipLost),
        ]
    );
}

fn append_entries_sequence(outputs: &[RaftOutput]) -> u64 {
    outputs
        .iter()
        .find_map(|output| match output {
            RaftOutput::Send {
                message: Message::AppendEntries(request),
                ..
            } => Some(request.sequence),
            _ => None,
        })
        .expect("leader output includes append entries")
}
