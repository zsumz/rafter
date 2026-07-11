use super::*;

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
