use super::*;

#[test]
fn restarted_node_recovers_persisted_log_entries() {
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
    let _ = runtime
        .step(RaftInput::ClientProposal {
            payload: b"create".to_vec(),
        })
        .expect("log entry persists");
    let hard_state_store = runtime.hard_state_store.clone();
    let log_segment = runtime.log_segment.clone();

    let restarted = durable_node_with_log(1, &[], hard_state_store, log_segment);

    assert_eq!(restarted.last_log_index(), LogIndex(2));
    assert_eq!(
        restarted.log_entries_from(LogIndex(1)),
        vec![
            LogEntry::noop(Term(1)),
            LogEntry::application(Term(1), b"create".to_vec())
        ]
    );
}

#[test]
fn restarted_runtime_drains_committed_entries_above_applied_floor_immediately() {
    let mut hard_state_store = InMemoryRaftHardStateStore::new();
    hard_state_store
        .write_hard_state(RaftHardState {
            current_term: Term(1),
            voted_for: None,
            commit_index: LogIndex(3),
            committed_configuration: None,
        })
        .expect("hard state writes");
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .append_entries(&[
            PersistedRaftLogEntry::application(LogIndex(1), Term(1), b"one".to_vec()),
            PersistedRaftLogEntry::application(LogIndex(2), Term(1), b"two".to_vec()),
            PersistedRaftLogEntry::application(LogIndex(3), Term(1), b"three".to_vec()),
        ])
        .expect("committed log writes");

    let mut runtime = DurableRaftNode::with_storage_and_snapshot_store_applied_through(
        raft_config(1, &[2, 3]),
        hard_state_store,
        log_segment,
        InMemoryRaftSnapshotStore::new(),
        LogIndex(1),
    )
    .expect("runtime restarts with an applied floor");

    assert_eq!(
        runtime.drain_committed_outputs(),
        vec![
            RaftOutput::Apply {
                index: LogIndex(2),
                term: Term(1),
                payload: b"two".to_vec().into(),
                local_proposal_id: None,
            },
            RaftOutput::Apply {
                index: LogIndex(3),
                term: Term(1),
                payload: b"three".to_vec().into(),
                local_proposal_id: None,
            },
        ],
        "recovery drain emits committed-but-unapplied entries without another Raft message"
    );
    assert!(runtime.drain_committed_outputs().is_empty());
}

#[test]
fn recovery_constructor_returns_committed_entries_above_applied_floor() {
    let mut hard_state_store = InMemoryRaftHardStateStore::new();
    hard_state_store
        .write_hard_state(RaftHardState {
            current_term: Term(1),
            voted_for: None,
            commit_index: LogIndex(3),
            committed_configuration: None,
        })
        .expect("hard state writes");
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .append_entries(&[
            PersistedRaftLogEntry::application(LogIndex(1), Term(1), b"one".to_vec()),
            PersistedRaftLogEntry::application(LogIndex(2), Term(1), b"two".to_vec()),
            PersistedRaftLogEntry::application(LogIndex(3), Term(1), b"three".to_vec()),
        ])
        .expect("committed log writes");

    let recovered = DurableRaftNode::recover_with_storage_and_snapshot_store_applied_through(
        raft_config(1, &[2, 3]),
        hard_state_store,
        log_segment,
        InMemoryRaftSnapshotStore::new(),
        LogIndex(1),
    )
    .expect("runtime recovers with explicit outputs");
    let (mut runtime, recovery_outputs) = recovered.into_parts();

    assert_eq!(
        recovery_outputs,
        vec![
            RaftOutput::Apply {
                index: LogIndex(2),
                term: Term(1),
                payload: b"two".to_vec().into(),
                local_proposal_id: None,
            },
            RaftOutput::Apply {
                index: LogIndex(3),
                term: Term(1),
                payload: b"three".to_vec().into(),
                local_proposal_id: None,
            },
        ],
        "recovery constructor returns committed-but-unapplied entries without another Raft message"
    );
    assert!(runtime.drain_committed_outputs().is_empty());
}

#[test]
fn restart_after_leadership_noop_commit_replays_unapplied_prior_entry() {
    let mut hard_state_store = InMemoryRaftHardStateStore::new();
    hard_state_store
        .write_hard_state(RaftHardState {
            current_term: Term(1),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
        })
        .expect("hard state writes");
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .append_entries(&[PersistedRaftLogEntry::application(
            LogIndex(1),
            Term(1),
            b"old-entry".to_vec(),
        )])
        .expect("prior uncommitted entry persists");

    let mut runtime =
        DurableRaftNode::with_storage(raft_config(1, &[]), hard_state_store, log_segment)
            .expect("runtime starts from prior entry");
    let outputs = runtime.step(RaftInput::Tick).expect("leader noop commits");
    assert_eq!(
        outputs,
        vec![RaftOutput::Apply {
            index: LogIndex(1),
            term: Term(1),
            payload: b"old-entry".to_vec().into(),
            local_proposal_id: None,
        }]
    );
    assert_eq!(runtime.commit_index(), LogIndex(2));

    let hard_state_store = runtime.hard_state_store.clone();
    let log_segment = runtime.log_segment.clone();
    let recovered = DurableRaftNode::recover_with_storage_and_snapshot_store_applied_through(
        raft_config(1, &[]),
        hard_state_store,
        log_segment,
        InMemoryRaftSnapshotStore::new(),
        LogIndex::ZERO,
    )
    .expect("runtime recovers after crash before app apply");
    let (_runtime, recovery_outputs) = recovered.into_parts();

    assert_eq!(
        recovery_outputs,
        vec![RaftOutput::Apply {
            index: LogIndex(1),
            term: Term(1),
            payload: b"old-entry".to_vec().into(),
            local_proposal_id: None,
        }],
        "the application entry is replayed after a crash before state-machine apply"
    );
}
