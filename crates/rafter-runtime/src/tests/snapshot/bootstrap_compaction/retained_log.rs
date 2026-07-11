use super::*;

#[test]
fn runtime_hydrates_snapshot_with_retained_full_log_without_compacting_storage() {
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .append_entries(&[
            persisted_entry(1, 1, b"compacted-one"),
            persisted_entry(2, 2, b"compacted-two"),
            persisted_entry(3, 3, b"retained-suffix"),
        ])
        .expect("retained full log persists");
    let mut runtime = DurableRaftNode::with_storage_and_snapshot(
        raft_config(2, &[1, 3]),
        hard_state_store(3, None),
        log_segment,
        Some(snapshot_metadata(2, 2, 3)),
    )
    .expect("runtime hydrates from snapshot and retained full log");

    assert_eq!(runtime.commit_index(), LogIndex(2));
    assert_eq!(runtime.last_log_index(), LogIndex(3));
    assert_eq!(
        runtime.log_entries_from(LogIndex(1)),
        vec![LogEntry::application(Term(3), b"retained-suffix".to_vec())]
    );

    let outputs = runtime
        .step(RaftInput::Message {
            from: RaftNodeId(1),
            message: Message::AppendEntries(AppendEntries {
                sequence: 0,
                term: Term(3),
                leader_id: RaftNodeId(1),
                prev_log_index: LogIndex(3),
                prev_log_term: Term(3),
                entries: vec![LogEntry::application(Term(3), b"new-suffix".to_vec())].into(),
                leader_commit: LogIndex(2),
            }),
        })
        .expect("retained compacted prefix is ignored during persistence repair");

    assert!(matches!(
        outputs.as_slice(),
        [RaftOutput::Send {
            message: Message::AppendEntriesResponse(response),
            ..
        }] if response.success && response.match_index == LogIndex(4)
    ));
    assert_eq!(
        runtime.log_segment.replay_entries(),
        vec![
            persisted_entry(1, 1, b"compacted-one"),
            persisted_entry(2, 2, b"compacted-two"),
            persisted_entry(3, 3, b"retained-suffix"),
            persisted_entry(4, 3, b"new-suffix"),
        ]
    );
}

#[test]
fn runtime_rejects_retained_boundary_entry_that_disagrees_with_snapshot() {
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .append_entries(&[
            persisted_entry(1, 1, b"compacted-one"),
            persisted_entry(2, 3, b"wrong-boundary-term"),
        ])
        .expect("retained full log persists");

    assert!(matches!(
        DurableRaftNode::with_storage_and_snapshot(
            raft_config(2, &[1, 3]),
            hard_state_store(3, None),
            log_segment,
            Some(snapshot_metadata(2, 2, 3)),
        ),
        Err(RaftRuntimeError::Bootstrap(
            BootstrapValidationError::SnapshotBoundaryTermMismatch {
                index: LogIndex(2),
                snapshot_term: Term(2),
                entry_term: Term(3),
            }
        ))
    ));
}

#[test]
fn runtime_compacts_log_through_committed_snapshot_boundary() {
    let snapshot = snapshot_metadata(2, 2, 3);
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .append_entries(&[
            persisted_entry(1, 1, b"compacted-one"),
            persisted_entry(2, 2, b"compacted-two"),
            persisted_entry(3, 3, b"retained-suffix"),
        ])
        .expect("full log persists before compaction");
    let mut runtime = DurableRaftNode::with_storage_and_snapshot(
        raft_config(2, &[1, 3]),
        hard_state_store(3, None),
        log_segment,
        Some(snapshot.clone()),
    )
    .expect("runtime hydrates from durable snapshot");

    runtime
        .compact_log_through_snapshot(&snapshot)
        .expect("committed durable snapshot can compact local log");

    assert_eq!(runtime.commit_index(), LogIndex(2));
    assert_eq!(runtime.last_log_index(), LogIndex(3));
    assert_eq!(
        runtime.log_segment.replay_entries(),
        vec![persisted_entry(3, 3, b"retained-suffix")]
    );
    runtime
        .log_segment
        .append_entries(&[persisted_entry(4, 3, b"post-compaction")])
        .expect("post-compaction append uses the retained suffix tail");

    let restarted = DurableRaftNode::with_storage_and_snapshot(
        raft_config(2, &[1, 3]),
        hard_state_store(3, None),
        runtime.log_segment.clone(),
        Some(snapshot),
    )
    .expect("runtime restarts from snapshot plus compacted suffix");

    assert_eq!(restarted.commit_index(), LogIndex(2));
    assert_eq!(restarted.last_log_index(), LogIndex(4));
    assert_eq!(
        restarted.log_entries_from(LogIndex(1)),
        vec![
            LogEntry::application(Term(3), b"retained-suffix".to_vec()),
            LogEntry::application(Term(3), b"post-compaction".to_vec()),
        ]
    );
}
