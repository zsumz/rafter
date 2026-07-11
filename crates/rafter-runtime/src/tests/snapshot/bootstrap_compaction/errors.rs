use super::*;

#[test]
fn runtime_rejects_log_compaction_ahead_of_local_commit() {
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
        Some(snapshot),
    )
    .expect("runtime hydrates from durable snapshot");

    assert_eq!(
        runtime.compact_log_through_snapshot(&snapshot_metadata(4, 4, 4)),
        Err(RaftRuntimeError::SnapshotAheadOfCommit {
            snapshot_index: LogIndex(4),
            commit_index: LogIndex(2),
        })
    );
    assert_eq!(
        runtime.log_segment.replay_entries(),
        vec![
            persisted_entry(1, 1, b"compacted-one"),
            persisted_entry(2, 2, b"compacted-two"),
            persisted_entry(3, 3, b"retained-suffix"),
        ]
    );
}

#[test]
fn runtime_compaction_failure_poisons_runtime_until_restart() {
    let snapshot = snapshot_metadata(2, 2, 3);
    let log_segment = FailingCompactRaftLogSegment {
        entries: vec![
            persisted_entry(1, 1, b"compacted-one"),
            persisted_entry(2, 2, b"compacted-two"),
            persisted_entry(3, 3, b"retained-suffix"),
        ],
    };
    let mut runtime = DurableRaftNode::with_storage_and_snapshot(
        raft_config(2, &[1, 3]),
        hard_state_store(3, None),
        log_segment,
        Some(snapshot.clone()),
    )
    .expect("runtime hydrates from durable snapshot");

    assert!(matches!(
        runtime.compact_log_through_snapshot(&snapshot),
        Err(RaftRuntimeError::LogCompact(
            RaftLogSegmentCompactError::Io {
                operation: "compact test raft log entries",
                ..
            }
        ))
    ));
    assert_poisoned_after_failure(&mut runtime, |cause| {
        matches!(cause, RaftRuntimeFatalError::LogCompact(_))
    });
}
