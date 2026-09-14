//! Local compaction refused, and local compaction that fails mid-write.
//!
//! A boundary above the durable commit index must be refused with the log left
//! exactly as it was; a segment whose compaction fails must poison rather than
//! leave the runtime describing a prefix the medium still holds.

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

#[test]
fn failed_prepared_snapshot_transition_remains_inert_until_restart() {
    let current = raft_snapshot(2, 2, 3, b"current snapshot");
    let advanced = raft_snapshot(3, 3, 3, b"advanced snapshot");
    let log_segment = FailingCompactRaftLogSegment {
        entries: vec![
            persisted_entry(1, 1, b"compacted-one"),
            persisted_entry(2, 2, b"compacted-two"),
            persisted_entry(3, 3, b"new-boundary"),
        ],
    };
    let mut hard_state = hard_state_store(3, None);
    hard_state
        .write_hard_state(RaftHardState {
            current_term: Term(3),
            voted_for: None,
            commit_index: LogIndex(3),
            committed_configuration: None,
        })
        .expect("committed hard state persists");
    let mut runtime = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        hard_state,
        log_segment,
        InMemoryRaftSnapshotStore::with_snapshot(current.clone()),
    )
    .expect("runtime hydrates from the current snapshot");
    let _ = runtime.drain_committed_outputs();

    let error = runtime
        .compact_log_with_snapshot(advanced.clone())
        .expect_err("log compaction fails after snapshot publication");
    assert!(
        matches!(
            error,
            RaftRuntimeError::LogCompact(RaftLogSegmentCompactError::Io {
                operation: "compact test raft log entries",
                ..
            })
        ),
        "unexpected compaction result: {error:?}"
    );
    assert_eq!(
        runtime.snapshot_index(),
        current.metadata.last_included_index,
        "a failed durable write must not commit the prepared kernel transition"
    );
    let durable_snapshot = runtime
        .snapshot_store
        .current()
        .expect("advanced snapshot was durably published before compaction failed");
    assert_eq!(
        durable_snapshot.metadata.last_included_index,
        advanced.metadata.last_included_index
    );
    assert_eq!(
        durable_snapshot.application_payload,
        advanced.application_payload
    );
    assert_eq!(
        runtime.log_segment.replay_entries(),
        vec![
            persisted_entry(1, 1, b"compacted-one"),
            persisted_entry(2, 2, b"compacted-two"),
            persisted_entry(3, 3, b"new-boundary"),
        ]
    );
    assert_poisoned_after_failure(&mut runtime, |cause| {
        matches!(cause, RaftRuntimeFatalError::LogCompact(_))
    });

    let restarted = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        runtime.hard_state_store.clone(),
        runtime.log_segment.clone(),
        runtime.snapshot_store.clone(),
    )
    .expect("restart adopts the durable snapshot that preceded the failed compaction");
    assert_eq!(restarted.snapshot_index(), LogIndex(3));
    assert_eq!(restarted.commit_index(), LogIndex(3));
    assert_eq!(restarted.last_log_index(), LogIndex(3));
}
