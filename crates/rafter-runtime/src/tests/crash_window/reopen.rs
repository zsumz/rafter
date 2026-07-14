use super::*;
use rafter_invariant_test::oracle_assert;

#[test]
fn reopen_without_crash_is_unchanged() {
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .append_entries(&[persisted_entry(1, 1, b"only-entry")])
        .expect("log persists");

    let runtime = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        hard_state_store(1, None),
        log_segment,
        InMemoryRaftSnapshotStore::new(),
    )
    .expect("normal reopen");

    assert_eq!(runtime.snapshot_index(), LogIndex::ZERO);
    assert_eq!(runtime.last_log_index(), LogIndex(1));
    assert_eq!(
        runtime.log_segment.replay_entries(),
        vec![persisted_entry(1, 1, b"only-entry")]
    );
}

#[test]
fn reopen_rejects_log_compacted_past_the_snapshot() {
    // A log compacted beyond what any snapshot covers is unrepairable
    // acknowledged-data loss and must fail loudly rather than silently boot.
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .append_entries(&[
            persisted_entry(1, 1, b"one"),
            persisted_entry(2, 1, b"two"),
            persisted_entry(3, 1, b"three"),
        ])
        .expect("log persists");
    log_segment
        .compact_prefix_through(LogIndex(3))
        .expect("over-compaction");

    let snapshot_store =
        InMemoryRaftSnapshotStore::with_snapshot(raft_snapshot(1, 1, 1, b"stale snapshot"));

    let error = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        hard_state_store(1, None),
        log_segment,
        snapshot_store,
    )
    .expect_err("over-compacted log must be rejected");

    oracle_assert!(matches!(
        error,
        RaftRuntimeError::CompactionAheadOfSnapshot {
            compacted_through: LogIndex(3),
            snapshot_index: LogIndex(1),
        }
    ));
}
