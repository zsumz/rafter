use super::*;
use rafter_invariant_test::oracle_assert_eq;
use rafter_storage::FileRaftNodeStores;

#[test]
fn reopen_completes_compaction_after_crash_between_snapshot_and_compaction() {
    let (log_segment, snapshot_store) = half_installed_stores(
        2,
        1,
        &[
            persisted_entry(1, 1, b"covered-one"),
            persisted_entry(2, 1, b"covered-two"),
            persisted_entry(3, 1, b"live-suffix"),
        ],
    );

    let runtime = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        hard_state_store(1, None),
        log_segment,
        snapshot_store,
    )
    .expect("reopen repairs the half-installed compaction");

    // The half-installed shape is indistinguishable from a retained full log
    // and boots correctly: the snapshot boundary is intact and the live
    // suffix survives at its true index, with no acknowledged entry lost.
    oracle_assert_eq!(runtime.snapshot_index(), LogIndex(2));
    oracle_assert_eq!(runtime.last_log_index(), LogIndex(3));
    oracle_assert_eq!(
        runtime.log_entries_from(LogIndex(1)),
        vec![LogEntry::application(Term(1), b"live-suffix".to_vec())]
    );
}

#[test]
fn file_backed_reopen_persists_repaired_compaction_after_snapshot_crash_window() {
    let directory = TestDirectory::new("snapshot-crash-window");
    let (mut hard_state_store, mut log_segment, mut snapshot_store) =
        FileRaftNodeStores::open(&directory.0)
            .expect("file-backed stores open")
            .into_parts();

    hard_state_store
        .write_hard_state(RaftHardState {
            current_term: Term(1),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
        })
        .expect("hard state persists");
    log_segment
        .append_entries(&[
            persisted_entry(1, 1, b"covered-one"),
            persisted_entry(2, 1, b"covered-two"),
        ])
        .expect("log entries persist");
    snapshot_store
        .write_snapshot(raft_snapshot(3, 1, 1, b"snapshot"))
        .expect("snapshot persists before compaction");

    assert_eq!(log_segment.compacted_through(), LogIndex::ZERO);
    assert_eq!(log_segment.next_index(), LogIndex(3));
    drop((hard_state_store, log_segment, snapshot_store));

    let (hard_state_store, log_segment, snapshot_store) = FileRaftNodeStores::open(&directory.0)
        .expect("half-installed file-backed stores reopen")
        .into_parts();
    assert_eq!(log_segment.compacted_through(), LogIndex::ZERO);
    assert_eq!(log_segment.next_index(), LogIndex(3));

    let runtime = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        hard_state_store,
        log_segment,
        snapshot_store,
    )
    .expect("runtime repairs the half-installed file-backed compaction");

    oracle_assert_eq!(runtime.snapshot_index(), LogIndex(3));
    oracle_assert_eq!(runtime.last_log_index(), LogIndex(3));
    oracle_assert_eq!(runtime.log_segment.compacted_through(), LogIndex(3));
    oracle_assert_eq!(runtime.log_segment.next_index(), LogIndex(4));
    oracle_assert_eq!(runtime.log_segment.replay_entries(), Vec::new());
    drop(runtime);

    let (_, reopened_log, _) = FileRaftNodeStores::open(&directory.0)
        .expect("repaired file-backed stores reopen")
        .into_parts();
    oracle_assert_eq!(reopened_log.compacted_through(), LogIndex(3));
    oracle_assert_eq!(reopened_log.next_index(), LogIndex(4));
    oracle_assert_eq!(reopened_log.replay_entries(), Vec::new());
}

#[test]
fn reopened_node_matches_one_that_never_crashed() {
    let log = [
        persisted_entry(1, 1, b"covered-one"),
        persisted_entry(2, 1, b"covered-two"),
        persisted_entry(3, 1, b"live-suffix"),
    ];

    // A: crashed in the window, then reopened and repaired.
    let (log_segment, snapshot_store) = half_installed_stores(2, 1, &log);
    let repaired = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        hard_state_store(1, None),
        log_segment,
        snapshot_store,
    )
    .expect("repaired reopen");

    // B: the same snapshot with an already-compacted log (no crash).
    let mut clean_log = InMemoryRaftLogSegment::new();
    clean_log.append_entries(&log).expect("clean log persists");
    clean_log
        .compact_prefix_through(LogIndex(2))
        .expect("clean compaction");
    let (_, clean_snapshot) = half_installed_stores(2, 1, &[]);
    let clean = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        hard_state_store(1, None),
        clean_log,
        clean_snapshot,
    )
    .expect("clean reopen");

    // The reopened node's logical state matches the never-crashed node's,
    // even though the crashed node's storage still physically retains the
    // covered prefix.
    assert_eq!(repaired.last_log_index(), clean.last_log_index());
    assert_eq!(repaired.snapshot_index(), clean.snapshot_index());
    assert_eq!(
        repaired.log_entries_from(LogIndex(1)),
        clean.log_entries_from(LogIndex(1))
    );
}

#[test]
fn append_after_repair_lands_at_the_correct_index() {
    let (log_segment, snapshot_store) = half_installed_stores(
        2,
        1,
        &[
            persisted_entry(1, 1, b"covered-one"),
            persisted_entry(2, 1, b"covered-two"),
        ],
    );
    let mut runtime = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        hard_state_store(1, None),
        log_segment,
        snapshot_store,
    )
    .expect("reopen repairs");

    // A leader append covering index 3 must land at 3, not be mislabelled
    // relative to the snapshot boundary.
    runtime
        .step(RaftInput::Message {
            from: RaftNodeId(1),
            message: Message::AppendEntries(AppendEntries {
                sequence: 0,
                term: Term(1),
                leader_id: RaftNodeId(1),
                prev_log_index: LogIndex(2),
                prev_log_term: Term(1),
                entries: vec![LogEntry::application(Term(1), b"post-repair".to_vec())].into(),
                leader_commit: LogIndex(2),
            }),
        })
        .expect("append after repair persists");

    // The append landed at index 3, not mislabelled relative to the snapshot
    // boundary; the covered prefix physically remains but is logically
    // superseded by the snapshot.
    assert_eq!(runtime.last_log_index(), LogIndex(3));
    assert_eq!(
        runtime.log_entries_from(LogIndex(1)),
        vec![LogEntry::application(Term(1), b"post-repair".to_vec())]
    );
}
