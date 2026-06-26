//! Reopen behaviour across the snapshot-persist / log-compaction crash
//! window. The durable write order is snapshot-first, then log compaction; a
//! crash between the two must be repaired at open so the reopened node is
//! indistinguishable from one that never crashed.

use super::snapshot::{persisted_entry, raft_snapshot};
use super::*;

/// Builds the half-installed on-disk shape a crash in the window leaves
/// behind: a durable snapshot through `snapshot_index`, but a log whose
/// compacted prefix is still behind it and still holds the covered entries.
fn half_installed_stores(
    snapshot_index: u64,
    snapshot_term: u64,
    log: &[PersistedRaftLogEntry],
) -> (InMemoryRaftLogSegment, InMemoryRaftSnapshotStore) {
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .append_entries(log)
        .expect("log entries persist");

    let snapshot = raft_snapshot(snapshot_index, snapshot_term, snapshot_term, b"snapshot");
    let snapshot_store = InMemoryRaftSnapshotStore::with_snapshot(snapshot);
    (log_segment, snapshot_store)
}

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
    assert_eq!(runtime.snapshot_index(), LogIndex(2));
    assert_eq!(runtime.last_log_index(), LogIndex(3));
    assert_eq!(
        runtime.log_entries_from(LogIndex(1)),
        vec![LogEntry::application(Term(1), b"live-suffix".to_vec())]
    );
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
                entries: vec![LogEntry::application(Term(1), b"post-repair".to_vec())],
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

#[test]
fn reopen_completes_compaction_when_snapshot_boundary_is_past_the_log_tail() {
    // The crash shape that is NOT equivalent to a retained full log: the
    // installed snapshot's boundary (3) lies beyond the segment's tail (2),
    // so the segment's next appendable index (3) disagrees with the
    // kernel's first appendable index (4). Reopen must complete the
    // interrupted compaction so later appends land at kernel indexes.
    let (log_segment, snapshot_store) = half_installed_stores(
        3,
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
    .expect("reopen completes the interrupted compaction");

    assert_eq!(runtime.log_segment.compacted_through(), LogIndex(3));
    assert_eq!(runtime.log_segment.next_index(), LogIndex(4));
    assert_eq!(runtime.log_segment.replay_entries(), Vec::new());
    assert_eq!(runtime.snapshot_index(), LogIndex(3));
    assert_eq!(runtime.last_log_index(), LogIndex(3));

    // The next replicated entry is kernel index 4 and must persist AT
    // segment index 4; the success acknowledgement may only escape with
    // the entry durable at its true index.
    let outputs = runtime
        .step(RaftInput::Message {
            from: RaftNodeId(1),
            message: Message::AppendEntries(AppendEntries {
                sequence: 0,
                term: Term(1),
                leader_id: RaftNodeId(1),
                prev_log_index: LogIndex(3),
                prev_log_term: Term(1),
                entries: vec![LogEntry::application(Term(1), b"acked".to_vec())],
                leader_commit: LogIndex(3),
            }),
        })
        .expect("append after repair persists");
    assert!(matches!(
        outputs.as_slice(),
        [RaftOutput::Send {
            message: Message::AppendEntriesResponse(response),
            ..
        }] if response.success && response.match_index == LogIndex(4)
    ));
    assert_eq!(
        runtime.log_segment.replay_entries(),
        vec![persisted_entry(4, 1, b"acked")]
    );

    // The acknowledged entry survives a second reopen instead of being
    // filtered out as mislabelled below the boundary.
    let reopened = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        runtime.hard_state_store.clone(),
        runtime.log_segment.clone(),
        runtime.snapshot_store.clone(),
    )
    .expect("second reopen");
    assert_eq!(reopened.last_log_index(), LogIndex(4));
    assert_eq!(
        reopened.log_entries_from(LogIndex(4)),
        vec![LogEntry::application(Term(1), b"acked".to_vec())]
    );
}

#[test]
fn append_behind_the_snapshot_boundary_is_refused_not_mislabelled() {
    // Defense in depth behind the open-time repair: a runtime whose segment
    // somehow reaches a persisting step with its next appendable index at
    // or below the snapshot boundary must refuse the append as fatal —
    // never stamp kernel entries with wrong segment indexes. The broken
    // shape is forged directly because the constructor repairs it.
    let (log_segment, snapshot_store) = half_installed_stores(3, 1, &[]);
    let mut runtime = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        hard_state_store(1, None),
        log_segment,
        snapshot_store,
    )
    .expect("clean reopen");
    let mut stale_segment = InMemoryRaftLogSegment::new();
    stale_segment
        .append_entries(&[
            persisted_entry(1, 1, b"covered-one"),
            persisted_entry(2, 1, b"covered-two"),
        ])
        .expect("stale segment persists");
    runtime.log_segment = stale_segment;

    let error = runtime
        .step(RaftInput::Message {
            from: RaftNodeId(1),
            message: Message::AppendEntries(AppendEntries {
                sequence: 0,
                term: Term(1),
                leader_id: RaftNodeId(1),
                prev_log_index: LogIndex(3),
                prev_log_term: Term(1),
                entries: vec![LogEntry::application(Term(1), b"would-mislabel".to_vec())],
                leader_commit: LogIndex(3),
            }),
        })
        .expect_err("append behind the boundary is refused");

    assert!(matches!(
        error,
        RaftRuntimeError::LogBehindSnapshotBoundary {
            segment_next_index: LogIndex(3),
            snapshot_index: LogIndex(3),
        }
    ));
    // Nothing was mislabelled into the segment, and the runtime is poisoned
    // so no acknowledgement of the refused entry can ever escape.
    assert_eq!(
        runtime.log_segment.replay_entries(),
        vec![
            persisted_entry(1, 1, b"covered-one"),
            persisted_entry(2, 1, b"covered-two"),
        ]
    );
    assert_poisoned_after_failure(&mut runtime, |cause| {
        matches!(
            cause,
            RaftRuntimeFatalError::LogBehindSnapshotBoundary { .. }
        )
    });
}

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

    assert!(matches!(
        error,
        RaftRuntimeError::CompactionAheadOfSnapshot {
            compacted_through: LogIndex(3),
            snapshot_index: LogIndex(1),
        }
    ));
}
