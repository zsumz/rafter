use super::*;
use rafter_invariant_test::{oracle_assert, oracle_assert_eq};

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

    oracle_assert_eq!(runtime.log_segment.compacted_through(), LogIndex(3));
    oracle_assert_eq!(runtime.log_segment.next_index(), LogIndex(4));
    oracle_assert_eq!(runtime.log_segment.replay_entries(), Vec::new());
    oracle_assert_eq!(runtime.snapshot_index(), LogIndex(3));
    oracle_assert_eq!(runtime.last_log_index(), LogIndex(3));

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
                entries: vec![LogEntry::application(Term(1), b"acked".to_vec())].into(),
                leader_commit: LogIndex(3),
            }),
        })
        .expect("append after repair persists");
    oracle_assert!(matches!(
        outputs.as_slice(),
        [RaftOutput::Send {
            message: Message::AppendEntriesResponse(response),
            ..
        }] if response.success && response.match_index == LogIndex(4)
    ));
    oracle_assert_eq!(
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
    oracle_assert_eq!(reopened.last_log_index(), LogIndex(4));
    oracle_assert_eq!(
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
                entries: vec![LogEntry::application(Term(1), b"would-mislabel".to_vec())].into(),
                leader_commit: LogIndex(3),
            }),
        })
        .expect_err("append behind the boundary is refused");

    oracle_assert!(matches!(
        error,
        RaftRuntimeError::LogBehindSnapshotBoundary {
            segment_next_index: LogIndex(3),
            snapshot_index: LogIndex(3),
        }
    ));
    // Nothing was mislabelled into the segment, and the runtime is poisoned
    // so no acknowledgement of the refused entry can ever escape.
    oracle_assert_eq!(
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
