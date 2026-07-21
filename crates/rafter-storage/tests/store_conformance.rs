//! Cross-implementation conformance traces for the public storage contracts.
//!
//! Each scenario executes the same logical operations against the in-memory
//! reference implementation and the file-backed implementation. The file store
//! is reopened after every successful operation so restart reconstruction is
//! part of the equivalence check rather than a separate happy-path assertion.

#[path = "store_conformance/support.rs"]
mod support;

use rafter::{CommittedConfiguration, ConfigurationId, LogIndex, NodeId, RaftSnapshot, Term};
use rafter_storage::{
    BorrowedPersistedRaftLogEntry, FileRaftHardStateStore, FileRaftLogSegment,
    FileRaftSnapshotStore, InMemoryRaftHardStateStore, InMemoryRaftLogSegment,
    InMemoryRaftSnapshotStore, PersistedRaftLogEntry, RaftHardState, RaftHardStateStore,
    RaftLogSegment, RaftLogSegmentAppendError, RaftLogSegmentTruncateError, RaftSnapshotStore,
    RaftSnapshotStoreWriteError,
};

use support::{
    assert_log_equivalent, assert_snapshot_equivalent, persisted_snapshot, staged_chunk,
    staged_chunks, TestWorkspace,
};

#[test]
fn hard_state_trace_matches_after_every_reopen() {
    let workspace = TestWorkspace::new("hard-state");
    let path = workspace.path("hard-state");
    let mut memory = InMemoryRaftHardStateStore::new();
    let mut file = FileRaftHardStateStore::open(&path).expect("file hard-state store opens");

    assert_eq!(memory.current(), file.current());
    assert!(!file.requires_reopen());

    let states = [
        RaftHardState {
            current_term: Term(1),
            voted_for: Some(NodeId(2)),
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
        },
        RaftHardState {
            current_term: Term(4),
            voted_for: None,
            commit_index: LogIndex(3),
            committed_configuration: Some(CommittedConfiguration {
                index: LogIndex(3),
                config_id: ConfigurationId(9),
            }),
        },
        RaftHardState {
            current_term: Term(5),
            voted_for: Some(NodeId(4)),
            commit_index: LogIndex(7),
            committed_configuration: Some(CommittedConfiguration {
                index: LogIndex(6),
                config_id: ConfigurationId(10),
            }),
        },
    ];

    for state in states {
        memory
            .write_hard_state(state)
            .expect("in-memory hard state writes");
        file.write_hard_state(state)
            .expect("file hard state writes");
        assert_eq!(memory.current(), file.current());

        file = FileRaftHardStateStore::open(&path).expect("hard-state store reopens");
        assert_eq!(memory.current(), file.current());
        assert!(!file.requires_reopen());
    }
}

#[test]
fn retained_log_trace_matches_after_every_reopen() {
    let workspace = TestWorkspace::new("retained-log");
    let path = workspace.path("log");
    let mut memory = InMemoryRaftLogSegment::new();
    let mut file = FileRaftLogSegment::open(&path).expect("file log segment opens");

    assert_log_equivalent(&memory, &file);

    let initial = vec![
        PersistedRaftLogEntry::application(LogIndex(1), Term(1), b"one".to_vec()),
        PersistedRaftLogEntry::application(LogIndex(2), Term(1), b"two".to_vec()),
        PersistedRaftLogEntry::application(LogIndex(3), Term(2), b"three".to_vec()),
    ];
    memory
        .append_entries(&initial)
        .expect("in-memory initial append succeeds");
    file.append_entries(&initial)
        .expect("file initial append succeeds");
    file = FileRaftLogSegment::open(&path).expect("log segment reopens after append");
    assert_log_equivalent(&memory, &file);

    let non_contiguous = [PersistedRaftLogEntry::noop(LogIndex(5), Term(2))];
    let expected = RaftLogSegmentAppendError::NonContiguous {
        expected: LogIndex(4),
        actual: LogIndex(5),
    };
    assert_eq!(
        memory.append_entries(&non_contiguous),
        Err(expected.clone())
    );
    assert_eq!(file.append_entries(&non_contiguous), Err(expected));
    assert_log_equivalent(&memory, &file);

    memory
        .truncate_suffix(LogIndex(3))
        .expect("in-memory suffix truncates");
    file.truncate_suffix(LogIndex(3))
        .expect("file suffix truncates");
    file = FileRaftLogSegment::open(&path).expect("log segment reopens after truncate");
    assert_log_equivalent(&memory, &file);

    let replacement = [
        PersistedRaftLogEntry::application(LogIndex(3), Term(3), b"replacement".to_vec()),
        PersistedRaftLogEntry::noop(LogIndex(4), Term(3)),
    ];
    memory
        .append_entries_borrowed(replacement.iter().map(BorrowedPersistedRaftLogEntry::from))
        .expect("in-memory borrowed replacement appends");
    file.append_entries_borrowed(replacement.iter().map(BorrowedPersistedRaftLogEntry::from))
        .expect("file borrowed replacement appends");
    file = FileRaftLogSegment::open(&path).expect("log segment reopens after replacement");
    assert_log_equivalent(&memory, &file);

    memory
        .compact_prefix_through(LogIndex(2))
        .expect("in-memory prefix compacts");
    file.compact_prefix_through(LogIndex(2))
        .expect("file prefix compacts");
    file = FileRaftLogSegment::open(&path).expect("log segment reopens after compaction");
    assert_log_equivalent(&memory, &file);

    memory
        .compact_prefix_through(LogIndex(2))
        .expect("in-memory repeated compaction is idempotent");
    file.compact_prefix_through(LogIndex(2))
        .expect("file repeated compaction is idempotent");
    file = FileRaftLogSegment::open(&path).expect("log segment reopens after repeated compaction");
    assert_log_equivalent(&memory, &file);

    let expected = RaftLogSegmentTruncateError::BeforeCompactedPrefix {
        compacted_through: LogIndex(2),
        actual: LogIndex(2),
    };
    assert_eq!(memory.truncate_suffix(LogIndex(2)), Err(expected.clone()));
    assert_eq!(file.truncate_suffix(LogIndex(2)), Err(expected));
    assert_log_equivalent(&memory, &file);

    memory
        .compact_prefix_through(LogIndex(6))
        .expect("in-memory compaction may advance past the tail");
    file.compact_prefix_through(LogIndex(6))
        .expect("file compaction may advance past the tail");
    file = FileRaftLogSegment::open(&path).expect("log segment reopens past local tail");
    assert_log_equivalent(&memory, &file);

    let after_snapshot = [PersistedRaftLogEntry::application(
        LogIndex(7),
        Term(4),
        b"after-snapshot".to_vec(),
    )];
    memory
        .append_entries(&after_snapshot)
        .expect("in-memory post-snapshot append succeeds");
    file.append_entries(&after_snapshot)
        .expect("file post-snapshot append succeeds");
    file = FileRaftLogSegment::open(&path).expect("log segment reopens after final append");
    assert_log_equivalent(&memory, &file);
}

#[test]
fn snapshot_trace_matches_after_every_reopen() {
    let workspace = TestWorkspace::new("snapshot");
    let directory = workspace.path("snapshots");
    let mut memory = InMemoryRaftSnapshotStore::new();
    let mut file = FileRaftSnapshotStore::open(&directory).expect("file snapshot store opens");

    assert_snapshot_equivalent(&memory, &file);

    let first = persisted_snapshot(3, 2, 4, b"first durable snapshot");
    memory
        .write_snapshot(first.clone())
        .expect("in-memory snapshot writes");
    file.write_snapshot(first).expect("file snapshot writes");
    file = FileRaftSnapshotStore::open(&directory).expect("snapshot store reopens after write");
    assert_snapshot_equivalent(&memory, &file);

    let streamed = persisted_snapshot(5, 4, 6, b"snapshot pulled through a bounded source");
    let streamed_descriptor =
        RaftSnapshot::from_payload(streamed.metadata.clone(), &streamed.application_payload);
    let streamed_source = InMemoryRaftSnapshotStore::with_snapshot(streamed);
    memory
        .write_snapshot_from_source(&streamed_descriptor, &streamed_source)
        .expect("in-memory streamed snapshot writes");
    file.write_snapshot_from_source(&streamed_descriptor, &streamed_source)
        .expect("file streamed snapshot writes");
    file = FileRaftSnapshotStore::open(&directory)
        .expect("snapshot store reopens after streamed write");
    assert_snapshot_equivalent(&memory, &file);

    let incoming = persisted_snapshot(7, 6, 8, b"incoming snapshot payload");
    let (descriptor, first_chunk, final_chunk) = staged_chunks(&incoming, 8);
    memory
        .stage_snapshot_chunk(&first_chunk)
        .expect("in-memory first chunk stages");
    file.stage_snapshot_chunk(&first_chunk)
        .expect("file first chunk stages");
    file =
        FileRaftSnapshotStore::open(&directory).expect("snapshot store reopens after first chunk");
    assert_snapshot_equivalent(&memory, &file);

    let staged_len =
        u64::try_from(first_chunk.bytes.len()).expect("staged test chunk length fits u64");
    let invalid_offset = staged_len + 1;
    let invalid = staged_chunk(
        &descriptor,
        first_chunk.leader_id,
        invalid_offset,
        incoming.application_payload[8..11].to_vec(),
        false,
    );
    let expected = RaftSnapshotStoreWriteError::StagedChunkOffsetMismatch {
        expected_offset: staged_len,
        offset: invalid_offset,
    };
    assert_eq!(memory.stage_snapshot_chunk(&invalid), Err(expected.clone()));
    assert_eq!(file.stage_snapshot_chunk(&invalid), Err(expected));
    assert_snapshot_equivalent(&memory, &file);

    memory
        .clear_pending_snapshot_transfer()
        .expect("in-memory pending transfer clears");
    file.clear_pending_snapshot_transfer()
        .expect("file pending transfer clears");
    file = FileRaftSnapshotStore::open(&directory).expect("snapshot store reopens after clear");
    assert_snapshot_equivalent(&memory, &file);

    memory
        .stage_snapshot_chunk(&first_chunk)
        .expect("in-memory first chunk restages");
    file.stage_snapshot_chunk(&first_chunk)
        .expect("file first chunk restages");
    file = FileRaftSnapshotStore::open(&directory).expect("snapshot store reopens after restaging");
    assert_snapshot_equivalent(&memory, &file);

    memory
        .stage_snapshot_chunk(&final_chunk)
        .expect("in-memory final chunk stages");
    file.stage_snapshot_chunk(&final_chunk)
        .expect("file final chunk stages");
    file =
        FileRaftSnapshotStore::open(&directory).expect("snapshot store reopens after final chunk");
    assert_snapshot_equivalent(&memory, &file);

    memory
        .promote_staged_snapshot(&descriptor)
        .expect("in-memory staged snapshot promotes");
    file.promote_staged_snapshot(&descriptor)
        .expect("file staged snapshot promotes");
    file = FileRaftSnapshotStore::open(&directory).expect("snapshot store reopens after promotion");
    assert_snapshot_equivalent(&memory, &file);

    let replacement = persisted_snapshot(10, 9, 11, b"replacement current snapshot");
    let (_, pending_chunk, _) = staged_chunks(&replacement, 5);
    memory
        .stage_snapshot_chunk(&pending_chunk)
        .expect("in-memory replacement transfer begins");
    file.stage_snapshot_chunk(&pending_chunk)
        .expect("file replacement transfer begins");
    file = FileRaftSnapshotStore::open(&directory)
        .expect("snapshot store reopens after replacement staging");
    assert_snapshot_equivalent(&memory, &file);

    memory
        .write_snapshot(replacement.clone())
        .expect("in-memory complete snapshot replaces pending transfer");
    file.write_snapshot(replacement)
        .expect("file complete snapshot replaces pending transfer");
    file =
        FileRaftSnapshotStore::open(&directory).expect("snapshot store reopens after replacement");
    assert_snapshot_equivalent(&memory, &file);
}
