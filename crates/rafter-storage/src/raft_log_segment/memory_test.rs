use super::test_support::entry;
use super::*;

#[test]
fn in_memory_raft_log_segment_preserves_append_order() {
    let mut segment = InMemoryRaftLogSegment::new();
    let entries = vec![entry(1, b"create"), entry(2, b"append")];

    assert_eq!(segment.append_entries(&entries), Ok(()));

    assert_eq!(segment.next_index(), LogIndex(3));
    assert_eq!(segment.replay_entries(), entries);
}

#[test]
fn in_memory_raft_log_segment_rejects_non_contiguous_append() {
    let mut segment = InMemoryRaftLogSegment::new();

    assert_eq!(
        segment.append_entries(&[entry(2, b"append")]),
        Err(RaftLogSegmentAppendError::NonContiguous {
            expected: LogIndex(1),
            actual: LogIndex(2),
        })
    );
    assert_eq!(segment.replay_entries(), Vec::new());
}

#[test]
fn in_memory_raft_log_segment_truncates_suffix() {
    let mut segment = InMemoryRaftLogSegment::new();
    segment
        .append_entries(&[entry(1, b"one"), entry(2, b"two"), entry(3, b"three")])
        .expect("entries append");

    assert_eq!(segment.truncate_suffix(LogIndex(2)), Ok(()));

    assert_eq!(segment.next_index(), LogIndex(2));
    assert_eq!(segment.replay_entries(), vec![entry(1, b"one")]);
    segment
        .append_entries(&[entry(2, b"replacement")])
        .expect("replacement appends");
    assert_eq!(
        segment.replay_entries(),
        vec![entry(1, b"one"), entry(2, b"replacement")]
    );
}

#[test]
fn in_memory_raft_log_segment_truncate_at_next_index_is_noop() {
    let mut segment = InMemoryRaftLogSegment::new();
    segment
        .append_entries(&[entry(1, b"one")])
        .expect("entry appends");

    assert_eq!(segment.truncate_suffix(LogIndex(2)), Ok(()));

    assert_eq!(segment.next_index(), LogIndex(2));
    assert_eq!(segment.replay_entries(), vec![entry(1, b"one")]);
}

#[test]
fn in_memory_raft_log_segment_compacts_prefix() {
    let mut segment = InMemoryRaftLogSegment::new();
    segment
        .append_entries(&[entry(1, b"one"), entry(2, b"two"), entry(3, b"three")])
        .expect("entries append");

    assert_eq!(segment.compact_prefix_through(LogIndex(2)), Ok(()));

    assert_eq!(segment.next_index(), LogIndex(4));
    assert_eq!(segment.replay_entries(), vec![entry(3, b"three")]);
    segment
        .append_entries(&[entry(4, b"four")])
        .expect("post-compaction append succeeds");
    assert_eq!(
        segment.replay_entries(),
        vec![entry(3, b"three"), entry(4, b"four")]
    );
}

#[test]
fn in_memory_raft_log_segment_compacts_past_local_tail_for_installed_snapshot() {
    let mut segment = InMemoryRaftLogSegment::new();
    segment
        .append_entries(&[entry(1, b"one")])
        .expect("entry appends");

    assert_eq!(segment.compact_prefix_through(LogIndex(2)), Ok(()));
    assert_eq!(segment.next_index(), LogIndex(3));
    assert_eq!(segment.replay_entries(), Vec::new());
}

#[test]
fn in_memory_raft_log_segment_rejects_truncate_past_next_index() {
    let mut segment = InMemoryRaftLogSegment::new();

    assert_eq!(
        segment.truncate_suffix(LogIndex(2)),
        Err(RaftLogSegmentTruncateError::OutOfBounds {
            next_index: LogIndex(1),
            actual: LogIndex(2),
        })
    );
}

#[test]
fn in_memory_raft_log_segment_rejects_truncate_through_compacted_prefix() {
    let mut segment = InMemoryRaftLogSegment::new();
    segment
        .append_entries(&[entry(1, b"one"), entry(2, b"two"), entry(3, b"three")])
        .expect("entries append");
    segment
        .compact_prefix_through(LogIndex(2))
        .expect("prefix compacts");

    assert_eq!(
        segment.truncate_suffix(LogIndex(2)),
        Err(RaftLogSegmentTruncateError::BeforeCompactedPrefix {
            compacted_through: LogIndex(2),
            actual: LogIndex(2),
        })
    );
    assert_eq!(segment.truncate_suffix(LogIndex(3)), Ok(()));
    assert_eq!(segment.next_index(), LogIndex(3));
    assert_eq!(segment.replay_entries(), Vec::new());
}
