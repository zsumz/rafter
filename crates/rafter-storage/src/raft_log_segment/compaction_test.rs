//! File-backed compacted-prefix publication and crash-residue scenarios.

use std::fs;

use super::test_support::{
    compact_marker_path_for_test, entry, remove_test_file, test_segment_path,
};
use super::*;

#[test]
fn file_raft_log_segment_compacts_prefix_and_replays_after_reopen() {
    let path = test_segment_path("compact");
    {
        let mut segment = FileRaftLogSegment::open(&path).expect("segment opens");
        segment
            .append_entries(&[entry(1, b"one"), entry(2, b"two"), entry(3, b"three")])
            .expect("entries append");

        assert_eq!(segment.compact_prefix_through(LogIndex(2)), Ok(()));
        assert_eq!(segment.next_index(), LogIndex(4));
        assert_eq!(segment.replay_entries(), vec![entry(3, b"three")]);
        segment
            .append_entries(&[entry(4, b"four")])
            .expect("post-compaction append succeeds");
    }

    let reopened = FileRaftLogSegment::open(&path).expect("segment reopens");
    assert_eq!(reopened.next_index(), LogIndex(5));
    assert_eq!(
        reopened.replay_entries(),
        vec![entry(3, b"three"), entry(4, b"four")]
    );
    remove_test_file(path);
}

#[test]
fn file_raft_log_segment_compacts_all_entries_and_preserves_next_index() {
    let path = test_segment_path("compact-all");
    {
        let mut segment = FileRaftLogSegment::open(&path).expect("segment opens");
        segment
            .append_entries(&[entry(1, b"one"), entry(2, b"two")])
            .expect("entries append");

        assert_eq!(segment.compact_prefix_through(LogIndex(2)), Ok(()));
        assert_eq!(segment.next_index(), LogIndex(3));
        assert_eq!(segment.replay_entries(), Vec::new());
        segment
            .append_entries(&[entry(3, b"three")])
            .expect("post-compaction append succeeds");
    }

    let reopened = FileRaftLogSegment::open(&path).expect("segment reopens");
    assert_eq!(reopened.next_index(), LogIndex(4));
    assert_eq!(reopened.replay_entries(), vec![entry(3, b"three")]);
    remove_test_file(path);
}

#[test]
fn file_raft_log_segment_compacts_past_local_tail_for_installed_snapshot() {
    let path = test_segment_path("compact-past-tail");
    let mut segment = FileRaftLogSegment::open(&path).expect("segment opens");
    segment
        .append_entries(&[entry(1, b"one")])
        .expect("entry appends");

    assert_eq!(segment.compact_prefix_through(LogIndex(2)), Ok(()));
    assert_eq!(segment.next_index(), LogIndex(3));
    assert_eq!(segment.replay_entries(), Vec::new());
    remove_test_file(path);
}

#[test]
fn file_raft_log_segment_filters_prefix_when_marker_wins_before_rewrite() {
    let path = test_segment_path("compact-marker-first");
    {
        let mut segment = FileRaftLogSegment::open(&path).expect("segment opens");
        segment
            .append_entries(&[entry(1, b"one"), entry(2, b"two"), entry(3, b"three")])
            .expect("entries append");
    }
    fs::write(
        compact_marker_path_for_test(&path),
        crate::raft_log_compaction::encode_raft_log_compaction_marker(LogIndex(2)),
    )
    .expect("compaction marker writes");

    let reopened = FileRaftLogSegment::open(&path).expect("segment reopens");

    assert_eq!(reopened.next_index(), LogIndex(4));
    assert_eq!(reopened.replay_entries(), vec![entry(3, b"three")]);
    remove_test_file(path);
}

#[test]
fn file_raft_log_segment_ignores_abandoned_rewrite_temp_before_rename() {
    let path = test_segment_path("abandoned-rewrite-temp");
    let temp_path = {
        let mut segment = FileRaftLogSegment::open(&path).expect("segment opens");
        segment
            .append_entries(&[entry(1, b"one"), entry(2, b"two"), entry(3, b"three")])
            .expect("entries append");
        segment.temp_rewrite_path()
    };
    let mut bytes = Vec::new();
    super::frames::append_raft_log_frames(&mut bytes, &[entry(1, b"one")])
        .expect("rewrite temp encodes");
    fs::write(&temp_path, bytes).expect("abandoned rewrite temp writes");

    let reopened = FileRaftLogSegment::open(&path).expect("segment reopens");

    assert_eq!(reopened.next_index(), LogIndex(4));
    assert_eq!(
        reopened.replay_entries(),
        vec![entry(1, b"one"), entry(2, b"two"), entry(3, b"three")]
    );
    remove_test_file(path);
}

#[test]
fn file_raft_log_segment_ignores_abandoned_compaction_marker_temp_before_rename() {
    let path = test_segment_path("abandoned-compact-marker-temp");
    let temp_path = {
        let mut segment = FileRaftLogSegment::open(&path).expect("segment opens");
        segment
            .append_entries(&[entry(1, b"one"), entry(2, b"two"), entry(3, b"three")])
            .expect("entries append");
        segment.temp_compaction_marker_path()
    };
    fs::write(
        &temp_path,
        crate::raft_log_compaction::encode_raft_log_compaction_marker(LogIndex(2)),
    )
    .expect("abandoned compaction marker temp writes");

    let reopened = FileRaftLogSegment::open(&path).expect("segment reopens");

    assert_eq!(reopened.compacted_through(), LogIndex::ZERO);
    assert_eq!(
        reopened.replay_entries(),
        vec![entry(1, b"one"), entry(2, b"two"), entry(3, b"three")]
    );
    remove_test_file(path);
}

#[test]
fn file_raft_log_segment_rejects_corrupt_compaction_marker() {
    let path = test_segment_path("compact-marker-corrupt");
    {
        let mut segment = FileRaftLogSegment::open(&path).expect("segment opens");
        segment
            .append_entries(&[entry(1, b"one")])
            .expect("entry appends");
    }
    fs::write(compact_marker_path_for_test(&path), b"bad").expect("corrupt marker writes");

    assert!(matches!(
        FileRaftLogSegment::open(&path),
        Err(OpenRaftLogSegmentError::CompactionMarker(_))
    ));
    remove_test_file(path);
}

#[test]
fn file_raft_log_segment_rejects_truncate_through_compacted_prefix() {
    let path = test_segment_path("truncate-compacted-prefix");
    let mut segment = FileRaftLogSegment::open(&path).expect("segment opens");
    segment
        .append_entries(&[entry(1, b"one"), entry(2, b"two"), entry(3, b"three")])
        .expect("entries append");
    segment
        .compact_prefix_through(LogIndex(2))
        .expect("prefix compacts");

    assert_eq!(
        segment.truncate_suffix(LogIndex(1)),
        Err(RaftLogSegmentTruncateError::BeforeCompactedPrefix {
            compacted_through: LogIndex(2),
            actual: LogIndex(1),
        })
    );
    assert_eq!(segment.truncate_suffix(LogIndex(3)), Ok(()));
    assert_eq!(segment.next_index(), LogIndex(3));
    assert_eq!(segment.replay_entries(), Vec::new());
    remove_test_file(path);
}
