//! File-handle poisoning and compaction commit-point recovery scenarios.

use std::fs::{self, OpenOptions};

use super::test_support::{entry, remove_test_file, test_segment_path};
use super::*;

#[test]
fn validation_failure_leaves_the_log_handle_healthy() {
    let path = test_segment_path("validation-keeps-healthy");
    let mut segment = FileRaftLogSegment::open(&path).expect("segment opens");

    assert_eq!(
        segment.append_entries(&[entry(2, b"gap")]),
        Err(RaftLogSegmentAppendError::NonContiguous {
            expected: LogIndex(1),
            actual: LogIndex(2),
        })
    );
    assert!(!segment.requires_reopen());
    segment
        .append_entries(&[entry(1, b"one")])
        .expect("healthy handle accepts a corrected append");

    remove_test_file(path);
}

#[test]
fn append_io_failure_requires_reopen_before_later_mutations() {
    let path = test_segment_path("append-reopen-required");
    let mut segment = FileRaftLogSegment::open(&path).expect("segment opens");
    segment
        .append_entries(&[entry(1, b"one")])
        .expect("initial entry appends");

    // Replace the live append handle with a read-only descriptor so the next
    // write fails deterministically without relying on platform permissions.
    segment.file = OpenOptions::new()
        .read(true)
        .open(&path)
        .expect("read-only log handle opens");

    let error = segment
        .append_entries(&[entry(2, b"two")])
        .expect_err("append through a read-only descriptor fails");

    assert!(matches!(
        error,
        RaftLogSegmentAppendError::Io {
            operation: "append raft log entries",
            ..
        }
    ));
    assert!(segment.requires_reopen());
    assert_eq!(
        segment.append_entries(&[entry(2, b"two")]),
        Err(RaftLogSegmentAppendError::StoreRequiresReopen)
    );
    drop(segment);

    let reopened = FileRaftLogSegment::open(&path).expect("segment reopens");
    assert!(!reopened.requires_reopen());
    assert_eq!(reopened.replay_entries(), vec![entry(1, b"one")]);
    remove_test_file(path);
}

#[test]
fn mutating_io_failure_requires_reopen_before_later_log_writes() {
    let path = test_segment_path("reopen-required");
    let mut segment = FileRaftLogSegment::open(&path).expect("segment opens");
    segment
        .append_entries(&[entry(1, b"one")])
        .expect("entry appends");

    let rewrite_temp = segment.temp_rewrite_path();
    fs::create_dir(&rewrite_temp).expect("directory blocks rewrite temp creation");
    let error = segment
        .truncate_suffix(LogIndex(1))
        .expect_err("rewrite temp open fails");

    assert!(matches!(
        error,
        RaftLogSegmentTruncateError::Io {
            operation: "open rewritten raft log segment",
            ..
        }
    ));
    assert!(segment.requires_reopen());
    assert_eq!(segment.replay_entries(), vec![entry(1, b"one")]);

    fs::remove_dir(&rewrite_temp).expect("blocking directory removes");
    assert_eq!(
        segment.append_entries(&[entry(2, b"two")]),
        Err(RaftLogSegmentAppendError::StoreRequiresReopen)
    );
    assert_eq!(
        segment.truncate_suffix(LogIndex(2)),
        Err(RaftLogSegmentTruncateError::StoreRequiresReopen)
    );
    assert_eq!(
        segment.compact_prefix_through(LogIndex(1)),
        Err(RaftLogSegmentCompactError::StoreRequiresReopen)
    );
    drop(segment);

    let mut reopened = FileRaftLogSegment::open(&path).expect("segment reopens");
    assert!(!reopened.requires_reopen());
    assert_eq!(reopened.replay_entries(), vec![entry(1, b"one")]);
    reopened
        .append_entries(&[entry(2, b"two")])
        .expect("fresh handle appends");
    remove_test_file(path);
}

#[test]
fn compaction_preparation_failure_leaves_the_boundary_uncommitted() {
    let path = test_segment_path("compact-preparation-failure");
    let mut segment = FileRaftLogSegment::open(&path).expect("segment opens");
    segment
        .append_entries(&[entry(1, b"one"), entry(2, b"two"), entry(3, b"three")])
        .expect("entries append");

    let rewrite_temp = segment.temp_rewrite_path();
    fs::create_dir(&rewrite_temp).expect("directory blocks rewrite preparation");
    let error = segment
        .compact_prefix_through(LogIndex(2))
        .expect_err("replacement preparation fails before marker publication");

    assert!(matches!(
        error,
        RaftLogSegmentCompactError::Io {
            operation: "open rewritten raft log segment",
            ..
        }
    ));
    assert!(segment.requires_reopen());
    assert_eq!(segment.compacted_through(), LogIndex::ZERO);
    assert_eq!(
        segment.replay_entries(),
        vec![entry(1, b"one"), entry(2, b"two"), entry(3, b"three")]
    );
    assert!(
        !segment.compaction_marker_path().exists(),
        "the logical compaction marker must not be published"
    );

    fs::remove_dir(&rewrite_temp).expect("blocking directory removes");
    drop(segment);
    let reopened = FileRaftLogSegment::open(&path).expect("segment reopens");
    assert_eq!(reopened.compacted_through(), LogIndex::ZERO);
    assert_eq!(
        reopened.replay_entries(),
        vec![entry(1, b"one"), entry(2, b"two"), entry(3, b"three")]
    );
    remove_test_file(path);
}

#[test]
fn compaction_marker_failure_leaves_the_boundary_uncommitted() {
    let path = test_segment_path("compact-marker-failure");
    let mut segment = FileRaftLogSegment::open(&path).expect("segment opens");
    segment
        .append_entries(&[entry(1, b"one"), entry(2, b"two"), entry(3, b"three")])
        .expect("entries append");

    let marker_temp = segment.temp_compaction_marker_path();
    fs::create_dir(&marker_temp).expect("directory blocks marker temp creation");
    let error = segment
        .compact_prefix_through(LogIndex(2))
        .expect_err("marker publication fails before commit");

    assert!(matches!(
        error,
        RaftLogSegmentCompactError::Io {
            operation: "open raft log compaction marker",
            ..
        }
    ));
    assert!(segment.requires_reopen());
    assert_eq!(segment.compacted_through(), LogIndex::ZERO);
    assert_eq!(
        segment.replay_entries(),
        vec![entry(1, b"one"), entry(2, b"two"), entry(3, b"three")]
    );

    fs::remove_dir(&marker_temp).expect("blocking directory removes");
    drop(segment);
    let reopened = FileRaftLogSegment::open(&path).expect("segment reopens");
    assert_eq!(reopened.compacted_through(), LogIndex::ZERO);
    assert_eq!(
        reopened.replay_entries(),
        vec![entry(1, b"one"), entry(2, b"two"), entry(3, b"three")]
    );
    remove_test_file(path);
}

#[test]
fn compaction_reports_a_committed_boundary_when_reclamation_fails() {
    let path = test_segment_path("compact-reclamation-failure");
    let mut segment = FileRaftLogSegment::open(&path).expect("segment opens");
    segment
        .append_entries(&[entry(1, b"one"), entry(2, b"two"), entry(3, b"three")])
        .expect("entries append");

    inject_log_rewrite_publication_failure();
    let error = segment
        .compact_prefix_through(LogIndex(2))
        .expect_err("reclamation fails after marker publication");

    assert!(matches!(
        error,
        RaftLogSegmentCompactError::CompactedButReclamationFailed {
            compacted_through: LogIndex(2),
            operation: "replace raft log segment",
            ..
        }
    ));
    assert!(segment.requires_reopen());
    assert_eq!(segment.compacted_through(), LogIndex(2));
    assert_eq!(segment.replay_entries(), vec![entry(3, b"three")]);

    drop(segment);
    let reopened = FileRaftLogSegment::open(&path).expect("committed marker reopens");
    assert_eq!(reopened.compacted_through(), LogIndex(2));
    assert_eq!(reopened.replay_entries(), vec![entry(3, b"three")]);
    remove_test_file(path);
}
