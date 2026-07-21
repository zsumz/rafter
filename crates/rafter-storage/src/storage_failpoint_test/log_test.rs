//! Retained-log failpoints pin append, replacement, and compaction commit points.

use rafter::LogIndex;

use crate::{
    FileRaftLogSegment, RaftLogSegment, RaftLogSegmentAppendError, RaftLogSegmentCompactError,
    RaftLogSegmentTruncateError,
};

use super::{
    arm,
    support_test::{
        log_entries, log_marker_path, log_marker_temp_path, log_rewrite_temp_path, TestWorkspace,
    },
    DurabilityPoint,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReopenedRewrite {
    Original,
    Truncated,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReopenedCompaction {
    Uncommitted,
    Committed,
}

#[test]
fn append_synced_before_cache_update_is_recovered_by_reopen() {
    let workspace = TestWorkspace::new("log-append-after-write");
    let path = workspace.path("log");
    let entries = log_entries();
    let mut segment = FileRaftLogSegment::open(&path).expect("log opens");

    let guard = arm(DurabilityPoint::LogAppendAfterSync);
    let error = segment
        .append_entries(&entries[..1])
        .expect_err("post-write failpoint fires");
    guard.assert_triggered();

    assert!(matches!(error, RaftLogSegmentAppendError::Io { .. }));
    assert!(segment.requires_reopen());
    assert!(segment.replay_entries().is_empty());

    drop(segment);
    let reopened = FileRaftLogSegment::open(&path).expect("log reopens");
    assert_eq!(reopened.replay_entries(), entries[..1].to_vec());
    assert_eq!(reopened.next_index(), LogIndex(2));
}

#[test]
fn suffix_replacement_matrix_reopens_to_the_selected_file() {
    let cases = [
        (
            DurabilityPoint::LogRewriteAfterTempSync,
            ReopenedRewrite::Original,
        ),
        (
            DurabilityPoint::LogRewriteBeforeRename,
            ReopenedRewrite::Original,
        ),
        (
            DurabilityPoint::LogRewriteAfterRename,
            ReopenedRewrite::Truncated,
        ),
        (
            DurabilityPoint::LogRewriteAfterDirectorySync,
            ReopenedRewrite::Truncated,
        ),
    ];

    for (point, expected) in cases {
        verify_suffix_rewrite_window(point, expected);
    }
}

fn verify_suffix_rewrite_window(point: DurabilityPoint, expected: ReopenedRewrite) {
    let workspace = TestWorkspace::new(&format!("log-rewrite-{point:?}"));
    let path = workspace.path("log");
    let entries = log_entries();
    let mut segment = FileRaftLogSegment::open(&path).expect("log opens");
    segment
        .append_entries(&entries)
        .expect("initial entries append");

    let guard = arm(point);
    let error = segment
        .truncate_suffix(LogIndex(3))
        .expect_err("armed rewrite point fails");
    guard.assert_triggered();

    assert!(matches!(error, RaftLogSegmentTruncateError::Io { .. }));
    assert!(segment.requires_reopen());
    assert_eq!(segment.replay_entries(), entries);
    let rewrite_temp_exists = log_rewrite_temp_path(&path).exists();
    if matches!(expected, ReopenedRewrite::Original) {
        assert!(rewrite_temp_exists);
    } else {
        assert!(!rewrite_temp_exists);
    }

    drop(segment);
    let reopened = FileRaftLogSegment::open(&path).expect("log reopens");
    let expected_entries = match expected {
        ReopenedRewrite::Original => entries,
        ReopenedRewrite::Truncated => entries[..2].to_vec(),
    };
    assert_eq!(reopened.replay_entries(), expected_entries);
    assert!(!reopened.requires_reopen());
}

#[test]
fn compaction_marker_matrix_reopens_at_the_marker_commit_point() {
    let cases = [
        (
            DurabilityPoint::LogMarkerAfterTempSync,
            ReopenedCompaction::Uncommitted,
        ),
        (
            DurabilityPoint::LogMarkerAfterRename,
            ReopenedCompaction::Committed,
        ),
        (
            DurabilityPoint::LogMarkerAfterDirectorySync,
            ReopenedCompaction::Committed,
        ),
    ];

    for (point, expected) in cases {
        verify_marker_window(point, expected);
    }
}

fn verify_marker_window(point: DurabilityPoint, expected: ReopenedCompaction) {
    let workspace = TestWorkspace::new(&format!("log-marker-{point:?}"));
    let path = workspace.path("log");
    let entries = log_entries();
    let mut segment = FileRaftLogSegment::open(&path).expect("log opens");
    segment
        .append_entries(&entries)
        .expect("initial entries append");

    let guard = arm(point);
    let error = segment
        .compact_prefix_through(LogIndex(2))
        .expect_err("armed marker point fails");
    guard.assert_triggered();

    assert!(matches!(error, RaftLogSegmentCompactError::Io { .. }));
    assert!(segment.requires_reopen());
    assert_eq!(segment.compacted_through(), LogIndex::ZERO);
    assert_eq!(segment.replay_entries(), entries);
    let marker_temp_exists = log_marker_temp_path(&path).exists();
    let marker_exists = log_marker_path(&path).exists();
    match expected {
        ReopenedCompaction::Uncommitted => {
            assert!(marker_temp_exists);
            assert!(!marker_exists);
        }
        ReopenedCompaction::Committed => {
            assert!(!marker_temp_exists);
            assert!(marker_exists);
        }
    }

    drop(segment);
    let reopened = FileRaftLogSegment::open(&path).expect("log reopens");
    match expected {
        ReopenedCompaction::Uncommitted => {
            assert_eq!(reopened.compacted_through(), LogIndex::ZERO);
            assert_eq!(reopened.replay_entries(), entries);
        }
        ReopenedCompaction::Committed => {
            assert_eq!(reopened.compacted_through(), LogIndex(2));
            assert_eq!(reopened.replay_entries(), entries[2..].to_vec());
        }
    }
}

#[test]
fn rewrite_failure_after_marker_reports_committed_compaction() {
    let workspace = TestWorkspace::new("log-committed-reclamation");
    let path = workspace.path("log");
    let entries = log_entries();
    let mut segment = FileRaftLogSegment::open(&path).expect("log opens");
    segment
        .append_entries(&entries)
        .expect("initial entries append");

    let guard = arm(DurabilityPoint::LogRewriteAfterRename);
    let error = segment
        .compact_prefix_through(LogIndex(2))
        .expect_err("replacement publication fails after marker commit");
    guard.assert_triggered();

    assert!(matches!(
        error,
        RaftLogSegmentCompactError::CompactedButReclamationFailed {
            compacted_through: LogIndex(2),
            ..
        }
    ));
    assert!(segment.requires_reopen());
    assert_eq!(segment.compacted_through(), LogIndex(2));
    assert_eq!(segment.replay_entries(), entries[2..].to_vec());

    drop(segment);
    let reopened = FileRaftLogSegment::open(&path).expect("log reopens");
    assert_eq!(reopened.compacted_through(), LogIndex(2));
    assert_eq!(reopened.replay_entries(), entries[2..].to_vec());
}
