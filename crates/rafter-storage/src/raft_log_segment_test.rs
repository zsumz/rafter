use std::{
    fs::{self, OpenOptions},
    io::Write,
};

use super::test_support::{entry, remove_test_file, test_segment_path};
use super::*;
use rafter_invariant_test::oracle_assert_eq;

#[test]
fn file_raft_log_segment_replays_entries_after_reopen() {
    let path = test_segment_path("replay");
    let entries = vec![entry(1, b"create"), entry(2, b"append")];
    {
        let mut segment = FileRaftLogSegment::open(&path).expect("segment opens");
        segment.append_entries(&entries).expect("entries append");
    }

    let reopened = FileRaftLogSegment::open(&path).expect("segment reopens");

    oracle_assert_eq!(reopened.next_index(), LogIndex(3));
    oracle_assert_eq!(reopened.replay_entries(), entries);
    remove_test_file(path);
}

#[test]
fn file_raft_log_segment_appends_borrowed_entries() {
    let path = test_segment_path("borrowed-append");
    let entries = vec![entry(1, b"create"), entry(2, b"append")];
    {
        let mut segment = FileRaftLogSegment::open(&path).expect("segment opens");
        segment
            .append_entries_borrowed(entries.iter().map(BorrowedPersistedRaftLogEntry::from))
            .expect("borrowed entries append");
    }

    let reopened = FileRaftLogSegment::open(&path).expect("segment reopens");

    assert_eq!(reopened.next_index(), LogIndex(3));
    assert_eq!(reopened.replay_entries(), entries);
    remove_test_file(path);
}

#[test]
fn file_raft_log_segment_truncates_suffix_and_replays_after_reopen() {
    let path = test_segment_path("truncate");
    {
        let mut segment = FileRaftLogSegment::open(&path).expect("segment opens");
        segment
            .append_entries(&[entry(1, b"one"), entry(2, b"two"), entry(3, b"three")])
            .expect("entries append");

        assert_eq!(segment.truncate_suffix(LogIndex(2)), Ok(()));
        assert_eq!(segment.next_index(), LogIndex(2));
        assert_eq!(segment.replay_entries(), vec![entry(1, b"one")]);
        segment
            .append_entries(&[entry(2, b"replacement")])
            .expect("replacement appends");
    }

    let reopened = FileRaftLogSegment::open(&path).expect("segment reopens");
    assert_eq!(reopened.next_index(), LogIndex(3));
    assert_eq!(
        reopened.replay_entries(),
        vec![entry(1, b"one"), entry(2, b"replacement")]
    );
    remove_test_file(path);
}

#[test]
fn file_raft_log_segment_truncate_empty_log_at_next_index_is_noop() {
    let path = test_segment_path("truncate-empty");
    let mut segment = FileRaftLogSegment::open(&path).expect("segment opens");

    assert_eq!(segment.truncate_suffix(LogIndex(1)), Ok(()));

    assert_eq!(segment.next_index(), LogIndex(1));
    assert_eq!(segment.replay_entries(), Vec::new());
    remove_test_file(path);
}

#[test]
fn file_raft_log_segment_rejects_non_contiguous_append_without_writing() {
    let path = test_segment_path("non-contiguous");
    let mut segment = FileRaftLogSegment::open(&path).expect("segment opens");

    assert_eq!(
        segment.append_entries(&[entry(2, b"append")]),
        Err(RaftLogSegmentAppendError::NonContiguous {
            expected: LogIndex(1),
            actual: LogIndex(2),
        })
    );
    assert_eq!(segment.replay_entries(), Vec::new());

    let reopened = FileRaftLogSegment::open(&path).expect("segment reopens");
    assert_eq!(reopened.replay_entries(), Vec::new());
    remove_test_file(path);
}

#[test]
fn file_raft_log_segment_reports_partial_tail_on_open() {
    let path = test_segment_path("partial-tail");
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .expect("test segment file is created");
    file.write_all(&[0, 0]).expect("partial frame is written");
    drop(file);

    assert_eq!(
        FileRaftLogSegment::open(&path).map(|segment| segment.replay_entries()),
        Err(OpenRaftLogSegmentError::Replay(
            RaftLogReplayError::PartialFrameHeader {
                offset: 0,
                remaining: 2,
            }
        ))
    );
    remove_test_file(path);
}

#[test]
fn file_raft_log_segment_repairs_partial_uncommitted_tail() {
    let path = test_segment_path("repair-partial-tail");
    let mut bytes = Vec::new();
    super::frames::append_raft_log_frames(&mut bytes, &[entry(1, b"one"), entry(2, b"two")])
        .expect("entries are encodable");
    bytes.extend_from_slice(&[0, 0]);
    fs::write(&path, bytes).expect("partial tail segment writes");

    let repaired = FileRaftLogSegment::open_repairing_uncommitted_tail(&path, LogIndex(2))
        .expect("uncommitted partial tail is repaired");

    assert_eq!(
        repaired.replay_entries(),
        vec![entry(1, b"one"), entry(2, b"two")]
    );
    assert_eq!(repaired.next_index(), LogIndex(3));
    drop(repaired);
    let reopened = FileRaftLogSegment::open(&path).expect("repaired segment reopens strictly");
    assert_eq!(
        reopened.replay_entries(),
        vec![entry(1, b"one"), entry(2, b"two")]
    );
    remove_test_file(path);
}

#[test]
fn file_raft_log_segment_rejects_partial_committed_tail_repair() {
    let path = test_segment_path("repair-partial-committed");
    let mut bytes = Vec::new();
    super::frames::append_raft_log_frame(&mut bytes, &entry(1, b"one"))
        .expect("entry is encodable");
    let partial_offset = bytes.len();
    bytes.extend_from_slice(&[0, 0]);
    fs::write(&path, bytes).expect("partial tail segment writes");

    assert_eq!(
        FileRaftLogSegment::open_repairing_uncommitted_tail(&path, LogIndex(2))
            .map(|segment| segment.replay_entries()),
        Err(OpenRaftLogSegmentError::Replay(
            RaftLogReplayError::PartialFrameHeader {
                offset: partial_offset,
                remaining: 2,
            }
        ))
    );
    assert!(matches!(
        FileRaftLogSegment::open(&path),
        Err(OpenRaftLogSegmentError::Replay(
            RaftLogReplayError::PartialFrameHeader { .. }
        ))
    ));
    remove_test_file(path);
}

#[test]
fn file_raft_log_segment_reports_corrupt_entry_on_open() {
    let path = test_segment_path("corrupt-entry");
    let mut bytes = Vec::new();
    super::frames::append_raft_log_frame(&mut bytes, &entry(1, b"create"))
        .expect("entry is encodable");
    let corrupt_byte = bytes.len() - 5;
    bytes[corrupt_byte] ^= 0xff;
    fs::write(&path, bytes).expect("corrupt segment is written");

    assert!(matches!(
        FileRaftLogSegment::open(&path),
        Err(OpenRaftLogSegmentError::Replay(
            RaftLogReplayError::CorruptEntry { offset: 0, .. }
        ))
    ));
    remove_test_file(path);
}

#[test]
fn file_raft_log_segment_repairs_corrupt_uncommitted_entry() {
    let path = test_segment_path("repair-corrupt-uncommitted");
    let mut bytes = Vec::new();
    super::frames::append_raft_log_frame(&mut bytes, &entry(1, b"one"))
        .expect("entry is encodable");
    let corrupt_offset = bytes.len();
    super::frames::append_raft_log_frame(&mut bytes, &entry(2, b"two"))
        .expect("entry is encodable");
    let corrupt_byte = bytes.len() - 5;
    bytes[corrupt_byte] ^= 0xff;
    fs::write(&path, bytes).expect("corrupt segment writes");

    let repaired = FileRaftLogSegment::open_repairing_uncommitted_tail(&path, LogIndex(1))
        .expect("uncommitted corrupt entry is repaired");

    assert_eq!(repaired.replay_entries(), vec![entry(1, b"one")]);
    assert_eq!(repaired.next_index(), LogIndex(2));
    drop(repaired);
    let reopened = FileRaftLogSegment::open(&path).expect("repaired segment reopens strictly");
    assert_eq!(reopened.replay_entries(), vec![entry(1, b"one")]);

    let repaired_len = fs::metadata(&path)
        .expect("repaired segment metadata is readable")
        .len();
    assert_eq!(repaired_len, corrupt_offset as u64);
    remove_test_file(path);
}

#[test]
fn file_raft_log_segment_rejects_corrupt_committed_entry_repair() {
    let path = test_segment_path("repair-corrupt-committed");
    let mut bytes = Vec::new();
    super::frames::append_raft_log_frame(&mut bytes, &entry(1, b"one"))
        .expect("entry is encodable");
    let corrupt_byte = bytes.len() - 5;
    bytes[corrupt_byte] ^= 0xff;
    fs::write(&path, bytes).expect("corrupt segment writes");

    assert!(matches!(
        FileRaftLogSegment::open_repairing_uncommitted_tail(&path, LogIndex(1)),
        Err(OpenRaftLogSegmentError::Replay(
            RaftLogReplayError::CorruptEntry { offset: 0, .. }
        ))
    ));
    remove_test_file(path);
}

#[test]
fn file_raft_log_segment_rejects_non_contiguous_replay() {
    let path = test_segment_path("replay-gap");
    let mut bytes = Vec::new();
    super::frames::append_raft_log_frame(&mut bytes, &entry(1, b"create"))
        .expect("entry is encodable");
    super::frames::append_raft_log_frame(&mut bytes, &entry(3, b"append"))
        .expect("entry is encodable");
    fs::write(&path, bytes).expect("gapped segment is written");

    assert_eq!(
        FileRaftLogSegment::open(&path).map(|segment| segment.replay_entries()),
        Err(OpenRaftLogSegmentError::NonContiguous {
            expected: LogIndex(2),
            actual: LogIndex(3),
        })
    );
    remove_test_file(path);
}

#[test]
fn file_raft_log_segment_repairs_non_contiguous_uncommitted_tail() {
    let path = test_segment_path("repair-gap-uncommitted");
    let mut bytes = Vec::new();
    super::frames::append_raft_log_frame(&mut bytes, &entry(1, b"one"))
        .expect("entry is encodable");
    let gap_offset = bytes.len();
    super::frames::append_raft_log_frame(&mut bytes, &entry(3, b"three"))
        .expect("entry is encodable");
    fs::write(&path, bytes).expect("gapped segment writes");

    let repaired = FileRaftLogSegment::open_repairing_uncommitted_tail(&path, LogIndex(1))
        .expect("uncommitted non-contiguous tail is repaired");

    assert_eq!(repaired.replay_entries(), vec![entry(1, b"one")]);
    drop(repaired);
    assert_eq!(
        fs::metadata(&path)
            .expect("repaired segment metadata is readable")
            .len(),
        gap_offset as u64
    );
    remove_test_file(path);
}

#[test]
fn file_raft_log_segment_rejects_non_contiguous_committed_repair() {
    let path = test_segment_path("repair-gap-committed");
    let mut bytes = Vec::new();
    super::frames::append_raft_log_frame(&mut bytes, &entry(1, b"one"))
        .expect("entry is encodable");
    super::frames::append_raft_log_frame(&mut bytes, &entry(3, b"three"))
        .expect("entry is encodable");
    fs::write(&path, bytes).expect("gapped segment writes");

    assert_eq!(
        FileRaftLogSegment::open_repairing_uncommitted_tail(&path, LogIndex(2))
            .map(|segment| segment.replay_entries()),
        Err(OpenRaftLogSegmentError::NonContiguous {
            expected: LogIndex(2),
            actual: LogIndex(3),
        })
    );
    remove_test_file(path);
}
