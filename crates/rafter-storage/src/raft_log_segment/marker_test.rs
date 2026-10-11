//! Compaction-marker admission bounds memory and preserves recovery diagnostics.
use super::*;
use crate::{
    crc32,
    raft_log_compaction::{decode_raft_log_compaction_marker, encode_raft_log_compaction_marker},
};
use std::io::Cursor;

fn with_signed_tail(tail: usize) -> Vec<u8> {
    let mut bytes = encode_raft_log_compaction_marker(LogIndex(2));
    bytes.truncate(bytes.len() - 4);
    bytes.resize(bytes.len() + tail, 0);
    let crc = crc32(&bytes);
    bytes.extend_from_slice(&crc.to_be_bytes());
    bytes
}

#[test]
fn oversized_compaction_marker_uses_bounded_read_requests() {
    let bytes = with_signed_tail(256 * 1024);
    let mut reader = BudgetReader(Cursor::new(bytes));
    assert!(matches!(
        read_marker(&mut reader),
        Err(ReadMarkerError::Decode(
            DecodeRaftLogCompactionMarkerError::TrailingBytes(262_144)
        ))
    ));
    assert_eq!(reader.0.position(), reader.0.get_ref().len() as u64);
}

struct BudgetReader(Cursor<Vec<u8>>);
impl Read for BudgetReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        assert!(
            output.len() <= 4096,
            "unbounded compaction-marker read request"
        );
        self.0.read(output)
    }
}

fn assert_matches_slice(bytes: &[u8]) {
    let expected = decode_raft_log_compaction_marker(bytes);
    for chunk in [1, 3, 4, 7, 4096] {
        let reader = ChunkedReader {
            inner: Cursor::new(bytes),
            chunk,
            interrupt: false,
        };
        let actual = match read_marker(reader) {
            Ok(index) => Ok(index),
            Err(ReadMarkerError::Decode(error)) => Err(error),
            Err(ReadMarkerError::Io(error)) => panic!("unexpected I/O: {error}"),
        };
        assert_eq!(actual, expected, "length {}, chunk {chunk}", bytes.len());
    }
}

#[test]
fn compaction_marker_stream_preserves_truncation_corruption_and_index_bounds() {
    for index in [0, 17, u64::MAX - 1, u64::MAX] {
        let valid = encode_raft_log_compaction_marker(LogIndex(index));
        for end in 0..=valid.len() {
            assert_matches_slice(&valid[..end]);
        }
        for end in 0..valid.len() - 4 {
            let mut body = valid[..end].to_vec();
            let crc = crc32(&body);
            body.extend_from_slice(&crc.to_be_bytes());
            assert_matches_slice(&body);
        }
        for offset in 0..valid.len() {
            let mut corrupt = valid.clone();
            corrupt[offset] ^= 0x80;
            assert_matches_slice(&corrupt);
            resign(&mut corrupt);
            assert_matches_slice(&corrupt);
        }
    }
}

#[test]
fn oversized_compaction_marker_preserves_checksum_schema_and_suffix_precedence() {
    for tail in [1, 2, 3, 4095, 4096, 4097] {
        let bytes = with_signed_tail(tail);
        assert_matches_slice(&bytes);
        for range in [0..4, 4..5, 5..13] {
            let mut invalid = bytes.clone();
            invalid[range].fill(0xff);
            assert_matches_slice(&invalid);
            resign(&mut invalid);
            assert_matches_slice(&invalid);
        }
    }
}

#[test]
fn compaction_marker_stream_preserves_io_errors_including_the_eof_probe() {
    let bytes = encode_raft_log_compaction_marker(LogIndex(8));
    for cut in 0..=bytes.len() {
        let reader = (&bytes[..cut]).chain(FailingRead);
        let ReadMarkerError::Io(error) = read_marker(reader).unwrap_err() else {
            panic!("read failure hidden at {cut}");
        };
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(error.to_string(), "injected compaction-marker read failure");
    }
}

#[test]
fn oversized_compaction_marker_refuses_log_repair_without_mutating_files() {
    use super::super::{
        test_support::{entry, remove_test_file, test_segment_path},
        FileRaftLogSegment, RaftLogSegment,
    };
    use std::fs;

    let path = test_segment_path("oversized-marker-repair");
    {
        let mut segment = FileRaftLogSegment::open(&path).unwrap();
        segment
            .append_entries(&[entry(1, b"one"), entry(2, b"two"), entry(3, b"three")])
            .unwrap();
    }
    let valid_log = fs::read(&path).unwrap();
    let mut damaged_log = valid_log.clone();
    damaged_log.extend_from_slice(b"partial uncommitted frame");
    fs::write(&path, &damaged_log).unwrap();
    let marker_path = compaction_marker_path(&path);
    let marker = with_signed_tail(8 * 1024 * 1024);
    fs::write(&marker_path, &marker).unwrap();
    for repairing in [false, true] {
        let opened = if repairing {
            FileRaftLogSegment::open_repairing_uncommitted_tail(&path, LogIndex(2))
        } else {
            FileRaftLogSegment::open(&path)
        };
        assert!(matches!(
            opened,
            Err(OpenRaftLogSegmentError::CompactionMarker(
                DecodeRaftLogCompactionMarkerError::TrailingBytes(8_388_608)
            ))
        ));
        assert_eq!(fs::read(&marker_path).unwrap(), marker);
        assert_eq!(fs::read(&path).unwrap(), damaged_log);
    }
    // Restoring a valid marker explicitly allows only the uncommitted-tail repair.
    fs::write(&marker_path, encode_raft_log_compaction_marker(LogIndex(2))).unwrap();
    let reopened = FileRaftLogSegment::open_repairing_uncommitted_tail(&path, LogIndex(2)).unwrap();
    assert_eq!(reopened.compacted_through(), LogIndex(2));
    assert_eq!(reopened.replay_entries(), vec![entry(3, b"three")]);
    assert_eq!(fs::read(&path).unwrap(), valid_log);
    remove_test_file(path);
}

fn resign(bytes: &mut [u8]) {
    let body_len = bytes.len() - 4;
    let crc = crc32(&bytes[..body_len]);
    bytes[body_len..].copy_from_slice(&crc.to_be_bytes());
}

struct ChunkedReader<'a> {
    inner: Cursor<&'a [u8]>,
    chunk: usize,
    interrupt: bool,
}
impl Read for ChunkedReader<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.interrupt = !self.interrupt;
        if self.interrupt {
            return Err(io::ErrorKind::Interrupted.into());
        }
        let size = output.len().min(self.chunk);
        self.inner.read(&mut output[..size])
    }
}

struct FailingRead;
impl Read for FailingRead {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "injected compaction-marker read failure",
        ))
    }
}
