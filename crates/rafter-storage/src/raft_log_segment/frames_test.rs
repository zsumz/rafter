//! Streaming retained-log frame read and write scenarios.

use std::io::{self, Cursor, Read, Write};

use super::*;
use crate::raft_log_segment::test_support::entry;

#[test]
fn streaming_scan_bounds_each_read_and_replays_every_entry() {
    let large_payload = vec![1; STREAM_READ_CHUNK_LEN * 3 + 17];
    let entries = vec![
        entry(1, &large_payload),
        entry(2, b"second"),
        entry(3, b"third"),
    ];
    let mut encoded = Vec::new();
    append_raft_log_frames(&mut encoded, &entries).expect("test log encodes");
    let encoded_len = u64::try_from(encoded.len()).expect("encoded length fits u64");
    let mut reader = ObservedReader::new(encoded, 7 * 1024);

    let actual = read_raft_log_frames(&mut reader, encoded_len).expect("streaming replay reads");

    assert_eq!(actual.replay_error, None);
    assert_eq!(
        actual
            .frames
            .into_iter()
            .map(|frame| frame.entry)
            .collect::<Vec<_>>(),
        entries
    );
    assert!(reader.largest_request <= STREAM_READ_CHUNK_LEN);
    assert!(
        reader.read_calls > 3,
        "large frames must be read incrementally"
    );
}

#[test]
fn streaming_scan_reports_the_exact_partial_entry_boundary() {
    let mut encoded = Vec::new();
    append_raft_log_frames(&mut encoded, &[entry(1, b"complete")]).expect("test log encodes");
    encoded.truncate(encoded.len() - 3);
    let available_entry_bytes = encoded.len() - RAFT_LOG_FRAME_HEADER_LEN;
    let encoded_len = u64::try_from(encoded.len()).expect("encoded length fits u64");
    let mut reader = ObservedReader::new(encoded, 2);

    let scan = read_raft_log_frames(&mut reader, encoded_len)
        .expect("partial frame is structural, not I/O");

    assert!(scan.frames.is_empty());
    assert!(matches!(
        scan.replay_error,
        Some((
            0,
            RaftLogReplayError::PartialEntry {
                offset: 0,
                remaining,
                ..
            }
        )) if remaining == available_entry_bytes
    ));
}

#[test]
fn streaming_scan_rejects_an_impossible_frame_before_reading_its_body() {
    let encoded = u32::MAX.to_be_bytes().to_vec();
    let encoded_len = u64::try_from(encoded.len()).expect("encoded length fits u64");
    let mut reader = ObservedReader::new(encoded, RAFT_LOG_FRAME_HEADER_LEN);

    let scan = read_raft_log_frames(&mut reader, encoded_len)
        .expect("an impossible frame length is structural, not I/O");

    assert!(scan.frames.is_empty());
    assert!(matches!(
        scan.replay_error,
        Some((
            0,
            RaftLogReplayError::PartialEntry {
                offset: 0,
                remaining: 0,
                ..
            }
        ))
    ));
    assert_eq!(reader.read_calls, 1, "only the frame header should be read");
    assert_eq!(reader.largest_request, RAFT_LOG_FRAME_HEADER_LEN);
}

#[test]
fn streaming_writer_matches_the_canonical_in_memory_frame_bytes() {
    let large_payload = vec![7; 128 * 1024];
    let entries = vec![
        entry(1, &large_payload),
        entry(2, b"two"),
        entry(3, b"three"),
    ];
    let mut expected = Vec::new();
    append_raft_log_frames(&mut expected, &entries).expect("canonical frames encode");
    let mut output = ObservedWriter::default();

    write_raft_log_frames(&mut output, &entries).expect("streaming frames encode");

    assert_eq!(output.bytes, expected);
    assert_eq!(output.write_calls, entries.len() * 2);
    assert!(
        output.largest_write < output.bytes.len(),
        "the complete replacement log must never be submitted as one write"
    );
}

struct ObservedReader {
    inner: Cursor<Vec<u8>>,
    max_return: usize,
    largest_request: usize,
    read_calls: usize,
}

impl ObservedReader {
    fn new(bytes: Vec<u8>, max_return: usize) -> Self {
        Self {
            inner: Cursor::new(bytes),
            max_return,
            largest_request: 0,
            read_calls: 0,
        }
    }
}

impl Read for ObservedReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.largest_request = self.largest_request.max(output.len());
        self.read_calls += 1;
        let len = output.len().min(self.max_return);
        self.inner.read(&mut output[..len])
    }
}

#[derive(Default)]
struct ObservedWriter {
    bytes: Vec<u8>,
    largest_write: usize,
    write_calls: usize,
}

impl Write for ObservedWriter {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        self.largest_write = self.largest_write.max(input.len());
        self.write_calls += 1;
        self.bytes.extend_from_slice(input);
        Ok(input.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
