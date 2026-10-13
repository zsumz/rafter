//! Short reads, truncation, reusable buffers, and failure checksum boundaries.
use super::*;
use std::io::{BufReader, Cursor, Read};

#[test]
fn exact_reads_reuse_buffers_and_preserve_the_unread_trailer() {
    let payload = vec![7; 32 * 1024 + 1];
    let mut source = payload.clone();
    source.extend_from_slice(b"end");
    let mut file = BufReader::with_capacity(19, Cursor::new(source));
    let mut bytes = Vec::new();
    let mut crc = RunningCrc32::new();
    read_tracked_into(&mut file, &mut crc, payload.len(), &mut bytes).unwrap();
    assert_eq!(bytes, payload);
    assert!(bytes.capacity() <= payload.len());
    assert_eq!(crc.value(), crate::crc32(&payload));
    let capacity = bytes.capacity();
    read_tracked_into(&mut file, &mut crc, 3, &mut bytes).unwrap();
    assert_eq!(bytes, b"end");
    assert_eq!(bytes.capacity(), capacity);
    let mut expected = payload;
    expected.extend_from_slice(b"end");
    assert_eq!(crc.value(), crate::crc32(&expected));
}

#[test]
fn truncated_lengths_allocate_only_for_the_bytes_that_arrive() {
    for declared in [1024, 64 * 1024 * 1024] {
        let mut file = BufReader::with_capacity(3, Cursor::new([7; 17]));
        let mut bytes = Vec::new();
        let mut crc = RunningCrc32::new();
        crc.update(b"prefix");
        let before = crc.value();
        let error = read_tracked_into(&mut file, &mut crc, declared, &mut bytes).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::UnexpectedEof);
        assert_eq!(error.to_string(), "failed to fill whole buffer");
        assert_eq!(bytes, [7; 17]);
        assert!(bytes.capacity() <= 2 * bytes.len());
        assert_eq!(crc.value(), before);
    }
}

#[test]
fn empty_reads_clear_reused_contents_without_consuming_input() {
    let mut file = Cursor::new(b"unread");
    let mut bytes = vec![9; 17];
    let mut crc = RunningCrc32::new();
    crc.update(b"prefix");
    let before = crc.value();
    read_tracked_into(&mut file, &mut crc, 0, &mut bytes).unwrap();
    assert!(bytes.is_empty());
    assert_eq!(file.position(), 0);
    assert_eq!(crc.value(), before);
}

struct ShortReader {
    remaining: usize,
    interrupt: bool,
}
impl Read for ShortReader {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if std::mem::take(&mut self.interrupt) {
            return Err(ErrorKind::Interrupted.into());
        }
        if self.remaining == 0 {
            return Err(io::Error::new(
                ErrorKind::PermissionDenied,
                "checkpoint read failed",
            ));
        }
        let count = bytes.len().min(self.remaining).min(2);
        bytes[..count].fill(7);
        self.remaining -= count;
        Ok(count)
    }
}

#[test]
fn interrupted_and_short_reads_complete_without_reading_past_the_entry() {
    let mut file = BufReader::new(ShortReader {
        remaining: 17,
        interrupt: true,
    });
    let mut bytes = Vec::new();
    let mut crc = RunningCrc32::new();
    read_tracked_into(&mut file, &mut crc, 17, &mut bytes).unwrap();
    assert_eq!(bytes, [7; 17]);
    assert_eq!(crc.value(), crate::crc32(&bytes));
}

#[test]
fn read_failures_keep_the_original_error_and_do_not_advance_the_checksum() {
    let mut file = BufReader::new(ShortReader {
        remaining: 17,
        interrupt: true,
    });
    let mut bytes = Vec::new();
    let mut crc = RunningCrc32::new();
    crc.update(b"prefix");
    let before = crc.value();
    let error = read_tracked_into(&mut file, &mut crc, 4096, &mut bytes).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    assert_eq!(error.to_string(), "checkpoint read failed");
    assert_eq!(bytes, [7; 17]);
    assert!(bytes.capacity() <= 2 * bytes.len());
    assert_eq!(crc.value(), before);
}
