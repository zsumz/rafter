//! Fixed-size WAL authority must be checked before reading an unbounded tail.
use super::*;
use std::io::Cursor;

fn canonical() -> Vec<u8> {
    include_str!("../../../tests/vectors/v1/wal-manifest.hex")
        .lines()
        .flat_map(|line| line.split('#').next().unwrap().split_whitespace())
        .map(|byte| u8::from_str_radix(byte, 16).unwrap())
        .collect()
}

#[test]
fn manifest_short_and_interrupted_reads_preserve_the_published_vector() {
    let bytes = canonical();
    let expected = Manifest {
        generation: 1,
        operation: 2,
        compacted: LogIndex(1),
    };
    assert_eq!(encode_manifest(&expected), bytes);
    let mut reader = ShortReads {
        inner: Cursor::new(bytes),
        interrupt: true,
    };
    assert_eq!(read_manifest_from(&mut reader).unwrap(), expected);
}

#[test]
fn manifest_truncation_corruption_and_trailing_bytes_are_refused() {
    let bytes = canonical();
    for end in 0..bytes.len() {
        assert_eq!(
            read_manifest_from(&bytes[..end]).unwrap_err().kind(),
            io::ErrorKind::InvalidData,
            "truncated at {end}"
        );
    }
    for offset in 0..bytes.len() {
        let mut corrupt = bytes.clone();
        corrupt[offset] ^= 1;
        assert_eq!(
            read_manifest_from(corrupt.as_slice()).unwrap_err().kind(),
            io::ErrorKind::InvalidData,
            "corrupt at {offset}"
        );
    }
    for tail in [1, 4096] {
        let mut extra = bytes.clone();
        extra.resize(extra.len() + tail, 0);
        assert_eq!(
            read_manifest_from(extra.as_slice()).unwrap_err().kind(),
            io::ErrorKind::InvalidData,
            "trailing bytes: {tail}"
        );
    }
}

#[test]
fn manifest_refuses_an_unbounded_tail_without_reading_past_the_first_extra_byte() {
    let mut bytes = canonical();
    bytes.push(0);
    let mut reader = BudgetReader { bytes, at: 0 };
    assert_eq!(
        read_manifest_from(&mut reader).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(reader.at, 37);
}

#[test]
fn manifest_io_failures_before_the_length_decision_are_preserved() {
    let bytes = canonical();
    for cut in 0..=bytes.len() {
        let failure = FailingRead;
        let reader = (&bytes[..cut]).chain(failure);
        let error = read_manifest_from(reader).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied, "cut {cut}");
        assert_eq!(error.to_string(), "injected manifest read failure");
    }
}

struct ShortReads<R> {
    inner: R,
    interrupt: bool,
}
impl<R: Read> Read for ShortReads<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.interrupt = !self.interrupt;
        if self.interrupt {
            return Err(io::ErrorKind::Interrupted.into());
        }
        let size = output.len().min(1);
        self.inner.read(&mut output[..size])
    }
}

struct BudgetReader {
    bytes: Vec<u8>,
    at: usize,
}
impl Read for BudgetReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        // The backing stream could continue forever. Touching its tail after
        // one extra byte has already proved corruption is a qualification failure.
        assert!(
            self.at < self.bytes.len(),
            "read beyond manifest admission bound"
        );
        assert!(output.len() <= self.bytes.len() - self.at);
        let size = output.len().min(3);
        output[..size].copy_from_slice(&self.bytes[self.at..self.at + size]);
        self.at += size;
        Ok(size)
    }
}

struct FailingRead;
impl Read for FailingRead {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "injected manifest read failure",
        ))
    }
}
