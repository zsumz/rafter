use std::io::{self, Read};

use super::*;

fn budget() -> ReceiveMemoryBudget {
    ReceiveMemoryBudget::new(crate::ReceiveMemoryLimits::default())
}

#[test]
fn an_idle_timeout_before_a_frame_preserves_the_connection() {
    let mut reader = ErrorReader::new(io::ErrorKind::TimedOut);
    let mut output = Vec::new();

    assert_eq!(
        read_peer_frame(&mut reader, 128, &budget(), &mut output).expect("idle read"),
        PeerFrameRead::Idle
    );
    assert!(output.is_empty());
}

#[test]
fn a_timeout_after_the_length_prefix_starts_is_a_connection_error() {
    let mut reader = PrefixThenError::new([0, 0, 0, 1], io::ErrorKind::TimedOut);
    let mut output = Vec::new();

    assert!(matches!(
        read_peer_frame(&mut reader, 128, &budget(), &mut output),
        Err(PeerFrameIoError::Io(error)) if error.kind() == io::ErrorKind::TimedOut
    ));
}

struct ErrorReader {
    kind: io::ErrorKind,
}

impl ErrorReader {
    const fn new(kind: io::ErrorKind) -> Self {
        Self { kind }
    }
}

impl Read for ErrorReader {
    fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::from(self.kind))
    }
}

struct PrefixThenError {
    prefix: [u8; 4],
    position: usize,
    kind: io::ErrorKind,
}

impl PrefixThenError {
    const fn new(prefix: [u8; 4], kind: io::ErrorKind) -> Self {
        Self {
            prefix,
            position: 0,
            kind,
        }
    }
}

impl Read for PrefixThenError {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.position < self.prefix.len() {
            let available = &self.prefix[self.position..];
            let copied = available.len().min(buffer.len());
            buffer[..copied].copy_from_slice(&available[..copied]);
            self.position += copied;
            return Ok(copied);
        }
        Err(io::Error::from(self.kind))
    }
}
