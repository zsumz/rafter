//! Verify a trailing checksum while retaining only a bounded body prefix.
use super::ChecksumError;
use crate::checksum::RunningCrc32;
use std::io::{self, Read};

#[derive(Debug)]
pub(crate) enum StreamChecksumError {
    Io(io::Error),
    Checksum(ChecksumError),
}

pub(crate) struct VerifiedChecksumPrefix {
    pub prefix: Vec<u8>,
    pub body_len: u64,
}

// The final four bytes are always the stored checksum, including malformed
// envelopes. Delay them until EOF so corruption has the same precedence as
// the slice verifier, even when an oversized body is not retained.
pub(crate) fn read_checksummed_prefix(
    mut reader: impl Read,
    prefix_limit: usize,
) -> Result<VerifiedChecksumPrefix, StreamChecksumError> {
    let mut tail = [0; 4];
    let mut filled = 0;
    while filled < tail.len() {
        let read = read_retrying(&mut reader, &mut tail[filled..])?;
        if read == 0 {
            return Err(StreamChecksumError::Checksum(
                ChecksumError::UnexpectedEof {
                    needed: tail.len(),
                    remaining: filled,
                },
            ));
        }
        filled += read;
    }
    let mut crc = RunningCrc32::new();
    let mut prefix = Vec::with_capacity(prefix_limit);
    let mut consume = |bytes: &[u8]| {
        crc.update(bytes);
        let keep = bytes.len().min(prefix_limit - prefix.len());
        prefix.extend_from_slice(&bytes[..keep]);
    };
    let mut body_len = 0_u64;
    let mut buffer = [0; 4096];
    loop {
        let read = read_retrying(&mut reader, &mut buffer)?;
        if read == 0 {
            break;
        }
        body_len = body_len.checked_add(read as u64).ok_or_else(|| {
            StreamChecksumError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "checksummed envelope length exceeds u64",
            ))
        })?;
        if read >= tail.len() {
            consume(&tail);
            consume(&buffer[..read - tail.len()]);
            tail.copy_from_slice(&buffer[read - 4..read]);
        } else {
            consume(&tail[..read]);
            tail.copy_within(read.., 0);
            tail[4 - read..].copy_from_slice(&buffer[..read]);
        }
    }
    let expected = u32::from_be_bytes(tail);
    let actual = crc.value();
    if expected != actual {
        return Err(StreamChecksumError::Checksum(ChecksumError::Mismatch {
            expected,
            actual,
        }));
    }
    Ok(VerifiedChecksumPrefix { prefix, body_len })
}

fn read_retrying(reader: &mut impl Read, output: &mut [u8]) -> Result<usize, StreamChecksumError> {
    loop {
        match reader.read(output) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            result => return result.map_err(StreamChecksumError::Io),
        }
    }
}
