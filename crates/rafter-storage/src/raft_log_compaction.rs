use std::{error::Error, fmt};

use rafter::LogIndex;

use crate::crc32;

const RAFT_LOG_COMPACTION_MAGIC: [u8; 4] = *b"RFLC";
const RAFT_LOG_COMPACTION_VERSION: u8 = 1;
const CHECKSUM_LEN: usize = 4;

/// Errors returned while decoding a log-compaction marker.
///
/// This enum is exhaustive because the marker format is closed over these
/// corruption and format failures.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecodeRaftLogCompactionMarkerError {
    UnexpectedEof { needed: usize, remaining: usize },
    InvalidMagic([u8; 4]),
    UnsupportedVersion(u8),
    ChecksumMismatch { expected: u32, actual: u32 },
    TrailingBytes(usize),
}

impl fmt::Display for DecodeRaftLogCompactionMarkerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof { needed, remaining } => write!(
                formatter,
                "Raft log compaction marker needs {needed} bytes but only {remaining} remain"
            ),
            Self::InvalidMagic(magic) => write!(
                formatter,
                "Raft log compaction marker magic {magic:02x?} is not RFLC"
            ),
            Self::UnsupportedVersion(version) => write!(
                formatter,
                "Raft log compaction marker version {version} is not supported"
            ),
            Self::ChecksumMismatch { expected, actual } => write!(
                formatter,
                "Raft log compaction marker stored checksum {expected:#010x} does not match computed checksum {actual:#010x}"
            ),
            Self::TrailingBytes(remaining) => write!(
                formatter,
                "Raft log compaction marker has {remaining} trailing bytes"
            ),
        }
    }
}

impl Error for DecodeRaftLogCompactionMarkerError {}

/// Encodes a compacted-prefix boundary marker.
///
/// The marker records the highest log index covered by a durable snapshot so
/// log replay can discard entries at or below that boundary.
pub fn encode_raft_log_compaction_marker(compacted_through: LogIndex) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&RAFT_LOG_COMPACTION_MAGIC);
    body.push(RAFT_LOG_COMPACTION_VERSION);
    body.extend_from_slice(&compacted_through.0.to_be_bytes());
    let checksum = crc32(&body);
    body.extend_from_slice(&checksum.to_be_bytes());
    body
}

/// Decodes and verifies a compacted-prefix boundary marker.
///
/// # Errors
///
/// Returns [`DecodeRaftLogCompactionMarkerError`] when the marker is malformed,
/// uses an unsupported version, has trailing bytes, or fails checksum
/// verification.
pub fn decode_raft_log_compaction_marker(
    input: &[u8],
) -> Result<LogIndex, DecodeRaftLogCompactionMarkerError> {
    if input.len() < CHECKSUM_LEN {
        return Err(DecodeRaftLogCompactionMarkerError::UnexpectedEof {
            needed: CHECKSUM_LEN,
            remaining: input.len(),
        });
    }

    let checksum_offset = input.len() - CHECKSUM_LEN;
    let body = &input[..checksum_offset];
    let checksum_bytes = &input[checksum_offset..];
    let expected = u32::from_be_bytes([
        checksum_bytes[0],
        checksum_bytes[1],
        checksum_bytes[2],
        checksum_bytes[3],
    ]);
    let actual = crc32(body);
    if actual != expected {
        return Err(DecodeRaftLogCompactionMarkerError::ChecksumMismatch { expected, actual });
    }

    let mut reader = MarkerReader::new(body);
    let magic = reader.magic()?;
    if magic != RAFT_LOG_COMPACTION_MAGIC {
        return Err(DecodeRaftLogCompactionMarkerError::InvalidMagic(magic));
    }
    let version = reader.u8()?;
    if version != RAFT_LOG_COMPACTION_VERSION {
        return Err(DecodeRaftLogCompactionMarkerError::UnsupportedVersion(
            version,
        ));
    }
    let compacted_through = LogIndex(reader.u64()?);
    reader.finish()?;
    Ok(compacted_through)
}

struct MarkerReader<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> MarkerReader<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn finish(&self) -> Result<(), DecodeRaftLogCompactionMarkerError> {
        let remaining = self.input.len() - self.offset;
        if remaining == 0 {
            Ok(())
        } else {
            Err(DecodeRaftLogCompactionMarkerError::TrailingBytes(remaining))
        }
    }

    fn magic(&mut self) -> Result<[u8; 4], DecodeRaftLogCompactionMarkerError> {
        let bytes = self.take(4)?;
        Ok([bytes[0], bytes[1], bytes[2], bytes[3]])
    }

    fn u8(&mut self) -> Result<u8, DecodeRaftLogCompactionMarkerError> {
        Ok(self.take(1)?[0])
    }

    fn u64(&mut self) -> Result<u64, DecodeRaftLogCompactionMarkerError> {
        let bytes = self.take(8)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], DecodeRaftLogCompactionMarkerError> {
        let remaining = self.input.len() - self.offset;
        if remaining < len {
            return Err(DecodeRaftLogCompactionMarkerError::UnexpectedEof {
                needed: len,
                remaining,
            });
        }
        let start = self.offset;
        self.offset += len;
        Ok(&self.input[start..self.offset])
    }
}
