//! Version-1 compacted-prefix marker encoding and strict decoding.

use std::{error::Error, fmt};

use rafter::LogIndex;

use crate::format::{
    finish_checksummed, verify_checksum, ChecksumError, CursorError, Reader, Writer,
};

const RAFT_LOG_COMPACTION_MAGIC: [u8; 4] = *b"RFLC";
const RAFT_LOG_COMPACTION_VERSION: u8 = 1;

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

impl From<CursorError> for DecodeRaftLogCompactionMarkerError {
    fn from(error: CursorError) -> Self {
        match error {
            CursorError::UnexpectedEof { needed, remaining } => {
                Self::UnexpectedEof { needed, remaining }
            }
            CursorError::TrailingBytes(remaining) => Self::TrailingBytes(remaining),
        }
    }
}

impl From<ChecksumError> for DecodeRaftLogCompactionMarkerError {
    fn from(error: ChecksumError) -> Self {
        match error {
            ChecksumError::UnexpectedEof { needed, remaining } => {
                Self::UnexpectedEof { needed, remaining }
            }
            ChecksumError::Mismatch { expected, actual } => {
                Self::ChecksumMismatch { expected, actual }
            }
        }
    }
}

/// Encodes a compacted-prefix boundary marker.
///
/// The marker records the highest log index covered by a durable snapshot so
/// log replay can discard entries at or below that boundary.
pub fn encode_raft_log_compaction_marker(compacted_through: LogIndex) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.bytes(&RAFT_LOG_COMPACTION_MAGIC);
    writer.u8(RAFT_LOG_COMPACTION_VERSION);
    writer.u64(compacted_through.0);
    finish_checksummed(writer)
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
    let body = verify_checksum(input)?;
    let mut reader = Reader::new(body);
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
