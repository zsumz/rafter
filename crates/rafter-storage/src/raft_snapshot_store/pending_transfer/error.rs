use std::{error::Error, fmt};

use crate::DecodeRaftSnapshotError;

/// Errors returned while decoding pending snapshot-transfer staging metadata.
///
/// This enum is exhaustive because pending-transfer recovery is closed over
/// envelope, body, and checksum validation failures.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecodePendingSnapshotTransferError {
    UnexpectedEof {
        needed: usize,
        remaining: usize,
    },
    InvalidMagic([u8; 4]),
    UnsupportedVersion(u8),
    EnvelopeChecksumMismatch {
        expected: u32,
        actual: u32,
    },
    SnapshotEnvelopeTooLarge {
        len: u64,
    },
    Snapshot(DecodeRaftSnapshotError),
    ReceivedPayloadTooLong {
        received_bytes: u64,
        total_payload_len: u64,
    },
    BodyTooShort {
        expected_bytes: u64,
        actual_bytes: u64,
    },
    BodyChecksumMismatch {
        expected: u32,
        actual: u32,
    },
    TrailingBytes(usize),
}

impl fmt::Display for DecodePendingSnapshotTransferError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof { needed, remaining } => write!(
                formatter,
                "pending Raft snapshot transfer manifest needs {needed} bytes but only {remaining} remain"
            ),
            Self::InvalidMagic(magic) => write!(
                formatter,
                "pending Raft snapshot transfer manifest magic {magic:02x?} is not RFPT"
            ),
            Self::UnsupportedVersion(version) => write!(
                formatter,
                "pending Raft snapshot transfer manifest version {version} is not supported"
            ),
            Self::EnvelopeChecksumMismatch { expected, actual } => write!(
                formatter,
                "pending Raft snapshot transfer manifest stored checksum {expected:#010x} does not match computed checksum {actual:#010x}"
            ),
            Self::SnapshotEnvelopeTooLarge { len } => write!(
                formatter,
                "pending Raft snapshot transfer snapshot envelope with length {len} is too large to decode"
            ),
            Self::Snapshot(error) => write!(
                formatter,
                "pending Raft snapshot transfer snapshot metadata is corrupt: {error}"
            ),
            Self::ReceivedPayloadTooLong {
                received_bytes,
                total_payload_len,
            } => write!(
                formatter,
                "pending Raft snapshot transfer received {received_bytes} bytes, more than the total payload length {total_payload_len}"
            ),
            Self::BodyTooShort {
                expected_bytes,
                actual_bytes,
            } => write!(
                formatter,
                "pending Raft snapshot transfer body holds {actual_bytes} bytes but expects {expected_bytes}"
            ),
            Self::BodyChecksumMismatch { expected, actual } => write!(
                formatter,
                "pending Raft snapshot transfer body stored checksum {expected:#010x} does not match computed checksum {actual:#010x}"
            ),
            Self::TrailingBytes(remaining) => write!(
                formatter,
                "pending Raft snapshot transfer manifest has {remaining} trailing bytes"
            ),
        }
    }
}

impl Error for DecodePendingSnapshotTransferError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Snapshot(error) => Some(error),
            Self::UnexpectedEof { .. }
            | Self::InvalidMagic(_)
            | Self::UnsupportedVersion(_)
            | Self::EnvelopeChecksumMismatch { .. }
            | Self::SnapshotEnvelopeTooLarge { .. }
            | Self::ReceivedPayloadTooLong { .. }
            | Self::BodyTooShort { .. }
            | Self::BodyChecksumMismatch { .. }
            | Self::TrailingBytes(_) => None,
        }
    }
}
