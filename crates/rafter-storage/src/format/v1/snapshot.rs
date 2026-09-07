//! Version-1 persisted-snapshot envelope and payload framing.
//!
//! Snapshot metadata field order lives in `format::v1::snapshot_metadata`; this
//! module owns the public value/error vocabulary, RFSN framing, payload bytes,
//! and the payload and envelope checksums.

use std::{error::Error, fmt};

use rafter::{MembershipValidationError, NodeId, SnapshotIdError, SnapshotMetadataError};

use crate::format::{ChecksumError, CursorError};

mod codec;
mod value;

pub use codec::{
    decode_raft_snapshot, encode_raft_snapshot, RAFT_SNAPSHOT_MAGIC, RAFT_SNAPSHOT_VERSION,
};
pub use value::PersistedRaftSnapshot;

pub(crate) use codec::{
    decode_raft_snapshot_header, encode_raft_snapshot_header,
    encode_raft_snapshot_metadata_envelope,
};
pub(crate) use value::SnapshotEnvelopeHeader;

/// Errors returned while encoding a persisted Raft snapshot envelope.
///
/// This enum is exhaustive because encode failures are limited to envelope
/// size bounds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EncodeRaftSnapshotError {
    /// A snapshot identity string does not fit in the u16 length prefix.
    StringTooLong {
        /// Snapshot identity field that exceeded its length prefix.
        field: &'static str,
        /// Encoded string length in bytes.
        len: usize,
    },
    /// A snapshot membership set contains more node ids than the format can
    /// represent.
    TooManyMembers {
        /// Membership set whose count exceeded the format.
        member_kind: &'static str,
        /// Number of node identifiers in the set.
        len: usize,
    },
}

/// Errors returned while decoding a persisted Raft snapshot envelope.
///
/// This enum is exhaustive because the envelope format is closed over these
/// corruption, format, and metadata-validation failures.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecodeRaftSnapshotError {
    /// The envelope ended before the requested field could be read.
    UnexpectedEof {
        /// Bytes required by the field.
        needed: usize,
        /// Bytes remaining in the envelope.
        remaining: usize,
    },
    /// The envelope magic was not [`RAFT_SNAPSHOT_MAGIC`].
    InvalidMagic([u8; 4]),
    /// The envelope version is not supported.
    UnsupportedVersion(u8),
    /// The snapshot group identifier was invalid.
    InvalidGroupId(SnapshotIdError),
    /// The application-kind identifier was invalid.
    InvalidApplicationKind(SnapshotIdError),
    /// A snapshot identity string was not valid UTF-8.
    InvalidUtf8 {
        /// Identity field that failed UTF-8 validation.
        field: &'static str,
    },
    /// The application-version component was invalid.
    InvalidApplicationVersion(SnapshotMetadataError),
    /// Snapshot position or checksum metadata was invalid.
    InvalidMetadata(SnapshotMetadataError),
    /// The stored stable or joint membership was invalid.
    InvalidMembership(MembershipValidationError),
    /// Member ids were valid but not stored in canonical ascending order.
    NonCanonicalMembershipOrder {
        /// Membership set being decoded.
        member_kind: &'static str,
        /// Prior node identifier in encoded order.
        previous: NodeId,
        /// Node identifier that broke ascending order.
        actual: NodeId,
    },
    /// The membership-presence tag was unknown.
    UnknownMembershipFlag(u8),
    /// The stable-or-joint membership tag was unknown.
    UnknownMembershipKind(u8),
    /// The application payload checksum did not match the payload bytes.
    PayloadChecksumMismatch {
        /// Checksum stored in the envelope.
        expected: u32,
        /// Checksum computed from the payload bytes.
        actual: u32,
    },
    /// The envelope checksum did not match the complete envelope.
    EnvelopeChecksumMismatch {
        /// Checksum stored in the envelope.
        expected: u32,
        /// Checksum computed from the envelope bytes.
        actual: u32,
    },
    /// Valid snapshot bytes were followed by unused trailing bytes.
    TrailingBytes(usize),
}

impl fmt::Display for EncodeRaftSnapshotError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StringTooLong { field, len } => write!(
                formatter,
                "Raft snapshot {field} with length {len} does not fit in the envelope format"
            ),
            Self::TooManyMembers { member_kind, len } => write!(
                formatter,
                "Raft snapshot membership with {len} {member_kind} does not fit in the envelope format"
            ),
        }
    }
}

impl Error for EncodeRaftSnapshotError {}

impl fmt::Display for DecodeRaftSnapshotError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof { needed, remaining } => write!(
                formatter,
                "Raft snapshot envelope needs {needed} bytes but only {remaining} remain"
            ),
            Self::InvalidMagic(magic) => write!(
                formatter,
                "Raft snapshot envelope magic {magic:02x?} is not RFSN"
            ),
            Self::UnsupportedVersion(version) => write!(
                formatter,
                "Raft snapshot envelope version {version} is not supported"
            ),
            Self::InvalidGroupId(error) => {
                write!(formatter, "Raft snapshot group id is invalid: {error}")
            }
            Self::InvalidApplicationKind(error) => write!(
                formatter,
                "Raft snapshot application kind is invalid: {error}"
            ),
            Self::InvalidUtf8 { field } => {
                write!(formatter, "Raft snapshot field {field} is not valid utf-8")
            }
            Self::InvalidApplicationVersion(error) => write!(
                formatter,
                "Raft snapshot application version is invalid: {error}"
            ),
            Self::InvalidMetadata(error) => {
                write!(formatter, "Raft snapshot metadata is invalid: {error}")
            }
            Self::InvalidMembership(error) => {
                write!(formatter, "Raft snapshot membership is invalid: {error}")
            }
            Self::NonCanonicalMembershipOrder {
                member_kind,
                previous,
                actual,
            } => write!(
                formatter,
                "Raft snapshot {member_kind} are not in canonical ascending node-id order: {} precedes {}",
                previous.0,
                actual.0
            ),
            Self::UnknownMembershipFlag(flag) => {
                write!(formatter, "Raft snapshot membership flag {flag} is unknown")
            }
            Self::UnknownMembershipKind(kind) => {
                write!(formatter, "Raft snapshot membership kind {kind} is unknown")
            }
            Self::PayloadChecksumMismatch { expected, actual } => write!(
                formatter,
                "Raft snapshot payload stored checksum {expected:#010x} does not match computed checksum {actual:#010x}"
            ),
            Self::EnvelopeChecksumMismatch { expected, actual } => write!(
                formatter,
                "Raft snapshot envelope stored checksum {expected:#010x} does not match computed checksum {actual:#010x}"
            ),
            Self::TrailingBytes(remaining) => write!(
                formatter,
                "Raft snapshot envelope has {remaining} trailing bytes"
            ),
        }
    }
}

impl Error for DecodeRaftSnapshotError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidGroupId(error) | Self::InvalidApplicationKind(error) => Some(error),
            Self::InvalidApplicationVersion(error) | Self::InvalidMetadata(error) => Some(error),
            Self::InvalidMembership(error) => Some(error),
            Self::UnexpectedEof { .. }
            | Self::InvalidMagic(_)
            | Self::UnsupportedVersion(_)
            | Self::InvalidUtf8 { .. }
            | Self::NonCanonicalMembershipOrder { .. }
            | Self::UnknownMembershipFlag(_)
            | Self::UnknownMembershipKind(_)
            | Self::PayloadChecksumMismatch { .. }
            | Self::EnvelopeChecksumMismatch { .. }
            | Self::TrailingBytes(_) => None,
        }
    }
}

impl From<CursorError> for DecodeRaftSnapshotError {
    fn from(error: CursorError) -> Self {
        match error {
            CursorError::UnexpectedEof { needed, remaining } => {
                Self::UnexpectedEof { needed, remaining }
            }
            CursorError::TrailingBytes(remaining) => Self::TrailingBytes(remaining),
        }
    }
}

impl From<ChecksumError> for DecodeRaftSnapshotError {
    fn from(error: ChecksumError) -> Self {
        match error {
            ChecksumError::UnexpectedEof { needed, remaining } => {
                Self::UnexpectedEof { needed, remaining }
            }
            ChecksumError::Mismatch { expected, actual } => {
                Self::EnvelopeChecksumMismatch { expected, actual }
            }
        }
    }
}

#[cfg(test)]
use rafter::RaftSnapshotMetadata;

#[cfg(test)]
use crate::checksum::crc32;

#[cfg(test)]
#[path = "raft_snapshot_codec_test.rs"]
mod tests;
