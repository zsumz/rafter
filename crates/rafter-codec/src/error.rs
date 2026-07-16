//! Public encoding and decoding failure taxonomy.

use std::{error::Error, fmt};

use rafter::{MembershipValidationError, SnapshotIdError, SnapshotMetadataError};

use crate::VERSION;

/// Error returned when a peer message cannot be encoded for the selected wire
/// version.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EncodePeerMessageError {
    /// A variable-length field exceeds the width of its wire length prefix.
    FieldTooLarge {
        /// Stable field name used in diagnostics.
        field: &'static str,
        /// Actual field length in bytes or elements.
        len: usize,
        /// Maximum length representable by the field's wire prefix.
        max: usize,
    },
    /// The core message has no representation in the current peer format.
    UnsupportedMessage {
        /// Stable core message name.
        message: &'static str,
        /// Explanation of the supported peer-transport alternative.
        reason: &'static str,
    },
}

/// Error returned when a peer message frame is malformed, unsupported, or
/// fails integrity checks.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecodePeerMessageError {
    /// The frame ended before the next value could be read.
    UnexpectedEof {
        /// Bytes required by the next value.
        needed: usize,
        /// Bytes remaining in the frame.
        remaining: usize,
    },
    /// The frame does not begin with [`crate::MAGIC`].
    InvalidMagic([u8; 4]),
    /// The frame uses a version this crate cannot decode.
    UnsupportedVersion(u8),
    /// The top-level message discriminant is not allocated in version 1.
    UnknownMessageType(u8),
    /// A boolean contains a byte other than zero or one.
    InvalidBoolean(u8),
    /// A length-prefixed string is not valid UTF-8.
    InvalidUtf8 {
        /// Stable field name used in diagnostics.
        field: &'static str,
    },
    /// Snapshot group identity validation failed.
    InvalidSnapshotGroupId(SnapshotIdError),
    /// Application snapshot kind validation failed.
    InvalidApplicationSnapshotKind(SnapshotIdError),
    /// Application snapshot version validation failed.
    InvalidApplicationSnapshotVersion(SnapshotMetadataError),
    /// Cross-field snapshot metadata validation failed.
    InvalidSnapshotMetadata(SnapshotMetadataError),
    /// Stable membership validation failed.
    InvalidMembership(MembershipValidationError),
    /// A valid membership list was not encoded in strictly increasing order.
    NonCanonicalMembershipOrder {
        /// The noncanonical voter or learner list.
        field: &'static str,
    },
    /// The log-entry discriminant is not allocated in version 1.
    UnknownLogEntryKind(u8),
    /// The stable/joint membership discriminant is not allocated in version 1.
    UnknownMembershipKind(u8),
    /// The stored frame checksum does not match the encoded frame body.
    FrameChecksumMismatch {
        /// Checksum stored in the frame.
        expected: u32,
        /// Checksum calculated over the frame body.
        actual: u32,
    },
    /// Bytes remain after the checksum of the single expected frame.
    TrailingBytes(usize),
}

impl fmt::Display for EncodePeerMessageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FieldTooLarge { field, len, max } => write!(
                formatter,
                "peer message field {field} has length {len}, exceeding the wire maximum {max}"
            ),
            Self::UnsupportedMessage { message, reason } => write!(
                formatter,
                "peer message {message} is not supported by the current wire format: {reason}"
            ),
        }
    }
}

impl Error for EncodePeerMessageError {}

impl fmt::Display for DecodePeerMessageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof { needed, remaining } => write!(
                formatter,
                "peer message needs {needed} bytes but only {remaining} remain"
            ),
            Self::InvalidMagic(magic) => {
                write!(formatter, "peer message magic {magic:02x?} is not RFPM")
            }
            Self::UnsupportedVersion(version) => write!(
                formatter,
                "peer message version {version} is not supported (supported version {VERSION})"
            ),
            Self::UnknownMessageType(message_type) => {
                write!(formatter, "peer message type {message_type} is unknown")
            }
            Self::InvalidBoolean(byte) => {
                write!(formatter, "peer message boolean byte {byte} is not 0 or 1")
            }
            Self::InvalidUtf8 { field } => {
                write!(formatter, "peer message field {field} is not valid utf-8")
            }
            Self::InvalidSnapshotGroupId(error) => {
                write!(
                    formatter,
                    "peer message snapshot group id is invalid: {error}"
                )
            }
            Self::InvalidApplicationSnapshotKind(error) => write!(
                formatter,
                "peer message application snapshot kind is invalid: {error}"
            ),
            Self::InvalidApplicationSnapshotVersion(error) => write!(
                formatter,
                "peer message application snapshot version is invalid: {error}"
            ),
            Self::InvalidSnapshotMetadata(error) => {
                write!(
                    formatter,
                    "peer message snapshot metadata is invalid: {error}"
                )
            }
            Self::InvalidMembership(error) => {
                write!(formatter, "peer message membership is invalid: {error}")
            }
            Self::NonCanonicalMembershipOrder { field } => write!(
                formatter,
                "peer message membership field {field} is not in strictly increasing wire order"
            ),
            Self::UnknownLogEntryKind(kind) => {
                write!(formatter, "peer message log entry kind {kind} is unknown")
            }
            Self::UnknownMembershipKind(kind) => {
                write!(formatter, "peer message membership kind {kind} is unknown")
            }
            Self::FrameChecksumMismatch { expected, actual } => write!(
                formatter,
                "peer message checksum mismatch: expected {expected:#010x}, actual {actual:#010x}"
            ),
            Self::TrailingBytes(remaining) => {
                write!(formatter, "peer message has {remaining} trailing bytes")
            }
        }
    }
}

impl Error for DecodePeerMessageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidSnapshotGroupId(error) | Self::InvalidApplicationSnapshotKind(error) => {
                Some(error)
            }
            Self::InvalidApplicationSnapshotVersion(error)
            | Self::InvalidSnapshotMetadata(error) => Some(error),
            Self::InvalidMembership(error) => Some(error),
            Self::UnexpectedEof { .. }
            | Self::InvalidMagic(_)
            | Self::UnsupportedVersion(_)
            | Self::UnknownMessageType(_)
            | Self::InvalidBoolean(_)
            | Self::InvalidUtf8 { .. }
            | Self::NonCanonicalMembershipOrder { .. }
            | Self::UnknownLogEntryKind(_)
            | Self::UnknownMembershipKind(_)
            | Self::FrameChecksumMismatch { .. }
            | Self::TrailingBytes(_) => None,
        }
    }
}
