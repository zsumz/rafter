//! Version-1 persisted-log-entry envelope grammar and canonical decoding.
//!
//! This module owns RFLE framing, log-entry tags, membership fields, and
//! checksum mapping. Segment framing and durable append live in the log store.

use std::{error::Error, fmt};

use rafter::{MembershipValidationError, NodeId};

use crate::format::{ChecksumError, CursorError};

mod codec;
mod entry;

pub use codec::{
    decode_raft_log_entry, encode_borrowed_raft_log_entry, encode_raft_log_entry,
    RAFT_LOG_ENTRY_MAGIC, RAFT_LOG_ENTRY_VERSION,
};
pub use entry::{BorrowedPersistedRaftLogEntry, PersistedRaftLogEntry};

/// Error returned when a Raft log entry cannot be encoded into the persisted
/// envelope format.
///
/// This enum is exhaustive because encode failures are limited to envelope
/// size bounds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EncodeRaftLogEntryError {
    /// Application payload length does not fit in the u32 length prefix.
    PayloadTooLarge {
        /// Encoded payload length in bytes.
        len: usize,
    },
    /// Stable or joint membership contains more node ids than the format can
    /// represent.
    TooManyMembers {
        /// Number of node identifiers in the membership.
        len: usize,
    },
    /// The entry sits at `u64::MAX`, the one log index with no successor.
    ///
    /// Encoding is refused so the format cannot durably record an entry that
    /// [`decode_raft_log_entry`] would then refuse to read back: replay walks
    /// every retained index with `LogIndex::next()`.
    IndexAtMaximum,
}

/// Error returned when a persisted Raft log entry envelope cannot be decoded or
/// verified.
///
/// This enum is exhaustive because the envelope format is closed over these
/// corruption and format failures.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecodeRaftLogEntryError {
    /// The envelope ended before the requested field could be read.
    UnexpectedEof {
        /// Bytes required by the field.
        needed: usize,
        /// Bytes remaining in the envelope.
        remaining: usize,
    },
    /// The envelope magic did not match [`RAFT_LOG_ENTRY_MAGIC`].
    InvalidMagic([u8; 4]),
    /// The version byte is not supported by this decoder.
    UnsupportedVersion(u8),
    /// The entry-kind tag is not a known application or configuration variant.
    UnknownEntryKind(u8),
    /// The stored index is `u64::MAX`, the one log index with no successor.
    ///
    /// Replay advances past every retained entry with `LogIndex::next()`, so
    /// this index cannot be admitted into the retained suffix.
    IndexAtMaximum,
    /// A stable or joint membership entry failed Raft membership validation.
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
    /// The stored checksum did not match the envelope bytes.
    ChecksumMismatch {
        /// Checksum stored in the envelope.
        expected: u32,
        /// Checksum computed from the envelope bytes.
        actual: u32,
    },
    /// Valid entry bytes were followed by unused trailing bytes.
    TrailingBytes(usize),
}

impl fmt::Display for EncodeRaftLogEntryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PayloadTooLarge { len } => write!(
                formatter,
                "Raft log entry payload with length {len} does not fit in the envelope format"
            ),
            Self::TooManyMembers { len } => write!(
                formatter,
                "Raft log entry membership with {len} node ids does not fit in the envelope format"
            ),
            Self::IndexAtMaximum => formatter.write_str(
                "Raft log entry sits at the maximum log index, which replay cannot advance past",
            ),
        }
    }
}

impl Error for EncodeRaftLogEntryError {}

impl fmt::Display for DecodeRaftLogEntryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof { needed, remaining } => write!(
                formatter,
                "Raft log-entry envelope needs {needed} bytes but only {remaining} remain"
            ),
            Self::InvalidMagic(magic) => write!(
                formatter,
                "Raft log-entry envelope magic {magic:02x?} is not RFLE"
            ),
            Self::UnsupportedVersion(version) => write!(
                formatter,
                "Raft log-entry envelope version {version} is not supported"
            ),
            Self::UnknownEntryKind(kind) => {
                write!(formatter, "Raft log entry kind {kind} is unknown")
            }
            Self::IndexAtMaximum => formatter.write_str(
                "Raft log-entry envelope stores the maximum log index, which replay cannot advance past",
            ),
            Self::InvalidMembership(error) => {
                write!(formatter, "Raft log entry membership is invalid: {error}")
            }
            Self::NonCanonicalMembershipOrder {
                member_kind,
                previous,
                actual,
            } => write!(
                formatter,
                "Raft log entry {member_kind} are not in canonical ascending node-id order: {} precedes {}",
                previous.0,
                actual.0
            ),
            Self::ChecksumMismatch { expected, actual } => write!(
                formatter,
                "Raft log-entry envelope stored checksum {expected:#010x} does not match computed checksum {actual:#010x}"
            ),
            Self::TrailingBytes(remaining) => write!(
                formatter,
                "Raft log-entry envelope has {remaining} trailing bytes"
            ),
        }
    }
}

impl Error for DecodeRaftLogEntryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidMembership(error) => Some(error),
            Self::UnexpectedEof { .. }
            | Self::InvalidMagic(_)
            | Self::UnsupportedVersion(_)
            | Self::UnknownEntryKind(_)
            | Self::IndexAtMaximum
            | Self::NonCanonicalMembershipOrder { .. }
            | Self::ChecksumMismatch { .. }
            | Self::TrailingBytes(_) => None,
        }
    }
}

impl From<CursorError> for DecodeRaftLogEntryError {
    fn from(error: CursorError) -> Self {
        match error {
            CursorError::UnexpectedEof { needed, remaining } => {
                Self::UnexpectedEof { needed, remaining }
            }
            CursorError::TrailingBytes(remaining) => Self::TrailingBytes(remaining),
        }
    }
}

impl From<ChecksumError> for DecodeRaftLogEntryError {
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

#[cfg(test)]
use rafter::{ConfigurationEntry, ConfigurationId, JointMembership, LogIndex, MembershipSet, Term};

#[cfg(test)]
#[path = "log_entry_test.rs"]
mod tests;
