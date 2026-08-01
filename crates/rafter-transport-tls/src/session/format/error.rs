//! Typed durable session-state codec failures.

use std::{error::Error, fmt};

use crate::{IdentityError, LimitError, PeerId};

use super::SessionIdentityField;

/// Durable session state could not be represented canonically.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EncodeTransportSessionStateError {
    /// One identity length did not fit the version-1 `u8` field.
    IdentityTooLong {
        /// Identity field being encoded.
        field: SessionIdentityField,
        /// Actual UTF-8 byte length.
        len: usize,
    },
    /// The configured peer bound did not fit the version-1 `u16` field.
    PeerLimitTooLarge {
        /// Configured peer-record bound.
        value: usize,
    },
    /// The retained record count did not fit the version-1 `u16` field.
    PeerCountTooLarge {
        /// Retained peer-record count.
        value: usize,
    },
    /// Retained records exceeded the envelope's configured bound.
    PeerCountExceedsLimit {
        /// Retained peer-record count.
        count: usize,
        /// Configured maximum peer records.
        maximum: usize,
    },
    /// An empty peer record had no high-water mark in either direction.
    EmptyPeerRecord,
}

impl fmt::Display for EncodeTransportSessionStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IdentityTooLong { field, len } => {
                write!(
                    formatter,
                    "session-state {field} is {len} bytes and does not fit u8"
                )
            }
            Self::PeerLimitTooLarge { value } => write!(
                formatter,
                "session-state peer limit {value} does not fit the version-1 u16 field"
            ),
            Self::PeerCountTooLarge { value } => write!(
                formatter,
                "session-state peer count {value} does not fit the version-1 u16 field"
            ),
            Self::PeerCountExceedsLimit { count, maximum } => write!(
                formatter,
                "session state retains {count} peers, exceeding configured maximum {maximum}"
            ),
            Self::EmptyPeerRecord => {
                formatter.write_str("session state cannot encode an empty peer record")
            }
        }
    }
}

impl Error for EncodeTransportSessionStateError {}

/// Durable session-state bytes were malformed, corrupt, or noncanonical.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DecodeTransportSessionStateError {
    /// The input ended before one fixed or declared-length field completed.
    UnexpectedEnd {
        /// Bytes required by the field.
        needed: usize,
        /// Bytes remaining in the input.
        remaining: usize,
    },
    /// The leading format tag was not [`super::SESSION_STATE_MAGIC`].
    InvalidMagic {
        /// Actual leading bytes.
        actual: [u8; 8],
    },
    /// The file uses a session-state version this crate does not read.
    UnsupportedVersion {
        /// Version carried by the file.
        version: u16,
    },
    /// One identity field was not valid UTF-8.
    InvalidUtf8 {
        /// Identity field being decoded.
        field: SessionIdentityField,
        /// Zero-based remote record index, when applicable.
        record: Option<usize>,
    },
    /// One decoded identity violated the bounded identity contract.
    InvalidIdentity {
        /// Identity field being decoded.
        field: SessionIdentityField,
        /// Zero-based remote record index, when applicable.
        record: Option<usize>,
        /// Identity validation failure.
        source: IdentityError,
    },
    /// The encoded peer-record limit was invalid.
    InvalidPeerLimit {
        /// Limit validation failure.
        source: LimitError,
    },
    /// The encoded record count exceeded the encoded peer bound.
    PeerCountExceedsLimit {
        /// Encoded record count.
        count: usize,
        /// Encoded maximum peer records.
        maximum: usize,
    },
    /// One peer record had neither an outbound nor inbound high-water.
    EmptyPeerRecord {
        /// Peer named by the empty record.
        peer: PeerId,
    },
    /// Peer records were duplicated or not strictly increasing by identity.
    NonCanonicalPeerOrder {
        /// Previous record's peer identity.
        previous: PeerId,
        /// Peer identity that did not follow it strictly.
        actual: PeerId,
    },
    /// The trailing CRC32 did not match the preceding state bytes.
    ChecksumMismatch {
        /// Checksum carried by the file.
        expected: u32,
        /// Checksum calculated from the file body.
        actual: u32,
    },
    /// Bytes remained after the checksum.
    TrailingBytes {
        /// Number of unconsumed bytes.
        remaining: usize,
    },
}

impl fmt::Display for DecodeTransportSessionStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEnd { needed, remaining } => write!(
                formatter,
                "session state needs {needed} bytes but only {remaining} remain"
            ),
            Self::InvalidMagic { actual } => {
                write!(formatter, "invalid session-state magic {actual:?}")
            }
            Self::UnsupportedVersion { version } => {
                write!(formatter, "unsupported session-state version {version}")
            }
            Self::InvalidUtf8 { field, record } => {
                write_identity_location(formatter, *field, *record)?;
                formatter.write_str(" is not valid UTF-8")
            }
            Self::InvalidIdentity {
                field,
                record,
                source,
            } => {
                write_identity_location(formatter, *field, *record)?;
                write!(formatter, " is invalid: {source}")
            }
            Self::InvalidPeerLimit { source } => {
                write!(formatter, "invalid session-state peer limit: {source}")
            }
            Self::PeerCountExceedsLimit { count, maximum } => write!(
                formatter,
                "session state carries {count} peer records, exceeding its bound {maximum}"
            ),
            Self::EmptyPeerRecord { peer } => {
                write!(
                    formatter,
                    "session state carries an empty record for {peer}"
                )
            }
            Self::NonCanonicalPeerOrder { previous, actual } => write!(
                formatter,
                "session-state peer {actual} does not sort strictly after {previous}"
            ),
            Self::ChecksumMismatch { expected, actual } => write!(
                formatter,
                "session-state checksum {expected:#010x} does not match {actual:#010x}"
            ),
            Self::TrailingBytes { remaining } => {
                write!(formatter, "session state has {remaining} trailing bytes")
            }
        }
    }
}

impl Error for DecodeTransportSessionStateError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidIdentity { source, .. } => Some(source),
            Self::InvalidPeerLimit { source } => Some(source),
            _ => None,
        }
    }
}

impl fmt::Display for SessionIdentityField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cluster => formatter.write_str("cluster identity"),
            Self::LocalPeer => formatter.write_str("local peer identity"),
            Self::RemotePeer => formatter.write_str("remote peer identity"),
        }
    }
}

fn write_identity_location(
    formatter: &mut fmt::Formatter<'_>,
    field: SessionIdentityField,
    record: Option<usize>,
) -> fmt::Result {
    match record {
        Some(record) => write!(formatter, "session-state {field} at record {record}"),
        None => write!(formatter, "session-state {field}"),
    }
}
