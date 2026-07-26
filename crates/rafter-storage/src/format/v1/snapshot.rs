//! Version-1 persisted-snapshot envelope and payload framing.
//!
//! Snapshot metadata field order lives in `format::v1::snapshot_metadata`; this
//! module owns the public value/error vocabulary, RFSN framing, payload bytes,
//! and the payload and envelope checksums.

use std::{error::Error, fmt};

use rafter::{
    MembershipValidationError, NodeId, RaftSnapshotMetadata, SnapshotIdError, SnapshotMetadataError,
};

use crate::{
    checksum::crc32,
    format::{
        finish_checksummed,
        v1::snapshot_metadata::{decode_snapshot_metadata, encode_snapshot_metadata},
        verify_checksum, ChecksumError, CursorError, Reader, Writer,
    },
};

/// Magic prefix for the persisted Raft snapshot envelope.
pub const RAFT_SNAPSHOT_MAGIC: [u8; 4] = *b"RFSN";
/// Current persisted Raft snapshot envelope version.
pub const RAFT_SNAPSHOT_VERSION: u8 = 1;

/// A complete persisted Raft snapshot envelope in memory.
///
/// The metadata is Raft-visible snapshot state. The payload is opaque
/// application state protected by the envelope checksum.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedRaftSnapshot {
    pub metadata: RaftSnapshotMetadata,
    pub application_payload: Vec<u8>,
}

/// Errors returned while encoding a persisted Raft snapshot envelope.
///
/// This enum is exhaustive because encode failures are limited to envelope
/// size bounds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EncodeRaftSnapshotError {
    /// A snapshot identity string does not fit in the u16 length prefix.
    StringTooLong { field: &'static str, len: usize },
    /// A snapshot membership set contains more node ids than the format can
    /// represent.
    TooManyMembers {
        member_kind: &'static str,
        len: usize,
    },
}

/// The decoded prefix of a snapshot envelope: everything before the payload
/// bytes. `header_len` is where the payload starts, so streaming readers can
/// verify or serve the payload without materializing it.
///
/// Deliberately carries no payload checksum. The checksum lives in the
/// envelope's trailer, past `payload_len` bytes this type has not seen, so
/// parsing a prefix cannot know it. A field here could only ever hold a
/// placeholder, and a placeholder that flows into `RaftSnapshot::new` produces
/// a wrong `transfer_id()` in silence. Callers that need the checksum verify
/// the payload and receive it from that verification instead.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SnapshotEnvelopeHeader {
    pub metadata: RaftSnapshotMetadata,
    pub payload_len: u64,
    pub header_len: u64,
}

/// Errors returned while decoding a persisted Raft snapshot envelope.
///
/// This enum is exhaustive because the envelope format is closed over these
/// corruption, format, and metadata-validation failures.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecodeRaftSnapshotError {
    UnexpectedEof {
        needed: usize,
        remaining: usize,
    },
    InvalidMagic([u8; 4]),
    UnsupportedVersion(u8),
    InvalidGroupId(SnapshotIdError),
    InvalidApplicationKind(SnapshotIdError),
    InvalidUtf8 {
        field: &'static str,
    },
    InvalidApplicationVersion(SnapshotMetadataError),
    InvalidMetadata(SnapshotMetadataError),
    InvalidMembership(MembershipValidationError),
    /// Member ids were valid but not stored in canonical ascending order.
    NonCanonicalMembershipOrder {
        member_kind: &'static str,
        previous: NodeId,
        actual: NodeId,
    },
    UnknownMembershipFlag(u8),
    UnknownMembershipKind(u8),
    PayloadChecksumMismatch {
        expected: u32,
        actual: u32,
    },
    EnvelopeChecksumMismatch {
        expected: u32,
        actual: u32,
    },
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

/// Encodes one Raft snapshot into a versioned, checksummed envelope.
///
/// Layout:
///
/// ```text
/// magic[4] | version[1] |
/// group_id_len[u16] | group_id |
/// writer_node_id[u64] |
/// last_included_index[u64] | last_included_term[u64] |
/// hard_state_term[u64] |
/// application_kind_len[u16] | application_kind |
/// application_version[u16] |
/// committed_configuration_present[u8] |
/// committed_configuration if present:
///   configuration_present[u8] |
///   configuration_index[u64] | configuration_id[u64] if present |
///   membership |
/// application_payload_len[u64] | application_payload |
/// application_payload_crc32[u32] |
/// envelope_crc32[u32]
/// ```
///
/// The application payload checksum covers only the opaque application payload.
/// The envelope checksum covers every byte before the envelope checksum field,
/// including the application payload checksum.
///
/// Both checksums are CRC32 accidental-corruption checks, not adversarial
/// integrity proofs.
///
/// # Errors
///
/// Returns [`EncodeRaftSnapshotError`] when snapshot metadata or membership
/// counts cannot be represented in the envelope format.
pub fn encode_raft_snapshot(
    snapshot: &PersistedRaftSnapshot,
) -> Result<Vec<u8>, EncodeRaftSnapshotError> {
    let header = encode_raft_snapshot_header(
        &snapshot.metadata,
        snapshot.application_payload.len() as u64,
    )?;
    let mut writer = Writer::new();
    writer.bytes(&header);
    writer.bytes(&snapshot.application_payload);
    writer.u32(crc32(&snapshot.application_payload));

    Ok(finish_checksummed(writer))
}

/// Encodes snapshot metadata as the metadata-only RFSN envelope embedded in an
/// RFPT pending-transfer manifest.
///
/// This intentionally preserves the version-1 nested-envelope bytes while
/// sharing the metadata field grammar with complete snapshot encoding.
pub(crate) fn encode_raft_snapshot_metadata_envelope(
    metadata: &RaftSnapshotMetadata,
) -> Result<Vec<u8>, EncodeRaftSnapshotError> {
    let header = encode_raft_snapshot_header(metadata, 0)?;
    let mut writer = Writer::new();
    writer.bytes(&header);
    writer.u32(crc32(&[]));
    Ok(finish_checksummed(writer))
}

/// Encodes the envelope prefix — every byte before the application payload —
/// at the current version. Streaming writers emit this, then the payload,
/// then the payload and envelope checksums.
///
/// # Errors
///
/// Returns [`EncodeRaftSnapshotError`] when snapshot metadata or membership
/// counts cannot be represented in the envelope format.
pub(crate) fn encode_raft_snapshot_header(
    metadata: &RaftSnapshotMetadata,
    payload_len: u64,
) -> Result<Vec<u8>, EncodeRaftSnapshotError> {
    let mut writer = Writer::new();
    writer.bytes(&RAFT_SNAPSHOT_MAGIC);
    writer.u8(RAFT_SNAPSHOT_VERSION);
    encode_snapshot_metadata(&mut writer, metadata)?;
    writer.u64(payload_len);
    Ok(writer.finish())
}

/// Decodes the envelope prefix from `prefix`, which must hold at least the
/// complete header (callers read a bounded prefix of the file; headers are
/// metadata-sized). Verifies no checksums — streaming readers verify the
/// payload and envelope checksums as they consume the payload bytes.
///
/// # Errors
///
/// Returns [`DecodeRaftSnapshotError`] when the header is malformed, uses an
/// unsupported version, or carries invalid typed metadata.
pub(crate) fn decode_raft_snapshot_header(
    prefix: &[u8],
) -> Result<SnapshotEnvelopeHeader, DecodeRaftSnapshotError> {
    let mut reader = Reader::new(prefix);
    let magic = reader.magic()?;
    if magic != RAFT_SNAPSHOT_MAGIC {
        return Err(DecodeRaftSnapshotError::InvalidMagic(magic));
    }

    let version = reader.u8()?;
    if version != RAFT_SNAPSHOT_VERSION {
        return Err(DecodeRaftSnapshotError::UnsupportedVersion(version));
    }

    let metadata = decode_snapshot_metadata(&mut reader)?;
    let payload_len = reader.u64()?;

    Ok(SnapshotEnvelopeHeader {
        metadata,
        payload_len,
        header_len: reader.position() as u64,
    })
}

/// Decodes and verifies one Raft snapshot envelope.
///
/// # Errors
///
/// Returns [`DecodeRaftSnapshotError`] when the envelope is malformed, uses an
/// unsupported version, fails checksum verification, or contains invalid typed
/// Raft snapshot metadata.
///
/// # Panics
///
/// Does not panic on any input: the internal length conversions are bounded
/// by the size of the in-memory envelope slice.
pub fn decode_raft_snapshot(
    envelope: &[u8],
) -> Result<PersistedRaftSnapshot, DecodeRaftSnapshotError> {
    let body = verify_checksum(envelope)?;
    let header = decode_raft_snapshot_header(body)?;
    let mut reader = Reader::new(body);
    let header_len =
        usize::try_from(header.header_len).map_err(|_| DecodeRaftSnapshotError::UnexpectedEof {
            needed: usize::MAX,
            remaining: body.len(),
        })?;
    reader.take(header_len)?;
    let remaining = body.len() - header_len;
    if header.payload_len > remaining as u64 {
        return Err(DecodeRaftSnapshotError::UnexpectedEof {
            needed: usize::try_from(header.payload_len).unwrap_or(usize::MAX),
            remaining,
        });
    }
    let payload_len = usize::try_from(header.payload_len).map_err(|_| {
        DecodeRaftSnapshotError::UnexpectedEof {
            needed: usize::MAX,
            remaining,
        }
    })?;
    let application_payload = reader.take(payload_len)?.to_vec();
    let expected_payload_checksum = reader.u32()?;
    let actual_payload_checksum = crc32(&application_payload);
    if expected_payload_checksum != actual_payload_checksum {
        return Err(DecodeRaftSnapshotError::PayloadChecksumMismatch {
            expected: expected_payload_checksum,
            actual: actual_payload_checksum,
        });
    }
    reader.finish()?;

    Ok(PersistedRaftSnapshot {
        metadata: header.metadata,
        application_payload,
    })
}

#[cfg(test)]
#[path = "raft_snapshot_codec_test.rs"]
mod tests;
