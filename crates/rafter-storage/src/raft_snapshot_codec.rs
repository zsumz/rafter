use std::{error::Error, fmt};

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion,
    CommittedConfiguration, ConfigurationId, LogIndex, MembershipConfig, MembershipSet,
    MembershipValidationError, NodeId, RaftSnapshotMetadata, SnapshotCommittedConfiguration,
    SnapshotGroupId, SnapshotIdError, SnapshotMetadataError, Term,
};

use crate::checksum::crc32;

mod cursor;

use cursor::{Reader, Writer};

/// Magic prefix for the persisted Raft snapshot envelope.
pub const RAFT_SNAPSHOT_MAGIC: [u8; 4] = *b"RFSN";
/// Current persisted Raft snapshot envelope version.
pub const RAFT_SNAPSHOT_VERSION: u8 = 1;
const MEMBERSHIP_ABSENT: u8 = 0;
const MEMBERSHIP_PRESENT: u8 = 1;
const MEMBERSHIP_STABLE: u8 = 0;
const MEMBERSHIP_JOINT: u8 = 1;

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
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SnapshotEnvelopeHeader {
    pub metadata: RaftSnapshotMetadata,
    pub payload_len: u64,
    pub payload_crc32: u32,
    pub header_len: u64,
}

/// Errors returned while decoding a persisted Raft snapshot envelope.
///
/// This enum is exhaustive because the envelope format is closed over these
/// corruption, format, and metadata-validation failures.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecodeRaftSnapshotError {
    UnexpectedEof { needed: usize, remaining: usize },
    InvalidMagic([u8; 4]),
    UnsupportedVersion(u8),
    InvalidGroupId(SnapshotIdError),
    InvalidApplicationKind(SnapshotIdError),
    InvalidUtf8 { field: &'static str },
    InvalidApplicationVersion(SnapshotMetadataError),
    InvalidMetadata(SnapshotMetadataError),
    InvalidMembership(MembershipValidationError),
    UnknownMembershipFlag(u8),
    UnknownMembershipKind(u8),
    PayloadChecksumMismatch { expected: u32, actual: u32 },
    EnvelopeChecksumMismatch { expected: u32, actual: u32 },
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
            | Self::UnknownMembershipFlag(_)
            | Self::UnknownMembershipKind(_)
            | Self::PayloadChecksumMismatch { .. }
            | Self::EnvelopeChecksumMismatch { .. }
            | Self::TrailingBytes(_) => None,
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

    let checksum = crc32(writer.as_slice());
    writer.u32(checksum);
    Ok(writer.finish())
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
    writer.string("snapshot group id", metadata.group_id.as_str())?;
    writer.u64(metadata.writer_id.0);
    writer.u64(metadata.last_included_index.0);
    writer.u64(metadata.last_included_term.0);
    writer.u64(metadata.hard_state_term.0);
    writer.string(
        "application snapshot kind",
        metadata.application.kind.as_str(),
    )?;
    writer.u16(metadata.application.version.get());
    encode_optional_committed_configuration(
        &mut writer,
        metadata.committed_configuration.as_ref(),
    )?;
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

    let group_id = SnapshotGroupId::new(reader.string("snapshot group id")?)
        .map_err(DecodeRaftSnapshotError::InvalidGroupId)?;
    let writer_id = NodeId(reader.u64()?);
    let last_included_index = LogIndex(reader.u64()?);
    let last_included_term = Term(reader.u64()?);
    let hard_state_term = Term(reader.u64()?);
    let application_kind =
        ApplicationSnapshotKind::new(reader.string("application snapshot kind")?)
            .map_err(DecodeRaftSnapshotError::InvalidApplicationKind)?;
    let application_version = ApplicationSnapshotVersion::new(reader.u16()?)
        .map_err(DecodeRaftSnapshotError::InvalidApplicationVersion)?;
    let committed_configuration = decode_optional_committed_configuration(&mut reader)?;
    let payload_len = reader.u64()?;

    let mut metadata = RaftSnapshotMetadata::new(
        group_id,
        writer_id,
        last_included_index,
        last_included_term,
        hard_state_term,
        ApplicationSnapshotMetadata::new(application_kind, application_version),
    )
    .map_err(DecodeRaftSnapshotError::InvalidMetadata)?;
    if let Some(committed_configuration) = committed_configuration {
        metadata = metadata.with_committed_configuration(committed_configuration);
    }

    Ok(SnapshotEnvelopeHeader {
        metadata,
        payload_len,
        payload_crc32: 0,
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
    let without_checksum_len =
        envelope
            .len()
            .checked_sub(4)
            .ok_or(DecodeRaftSnapshotError::UnexpectedEof {
                needed: 4,
                remaining: envelope.len(),
            })?;
    let expected_checksum = {
        let checksum_bytes = &envelope[without_checksum_len..];
        u32::from_be_bytes([
            checksum_bytes[0],
            checksum_bytes[1],
            checksum_bytes[2],
            checksum_bytes[3],
        ])
    };
    let actual_checksum = crc32(&envelope[..without_checksum_len]);
    if expected_checksum != actual_checksum {
        return Err(DecodeRaftSnapshotError::EnvelopeChecksumMismatch {
            expected: expected_checksum,
            actual: actual_checksum,
        });
    }

    let body = &envelope[..without_checksum_len];
    let header = decode_raft_snapshot_header(body)?;
    let mut reader = Reader::new(body);
    let header_len =
        usize::try_from(header.header_len).map_err(|_| DecodeRaftSnapshotError::UnexpectedEof {
            needed: usize::MAX,
            remaining: body.len(),
        })?;
    reader.take(header_len)?;
    let remaining = without_checksum_len - header_len;
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

fn encode_optional_committed_configuration(
    writer: &mut Writer,
    committed: Option<&SnapshotCommittedConfiguration>,
) -> Result<(), EncodeRaftSnapshotError> {
    if let Some(committed) = committed {
        writer.u8(MEMBERSHIP_PRESENT);
        if let Some(configuration) = committed.configuration {
            writer.u8(MEMBERSHIP_PRESENT);
            writer.u64(configuration.index.0);
            writer.u64(configuration.config_id.0);
        } else {
            writer.u8(MEMBERSHIP_ABSENT);
        }
        encode_membership_config(writer, &committed.membership)?;
    } else {
        writer.u8(MEMBERSHIP_ABSENT);
    }
    Ok(())
}

fn encode_membership_config(
    writer: &mut Writer,
    membership: &MembershipConfig,
) -> Result<(), EncodeRaftSnapshotError> {
    match membership {
        MembershipConfig::Stable(stable) => {
            writer.u8(MEMBERSHIP_STABLE);
            encode_membership_set(writer, stable)?;
        }
        MembershipConfig::Joint(joint) => {
            writer.u8(MEMBERSHIP_JOINT);
            encode_membership_set(writer, joint.old())?;
            encode_membership_set(writer, joint.new_membership())?;
        }
    }
    Ok(())
}

fn encode_membership_set(
    writer: &mut Writer,
    membership: &MembershipSet,
) -> Result<(), EncodeRaftSnapshotError> {
    let voter_count = u16::try_from(membership.voters().len()).map_err(|_| {
        EncodeRaftSnapshotError::TooManyMembers {
            member_kind: "voters",
            len: membership.voters().len(),
        }
    })?;
    writer.u16(voter_count);
    for voter in membership.voters() {
        writer.u64(voter.0);
    }

    let learner_count = u16::try_from(membership.learners().len()).map_err(|_| {
        EncodeRaftSnapshotError::TooManyMembers {
            member_kind: "learners",
            len: membership.learners().len(),
        }
    })?;
    writer.u16(learner_count);
    for learner in membership.learners() {
        writer.u64(learner.0);
    }
    Ok(())
}

fn decode_optional_committed_configuration(
    reader: &mut Reader<'_>,
) -> Result<Option<SnapshotCommittedConfiguration>, DecodeRaftSnapshotError> {
    match reader.u8()? {
        MEMBERSHIP_ABSENT => Ok(None),
        MEMBERSHIP_PRESENT => {
            let configuration = match reader.u8()? {
                MEMBERSHIP_ABSENT => None,
                MEMBERSHIP_PRESENT => Some(CommittedConfiguration {
                    index: LogIndex(reader.u64()?),
                    config_id: ConfigurationId(reader.u64()?),
                }),
                flag => return Err(DecodeRaftSnapshotError::UnknownMembershipFlag(flag)),
            };
            let membership = decode_membership_config(reader)?;
            Ok(Some(SnapshotCommittedConfiguration::new(
                configuration,
                membership,
            )))
        }
        flag => Err(DecodeRaftSnapshotError::UnknownMembershipFlag(flag)),
    }
}

fn decode_membership_config(
    reader: &mut Reader<'_>,
) -> Result<MembershipConfig, DecodeRaftSnapshotError> {
    match reader.u8()? {
        MEMBERSHIP_STABLE => decode_membership_set(reader).map(MembershipConfig::stable),
        MEMBERSHIP_JOINT => {
            let old = decode_membership_set(reader)?;
            let new = decode_membership_set(reader)?;
            Ok(MembershipConfig::joint(old, new))
        }
        other => Err(DecodeRaftSnapshotError::UnknownMembershipKind(other)),
    }
}

fn decode_membership_set(
    reader: &mut Reader<'_>,
) -> Result<MembershipSet, DecodeRaftSnapshotError> {
    let voters = decode_node_set(reader)?;
    let learners = decode_node_set(reader)?;
    MembershipSet::new(voters, learners).map_err(DecodeRaftSnapshotError::InvalidMembership)
}

fn decode_node_set(reader: &mut Reader<'_>) -> Result<Vec<NodeId>, DecodeRaftSnapshotError> {
    let count = reader.u16()? as usize;
    (0..count)
        .map(|_| reader.u64().map(NodeId))
        .collect::<Result<Vec<_>, _>>()
}

#[cfg(test)]
#[path = "raft_snapshot_codec_test.rs"]
mod tests;
