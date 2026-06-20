use std::{error::Error, fmt};

use rafter::{
    ConfigurationEntry, ConfigurationId, JointMembership, LogEntryKind, LogIndex, MembershipSet,
    MembershipValidationError, NodeId, Term,
};

use crate::checksum::crc32;

/// Magic prefix for the persisted Raft log-entry envelope.
pub const RAFT_LOG_ENTRY_MAGIC: [u8; 4] = *b"RFLE";
/// Current persisted Raft log-entry envelope version.
pub const RAFT_LOG_ENTRY_VERSION: u8 = 1;

const RAFT_LOG_ENTRY_KIND_APPLICATION: u8 = 0;
const RAFT_LOG_ENTRY_KIND_STABLE_CONFIGURATION: u8 = 1;
const RAFT_LOG_ENTRY_KIND_JOINT_CONFIGURATION: u8 = 2;
const RAFT_LOG_ENTRY_KIND_NOOP: u8 = 3;

/// One Raft log entry after assigning its durable log index.
///
/// `kind` carries either an opaque application payload or a stable/joint
/// configuration entry for membership changes. The codec preserves that
/// distinction so replay can rebuild both application history and Raft
/// membership state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedRaftLogEntry {
    /// Durable Raft log index assigned by the log segment.
    pub index: LogIndex,
    /// Raft term stored with the entry.
    pub term: Term,
    /// Application payload or configuration-membership entry.
    pub kind: LogEntryKind,
}

impl PersistedRaftLogEntry {
    /// Builds one persisted application log entry.
    #[must_use]
    pub fn application(index: LogIndex, term: Term, payload: Vec<u8>) -> Self {
        Self {
            index,
            term,
            kind: LogEntryKind::application(payload),
        }
    }

    /// Builds one persisted membership-configuration log entry.
    #[must_use]
    pub fn configuration(index: LogIndex, term: Term, configuration: ConfigurationEntry) -> Self {
        Self {
            index,
            term,
            kind: LogEntryKind::configuration(configuration),
        }
    }

    /// Builds one persisted no-op log entry.
    #[must_use]
    pub const fn noop(index: LogIndex, term: Term) -> Self {
        Self {
            index,
            term,
            kind: LogEntryKind::noop(),
        }
    }

    /// Returns the application payload when this entry carries application
    /// data.
    #[must_use]
    pub fn application_payload(&self) -> Option<&[u8]> {
        self.kind.application_payload()
    }

    /// Returns the configuration entry when this entry carries a membership
    /// change.
    #[must_use]
    pub fn configuration_entry(&self) -> Option<&ConfigurationEntry> {
        self.kind.configuration_entry()
    }
}

/// Error returned when a Raft log entry cannot be encoded into the persisted
/// envelope format.
///
/// This enum is exhaustive because encode failures are limited to envelope
/// size bounds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EncodeRaftLogEntryError {
    /// Application payload length does not fit in the u32 length prefix.
    PayloadTooLarge { len: usize },
    /// Stable or joint membership contains more node ids than the format can
    /// represent.
    TooManyMembers { len: usize },
}

/// Error returned when a persisted Raft log entry envelope cannot be decoded or
/// verified.
///
/// This enum is exhaustive because the envelope format is closed over these
/// corruption and format failures.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecodeRaftLogEntryError {
    /// The envelope ended before the requested field could be read.
    UnexpectedEof { needed: usize, remaining: usize },
    /// The envelope magic did not match [`RAFT_LOG_ENTRY_MAGIC`].
    InvalidMagic([u8; 4]),
    /// The version byte is not supported by this decoder.
    UnsupportedVersion(u8),
    /// The entry-kind tag is not a known application or configuration variant.
    UnknownEntryKind(u8),
    /// A stable or joint membership entry failed Raft membership validation.
    InvalidMembership(MembershipValidationError),
    /// The stored checksum did not match the envelope bytes.
    ChecksumMismatch { expected: u32, actual: u32 },
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
            Self::InvalidMembership(error) => {
                write!(formatter, "Raft log entry membership is invalid: {error}")
            }
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
            | Self::ChecksumMismatch { .. }
            | Self::TrailingBytes(_) => None,
        }
    }
}

/// Encodes one persisted Raft log entry into a versioned, checksummed envelope.
///
/// Layout:
///
/// ```text
/// magic[4] | version[1] | index[u64] | term[u64] | kind[u8] |
/// kind_payload | crc32[u32]
/// ```
///
/// The checksum covers every byte before the checksum field.
///
/// # Errors
///
/// Returns [`EncodeRaftLogEntryError::PayloadTooLarge`] when the payload cannot
/// be represented in the envelope format.
pub fn encode_raft_log_entry(
    entry: &PersistedRaftLogEntry,
) -> Result<Vec<u8>, EncodeRaftLogEntryError> {
    let mut writer = Writer::new();
    writer.bytes(&RAFT_LOG_ENTRY_MAGIC);
    writer.u8(RAFT_LOG_ENTRY_VERSION);
    writer.u64(entry.index.0);
    writer.u64(entry.term.0);
    write_log_entry_kind(&mut writer, &entry.kind)?;

    let checksum = crc32(writer.as_slice());
    writer.u32(checksum);
    Ok(writer.finish())
}

/// Decodes and verifies one persisted Raft log-entry envelope.
///
/// # Errors
///
/// Returns [`DecodeRaftLogEntryError`] when the envelope is malformed, uses an
/// unsupported version, has trailing bytes, or fails checksum verification.
pub fn decode_raft_log_entry(
    envelope: &[u8],
) -> Result<PersistedRaftLogEntry, DecodeRaftLogEntryError> {
    let without_checksum_len =
        envelope
            .len()
            .checked_sub(4)
            .ok_or(DecodeRaftLogEntryError::UnexpectedEof {
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
        return Err(DecodeRaftLogEntryError::ChecksumMismatch {
            expected: expected_checksum,
            actual: actual_checksum,
        });
    }

    let mut reader = Reader::new(&envelope[..without_checksum_len]);
    let magic = reader.magic()?;
    if magic != RAFT_LOG_ENTRY_MAGIC {
        return Err(DecodeRaftLogEntryError::InvalidMagic(magic));
    }

    let version = reader.u8()?;
    if version != RAFT_LOG_ENTRY_VERSION {
        return Err(DecodeRaftLogEntryError::UnsupportedVersion(version));
    }

    let index = LogIndex(reader.u64()?);
    let term = Term(reader.u64()?);
    let kind = read_log_entry_kind(&mut reader)?;
    reader.finish()?;

    Ok(PersistedRaftLogEntry { index, term, kind })
}

fn write_log_entry_kind(
    writer: &mut Writer,
    kind: &LogEntryKind,
) -> Result<(), EncodeRaftLogEntryError> {
    match kind {
        LogEntryKind::Application(payload) => {
            writer.u8(RAFT_LOG_ENTRY_KIND_APPLICATION);
            write_payload(writer, payload)?;
        }
        LogEntryKind::Configuration(ConfigurationEntry::Stable {
            config_id,
            membership,
        }) => {
            writer.u8(RAFT_LOG_ENTRY_KIND_STABLE_CONFIGURATION);
            writer.u64(config_id.0);
            write_membership_set(writer, membership)?;
        }
        LogEntryKind::Configuration(ConfigurationEntry::Joint {
            config_id,
            membership,
        }) => {
            writer.u8(RAFT_LOG_ENTRY_KIND_JOINT_CONFIGURATION);
            writer.u64(config_id.0);
            write_membership_set(writer, membership.old())?;
            write_membership_set(writer, membership.new_membership())?;
        }
        LogEntryKind::Noop => {
            writer.u8(RAFT_LOG_ENTRY_KIND_NOOP);
        }
    }
    Ok(())
}

fn write_payload(writer: &mut Writer, payload: &[u8]) -> Result<(), EncodeRaftLogEntryError> {
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| EncodeRaftLogEntryError::PayloadTooLarge { len: payload.len() })?;
    writer.u32(payload_len);
    writer.bytes(payload);
    Ok(())
}

fn write_membership_set(
    writer: &mut Writer,
    membership: &MembershipSet,
) -> Result<(), EncodeRaftLogEntryError> {
    write_node_ids(writer, membership.voters())?;
    write_node_ids(writer, membership.learners())
}

fn write_node_ids(writer: &mut Writer, node_ids: &[NodeId]) -> Result<(), EncodeRaftLogEntryError> {
    let len =
        u32::try_from(node_ids.len()).map_err(|_| EncodeRaftLogEntryError::TooManyMembers {
            len: node_ids.len(),
        })?;
    writer.u32(len);
    for node_id in node_ids {
        writer.u64(node_id.0);
    }
    Ok(())
}

fn read_application_kind(reader: &mut Reader<'_>) -> Result<LogEntryKind, DecodeRaftLogEntryError> {
    let payload_len = reader.u32()? as usize;
    let payload = reader.take(payload_len)?.to_vec();
    Ok(LogEntryKind::application(payload))
}

fn read_log_entry_kind(reader: &mut Reader<'_>) -> Result<LogEntryKind, DecodeRaftLogEntryError> {
    let kind = reader.u8()?;
    match kind {
        RAFT_LOG_ENTRY_KIND_APPLICATION => read_application_kind(reader),
        RAFT_LOG_ENTRY_KIND_STABLE_CONFIGURATION => {
            let config_id = ConfigurationId(reader.u64()?);
            let membership = read_membership_set(reader)?;
            Ok(LogEntryKind::configuration(ConfigurationEntry::stable(
                config_id, membership,
            )))
        }
        RAFT_LOG_ENTRY_KIND_JOINT_CONFIGURATION => {
            let config_id = ConfigurationId(reader.u64()?);
            let old = read_membership_set(reader)?;
            let new = read_membership_set(reader)?;
            Ok(LogEntryKind::configuration(ConfigurationEntry::joint(
                config_id,
                JointMembership::new(old, new),
            )))
        }
        RAFT_LOG_ENTRY_KIND_NOOP => Ok(LogEntryKind::noop()),
        unknown => Err(DecodeRaftLogEntryError::UnknownEntryKind(unknown)),
    }
}

fn read_membership_set(reader: &mut Reader<'_>) -> Result<MembershipSet, DecodeRaftLogEntryError> {
    let voters = read_node_ids(reader)?;
    let learners = read_node_ids(reader)?;
    MembershipSet::new(voters, learners).map_err(DecodeRaftLogEntryError::InvalidMembership)
}

fn read_node_ids(reader: &mut Reader<'_>) -> Result<Vec<NodeId>, DecodeRaftLogEntryError> {
    let len = reader.u32()? as usize;
    // Bound the reservation by remaining bytes: a corrupt persisted entry
    // could claim billions of members it has no bytes for.
    let mut node_ids = Vec::with_capacity(len.min(reader.remaining()));
    for _ in 0..len {
        node_ids.push(NodeId(reader.u64()?));
    }
    Ok(node_ids)
}

struct Writer {
    bytes: Vec<u8>,
}

impl Writer {
    fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }

    fn bytes(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
    }

    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }
}

struct Reader<'a> {
    envelope: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    fn new(envelope: &'a [u8]) -> Self {
        Self {
            envelope,
            position: 0,
        }
    }

    fn finish(&self) -> Result<(), DecodeRaftLogEntryError> {
        let remaining = self.remaining();
        if remaining == 0 {
            Ok(())
        } else {
            Err(DecodeRaftLogEntryError::TrailingBytes(remaining))
        }
    }

    /// Bytes not yet consumed — the ceiling on how many elements a decoded
    /// count could legitimately introduce, so a hostile length prefix
    /// cannot force an unbounded reservation.
    fn remaining(&self) -> usize {
        self.envelope.len() - self.position
    }

    fn magic(&mut self) -> Result<[u8; 4], DecodeRaftLogEntryError> {
        let bytes = self.take(4)?;
        Ok([bytes[0], bytes[1], bytes[2], bytes[3]])
    }

    fn u8(&mut self) -> Result<u8, DecodeRaftLogEntryError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, DecodeRaftLogEntryError> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn u64(&mut self) -> Result<u64, DecodeRaftLogEntryError> {
        let bytes = self.take(8)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], DecodeRaftLogEntryError> {
        let remaining = self.envelope.len() - self.position;
        if remaining < len {
            return Err(DecodeRaftLogEntryError::UnexpectedEof {
                needed: len,
                remaining,
            });
        }

        let start = self.position;
        self.position += len;
        Ok(&self.envelope[start..self.position])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(payload: &[u8]) -> PersistedRaftLogEntry {
        PersistedRaftLogEntry::application(LogIndex(42), Term(7), payload.to_vec())
    }

    fn stable_configuration_entry() -> PersistedRaftLogEntry {
        PersistedRaftLogEntry::configuration(
            LogIndex(42),
            Term(7),
            ConfigurationEntry::stable(
                ConfigurationId(3),
                MembershipSet::new(vec![NodeId(1), NodeId(2)], vec![NodeId(3)])
                    .expect("membership is valid"),
            ),
        )
    }

    fn joint_configuration_entry() -> PersistedRaftLogEntry {
        PersistedRaftLogEntry::configuration(
            LogIndex(42),
            Term(7),
            ConfigurationEntry::joint(
                ConfigurationId(4),
                JointMembership::new(
                    MembershipSet::new(vec![NodeId(1), NodeId(2)], vec![NodeId(3)])
                        .expect("old membership is valid"),
                    MembershipSet::new(vec![NodeId(1), NodeId(3)], Vec::new())
                        .expect("new membership is valid"),
                ),
            ),
        )
    }

    fn replace_checksum(encoded: &mut [u8]) {
        let checksum_position = encoded.len() - 4;
        let checksum = crc32(&encoded[..checksum_position]);
        encoded[checksum_position..].copy_from_slice(&checksum.to_be_bytes());
    }

    #[test]
    fn log_entry_round_trips_through_envelope() {
        let entry = entry(b"\0opaque raft payload\0");

        let encoded = encode_raft_log_entry(&entry).expect("entry encodes");

        assert_eq!(&encoded[..4], &RAFT_LOG_ENTRY_MAGIC);
        assert_eq!(encoded[4], RAFT_LOG_ENTRY_VERSION);
        assert_eq!(decode_raft_log_entry(&encoded), Ok(entry));
    }

    #[test]
    fn stable_configuration_log_entry_round_trips_through_envelope() {
        let entry = stable_configuration_entry();

        let encoded = encode_raft_log_entry(&entry).expect("entry encodes");

        assert_eq!(decode_raft_log_entry(&encoded), Ok(entry));
    }

    #[test]
    fn joint_configuration_log_entry_round_trips_through_envelope() {
        let entry = joint_configuration_entry();

        let encoded = encode_raft_log_entry(&entry).expect("entry encodes");

        assert_eq!(decode_raft_log_entry(&encoded), Ok(entry));
    }

    #[test]
    fn noop_log_entry_round_trips_through_envelope() {
        let entry = PersistedRaftLogEntry::noop(LogIndex(42), Term(7));

        let encoded = encode_raft_log_entry(&entry).expect("entry encodes");

        assert_eq!(decode_raft_log_entry(&encoded), Ok(entry));
    }

    #[test]
    fn empty_payload_log_entry_round_trips_through_envelope() {
        let entry = entry(b"");

        let encoded = encode_raft_log_entry(&entry).expect("entry encodes");

        assert_eq!(decode_raft_log_entry(&encoded), Ok(entry));
    }

    #[test]
    fn larger_payload_log_entry_round_trips_through_envelope() {
        let payload: Vec<_> = (0..=255).cycle().take(1024).collect();
        let entry = entry(&payload);

        let encoded = encode_raft_log_entry(&entry).expect("entry encodes");

        assert_eq!(decode_raft_log_entry(&encoded), Ok(entry));
    }

    #[test]
    fn decode_rejects_corrupt_log_entry_checksum() {
        let mut encoded = encode_raft_log_entry(&entry(b"command")).expect("entry encodes");
        let last_payload_byte = encoded.len() - 5;
        encoded[last_payload_byte] ^= 0xff;

        assert!(matches!(
            decode_raft_log_entry(&encoded),
            Err(DecodeRaftLogEntryError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn decode_rejects_invalid_magic_after_checksum_passes() {
        let mut encoded = encode_raft_log_entry(&entry(b"command")).expect("entry encodes");
        encoded[0] = b'X';
        replace_checksum(&mut encoded);

        assert_eq!(
            decode_raft_log_entry(&encoded),
            Err(DecodeRaftLogEntryError::InvalidMagic(*b"XFLE"))
        );
    }

    #[test]
    fn decode_rejects_unsupported_version_after_checksum_passes() {
        let mut encoded = encode_raft_log_entry(&entry(b"command")).expect("entry encodes");
        encoded[4] = 99;
        replace_checksum(&mut encoded);

        assert_eq!(
            decode_raft_log_entry(&encoded),
            Err(DecodeRaftLogEntryError::UnsupportedVersion(99))
        );
    }

    #[test]
    fn decode_rejects_truncated_log_entry() {
        assert_eq!(
            decode_raft_log_entry(&[]),
            Err(DecodeRaftLogEntryError::UnexpectedEof {
                needed: 4,
                remaining: 0,
            })
        );
    }

    #[test]
    fn decode_rejects_truncated_payload_after_checksum_passes() {
        let mut encoded = encode_raft_log_entry(&entry(b"command")).expect("entry encodes");
        encoded[25] = 99;
        replace_checksum(&mut encoded);

        assert_eq!(
            decode_raft_log_entry(&encoded),
            Err(DecodeRaftLogEntryError::UnexpectedEof {
                needed: 99,
                remaining: 7,
            })
        );
    }

    #[test]
    fn decode_rejects_trailing_bytes_after_checksum_passes() {
        let mut encoded = encode_raft_log_entry(&entry(b"command")).expect("entry encodes");
        encoded[25] = 3;
        replace_checksum(&mut encoded);

        assert_eq!(
            decode_raft_log_entry(&encoded),
            Err(DecodeRaftLogEntryError::TrailingBytes(4))
        );
    }
}
