//! Version-1 persisted-log-entry envelope grammar and canonical decoding.
//!
//! This module owns RFLE framing, log-entry tags, membership fields, and
//! checksum mapping. Segment framing and durable append live in the log store.

use std::{error::Error, fmt};

use rafter::{
    ConfigurationEntry, ConfigurationId, JointMembership, LogEntryKind, LogIndex, MembershipSet,
    MembershipValidationError, NodeId, Term,
};

use crate::format::{
    advanceable_log_index, finish_checksummed, verify_checksum, ChecksumError, CursorError, Reader,
    Writer,
};

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

/// Borrowed persisted log entry used when a caller already owns kernel log
/// entries and only needs to stamp durable indexes while encoding them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BorrowedPersistedRaftLogEntry<'a> {
    /// Durable Raft log index assigned by the log segment.
    pub index: LogIndex,
    /// Raft term stored with the entry.
    pub term: Term,
    /// Application payload or configuration-membership entry.
    pub kind: &'a LogEntryKind,
}

impl<'a> BorrowedPersistedRaftLogEntry<'a> {
    /// Builds a borrowed persisted log entry view.
    #[must_use]
    pub const fn new(index: LogIndex, term: Term, kind: &'a LogEntryKind) -> Self {
        Self { index, term, kind }
    }
}

impl<'a> From<&'a PersistedRaftLogEntry> for BorrowedPersistedRaftLogEntry<'a> {
    fn from(entry: &'a PersistedRaftLogEntry) -> Self {
        Self::new(entry.index, entry.term, &entry.kind)
    }
}

impl From<BorrowedPersistedRaftLogEntry<'_>> for PersistedRaftLogEntry {
    fn from(entry: BorrowedPersistedRaftLogEntry<'_>) -> Self {
        Self {
            index: entry.index,
            term: entry.term,
            kind: entry.kind.clone(),
        }
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
/// be represented in the envelope format, or
/// [`EncodeRaftLogEntryError::IndexAtMaximum`] when the entry sits at the one
/// log index replay could not advance past.
pub fn encode_raft_log_entry(
    entry: &PersistedRaftLogEntry,
) -> Result<Vec<u8>, EncodeRaftLogEntryError> {
    encode_borrowed_raft_log_entry(BorrowedPersistedRaftLogEntry::from(entry))
}

/// Encodes one borrowed persisted Raft log entry into a versioned, checksummed
/// envelope.
///
/// # Errors
///
/// Returns [`EncodeRaftLogEntryError::PayloadTooLarge`] when the payload cannot
/// be represented in the envelope format, or
/// [`EncodeRaftLogEntryError::IndexAtMaximum`] when the entry sits at the one
/// log index replay could not advance past.
pub fn encode_borrowed_raft_log_entry(
    entry: BorrowedPersistedRaftLogEntry<'_>,
) -> Result<Vec<u8>, EncodeRaftLogEntryError> {
    if advanceable_log_index(entry.index.0).is_none() {
        return Err(EncodeRaftLogEntryError::IndexAtMaximum);
    }
    let mut writer = Writer::new();
    writer.bytes(&RAFT_LOG_ENTRY_MAGIC);
    writer.u8(RAFT_LOG_ENTRY_VERSION);
    writer.u64(entry.index.0);
    writer.u64(entry.term.0);
    write_log_entry_kind(&mut writer, entry.kind)?;

    Ok(finish_checksummed(writer))
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
    let body = verify_checksum(envelope)?;
    let mut reader = Reader::new(body);
    let magic = reader.magic()?;
    if magic != RAFT_LOG_ENTRY_MAGIC {
        return Err(DecodeRaftLogEntryError::InvalidMagic(magic));
    }

    let version = reader.u8()?;
    if version != RAFT_LOG_ENTRY_VERSION {
        return Err(DecodeRaftLogEntryError::UnsupportedVersion(version));
    }

    let index =
        advanceable_log_index(reader.u64()?).ok_or(DecodeRaftLogEntryError::IndexAtMaximum)?;
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
    let voters = read_node_ids(reader, "voters")?;
    let learners = read_node_ids(reader, "learners")?;
    MembershipSet::new(voters, learners).map_err(DecodeRaftLogEntryError::InvalidMembership)
}

fn read_node_ids(
    reader: &mut Reader<'_>,
    member_kind: &'static str,
) -> Result<Vec<NodeId>, DecodeRaftLogEntryError> {
    let len = reader.u32()? as usize;
    // Every node id costs eight bytes. Cap speculative reservation by the
    // remaining encoded budget rather than by a hostile count prefix.
    let mut node_ids = Vec::with_capacity(len.min(reader.remaining() / 8));
    for _ in 0..len {
        let node_id = NodeId(reader.u64()?);
        if let Some(previous) = node_ids.last() {
            if *previous > node_id {
                return Err(DecodeRaftLogEntryError::NonCanonicalMembershipOrder {
                    member_kind,
                    previous: *previous,
                    actual: node_id,
                });
            }
        }
        node_ids.push(node_id);
    }
    Ok(node_ids)
}

#[cfg(test)]
#[path = "log_entry_test.rs"]
mod tests;
