//! Version-1 persisted-log-entry envelope encoding and canonical decoding.
//!
//! This module owns the RFLE magic, version, entry-kind tags, and field order,
//! plus the canonical ascending member-id rule replay depends on.

use rafter::{
    ConfigurationEntry, ConfigurationId, JointMembership, LogEntryKind, MembershipSet, NodeId, Term,
};

use crate::format::{advanceable_log_index, finish_checksummed, verify_checksum, Reader, Writer};

use super::{
    BorrowedPersistedRaftLogEntry, DecodeRaftLogEntryError, EncodeRaftLogEntryError,
    PersistedRaftLogEntry,
};

/// Magic prefix for the persisted Raft log-entry envelope.
pub const RAFT_LOG_ENTRY_MAGIC: [u8; 4] = *b"RFLE";
/// Current persisted Raft log-entry envelope version.
pub const RAFT_LOG_ENTRY_VERSION: u8 = 1;

const RAFT_LOG_ENTRY_KIND_APPLICATION: u8 = 0;
const RAFT_LOG_ENTRY_KIND_STABLE_CONFIGURATION: u8 = 1;
const RAFT_LOG_ENTRY_KIND_JOINT_CONFIGURATION: u8 = 2;
const RAFT_LOG_ENTRY_KIND_NOOP: u8 = 3;

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
