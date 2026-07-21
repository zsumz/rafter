//! Version-1 Raft snapshot-metadata field grammar.
//!
//! This module owns the bytes from `group_id_len` through committed membership.
//! It deliberately does not own RFSN magic/version framing, payload length,
//! payload bytes, or checksums.

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion,
    CommittedConfiguration, ConfigurationId, LogIndex, MembershipConfig, MembershipSet, NodeId,
    RaftSnapshotMetadata, SnapshotCommittedConfiguration, SnapshotGroupId, Term,
};

use super::super::{Reader, Writer};
use crate::raft_snapshot_codec::{DecodeRaftSnapshotError, EncodeRaftSnapshotError};

const MEMBERSHIP_ABSENT: u8 = 0;
const MEMBERSHIP_PRESENT: u8 = 1;
const MEMBERSHIP_STABLE: u8 = 0;
const MEMBERSHIP_JOINT: u8 = 1;

/// Appends the canonical version-1 snapshot metadata fields to `writer`.
///
/// # Errors
///
/// Returns [`EncodeRaftSnapshotError`] when a string or membership count does
/// not fit its version-1 length prefix.
pub(crate) fn encode_snapshot_metadata(
    writer: &mut Writer,
    metadata: &RaftSnapshotMetadata,
) -> Result<(), EncodeRaftSnapshotError> {
    encode_string(writer, "snapshot group id", metadata.group_id.as_str())?;
    writer.u64(metadata.writer_id.0);
    writer.u64(metadata.last_included_index.0);
    writer.u64(metadata.last_included_term.0);
    writer.u64(metadata.hard_state_term.0);
    encode_string(
        writer,
        "application snapshot kind",
        metadata.application.kind.as_str(),
    )?;
    writer.u16(metadata.application.version.get());
    encode_optional_committed_configuration(writer, metadata.committed_configuration.as_ref())
}

/// Decodes and validates canonical version-1 snapshot metadata fields.
///
/// # Errors
///
/// Returns [`DecodeRaftSnapshotError`] when fields are malformed, metadata is
/// invalid, or membership ids are not in canonical order.
pub(crate) fn decode_snapshot_metadata(
    reader: &mut Reader<'_>,
) -> Result<RaftSnapshotMetadata, DecodeRaftSnapshotError> {
    let group_id = SnapshotGroupId::new(decode_string(reader, "snapshot group id")?)
        .map_err(DecodeRaftSnapshotError::InvalidGroupId)?;
    let writer_id = NodeId(reader.u64()?);
    let last_included_index = LogIndex(reader.u64()?);
    let last_included_term = Term(reader.u64()?);
    let hard_state_term = Term(reader.u64()?);
    let application_kind =
        ApplicationSnapshotKind::new(decode_string(reader, "application snapshot kind")?)
            .map_err(DecodeRaftSnapshotError::InvalidApplicationKind)?;
    let application_version = ApplicationSnapshotVersion::new(reader.u16()?)
        .map_err(DecodeRaftSnapshotError::InvalidApplicationVersion)?;
    let committed_configuration = decode_optional_committed_configuration(reader)?;

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
    Ok(metadata)
}

fn encode_string(
    writer: &mut Writer,
    field: &'static str,
    value: &str,
) -> Result<(), EncodeRaftSnapshotError> {
    let bytes = value.as_bytes();
    let len = u16::try_from(bytes.len()).map_err(|_| EncodeRaftSnapshotError::StringTooLong {
        field,
        len: bytes.len(),
    })?;
    writer.u16(len);
    writer.bytes(bytes);
    Ok(())
}

fn decode_string(
    reader: &mut Reader<'_>,
    field: &'static str,
) -> Result<String, DecodeRaftSnapshotError> {
    let len = usize::from(reader.u16()?);
    let bytes = reader.take(len)?;
    let value =
        std::str::from_utf8(bytes).map_err(|_| DecodeRaftSnapshotError::InvalidUtf8 { field })?;
    Ok(value.to_owned())
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
    encode_node_set(writer, "voters", membership.voters())?;
    encode_node_set(writer, "learners", membership.learners())
}

fn encode_node_set(
    writer: &mut Writer,
    member_kind: &'static str,
    node_ids: &[NodeId],
) -> Result<(), EncodeRaftSnapshotError> {
    let count =
        u16::try_from(node_ids.len()).map_err(|_| EncodeRaftSnapshotError::TooManyMembers {
            member_kind,
            len: node_ids.len(),
        })?;
    writer.u16(count);
    for node_id in node_ids {
        writer.u64(node_id.0);
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
    let voters = decode_node_set(reader, "voters")?;
    let learners = decode_node_set(reader, "learners")?;
    MembershipSet::new(voters, learners).map_err(DecodeRaftSnapshotError::InvalidMembership)
}

fn decode_node_set(
    reader: &mut Reader<'_>,
    member_kind: &'static str,
) -> Result<Vec<NodeId>, DecodeRaftSnapshotError> {
    let count = usize::from(reader.u16()?);
    let mut node_ids =
        Vec::with_capacity(count.min(reader.remaining() / std::mem::size_of::<u64>()));
    for _ in 0..count {
        let node_id = NodeId(reader.u64()?);
        if let Some(previous) = node_ids.last() {
            if *previous > node_id {
                return Err(DecodeRaftSnapshotError::NonCanonicalMembershipOrder {
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
