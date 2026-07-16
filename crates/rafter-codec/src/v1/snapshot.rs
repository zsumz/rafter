//! Snapshot-transfer messages and nested metadata payload grammar.

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion,
    CommittedConfiguration, ConfigurationId, InstallSnapshotChunk, InstallSnapshotResponse,
    LogIndex, NodeId, RaftSnapshotMetadata, SnapshotCommittedConfiguration, SnapshotGroupId,
    SnapshotTransferId, Term,
};

use crate::{
    v1::membership,
    wire::{Reader, Sink, Writer},
    DecodePeerMessageError, EncodePeerMessageError,
};

pub(super) fn encode_chunk<S: Sink>(
    writer: &mut Writer<S>,
    request: &InstallSnapshotChunk,
) -> Result<(), EncodePeerMessageError> {
    writer.u64(request.term.0);
    writer.u64(request.leader_id.0);
    writer.u64(request.transfer_id.0);
    encode_metadata(writer, &request.metadata)?;
    writer.u64(request.total_payload_len);
    writer.u32(request.application_payload_crc32);
    writer.u64(request.offset);
    writer.blob("install_snapshot_chunk", &request.chunk)?;
    writer.bool(request.done);
    Ok(())
}

pub(super) fn decode_chunk(
    reader: &mut Reader<'_>,
) -> Result<InstallSnapshotChunk, DecodePeerMessageError> {
    Ok(InstallSnapshotChunk {
        term: Term(reader.u64()?),
        leader_id: NodeId(reader.u64()?),
        transfer_id: SnapshotTransferId(reader.u64()?),
        metadata: decode_metadata(reader)?,
        total_payload_len: reader.u64()?,
        application_payload_crc32: reader.u32()?,
        offset: reader.u64()?,
        chunk: reader.blob()?,
        done: reader.bool()?,
    })
}

pub(super) fn encode_response<S: Sink>(writer: &mut Writer<S>, response: &InstallSnapshotResponse) {
    writer.u64(response.term.0);
    writer.u64(response.follower_id.0);
    writer.bool(response.success);
    writer.u64(response.last_included_index.0);
    writer.bool(response.transfer_id.is_some());
    if let Some(transfer_id) = response.transfer_id {
        writer.u64(transfer_id.0);
    }
    writer.u64(response.next_offset);
}

pub(super) fn decode_response(
    reader: &mut Reader<'_>,
) -> Result<InstallSnapshotResponse, DecodePeerMessageError> {
    let term = Term(reader.u64()?);
    let follower_id = NodeId(reader.u64()?);
    let success = reader.bool()?;
    let last_included_index = LogIndex(reader.u64()?);
    let transfer_id = if reader.bool()? {
        Some(SnapshotTransferId(reader.u64()?))
    } else {
        None
    };
    let next_offset = reader.u64()?;
    Ok(InstallSnapshotResponse {
        term,
        follower_id,
        success,
        last_included_index,
        transfer_id,
        next_offset,
    })
}

fn encode_metadata<S: Sink>(
    writer: &mut Writer<S>,
    metadata: &RaftSnapshotMetadata,
) -> Result<(), EncodePeerMessageError> {
    writer.string("snapshot_group_id", metadata.group_id.as_str())?;
    writer.u64(metadata.writer_id.0);
    writer.u64(metadata.last_included_index.0);
    writer.u64(metadata.last_included_term.0);
    writer.u64(metadata.hard_state_term.0);
    writer.string(
        "application_snapshot_kind",
        metadata.application.kind.as_str(),
    )?;
    writer.u16(metadata.application.version.get());
    encode_committed_configuration(writer, metadata.committed_configuration.as_ref())
}

fn decode_metadata(
    reader: &mut Reader<'_>,
) -> Result<RaftSnapshotMetadata, DecodePeerMessageError> {
    let group_id = SnapshotGroupId::new(reader.string("snapshot_group_id")?)
        .map_err(DecodePeerMessageError::InvalidSnapshotGroupId)?;
    let writer_id = NodeId(reader.u64()?);
    let last_included_index = LogIndex(reader.u64()?);
    let last_included_term = Term(reader.u64()?);
    let hard_state_term = Term(reader.u64()?);
    let application_kind =
        ApplicationSnapshotKind::new(reader.string("application_snapshot_kind")?)
            .map_err(DecodePeerMessageError::InvalidApplicationSnapshotKind)?;
    let application_version = ApplicationSnapshotVersion::new(reader.u16()?)
        .map_err(DecodePeerMessageError::InvalidApplicationSnapshotVersion)?;
    let committed_configuration = decode_committed_configuration(reader)?;
    let mut metadata = RaftSnapshotMetadata::new(
        group_id,
        writer_id,
        last_included_index,
        last_included_term,
        hard_state_term,
        ApplicationSnapshotMetadata::new(application_kind, application_version),
    )
    .map_err(DecodePeerMessageError::InvalidSnapshotMetadata)?;
    if let Some(committed_configuration) = committed_configuration {
        metadata = metadata.with_committed_configuration(committed_configuration);
    }
    Ok(metadata)
}

fn encode_committed_configuration<S: Sink>(
    writer: &mut Writer<S>,
    committed: Option<&SnapshotCommittedConfiguration>,
) -> Result<(), EncodePeerMessageError> {
    writer.bool(committed.is_some());
    if let Some(committed) = committed {
        writer.bool(committed.configuration.is_some());
        if let Some(configuration) = committed.configuration {
            writer.u64(configuration.index.0);
            writer.u64(configuration.config_id.0);
        }
        membership::encode_config(writer, &committed.membership)?;
    }
    Ok(())
}

fn decode_committed_configuration(
    reader: &mut Reader<'_>,
) -> Result<Option<SnapshotCommittedConfiguration>, DecodePeerMessageError> {
    if !reader.bool()? {
        return Ok(None);
    }
    let configuration = if reader.bool()? {
        Some(CommittedConfiguration {
            index: LogIndex(reader.u64()?),
            config_id: ConfigurationId(reader.u64()?),
        })
    } else {
        None
    };
    let membership = membership::decode_config(reader)?;
    Ok(Some(SnapshotCommittedConfiguration::new(
        configuration,
        membership,
    )))
}
