//! `AppendEntries`, `AppendEntriesResponse`, and log-entry payload grammar.

use rafter::{
    AppendEntries, AppendEntriesResponse, ConfigurationEntry, ConfigurationId, LogEntry,
    LogEntryKind, LogIndex, NodeId, SharedPayload, Term,
};

use crate::{
    v1::{membership, tags::LogEntryTag},
    wire::{Reader, Sink, Writer},
    DecodePeerMessageError, EncodePeerMessageError,
};

pub(crate) const MIN_ENCODED_LOG_ENTRY_BYTES: usize = 8 + 1;

pub(super) fn encode_request<S: Sink>(
    writer: &mut Writer<S>,
    request: &AppendEntries,
) -> Result<(), EncodePeerMessageError> {
    writer.u64(request.term.0);
    writer.u64(request.leader_id.0);
    writer.u64(request.prev_log_index.0);
    writer.u64(request.prev_log_term.0);
    writer.length_u32("entry_count", request.entries.len())?;
    for entry in &request.entries {
        encode_log_entry(writer, entry)?;
    }
    writer.u64(request.leader_commit.0);
    writer.u64(request.sequence);
    Ok(())
}

pub(super) fn decode_request(
    reader: &mut Reader<'_>,
) -> Result<AppendEntries, DecodePeerMessageError> {
    let term = Term(reader.u64()?);
    let leader_id = NodeId(reader.u64()?);
    let prev_log_index = LogIndex(reader.u64()?);
    let prev_log_term = Term(reader.u64()?);
    let entry_count = reader.u32()? as usize;
    let capacity = append_entries_entry_capacity(entry_count, reader.remaining());
    let mut entries = Vec::with_capacity(capacity);
    for _ in 0..entry_count {
        entries.push(decode_log_entry(reader)?);
    }
    let leader_commit = LogIndex(reader.u64()?);
    let sequence = reader.u64()?;
    Ok(AppendEntries {
        term,
        leader_id,
        prev_log_index,
        prev_log_term,
        entries: entries.into(),
        leader_commit,
        sequence,
    })
}

pub(super) fn encode_response<S: Sink>(writer: &mut Writer<S>, response: &AppendEntriesResponse) {
    writer.u64(response.term.0);
    writer.u64(response.follower_id.0);
    writer.bool(response.success);
    writer.u64(response.match_index.0);
    writer.u64(response.sequence);
}

pub(super) fn decode_response(
    reader: &mut Reader<'_>,
) -> Result<AppendEntriesResponse, DecodePeerMessageError> {
    Ok(AppendEntriesResponse {
        term: Term(reader.u64()?),
        follower_id: NodeId(reader.u64()?),
        success: reader.bool()?,
        match_index: LogIndex(reader.u64()?),
        sequence: reader.u64()?,
    })
}

fn encode_log_entry<S: Sink>(
    writer: &mut Writer<S>,
    entry: &LogEntry,
) -> Result<(), EncodePeerMessageError> {
    writer.u64(entry.term.0);
    match &entry.kind {
        LogEntryKind::Application(payload) => {
            writer.u8(LogEntryTag::Application.into());
            writer.blob("entry_payload", payload)
        }
        LogEntryKind::Configuration(ConfigurationEntry::Stable {
            config_id,
            membership,
        }) => {
            writer.u8(LogEntryTag::ConfigurationStable.into());
            writer.u64(config_id.0);
            membership::encode_set(writer, membership)
        }
        LogEntryKind::Configuration(ConfigurationEntry::Joint {
            config_id,
            membership,
        }) => {
            writer.u8(LogEntryTag::ConfigurationJoint.into());
            writer.u64(config_id.0);
            membership::encode_set(writer, membership.old())?;
            membership::encode_set(writer, membership.new_membership())
        }
        LogEntryKind::Noop => {
            writer.u8(LogEntryTag::Noop.into());
            Ok(())
        }
    }
}

fn decode_log_entry(reader: &mut Reader<'_>) -> Result<LogEntry, DecodePeerMessageError> {
    let term = Term(reader.u64()?);
    match LogEntryTag::try_from(reader.u8()?)? {
        LogEntryTag::Application => {
            let (bytes, range) = reader.shared_blob_range()?;
            let needed = range.end;
            let remaining = bytes.len();
            let payload = SharedPayload::from_shared_range(bytes, range)
                .ok_or(DecodePeerMessageError::UnexpectedEof { needed, remaining })?;
            Ok(LogEntry::application(term, payload))
        }
        LogEntryTag::ConfigurationStable => {
            let config_id = ConfigurationId(reader.u64()?);
            let membership = membership::decode_set(reader)?;
            Ok(LogEntry::configuration(
                term,
                ConfigurationEntry::stable(config_id, membership),
            ))
        }
        LogEntryTag::ConfigurationJoint => {
            let config_id = ConfigurationId(reader.u64()?);
            let old = membership::decode_set(reader)?;
            let new = membership::decode_set(reader)?;
            Ok(LogEntry::configuration(
                term,
                ConfigurationEntry::joint(config_id, membership::joint(old, new)),
            ))
        }
        LogEntryTag::Noop => Ok(LogEntry::noop(term)),
    }
}

pub(crate) fn append_entries_entry_capacity(entry_count: usize, remaining: usize) -> usize {
    entry_count.min(remaining / MIN_ENCODED_LOG_ENTRY_BYTES)
}
