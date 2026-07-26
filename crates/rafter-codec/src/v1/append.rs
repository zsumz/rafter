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
    let mut entries = Vec::with_capacity(entry_reservation(entry_count, reader));
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

/// Entries to reserve before decoding the first one.
///
/// A declared count the remaining bytes can comfortably hold is reserved
/// exactly and costs nothing to check. A count larger than that is not
/// trusted: `Vec` reserves `capacity * size_of::<LogEntry>()` bytes, and
/// `size_of::<LogEntry>()` is 12.4x `MIN_ENCODED_LOG_ENTRY_BYTES`, so trusting
/// it let a frame at a transport's receive limit reserve 12.4x its own size in
/// heap before a single entry had been validated.
///
/// Clamping to the byte-aware bound is not enough on its own, and the
/// difference is measurable: clamping leaves `Vec` to reach the frame's real
/// entry count by doubling, which overshoots. On a 524,299-byte frame of
/// minimum-size entries, trusting the count peaked at 13.44x the wire size and
/// clamping peaked at 17.00x — worse.
///
/// So an untrustworthy count is replaced by the number of entries the frame
/// actually contains, found by scanning with `decode_log_entry` itself. Using
/// the real decoder is the point: a bespoke "skip one entry" walker would be a
/// second grammar that could drift from the first. The scan runs only when the
/// declared count already exceeds what the bytes justify, so well-formed
/// batches never pay for it, and a scan that stops early on a malformed entry
/// simply reserves less — the real pass fails at the same entry.
fn entry_reservation(entry_count: usize, reader: &Reader<'_>) -> usize {
    if append_entries_entry_capacity(entry_count, reader.remaining()) == entry_count {
        return entry_count;
    }

    let mut probe = reader.probe();
    let mut actual = 0;
    while actual < entry_count && decode_log_entry(&mut probe).is_ok() {
        actual += 1;
    }
    actual
}

/// Whether a declared entry count is credible for the bytes that remain, as
/// the largest count those bytes could justify reserving.
///
/// Two ceilings, both derived from bytes already in hand:
///
/// * `remaining / MIN_ENCODED_LOG_ENTRY_BYTES` — no more entries than the
///   unread bytes could possibly encode;
/// * `remaining / size_of::<LogEntry>()` — no more *heap* than the unread
///   bytes themselves occupy.
///
/// The second binds today. Which one binds is a property of `LogEntry`'s
/// layout, not of the wire format, so both are kept rather than folded into
/// whichever happens to be smaller now.
pub(crate) fn append_entries_entry_capacity(entry_count: usize, remaining: usize) -> usize {
    let by_wire = remaining / MIN_ENCODED_LOG_ENTRY_BYTES;
    let by_heap = remaining / core::mem::size_of::<LogEntry>();
    entry_count.min(by_wire).min(by_heap)
}
