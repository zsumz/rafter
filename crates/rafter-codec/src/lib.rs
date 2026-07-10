//! Versioned codec for Raft peer messages at the process runtime boundary.
//!
//! This module deliberately lives outside `rafter`: it is a wire-format
//! concern, while the Raft crate remains a deterministic protocol kernel.
//! It owns the binary frame vocabulary, frame version byte, and corruption
//! checks for peer messages. It does not own
//! transport delivery, authentication, peer fencing, backpressure, storage, or
//! runtime scheduling.
//!
//! # Frame sizing
//!
//! The codec imposes no frame-size limit of its own; transports derive one
//! from the kernel's append budget. An append-entries frame is bounded by
//! `NodeConfig::max_append_entries_bytes` (default 512 KiB) plus fixed
//! headers, because the kernel batches entries by
//! [`LogEntry::replication_bytes`], a documented upper bound of each entry's
//! encoding here. Snapshot transfer reuses that headroom: chunk frames carry
//! at most 64 KiB of payload plus metadata, so any transport sized for the
//! append budget carries every snapshot chunk. Whole
//! [`Message::InstallSnapshot`] values remain part of the core protocol for
//! direct/internal use, but the current peer wire format serializes chunked
//! snapshot transfers only.
//!
//! # Integrity Model
//!
//! The frame checksum is CRC32. It catches accidental corruption and misframing
//! in a non-Byzantine system, but it is not an authentication tag and does not
//! provide adversarial message integrity. Production transports that cross an
//! untrusted boundary should authenticate the channel or the frames outside
//! this codec.
//!
//! # Wire Compatibility
//!
//! This pre-release crate supports exactly one peer wire format. The encoder
//! emits [`VERSION`], and the decoder rejects every other version with
//! [`DecodePeerMessageError::UnsupportedVersion`]. Rolling compatibility
//! starts only after Rafter has a public wire compatibility promise.

use std::{error::Error, fmt};

use rafter::{
    AppendEntries, AppendEntriesResponse, ConfigurationEntry, ConfigurationId,
    InstallSnapshotChunk, InstallSnapshotResponse, JointMembership, LogEntry, LogEntryKind,
    MembershipValidationError, Message, PreVote, PreVoteResponse, RequestVote, RequestVoteResponse,
    SnapshotIdError, SnapshotMetadataError, TimeoutNow,
};
use rafter_crc32::crc32;

mod cursor;

use cursor::{Reader, Writer};

/// Wire-format magic tag identifying a Rafter Peer Message frame.
pub const MAGIC: [u8; 4] = *b"RFPM";
/// Peer wire-format version emitted by this codec.
///
/// This is the first public peer-wire version. Earlier internal draft formats
/// are intentionally unsupported.
pub const VERSION: u8 = 1;

const MSG_REQUEST_VOTE: u8 = 1;
const MSG_REQUEST_VOTE_RESPONSE: u8 = 2;
const MSG_APPEND_ENTRIES: u8 = 3;
const MSG_APPEND_ENTRIES_RESPONSE: u8 = 4;
const MSG_INSTALL_SNAPSHOT_RESPONSE: u8 = 6;
const MSG_INSTALL_SNAPSHOT_CHUNK: u8 = 7;
const MSG_PRE_VOTE: u8 = 8;
const MSG_PRE_VOTE_RESPONSE: u8 = 9;
const MSG_TIMEOUT_NOW: u8 = 10;
const ENTRY_APPLICATION: u8 = 0;
const ENTRY_CONFIGURATION_STABLE: u8 = 1;
const ENTRY_CONFIGURATION_JOINT: u8 = 2;
const ENTRY_NOOP: u8 = 3;

const MIN_ENCODED_LOG_ENTRY_BYTES: usize = 8 + 1;

/// Error returned when a peer message cannot be encoded for the selected wire
/// version.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EncodePeerMessageError {
    FieldTooLarge {
        field: &'static str,
        len: usize,
    },
    UnsupportedMessage {
        message: &'static str,
        reason: &'static str,
    },
}

/// Error returned when a peer message frame is malformed, unsupported, or
/// fails integrity checks.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecodePeerMessageError {
    UnexpectedEof { needed: usize, remaining: usize },
    InvalidMagic([u8; 4]),
    UnsupportedVersion(u8),
    UnknownMessageType(u8),
    InvalidBoolean(u8),
    InvalidUtf8 { field: &'static str },
    InvalidSnapshotGroupId(SnapshotIdError),
    InvalidApplicationSnapshotKind(SnapshotIdError),
    InvalidApplicationSnapshotVersion(SnapshotMetadataError),
    InvalidSnapshotMetadata(SnapshotMetadataError),
    InvalidMembership(MembershipValidationError),
    UnknownLogEntryKind(u8),
    UnknownMembershipFlag(u8),
    UnknownMembershipKind(u8),
    FrameChecksumMismatch { expected: u32, actual: u32 },
    TrailingBytes(usize),
}

impl fmt::Display for EncodePeerMessageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FieldTooLarge { field, len } => write!(
                formatter,
                "peer message field {field} with length {len} does not fit in the wire format"
            ),
            Self::UnsupportedMessage { message, reason } => {
                write!(
                    formatter,
                    "peer message {message} is not supported by the current wire format: {reason}"
                )
            }
        }
    }
}

impl Error for EncodePeerMessageError {}

impl fmt::Display for DecodePeerMessageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof { needed, remaining } => write!(
                formatter,
                "peer message needs {needed} bytes but only {remaining} remain"
            ),
            Self::InvalidMagic(magic) => {
                write!(formatter, "peer message magic {magic:02x?} is not RFPM")
            }
            Self::UnsupportedVersion(version) => write!(
                formatter,
                "peer message version {version} is not supported (supported version {VERSION})"
            ),
            Self::UnknownMessageType(message_type) => {
                write!(formatter, "peer message type {message_type} is unknown")
            }
            Self::InvalidBoolean(byte) => {
                write!(formatter, "peer message boolean byte {byte} is not 0 or 1")
            }
            Self::InvalidUtf8 { field } => {
                write!(formatter, "peer message field {field} is not valid utf-8")
            }
            Self::InvalidSnapshotGroupId(error) => {
                write!(
                    formatter,
                    "peer message snapshot group id is invalid: {error}"
                )
            }
            Self::InvalidApplicationSnapshotKind(error) => write!(
                formatter,
                "peer message application snapshot kind is invalid: {error}"
            ),
            Self::InvalidApplicationSnapshotVersion(error) => write!(
                formatter,
                "peer message application snapshot version is invalid: {error}"
            ),
            Self::InvalidSnapshotMetadata(error) => write!(
                formatter,
                "peer message snapshot metadata is invalid: {error}"
            ),
            Self::InvalidMembership(error) => {
                write!(formatter, "peer message membership is invalid: {error}")
            }
            Self::UnknownLogEntryKind(kind) => {
                write!(formatter, "peer message log entry kind {kind} is unknown")
            }
            Self::UnknownMembershipFlag(flag) => {
                write!(formatter, "peer message membership flag {flag} is unknown")
            }
            Self::UnknownMembershipKind(kind) => {
                write!(formatter, "peer message membership kind {kind} is unknown")
            }
            Self::FrameChecksumMismatch { expected, actual } => write!(
                formatter,
                "peer message checksum mismatch: expected {expected:#010x}, actual {actual:#010x}"
            ),
            Self::TrailingBytes(remaining) => {
                write!(formatter, "peer message has {remaining} trailing bytes")
            }
        }
    }
}

impl Error for DecodePeerMessageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidSnapshotGroupId(error) | Self::InvalidApplicationSnapshotKind(error) => {
                Some(error)
            }
            Self::InvalidApplicationSnapshotVersion(error)
            | Self::InvalidSnapshotMetadata(error) => Some(error),
            Self::InvalidMembership(error) => Some(error),
            Self::UnexpectedEof { .. }
            | Self::InvalidMagic(_)
            | Self::UnsupportedVersion(_)
            | Self::UnknownMessageType(_)
            | Self::InvalidBoolean(_)
            | Self::InvalidUtf8 { .. }
            | Self::UnknownLogEntryKind(_)
            | Self::UnknownMembershipFlag(_)
            | Self::UnknownMembershipKind(_)
            | Self::FrameChecksumMismatch { .. }
            | Self::TrailingBytes(_) => None,
        }
    }
}

/// Encodes a Raft peer message as the current versioned byte payload.
///
/// # Errors
///
/// Returns [`EncodePeerMessageError`] when a variable-length field cannot be
/// represented in the peer message format.
pub fn encode_message(message: &Message) -> Result<Vec<u8>, EncodePeerMessageError> {
    let mut encoded = Vec::new();
    encode_message_into(&mut encoded, message)?;
    Ok(encoded)
}

/// Encodes a Raft peer message into a caller-owned reusable buffer.
///
/// The buffer is cleared before encoding. On success it contains exactly one
/// current-version peer message frame; on error it is cleared again so callers
/// never accidentally send a partial frame.
///
/// # Errors
///
/// Returns [`EncodePeerMessageError`] when a variable-length field cannot be
/// represented in the peer message format.
pub fn encode_message_into(
    output: &mut Vec<u8>,
    message: &Message,
) -> Result<(), EncodePeerMessageError> {
    let capacity = encoded_len_hint(message);
    let payload_result = {
        let mut writer = Writer::with_capacity(output, capacity);
        writer.bytes(&MAGIC);
        writer.u8(VERSION);
        encode_message_payload(&mut writer, message)
    };
    if let Err(error) = payload_result {
        output.clear();
        return Err(error);
    }

    let checksum = crc32(output);
    output.extend_from_slice(&checksum.to_be_bytes());
    Ok(())
}

fn encode_message_payload(
    writer: &mut Writer,
    message: &Message,
) -> Result<(), EncodePeerMessageError> {
    match message {
        Message::RequestVote(request) => {
            writer.u8(MSG_REQUEST_VOTE);
            writer.term(request.term);
            writer.node_id(request.candidate_id);
            writer.log_index(request.last_log_index);
            writer.term(request.last_log_term);
        }
        Message::RequestVoteResponse(response) => {
            writer.u8(MSG_REQUEST_VOTE_RESPONSE);
            writer.term(response.term);
            writer.node_id(response.voter_id);
            writer.bool(response.vote_granted);
        }
        Message::PreVote(request) => {
            writer.u8(MSG_PRE_VOTE);
            writer.term(request.term);
            writer.node_id(request.candidate_id);
            writer.log_index(request.last_log_index);
            writer.term(request.last_log_term);
        }
        Message::PreVoteResponse(response) => {
            writer.u8(MSG_PRE_VOTE_RESPONSE);
            writer.term(response.term);
            writer.node_id(response.voter_id);
            writer.bool(response.vote_granted);
        }
        Message::TimeoutNow(request) => {
            writer.u8(MSG_TIMEOUT_NOW);
            writer.term(request.term);
            writer.node_id(request.leader_id);
        }
        Message::AppendEntries(request) => {
            writer.u8(MSG_APPEND_ENTRIES);
            writer.term(request.term);
            writer.node_id(request.leader_id);
            writer.log_index(request.prev_log_index);
            writer.term(request.prev_log_term);
            writer.u32("entry_count", request.entries.len())?;
            for entry in &request.entries {
                encode_log_entry(writer, entry)?;
            }
            writer.log_index(request.leader_commit);
            writer.u64(request.sequence);
        }
        Message::AppendEntriesResponse(response) => {
            writer.u8(MSG_APPEND_ENTRIES_RESPONSE);
            writer.term(response.term);
            writer.node_id(response.follower_id);
            writer.bool(response.success);
            writer.log_index(response.match_index);
            writer.u64(response.sequence);
        }
        Message::InstallSnapshot(_) => {
            return Err(EncodePeerMessageError::UnsupportedMessage {
                message: "InstallSnapshot",
                reason: "use InstallSnapshotChunk for peer transport",
            });
        }
        Message::InstallSnapshotChunk(request) => {
            writer.u8(MSG_INSTALL_SNAPSHOT_CHUNK);
            writer.term(request.term);
            writer.node_id(request.leader_id);
            writer.snapshot_transfer_id(request.transfer_id);
            writer.snapshot_metadata(&request.metadata)?;
            writer.u64(request.total_payload_len);
            writer.raw_u32(request.application_payload_crc32);
            writer.u64(request.offset);
            writer.blob("install_snapshot_chunk", &request.chunk)?;
            writer.bool(request.done);
        }
        Message::InstallSnapshotResponse(response) => {
            writer.u8(MSG_INSTALL_SNAPSHOT_RESPONSE);
            writer.term(response.term);
            writer.node_id(response.follower_id);
            writer.bool(response.success);
            writer.log_index(response.last_included_index);
            writer.optional_snapshot_transfer_id(response.transfer_id);
            writer.u64(response.next_offset);
        }
    }
    Ok(())
}

fn encoded_len_hint(message: &Message) -> usize {
    MAGIC
        .len()
        .saturating_add(1)
        .saturating_add(encoded_message_payload_len_hint(message))
        .saturating_add(4)
}

fn encoded_message_payload_len_hint(message: &Message) -> usize {
    match message {
        Message::RequestVote(_) | Message::PreVote(_) => 1 + 8 + 8 + 8 + 8,
        Message::RequestVoteResponse(_) | Message::PreVoteResponse(_) => 1 + 8 + 8 + 1,
        Message::TimeoutNow(_) => 1 + 8 + 8,
        Message::AppendEntries(request) => {
            let mut len = 1 + 8 + 8 + 8 + 8 + 4 + 8 + 8;
            for entry in &request.entries {
                add_len(&mut len, log_entry_len_hint(entry));
            }
            len
        }
        Message::AppendEntriesResponse(_) => 1 + 8 + 8 + 1 + 8 + 8,
        Message::InstallSnapshot(_) => 1,
        Message::InstallSnapshotResponse(response) => {
            let mut len = 1 + 8 + 8 + 1 + 8 + 1 + 8;
            if response.transfer_id.is_some() {
                add_len(&mut len, 8);
            }
            len
        }
        Message::InstallSnapshotChunk(request) => {
            let mut len = 1 + 8 + 8 + 8;
            add_len(&mut len, snapshot_metadata_len_hint(&request.metadata));
            add_len(&mut len, 8 + 4 + 8);
            add_len(&mut len, blob_len_hint(request.chunk.len()));
            add_len(&mut len, 1);
            len
        }
    }
}

fn log_entry_len_hint(entry: &LogEntry) -> usize {
    let mut len = 8 + 1;
    match &entry.kind {
        LogEntryKind::Application(payload) => add_len(&mut len, blob_len_hint(payload.len())),
        LogEntryKind::Configuration(ConfigurationEntry::Stable { membership, .. }) => {
            add_len(&mut len, 8);
            add_len(&mut len, membership_set_len_hint(membership));
        }
        LogEntryKind::Configuration(ConfigurationEntry::Joint { membership, .. }) => {
            add_len(&mut len, 8);
            add_len(&mut len, membership_set_len_hint(membership.old()));
            add_len(
                &mut len,
                membership_set_len_hint(membership.new_membership()),
            );
        }
        LogEntryKind::Noop => {}
    }
    len
}

fn snapshot_metadata_len_hint(metadata: &rafter::RaftSnapshotMetadata) -> usize {
    let mut len = string_len_hint(metadata.group_id.as_str().len())
        .saturating_add(8 + 8 + 8 + 8)
        .saturating_add(string_len_hint(metadata.application.kind.as_str().len()))
        .saturating_add(2)
        .saturating_add(1);
    if let Some(committed) = &metadata.committed_configuration {
        add_len(&mut len, 1);
        if committed.configuration.is_some() {
            add_len(&mut len, 8 + 8);
        }
        add_len(&mut len, membership_config_len_hint(&committed.membership));
    }
    len
}

fn membership_config_len_hint(membership: &rafter::MembershipConfig) -> usize {
    match membership {
        rafter::MembershipConfig::Stable(stable) => 1 + membership_set_len_hint(stable),
        rafter::MembershipConfig::Joint(joint) => {
            let mut len = 1;
            add_len(&mut len, membership_set_len_hint(joint.old()));
            add_len(&mut len, membership_set_len_hint(joint.new_membership()));
            len
        }
    }
}

fn membership_set_len_hint(membership: &rafter::MembershipSet) -> usize {
    2_usize
        .saturating_add(membership.voters().len().saturating_mul(8))
        .saturating_add(2)
        .saturating_add(membership.learners().len().saturating_mul(8))
}

fn string_len_hint(len: usize) -> usize {
    2_usize.saturating_add(len.min(u16::MAX as usize))
}

fn blob_len_hint(len: usize) -> usize {
    4_usize.saturating_add(len.min(u32::MAX as usize))
}

fn add_len(total: &mut usize, len: usize) {
    *total = total.saturating_add(len);
}

/// Decodes a Raft peer message from the current peer wire format.
///
/// # Errors
///
/// Returns [`DecodePeerMessageError`] when the payload is malformed or uses an
/// unsupported peer message version.
pub fn decode_message(payload: &[u8]) -> Result<Message, DecodePeerMessageError> {
    let mut reader = Reader::new(payload);
    let magic = reader.magic()?;
    if magic != MAGIC {
        return Err(DecodePeerMessageError::InvalidMagic(magic));
    }

    let version = reader.u8()?;
    if version != VERSION {
        return Err(DecodePeerMessageError::UnsupportedVersion(version));
    }

    let message_type = reader.u8()?;
    let message = match message_type {
        MSG_REQUEST_VOTE => Message::RequestVote(RequestVote {
            term: reader.term()?,
            candidate_id: reader.node_id()?,
            last_log_index: reader.log_index()?,
            last_log_term: reader.term()?,
        }),
        MSG_REQUEST_VOTE_RESPONSE => Message::RequestVoteResponse(RequestVoteResponse {
            term: reader.term()?,
            voter_id: reader.node_id()?,
            vote_granted: reader.bool()?,
        }),
        MSG_PRE_VOTE => Message::PreVote(PreVote {
            term: reader.term()?,
            candidate_id: reader.node_id()?,
            last_log_index: reader.log_index()?,
            last_log_term: reader.term()?,
        }),
        MSG_PRE_VOTE_RESPONSE => Message::PreVoteResponse(PreVoteResponse {
            term: reader.term()?,
            voter_id: reader.node_id()?,
            vote_granted: reader.bool()?,
        }),
        MSG_TIMEOUT_NOW => Message::TimeoutNow(TimeoutNow {
            term: reader.term()?,
            leader_id: reader.node_id()?,
        }),
        MSG_APPEND_ENTRIES => decode_append_entries(&mut reader)?,
        MSG_APPEND_ENTRIES_RESPONSE => decode_append_entries_response(&mut reader)?,
        MSG_INSTALL_SNAPSHOT_RESPONSE => {
            Message::InstallSnapshotResponse(InstallSnapshotResponse {
                term: reader.term()?,
                follower_id: reader.node_id()?,
                success: reader.bool()?,
                last_included_index: reader.log_index()?,
                transfer_id: reader.optional_snapshot_transfer_id()?,
                next_offset: reader.u64()?,
            })
        }
        MSG_INSTALL_SNAPSHOT_CHUNK => Message::InstallSnapshotChunk(InstallSnapshotChunk {
            term: reader.term()?,
            leader_id: reader.node_id()?,
            transfer_id: reader.snapshot_transfer_id()?,
            metadata: reader.snapshot_metadata()?,
            total_payload_len: reader.u64()?,
            application_payload_crc32: reader.u32()?,
            offset: reader.u64()?,
            chunk: reader.blob()?,
            done: reader.bool()?,
        }),
        other => return Err(DecodePeerMessageError::UnknownMessageType(other)),
    };

    let checksum_start = reader.position();
    let expected = reader.u32()?;
    let actual = crc32(&payload[..checksum_start]);
    if expected != actual {
        return Err(DecodePeerMessageError::FrameChecksumMismatch { expected, actual });
    }

    reader.finish()?;
    Ok(message)
}

fn encode_log_entry(writer: &mut Writer, entry: &LogEntry) -> Result<(), EncodePeerMessageError> {
    writer.term(entry.term);
    match &entry.kind {
        LogEntryKind::Application(payload) => {
            writer.u8(ENTRY_APPLICATION);
            writer.blob("entry_payload", payload)
        }
        LogEntryKind::Configuration(ConfigurationEntry::Stable {
            config_id,
            membership,
        }) => {
            writer.u8(ENTRY_CONFIGURATION_STABLE);
            writer.u64(config_id.0);
            writer.membership_set(membership)
        }
        LogEntryKind::Configuration(ConfigurationEntry::Joint {
            config_id,
            membership,
        }) => {
            writer.u8(ENTRY_CONFIGURATION_JOINT);
            writer.u64(config_id.0);
            writer.membership_set(membership.old())?;
            writer.membership_set(membership.new_membership())
        }
        LogEntryKind::Noop => {
            writer.u8(ENTRY_NOOP);
            Ok(())
        }
    }
}

fn decode_append_entries(reader: &mut Reader<'_>) -> Result<Message, DecodePeerMessageError> {
    let term = reader.term()?;
    let leader_id = reader.node_id()?;
    let prev_log_index = reader.log_index()?;
    let prev_log_term = reader.term()?;
    let entry_count = reader.u32()? as usize;
    // Cap the reservation by the smallest current-format entry. `remaining()`
    // is bytes, while vector capacity is entries; using bytes directly still
    // allowed large transient allocations.
    let capacity = append_entries_entry_capacity(entry_count, reader.remaining());
    let mut entries = Vec::with_capacity(capacity);
    for _ in 0..entry_count {
        entries.push(decode_log_entry(reader)?);
    }
    let leader_commit = reader.log_index()?;
    let sequence = reader.u64()?;
    Ok(Message::AppendEntries(AppendEntries {
        term,
        leader_id,
        prev_log_index,
        prev_log_term,
        entries: entries.into(),
        leader_commit,
        sequence,
    }))
}

fn append_entries_entry_capacity(entry_count: usize, remaining: usize) -> usize {
    entry_count.min(remaining / MIN_ENCODED_LOG_ENTRY_BYTES)
}

fn decode_append_entries_response(
    reader: &mut Reader<'_>,
) -> Result<Message, DecodePeerMessageError> {
    let term = reader.term()?;
    let follower_id = reader.node_id()?;
    let success = reader.bool()?;
    let match_index = reader.log_index()?;
    let sequence = reader.u64()?;
    Ok(Message::AppendEntriesResponse(AppendEntriesResponse {
        term,
        follower_id,
        success,
        match_index,
        sequence,
    }))
}

fn decode_log_entry(reader: &mut Reader<'_>) -> Result<LogEntry, DecodePeerMessageError> {
    let term = reader.term()?;

    match reader.u8()? {
        ENTRY_APPLICATION => Ok(LogEntry::application(term, reader.shared_blob_payload()?)),
        ENTRY_CONFIGURATION_STABLE => {
            let config_id = ConfigurationId(reader.u64()?);
            let membership = reader.membership_set()?;
            Ok(LogEntry::configuration(
                term,
                ConfigurationEntry::stable(config_id, membership),
            ))
        }
        ENTRY_CONFIGURATION_JOINT => {
            let config_id = ConfigurationId(reader.u64()?);
            let old = reader.membership_set()?;
            let new = reader.membership_set()?;
            Ok(LogEntry::configuration(
                term,
                ConfigurationEntry::joint(config_id, JointMembership::new(old, new)),
            ))
        }
        ENTRY_NOOP => Ok(LogEntry::noop(term)),
        other => Err(DecodePeerMessageError::UnknownLogEntryKind(other)),
    }
}

#[cfg(test)]
mod tests;
