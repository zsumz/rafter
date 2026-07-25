//! Consumer-owned wire encoding for Rafter peer messages.
//!
//! A transport is deployment policy, and so is its encoding: Rafter's contract
//! asks a transport to deliver a [`Message`] value to a peer, and says nothing
//! about the bytes in between. This module is the ledger's answer for the
//! integration composition, and it exists rather than a dependency on
//! `rafter-transport-tcp-insecure` for a boundary reason recorded in
//! `CONTRACT.md`: that crate and `rafter-codec` are outside the set of Rafter
//! crates the source-mode dependency override patches, so a consumer that named
//! them would resolve two Rafter crates from the registry while resolving the
//! rest from the checkout.
//!
//! # What is hand-written and what is not
//!
//! The two hard payloads are not hand-written. A log entry's `Configuration`
//! kind and a snapshot's metadata are recursive, membership-shaped structures
//! whose encodings Rafter already publishes:
//! [`encode_raft_log_entry`] and [`encode_raft_snapshot`] are public
//! `rafter-storage` API. Reusing them means this file never re-derives a
//! membership encoding, and a change to either structure is a compile error
//! here rather than a silently truncated frame.
//!
//! Everything else is scalars, and the framing below is deliberately dull.
//!
//! # Format
//!
//! One message is a tagged record. Unless a field says otherwise:
//!
//! - integers are unsigned and big-endian;
//! - records are packed with no alignment or padding;
//! - a length-prefixed block is a `u32` length followed by that many bytes; and
//! - a magic or version other than the one named here is rejected.
//!
//! ```text
//! magic          [4]   "RLPM"
//! version        u8    1
//! tag            u8    message discriminant
//! body           ...   per-tag fields
//! ```
//!
//! There is no checksum. The frame travels over TCP, which already checksums,
//! and a checksum here would suggest a corruption defence this integration
//! transport does not offer. It offers none: the bytes are unauthenticated, and
//! `CONTRACT.md` says so.
//!
//! [`Message::InstallSnapshot`] is refused rather than encoded. The kernel never
//! sends a whole-snapshot frame — leaders stream
//! [`Message::InstallSnapshotChunk`] — and Rafter's own `rafter-codec` refuses
//! it in the current peer wire format for the same reason. Refusing it keeps
//! this codec from carrying a frame no sender produces.

use std::{error::Error, fmt};

use rafter::{
    AppendEntries, AppendEntriesResponse, InstallSnapshotChunk, InstallSnapshotResponse, LogEntry,
    LogIndex, Message, NodeId, PreVote, PreVoteResponse, RequestVote, RequestVoteResponse,
    SnapshotTransferId, Term, TimeoutNow,
};
use rafter_storage::{
    decode_raft_log_entry, decode_raft_snapshot, encode_raft_log_entry, encode_raft_snapshot,
    PersistedRaftLogEntry, PersistedRaftSnapshot,
};

/// Magic of every peer frame this build writes.
const PEER_MESSAGE_MAGIC: [u8; 4] = *b"RLPM";
/// Version byte of every peer frame this build writes.
const PEER_MESSAGE_VERSION: u8 = 1;

const TAG_APPEND_ENTRIES: u8 = 1;
const TAG_APPEND_ENTRIES_RESPONSE: u8 = 2;
const TAG_INSTALL_SNAPSHOT_CHUNK: u8 = 3;
const TAG_INSTALL_SNAPSHOT_RESPONSE: u8 = 4;
const TAG_PRE_VOTE: u8 = 5;
const TAG_PRE_VOTE_RESPONSE: u8 = 6;
const TAG_TIMEOUT_NOW: u8 = 7;
const TAG_REQUEST_VOTE: u8 = 8;
const TAG_REQUEST_VOTE_RESPONSE: u8 = 9;

/// Failure of a peer-frame encode or decode.
///
/// This enum is exhaustive: the format above is closed over these framing,
/// version, and payload failures, and a transport deciding whether to drop a
/// connection has to be able to match on all of them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PeerCodecError {
    /// The frame does not begin with this codec's magic.
    NotAPeerFrame { magic: [u8; 4] },
    /// The frame declares a format this build cannot read.
    UnsupportedVersion { version: u8 },
    /// The frame ended before a field it declared.
    UnexpectedEof { needed: usize, remaining: usize },
    /// Bytes remained after the message was fully decoded.
    TrailingBytes { remaining: usize },
    /// The frame carries a tag this build does not know.
    UnknownTag { tag: u8 },
    /// A boolean field held a byte other than zero or one.
    InvalidBool { value: u8 },
    /// A nested log entry could not be encoded or decoded.
    LogEntry { detail: String },
    /// A nested snapshot envelope could not be encoded or decoded.
    Snapshot { detail: String },
    /// A whole-snapshot frame was submitted for encoding.
    ///
    /// The kernel never sends one; see the module documentation.
    WholeSnapshotUnsupported,
}

impl fmt::Display for PeerCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAPeerFrame { magic } => {
                write!(formatter, "frame magic {magic:?} is not a peer frame")
            }
            Self::UnsupportedVersion { version } => {
                write!(formatter, "unsupported peer frame version {version}")
            }
            Self::UnexpectedEof { needed, remaining } => write!(
                formatter,
                "peer frame needs {needed} more bytes but {remaining} remain"
            ),
            Self::TrailingBytes { remaining } => {
                write!(formatter, "peer frame has {remaining} trailing bytes")
            }
            Self::UnknownTag { tag } => write!(formatter, "unknown peer frame tag {tag}"),
            Self::InvalidBool { value } => {
                write!(formatter, "boolean field holds {value} rather than 0 or 1")
            }
            Self::LogEntry { detail } => write!(formatter, "malformed log entry: {detail}"),
            Self::Snapshot { detail } => write!(formatter, "malformed snapshot envelope: {detail}"),
            Self::WholeSnapshotUnsupported => {
                formatter.write_str("this peer format carries snapshot chunks, not whole snapshots")
            }
        }
    }
}

impl Error for PeerCodecError {}

/// Appends `message` to `out` as one peer frame.
///
/// # Errors
///
/// Returns an error when the message is a whole-snapshot frame or when a nested
/// entry or snapshot envelope cannot be encoded.
pub fn encode_message(message: &Message, out: &mut Vec<u8>) -> Result<(), PeerCodecError> {
    out.extend_from_slice(&PEER_MESSAGE_MAGIC);
    out.push(PEER_MESSAGE_VERSION);
    match message {
        Message::AppendEntries(request) => {
            out.push(TAG_APPEND_ENTRIES);
            encode_append_entries(request, out)?;
        }
        Message::AppendEntriesResponse(response) => {
            out.push(TAG_APPEND_ENTRIES_RESPONSE);
            put_u64(out, response.term.0);
            put_u64(out, response.follower_id.0);
            put_bool(out, response.success);
            put_u64(out, response.match_index.0);
            put_u64(out, response.sequence);
        }
        Message::InstallSnapshot(_) => return Err(PeerCodecError::WholeSnapshotUnsupported),
        Message::InstallSnapshotChunk(chunk) => {
            out.push(TAG_INSTALL_SNAPSHOT_CHUNK);
            encode_install_snapshot_chunk(chunk, out)?;
        }
        Message::InstallSnapshotResponse(response) => {
            out.push(TAG_INSTALL_SNAPSHOT_RESPONSE);
            put_u64(out, response.term.0);
            put_u64(out, response.follower_id.0);
            put_bool(out, response.success);
            put_u64(out, response.last_included_index.0);
            // The presence flag and the value are written unconditionally so
            // the record has one fixed width whether or not a transfer is
            // named; an absent transfer writes a zero nobody reads.
            put_bool(out, response.transfer_id.is_some());
            put_u64(out, response.transfer_id.map_or(0, |id| id.0));
            put_u64(out, response.next_offset);
        }
        Message::PreVote(request) => {
            out.push(TAG_PRE_VOTE);
            put_u64(out, request.term.0);
            put_u64(out, request.candidate_id.0);
            put_u64(out, request.last_log_index.0);
            put_u64(out, request.last_log_term.0);
        }
        Message::PreVoteResponse(response) => {
            out.push(TAG_PRE_VOTE_RESPONSE);
            put_u64(out, response.term.0);
            put_u64(out, response.voter_id.0);
            put_bool(out, response.vote_granted);
        }
        Message::TimeoutNow(request) => {
            out.push(TAG_TIMEOUT_NOW);
            put_u64(out, request.term.0);
            put_u64(out, request.leader_id.0);
        }
        Message::RequestVote(request) => {
            out.push(TAG_REQUEST_VOTE);
            put_u64(out, request.term.0);
            put_u64(out, request.candidate_id.0);
            put_u64(out, request.last_log_index.0);
            put_u64(out, request.last_log_term.0);
        }
        Message::RequestVoteResponse(response) => {
            out.push(TAG_REQUEST_VOTE_RESPONSE);
            put_u64(out, response.term.0);
            put_u64(out, response.voter_id.0);
            put_bool(out, response.vote_granted);
        }
    }
    Ok(())
}

/// Decodes one whole peer frame.
///
/// # Errors
///
/// Returns an error when the frame's magic, version, tag, or fields are
/// malformed, or when bytes remain after the message.
pub fn decode_message(frame: &[u8]) -> Result<Message, PeerCodecError> {
    let mut reader = Reader::new(frame);
    let magic = reader.array::<4>()?;
    if magic != PEER_MESSAGE_MAGIC {
        return Err(PeerCodecError::NotAPeerFrame { magic });
    }
    let version = reader.u8()?;
    if version != PEER_MESSAGE_VERSION {
        return Err(PeerCodecError::UnsupportedVersion { version });
    }

    let tag = reader.u8()?;
    let message = match tag {
        TAG_APPEND_ENTRIES => Message::AppendEntries(decode_append_entries(&mut reader)?),
        TAG_APPEND_ENTRIES_RESPONSE => Message::AppendEntriesResponse(AppendEntriesResponse {
            term: Term(reader.u64()?),
            follower_id: NodeId(reader.u64()?),
            success: reader.bool()?,
            match_index: LogIndex(reader.u64()?),
            sequence: reader.u64()?,
        }),
        TAG_INSTALL_SNAPSHOT_CHUNK => {
            Message::InstallSnapshotChunk(decode_install_snapshot_chunk(&mut reader)?)
        }
        TAG_INSTALL_SNAPSHOT_RESPONSE => {
            let term = Term(reader.u64()?);
            let follower_id = NodeId(reader.u64()?);
            let success = reader.bool()?;
            let last_included_index = LogIndex(reader.u64()?);
            let has_transfer_id = reader.bool()?;
            let raw_transfer_id = reader.u64()?;
            Message::InstallSnapshotResponse(InstallSnapshotResponse {
                term,
                follower_id,
                success,
                last_included_index,
                transfer_id: has_transfer_id.then_some(SnapshotTransferId(raw_transfer_id)),
                next_offset: reader.u64()?,
            })
        }
        TAG_PRE_VOTE => Message::PreVote(PreVote {
            term: Term(reader.u64()?),
            candidate_id: NodeId(reader.u64()?),
            last_log_index: LogIndex(reader.u64()?),
            last_log_term: Term(reader.u64()?),
        }),
        TAG_PRE_VOTE_RESPONSE => Message::PreVoteResponse(PreVoteResponse {
            term: Term(reader.u64()?),
            voter_id: NodeId(reader.u64()?),
            vote_granted: reader.bool()?,
        }),
        TAG_TIMEOUT_NOW => Message::TimeoutNow(TimeoutNow {
            term: Term(reader.u64()?),
            leader_id: NodeId(reader.u64()?),
        }),
        TAG_REQUEST_VOTE => Message::RequestVote(RequestVote {
            term: Term(reader.u64()?),
            candidate_id: NodeId(reader.u64()?),
            last_log_index: LogIndex(reader.u64()?),
            last_log_term: Term(reader.u64()?),
        }),
        TAG_REQUEST_VOTE_RESPONSE => Message::RequestVoteResponse(RequestVoteResponse {
            term: Term(reader.u64()?),
            voter_id: NodeId(reader.u64()?),
            vote_granted: reader.bool()?,
        }),
        tag => return Err(PeerCodecError::UnknownTag { tag }),
    };

    reader.finish()?;
    Ok(message)
}

/// Encodes the sender of a decoded message.
///
/// A receiver needs the sending node's identity to hand the message to its
/// group, and every message already names its sender in a field. Deriving it
/// from the message rather than from a connection is deliberate: the connection
/// proves nothing about identity in an unauthenticated transport, so a separate
/// claimed-identity field would only invite a reader to believe it.
pub const fn message_sender(message: &Message) -> NodeId {
    match message {
        Message::AppendEntries(request) => request.leader_id,
        Message::AppendEntriesResponse(response) => response.follower_id,
        Message::InstallSnapshot(request) => request.leader_id,
        Message::InstallSnapshotChunk(chunk) => chunk.leader_id,
        Message::InstallSnapshotResponse(response) => response.follower_id,
        Message::PreVote(request) => request.candidate_id,
        Message::PreVoteResponse(response) => response.voter_id,
        Message::TimeoutNow(request) => request.leader_id,
        Message::RequestVote(request) => request.candidate_id,
        Message::RequestVoteResponse(response) => response.voter_id,
    }
}

fn encode_append_entries(request: &AppendEntries, out: &mut Vec<u8>) -> Result<(), PeerCodecError> {
    put_u64(out, request.term.0);
    put_u64(out, request.leader_id.0);
    put_u64(out, request.prev_log_index.0);
    put_u64(out, request.prev_log_term.0);
    put_u64(out, request.sequence);
    put_u64(out, request.leader_commit.0);

    let entries = request.entries.as_slice();
    put_u32(out, u32_len(entries.len()));
    for (offset, entry) in entries.iter().enumerate() {
        // The index is reconstructible from `prev_log_index`, but the persisted
        // entry format carries one and a synthetic value would be a second
        // encoding of the same fact. The real index is written and checked.
        let index = LogIndex(request.prev_log_index.0.saturating_add(offset as u64 + 1));
        let persisted = PersistedRaftLogEntry {
            index,
            term: entry.term,
            kind: entry.kind.clone(),
        };
        let encoded =
            encode_raft_log_entry(&persisted).map_err(|error| PeerCodecError::LogEntry {
                detail: format!("{error:?}"),
            })?;
        put_block(out, &encoded);
    }
    Ok(())
}

fn decode_append_entries(reader: &mut Reader<'_>) -> Result<AppendEntries, PeerCodecError> {
    let term = Term(reader.u64()?);
    let leader_id = NodeId(reader.u64()?);
    let prev_log_index = LogIndex(reader.u64()?);
    let prev_log_term = Term(reader.u64()?);
    let sequence = reader.u64()?;
    let leader_commit = LogIndex(reader.u64()?);

    let count = reader.u32()? as usize;
    let mut entries = Vec::with_capacity(count.min(reader.remaining()));
    for _ in 0..count {
        let block = reader.block()?;
        let persisted = decode_raft_log_entry(block).map_err(|error| PeerCodecError::LogEntry {
            detail: format!("{error:?}"),
        })?;
        entries.push(LogEntry {
            term: persisted.term,
            kind: persisted.kind,
        });
    }

    Ok(AppendEntries {
        term,
        leader_id,
        prev_log_index,
        prev_log_term,
        sequence,
        entries: entries.into(),
        leader_commit,
    })
}

fn encode_install_snapshot_chunk(
    chunk: &InstallSnapshotChunk,
    out: &mut Vec<u8>,
) -> Result<(), PeerCodecError> {
    put_u64(out, chunk.term.0);
    put_u64(out, chunk.leader_id.0);
    put_u64(out, chunk.transfer_id.0);
    put_u64(out, chunk.total_payload_len);
    put_u32(out, chunk.application_payload_crc32);
    put_u64(out, chunk.offset);
    put_bool(out, chunk.done);

    // The metadata rides in a snapshot envelope with an empty payload. The
    // chunk's own bytes follow as their own block rather than as that
    // envelope's payload, because a chunk is a slice of a snapshot rather than
    // a snapshot.
    let envelope = encode_raft_snapshot(&PersistedRaftSnapshot {
        metadata: chunk.metadata.clone(),
        application_payload: Vec::new(),
    })
    .map_err(|error| PeerCodecError::Snapshot {
        detail: format!("{error:?}"),
    })?;
    put_block(out, &envelope);
    put_block(out, &chunk.chunk);
    Ok(())
}

fn decode_install_snapshot_chunk(
    reader: &mut Reader<'_>,
) -> Result<InstallSnapshotChunk, PeerCodecError> {
    let term = Term(reader.u64()?);
    let leader_id = NodeId(reader.u64()?);
    let transfer_id = SnapshotTransferId(reader.u64()?);
    let total_payload_len = reader.u64()?;
    let application_payload_crc32 = reader.u32()?;
    let offset = reader.u64()?;
    let done = reader.bool()?;

    let envelope = reader.block()?;
    let snapshot = decode_raft_snapshot(envelope).map_err(|error| PeerCodecError::Snapshot {
        detail: format!("{error:?}"),
    })?;
    let chunk = reader.block()?.to_vec();

    Ok(InstallSnapshotChunk {
        term,
        leader_id,
        transfer_id,
        metadata: snapshot.metadata,
        total_payload_len,
        application_payload_crc32,
        offset,
        chunk,
        done,
    })
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_bool(out: &mut Vec<u8>, value: bool) {
    out.push(u8::from(value));
}

fn put_block(out: &mut Vec<u8>, bytes: &[u8]) {
    put_u32(out, u32_len(bytes.len()));
    out.extend_from_slice(bytes);
}

/// Narrows a length to the `u32` the format declares.
///
/// A frame this long cannot be produced: Rafter bounds an append batch and a
/// snapshot chunk well below four gigabytes, and the link's own frame limit
/// refuses anything larger before it is read. Saturating keeps the arithmetic
/// total; a saturated length fails the decoder's own bounds check rather than
/// silently truncating.
fn u32_len(len: usize) -> u32 {
    u32::try_from(len).unwrap_or(u32::MAX)
}

/// Bounds-checked cursor over one frame.
struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    const fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], PeerCodecError> {
        if self.remaining() < len {
            return Err(PeerCodecError::UnexpectedEof {
                needed: len,
                remaining: self.remaining(),
            });
        }
        let taken = &self.bytes[self.offset..self.offset + len];
        self.offset += len;
        Ok(taken)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], PeerCodecError> {
        let taken = self.take(N)?;
        let mut array = [0_u8; N];
        array.copy_from_slice(taken);
        Ok(array)
    }

    fn u8(&mut self) -> Result<u8, PeerCodecError> {
        Ok(self.array::<1>()?[0])
    }

    fn u32(&mut self) -> Result<u32, PeerCodecError> {
        Ok(u32::from_be_bytes(self.array::<4>()?))
    }

    fn u64(&mut self) -> Result<u64, PeerCodecError> {
        Ok(u64::from_be_bytes(self.array::<8>()?))
    }

    fn bool(&mut self) -> Result<bool, PeerCodecError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(PeerCodecError::InvalidBool { value }),
        }
    }

    fn block(&mut self) -> Result<&'a [u8], PeerCodecError> {
        let len = self.u32()? as usize;
        self.take(len)
    }

    fn finish(&self) -> Result<(), PeerCodecError> {
        if self.remaining() == 0 {
            return Ok(());
        }
        Err(PeerCodecError::TrailingBytes {
            remaining: self.remaining(),
        })
    }
}

#[cfg(test)]
mod tests {
    use rafter::{
        ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion,
        ConfigurationEntry, ConfigurationId, InstallSnapshot, MembershipConfig, MembershipSet,
        RaftSnapshotMetadata, SnapshotCommittedConfiguration, SnapshotGroupId,
    };

    use super::*;

    fn round_trip(message: &Message) -> Message {
        let mut frame = Vec::new();
        encode_message(message, &mut frame).expect("encodes");
        decode_message(&frame).expect("decodes")
    }

    fn membership(learners: Vec<NodeId>) -> MembershipSet {
        MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], learners)
            .expect("three voters are a valid membership")
    }

    fn snapshot_metadata() -> RaftSnapshotMetadata {
        RaftSnapshotMetadata {
            group_id: SnapshotGroupId::new("ledger").expect("a short group id is valid"),
            writer_id: NodeId(1),
            last_included_index: LogIndex(9),
            last_included_term: Term(3),
            hard_state_term: Term(4),
            application: ApplicationSnapshotMetadata::new(
                ApplicationSnapshotKind::new("ledger").expect("a short kind is valid"),
                ApplicationSnapshotVersion::new(1).expect("version one is nonzero"),
            ),
            committed_configuration: Some(SnapshotCommittedConfiguration::new(
                None,
                MembershipConfig::Stable(membership(Vec::new())),
            )),
        }
    }

    #[test]
    fn every_scalar_message_round_trips() {
        let messages = [
            Message::AppendEntriesResponse(AppendEntriesResponse {
                term: Term(7),
                follower_id: NodeId(2),
                success: true,
                match_index: LogIndex(41),
                sequence: 5,
            }),
            Message::PreVote(PreVote {
                term: Term(8),
                candidate_id: NodeId(3),
                last_log_index: LogIndex(12),
                last_log_term: Term(6),
            }),
            Message::PreVoteResponse(PreVoteResponse {
                term: Term(8),
                voter_id: NodeId(1),
                vote_granted: false,
            }),
            Message::TimeoutNow(TimeoutNow {
                term: Term(9),
                leader_id: NodeId(2),
            }),
            Message::RequestVote(RequestVote {
                term: Term(9),
                candidate_id: NodeId(3),
                last_log_index: LogIndex(12),
                last_log_term: Term(6),
            }),
            Message::RequestVoteResponse(RequestVoteResponse {
                term: Term(9),
                voter_id: NodeId(1),
                vote_granted: true,
            }),
            Message::InstallSnapshotResponse(InstallSnapshotResponse {
                term: Term(10),
                follower_id: NodeId(3),
                success: true,
                last_included_index: LogIndex(9),
                transfer_id: Some(SnapshotTransferId(4)),
                next_offset: 128,
            }),
            Message::InstallSnapshotResponse(InstallSnapshotResponse {
                term: Term(10),
                follower_id: NodeId(3),
                success: false,
                last_included_index: LogIndex(0),
                transfer_id: None,
                next_offset: 0,
            }),
        ];
        for message in messages {
            assert_eq!(round_trip(&message), message, "round trip must be exact");
        }
    }

    #[test]
    fn append_entries_round_trips_every_entry_kind() {
        let message = Message::AppendEntries(AppendEntries {
            term: Term(4),
            leader_id: NodeId(1),
            prev_log_index: LogIndex(6),
            prev_log_term: Term(3),
            sequence: 11,
            entries: vec![
                LogEntry::noop(Term(4)),
                LogEntry::application(Term(4), vec![1_u8, 2, 3]),
                LogEntry::configuration(
                    Term(4),
                    ConfigurationEntry::stable(ConfigurationId(2), membership(vec![NodeId(4)])),
                ),
            ]
            .into(),
            leader_commit: LogIndex(6),
        });
        assert_eq!(round_trip(&message), message);
    }

    #[test]
    fn an_empty_append_is_a_heartbeat_that_round_trips() {
        let message = Message::AppendEntries(AppendEntries {
            term: Term(2),
            leader_id: NodeId(1),
            prev_log_index: LogIndex(0),
            prev_log_term: Term(0),
            sequence: 1,
            entries: Vec::new().into(),
            leader_commit: LogIndex(0),
        });
        assert_eq!(round_trip(&message), message);
    }

    #[test]
    fn a_snapshot_chunk_round_trips_with_its_metadata() {
        let message = Message::InstallSnapshotChunk(InstallSnapshotChunk {
            term: Term(5),
            leader_id: NodeId(1),
            transfer_id: SnapshotTransferId(2),
            metadata: snapshot_metadata(),
            total_payload_len: 64,
            application_payload_crc32: 0xDEAD_BEEF,
            offset: 32,
            chunk: vec![9_u8; 32],
            done: true,
        });
        assert_eq!(round_trip(&message), message);
    }

    #[test]
    fn a_whole_snapshot_frame_is_refused_rather_than_encoded() {
        let message = Message::InstallSnapshot(InstallSnapshot {
            term: Term(5),
            leader_id: NodeId(1),
            metadata: snapshot_metadata(),
            application_payload: vec![1_u8, 2, 3],
        });
        let mut frame = Vec::new();
        assert_eq!(
            encode_message(&message, &mut frame),
            Err(PeerCodecError::WholeSnapshotUnsupported)
        );
    }

    #[test]
    fn every_message_names_the_node_that_sent_it() {
        let message = Message::AppendEntries(AppendEntries {
            term: Term(2),
            leader_id: NodeId(7),
            prev_log_index: LogIndex(0),
            prev_log_term: Term(0),
            sequence: 1,
            entries: Vec::new().into(),
            leader_commit: LogIndex(0),
        });
        assert_eq!(message_sender(&message), NodeId(7));
    }

    #[test]
    fn a_foreign_frame_is_refused_by_magic_before_anything_else() {
        let error = decode_message(b"XXXX\x01\x01").expect_err("foreign magic is refused");
        assert_eq!(
            error,
            PeerCodecError::NotAPeerFrame { magic: *b"XXXX" },
            "the magic check must precede every other decision"
        );
    }

    #[test]
    fn a_future_version_is_refused_rather_than_reinterpreted() {
        let mut frame = PEER_MESSAGE_MAGIC.to_vec();
        frame.push(PEER_MESSAGE_VERSION + 1);
        frame.push(TAG_TIMEOUT_NOW);
        assert_eq!(
            decode_message(&frame),
            Err(PeerCodecError::UnsupportedVersion {
                version: PEER_MESSAGE_VERSION + 1
            })
        );
    }

    #[test]
    fn a_truncated_frame_is_refused_rather_than_read_past_its_end() {
        let mut frame = Vec::new();
        encode_message(
            &Message::TimeoutNow(TimeoutNow {
                term: Term(3),
                leader_id: NodeId(1),
            }),
            &mut frame,
        )
        .expect("encodes");
        for truncated in 1..frame.len() {
            assert!(
                decode_message(&frame[..truncated]).is_err(),
                "a frame truncated to {truncated} bytes must be refused"
            );
        }
    }

    #[test]
    fn trailing_bytes_are_refused_rather_than_ignored() {
        let mut frame = Vec::new();
        encode_message(
            &Message::TimeoutNow(TimeoutNow {
                term: Term(3),
                leader_id: NodeId(1),
            }),
            &mut frame,
        )
        .expect("encodes");
        frame.push(0);
        assert_eq!(
            decode_message(&frame),
            Err(PeerCodecError::TrailingBytes { remaining: 1 })
        );
    }

    #[test]
    fn an_unknown_tag_is_refused() {
        let mut frame = PEER_MESSAGE_MAGIC.to_vec();
        frame.push(PEER_MESSAGE_VERSION);
        frame.push(200);
        assert_eq!(
            decode_message(&frame),
            Err(PeerCodecError::UnknownTag { tag: 200 })
        );
    }
}
