//! Length-prefixed framing around `rafter-codec` peer messages.
//!
//! This module owns the four-byte big-endian prefix, the receive bound applied
//! before a payload is allocated, and the sender identity a decoded message
//! claims. It owns no connection: callers hand it any reader or writer.

use std::io::{Read, Write};

use rafter::{Message, NodeId};
use rafter_codec::{decode_message, encode_message_into};

use crate::{ReadFrameError, WriteFrameError};

/// Default maximum accepted frame payload: 1 MiB after the length prefix.
pub const DEFAULT_MAX_FRAME_LEN: usize = 1024 * 1024;

/// Writes one length-prefixed `rafter-codec` peer-message frame.
///
/// # Errors
///
/// Returns [`WriteFrameError`] if encoding fails, the encoded payload is too
/// large for the u32 length prefix, or the writer fails.
pub fn write_message_frame(
    writer: &mut impl Write,
    message: &Message,
) -> Result<(), WriteFrameError> {
    let mut scratch = Vec::new();
    write_message_frame_into(writer, &mut scratch, message)
}

/// Writes one length-prefixed peer-message frame using a reusable encode buffer.
///
/// # Errors
///
/// Returns [`WriteFrameError`] if encoding fails, the encoded payload is too
/// large for the u32 length prefix, or the writer fails.
pub fn write_message_frame_into(
    writer: &mut impl Write,
    scratch: &mut Vec<u8>,
    message: &Message,
) -> Result<(), WriteFrameError> {
    encode_message_into(scratch, message).map_err(WriteFrameError::Encode)?;
    let len = u32::try_from(scratch.len())
        .map_err(|_| WriteFrameError::FrameTooLarge { len: scratch.len() })?;
    writer
        .write_all(&len.to_be_bytes())
        .map_err(WriteFrameError::Io)?;
    writer.write_all(scratch).map_err(WriteFrameError::Io)
}

/// Reads one length-prefixed `rafter-codec` peer-message frame.
///
/// # Errors
///
/// Returns [`ReadFrameError`] if the reader fails, the frame exceeds
/// `max_frame_len`, or `rafter-codec` rejects the payload.
pub fn read_message_frame(
    reader: &mut impl Read,
    max_frame_len: usize,
) -> Result<Message, ReadFrameError> {
    let mut len_bytes = [0u8; 4];
    reader
        .read_exact(&mut len_bytes)
        .map_err(ReadFrameError::Io)?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    if len > max_frame_len {
        return Err(ReadFrameError::FrameTooLarge {
            len,
            max: max_frame_len,
        });
    }

    let mut payload = vec![0u8; len];
    reader
        .read_exact(&mut payload)
        .map_err(ReadFrameError::Io)?;
    decode_message(&payload).map_err(ReadFrameError::Decode)
}

/// Returns the Raft sender id embedded in a peer message.
#[must_use]
pub const fn message_sender(message: &Message) -> NodeId {
    match message {
        Message::RequestVote(request) => request.candidate_id,
        Message::RequestVoteResponse(response) => response.voter_id,
        Message::PreVote(request) => request.candidate_id,
        Message::PreVoteResponse(response) => response.voter_id,
        Message::TimeoutNow(request) => request.leader_id,
        Message::AppendEntries(request) => request.leader_id,
        Message::AppendEntriesResponse(response) => response.follower_id,
        Message::InstallSnapshot(request) => request.leader_id,
        Message::InstallSnapshotResponse(response) => response.follower_id,
        Message::InstallSnapshotChunk(request) => request.leader_id,
    }
}
