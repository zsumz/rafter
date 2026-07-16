//! RFPM frame envelope, version dispatch, and checksum verification.

use rafter::Message;
use rafter_crc32::crc32;

use crate::{
    v1,
    wire::{CountingSink, Reader, Sink, VecSink, Writer},
    DecodePeerMessageError, EncodePeerMessageError,
};

/// Wire-format magic tag identifying a Rafter Peer Message frame.
pub const MAGIC: [u8; 4] = *b"RFPM";

/// Peer wire-format version emitted by this codec.
///
/// This is the first public peer-wire version. Earlier internal draft formats
/// are intentionally unsupported.
pub const VERSION: u8 = 1;

/// Encodes a Raft peer message as the current versioned byte payload.
///
/// # Errors
///
/// Returns [`EncodePeerMessageError`] when a variable-length field cannot be
/// represented or the message has no current peer-wire representation.
pub fn encode_message(message: &Message) -> Result<Vec<u8>, EncodePeerMessageError> {
    let mut encoded = Vec::new();
    encode_message_into(&mut encoded, message)?;
    Ok(encoded)
}

/// Encodes a Raft peer message into a caller-owned reusable buffer.
///
/// The buffer is cleared before encoding. On success it contains exactly one
/// current-version peer message frame; on error it is empty. The same encoder
/// first writes to a counting sink so capacity sizing cannot drift from the
/// version 1 grammar.
///
/// # Errors
///
/// Returns [`EncodePeerMessageError`] when a variable-length field cannot be
/// represented or the message has no current peer-wire representation.
pub fn encode_message_into(
    output: &mut Vec<u8>,
    message: &Message,
) -> Result<(), EncodePeerMessageError> {
    output.clear();

    let mut counter = Writer::new(CountingSink::default());
    encode_frame_body(&mut counter, message)?;
    output.reserve(counter.position().saturating_add(4));

    let result = {
        let mut writer = Writer::new(VecSink::new(output));
        encode_frame_body(&mut writer, message)
    };
    if let Err(error) = result {
        output.clear();
        return Err(error);
    }

    let checksum = crc32(output);
    output.extend_from_slice(&checksum.to_be_bytes());
    Ok(())
}

fn encode_frame_body<S: Sink>(
    writer: &mut Writer<S>,
    message: &Message,
) -> Result<(), EncodePeerMessageError> {
    writer.bytes(&MAGIC);
    writer.u8(VERSION);
    v1::encode_payload(writer, message)
}

/// Decodes exactly one Raft peer message frame.
///
/// The codec imposes no receive-size limit. Stream transports must supply
/// outer framing and reject oversized frames before allocating their payload.
///
/// # Errors
///
/// Returns [`DecodePeerMessageError`] when the frame is malformed, corrupt,
/// noncanonical, or uses an unsupported peer-wire version.
pub fn decode_message(payload: &[u8]) -> Result<Message, DecodePeerMessageError> {
    let mut reader = Reader::new(payload);
    let magic = reader.array_4()?;
    if magic != MAGIC {
        return Err(DecodePeerMessageError::InvalidMagic(magic));
    }

    let version = reader.u8()?;
    if version != VERSION {
        return Err(DecodePeerMessageError::UnsupportedVersion(version));
    }

    let message = v1::decode_payload(&mut reader)?;
    let checksum_start = reader.position();
    let expected = reader.u32()?;
    let actual = crc32(&payload[..checksum_start]);
    if expected != actual {
        return Err(DecodePeerMessageError::FrameChecksumMismatch { expected, actual });
    }

    reader.finish()?;
    Ok(message)
}
