//! Canonical client and server hello encoding.

use std::{
    num::{NonZeroU16, NonZeroU32},
    str,
};

use crate::{ClusterId, ConnectionSession, PeerId};

use super::{
    ClientHello, DecodeHandshakeError, HandshakeField, ServerHello, ServerHelloStatus,
    ServerRefusal, VersionRange, HANDSHAKE_MAGIC, MAX_CLIENT_HELLO_BYTES, MAX_SERVER_HELLO_BYTES,
};
use crate::wire::read::{put_u16, put_u32, put_u64, put_u8, Reader, UnexpectedEnd};

/// Writes one canonical client hello, replacing `output`.
pub fn encode_client_hello_into(output: &mut Vec<u8>, hello: &ClientHello) {
    output.clear();
    output.reserve(MAX_CLIENT_HELLO_BYTES);
    output.extend_from_slice(&HANDSHAKE_MAGIC);
    put_u16(output, hello.transport_versions().minimum());
    put_u16(output, hello.transport_versions().maximum());
    put_u16(output, hello.peer_codec_versions().minimum());
    put_u16(output, hello.peer_codec_versions().maximum());
    put_identity(
        output,
        hello.cluster_id().as_str(),
        hello.cluster_id().wire_len(),
    );
    put_identity(
        output,
        hello.claimed_peer_id().as_str(),
        hello.claimed_peer_id().wire_len(),
    );
    put_u64(output, hello.connection_session().get());
    put_u32(output, hello.max_send_frame_bytes().get());
}

/// Decodes exactly one canonical client hello.
///
/// # Errors
///
/// Returns [`DecodeHandshakeError`] for truncation, malformed identities or
/// ranges, zero session/frame values, invalid magic, or trailing bytes.
pub fn decode_client_hello(input: &[u8]) -> Result<ClientHello, DecodeHandshakeError> {
    if input.len() > MAX_CLIENT_HELLO_BYTES {
        return Err(DecodeHandshakeError::TooLong {
            actual: input.len(),
            maximum: MAX_CLIENT_HELLO_BYTES,
        });
    }

    let mut reader = Reader::new(input);
    read_magic(&mut reader)?;
    let transport_versions = read_version_range(&mut reader, HandshakeField::TransportVersions)?;
    let peer_codec_versions = read_version_range(&mut reader, HandshakeField::PeerCodecVersions)?;
    let cluster_id = read_cluster_id(&mut reader)?;
    let claimed_peer_id = read_peer_id(&mut reader, HandshakeField::ClaimedPeerId)?;
    let connection_session = ConnectionSession::new(read_u64(&mut reader)?)
        .map_err(|_| DecodeHandshakeError::ZeroSession)?;
    let max_send_frame_bytes =
        NonZeroU32::new(read_u32(&mut reader)?).ok_or(DecodeHandshakeError::ZeroFrameLimit)?;
    finish(&reader)?;

    Ok(ClientHello::new(
        transport_versions,
        peer_codec_versions,
        cluster_id,
        claimed_peer_id,
        connection_session,
        max_send_frame_bytes,
    ))
}

/// Writes one canonical server hello, replacing `output`.
pub fn encode_server_hello_into(output: &mut Vec<u8>, hello: &ServerHello) {
    output.clear();
    output.reserve(MAX_SERVER_HELLO_BYTES);
    output.extend_from_slice(&HANDSHAKE_MAGIC);
    put_u16(
        output,
        hello
            .selected_transport_version()
            .map_or(0, NonZeroU16::get),
    );
    put_u16(
        output,
        hello
            .selected_peer_codec_version()
            .map_or(0, NonZeroU16::get),
    );
    put_identity(
        output,
        hello.cluster_id().as_str(),
        hello.cluster_id().wire_len(),
    );
    put_identity(
        output,
        hello.server_peer_id().as_str(),
        hello.server_peer_id().wire_len(),
    );
    put_u32(
        output,
        hello.accepted_frame_bytes().map_or(0, NonZeroU32::get),
    );
    put_u8(
        output,
        match hello.status() {
            ServerHelloStatus::Accepted => 0,
            ServerHelloStatus::Refused(refusal) => refusal.wire_tag(),
        },
    );
}

/// Decodes exactly one canonical server hello.
///
/// # Errors
///
/// Returns [`DecodeHandshakeError`] for truncation, malformed identities,
/// unknown status, noncanonical accepted/refused fields, invalid magic, or
/// trailing bytes.
pub fn decode_server_hello(input: &[u8]) -> Result<ServerHello, DecodeHandshakeError> {
    if input.len() > MAX_SERVER_HELLO_BYTES {
        return Err(DecodeHandshakeError::TooLong {
            actual: input.len(),
            maximum: MAX_SERVER_HELLO_BYTES,
        });
    }

    let mut reader = Reader::new(input);
    read_magic(&mut reader)?;
    let selected_transport_version = read_u16(&mut reader)?;
    let selected_peer_codec_version = read_u16(&mut reader)?;
    let cluster_id = read_cluster_id(&mut reader)?;
    let server_peer_id = read_peer_id(&mut reader, HandshakeField::ServerPeerId)?;
    let accepted_frame_bytes = read_u32(&mut reader)?;
    let status = read_u8(&mut reader)?;
    finish(&reader)?;

    if status == 0 {
        let selected_transport_version = NonZeroU16::new(selected_transport_version)
            .ok_or(DecodeHandshakeError::NonCanonicalAccepted)?;
        let selected_peer_codec_version = NonZeroU16::new(selected_peer_codec_version)
            .ok_or(DecodeHandshakeError::NonCanonicalAccepted)?;
        let accepted_frame_bytes = NonZeroU32::new(accepted_frame_bytes)
            .ok_or(DecodeHandshakeError::NonCanonicalAccepted)?;
        return Ok(ServerHello::accepted(
            selected_transport_version,
            selected_peer_codec_version,
            cluster_id,
            server_peer_id,
            accepted_frame_bytes,
        ));
    }

    let refusal = ServerRefusal::from_wire_tag(status)
        .ok_or(DecodeHandshakeError::UnknownServerStatus(status))?;
    if selected_transport_version != 0
        || selected_peer_codec_version != 0
        || accepted_frame_bytes != 0
    {
        return Err(DecodeHandshakeError::NonCanonicalRefusal);
    }
    Ok(ServerHello::refused(cluster_id, server_peer_id, refusal))
}

fn put_identity(output: &mut Vec<u8>, value: &str, len: u8) {
    put_u8(output, len);
    output.extend_from_slice(value.as_bytes());
}

fn read_magic(reader: &mut Reader<'_>) -> Result<(), DecodeHandshakeError> {
    let magic = reader
        .bytes(HANDSHAKE_MAGIC.len())
        .map_err(map_unexpected_end)?;
    if magic == HANDSHAKE_MAGIC.as_slice() {
        Ok(())
    } else {
        Err(DecodeHandshakeError::InvalidMagic)
    }
}

fn read_version_range(
    reader: &mut Reader<'_>,
    field: HandshakeField,
) -> Result<VersionRange, DecodeHandshakeError> {
    let minimum = read_u16(reader)?;
    let maximum = read_u16(reader)?;
    VersionRange::new(minimum, maximum)
        .map_err(|source| DecodeHandshakeError::InvalidVersionRange { field, source })
}

fn read_cluster_id(reader: &mut Reader<'_>) -> Result<ClusterId, DecodeHandshakeError> {
    let value = read_identity(reader, HandshakeField::ClusterId)?;
    ClusterId::new(value).map_err(|source| DecodeHandshakeError::InvalidIdentity {
        field: HandshakeField::ClusterId,
        source,
    })
}

fn read_peer_id(
    reader: &mut Reader<'_>,
    field: HandshakeField,
) -> Result<PeerId, DecodeHandshakeError> {
    let value = read_identity(reader, field)?;
    PeerId::new(value).map_err(|source| DecodeHandshakeError::InvalidIdentity { field, source })
}

fn read_identity<'a>(
    reader: &mut Reader<'a>,
    field: HandshakeField,
) -> Result<&'a str, DecodeHandshakeError> {
    let len = usize::from(read_u8(reader)?);
    let bytes = reader.bytes(len).map_err(map_unexpected_end)?;
    str::from_utf8(bytes).map_err(|_| DecodeHandshakeError::InvalidUtf8 { field })
}

fn read_u8(reader: &mut Reader<'_>) -> Result<u8, DecodeHandshakeError> {
    reader.u8().map_err(map_unexpected_end)
}

fn read_u16(reader: &mut Reader<'_>) -> Result<u16, DecodeHandshakeError> {
    reader.u16().map_err(map_unexpected_end)
}

fn read_u32(reader: &mut Reader<'_>) -> Result<u32, DecodeHandshakeError> {
    reader.u32().map_err(map_unexpected_end)
}

fn read_u64(reader: &mut Reader<'_>) -> Result<u64, DecodeHandshakeError> {
    reader.u64().map_err(map_unexpected_end)
}

fn finish(reader: &Reader<'_>) -> Result<(), DecodeHandshakeError> {
    if reader.remaining() == 0 {
        Ok(())
    } else {
        Err(DecodeHandshakeError::TrailingBytes {
            remaining: reader.remaining(),
        })
    }
}

const fn map_unexpected_end(_: UnexpectedEnd) -> DecodeHandshakeError {
    DecodeHandshakeError::Truncated
}
