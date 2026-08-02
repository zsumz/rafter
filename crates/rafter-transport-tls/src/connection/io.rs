//! Bounded stream reads and exact handshake/frame writes.

use std::{
    error::Error,
    fmt,
    io::{self, Read, Write},
    net::TcpStream,
};

use crate::queue::{ReceiveMemoryBudget, ReceiveMemoryPermit};
use crate::{
    decode_client_hello, decode_server_hello, encode_client_hello_into, encode_server_hello_into,
    ClientHello, DecodeHandshakeError, ServerHello, HANDSHAKE_MAGIC, MAX_CLIENT_HELLO_BYTES,
    MAX_ID_BYTES, MAX_SERVER_HELLO_BYTES, PEER_FRAME_LENGTH_PREFIX_BYTES,
};

use super::deadline::HandshakeDeadline;

#[derive(Debug)]
pub(crate) enum HandshakeIoError {
    Io(io::Error),
    Decode(DecodeHandshakeError),
}

impl fmt::Display for HandshakeIoError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "handshake I/O failed: {error}"),
            Self::Decode(error) => write!(formatter, "handshake decoding failed: {error}"),
        }
    }
}

impl Error for HandshakeIoError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Decode(error) => Some(error),
        }
    }
}

#[derive(Debug)]
pub(crate) enum PeerFrameIoError {
    Io(io::Error),
    LengthUnsupported(u32),
    TooLarge { actual: usize, maximum: usize },
    ReceiveMemoryFull { required: usize, maximum: usize },
}

impl fmt::Display for PeerFrameIoError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "peer-frame I/O failed: {error}"),
            Self::LengthUnsupported(length) => write!(
                formatter,
                "peer-frame body length {length} cannot fit local address space"
            ),
            Self::TooLarge { actual, maximum } => write!(
                formatter,
                "peer frame is {actual} bytes, exceeding negotiated maximum {maximum}"
            ),
            Self::ReceiveMemoryFull { required, maximum } => write!(
                formatter,
                "peer frame requires {required} weighted receive bytes, exceeding the available \
                 transport-runtime budget of {maximum} bytes"
            ),
        }
    }
}

#[derive(Debug)]
pub(crate) enum PeerFrameRead {
    Closed,
    Idle,
    Complete {
        bytes: usize,
        memory: ReceiveMemoryPermit,
    },
}

impl PartialEq for PeerFrameRead {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Closed, Self::Closed) | (Self::Idle, Self::Idle) => true,
            (Self::Complete { bytes: left, .. }, Self::Complete { bytes: right, .. }) => {
                left == right
            }
            _ => false,
        }
    }
}

impl Eq for PeerFrameRead {}

impl Error for PeerFrameIoError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::LengthUnsupported(_) | Self::TooLarge { .. } | Self::ReceiveMemoryFull { .. } => {
                None
            }
        }
    }
}

pub(crate) fn complete_client_tls(
    connection: &mut rustls::ClientConnection,
    socket: &mut TcpStream,
    deadline: HandshakeDeadline,
) -> io::Result<()> {
    let mut socket = deadline.socket(socket);
    while connection.is_handshaking() {
        connection.complete_io(&mut socket)?;
    }
    Ok(())
}

pub(crate) fn complete_server_tls(
    connection: &mut rustls::ServerConnection,
    socket: &mut TcpStream,
    deadline: HandshakeDeadline,
) -> io::Result<()> {
    let mut socket = deadline.socket(socket);
    while connection.is_handshaking() {
        connection.complete_io(&mut socket)?;
    }
    Ok(())
}

pub(crate) fn write_client_hello(
    writer: &mut impl Write,
    hello: &ClientHello,
    scratch: &mut Vec<u8>,
) -> Result<(), HandshakeIoError> {
    encode_client_hello_into(scratch, hello);
    write_all_flush(writer, scratch).map_err(HandshakeIoError::Io)
}

pub(crate) fn read_client_hello(
    reader: &mut impl Read,
    scratch: &mut Vec<u8>,
) -> Result<ClientHello, HandshakeIoError> {
    read_hello(reader, scratch, true, MAX_CLIENT_HELLO_BYTES)?;
    decode_client_hello(scratch).map_err(HandshakeIoError::Decode)
}

pub(crate) fn write_server_hello(
    writer: &mut impl Write,
    hello: &ServerHello,
    scratch: &mut Vec<u8>,
) -> Result<(), HandshakeIoError> {
    encode_server_hello_into(scratch, hello);
    write_all_flush(writer, scratch).map_err(HandshakeIoError::Io)
}

pub(crate) fn read_server_hello(
    reader: &mut impl Read,
    scratch: &mut Vec<u8>,
) -> Result<ServerHello, HandshakeIoError> {
    read_hello(reader, scratch, false, MAX_SERVER_HELLO_BYTES)?;
    decode_server_hello(scratch).map_err(HandshakeIoError::Decode)
}

pub(crate) fn read_peer_frame(
    reader: &mut impl Read,
    maximum: usize,
    memory: &ReceiveMemoryBudget,
    output: &mut Vec<u8>,
) -> Result<PeerFrameRead, PeerFrameIoError> {
    let mut prefix = [0_u8; PEER_FRAME_LENGTH_PREFIX_BYTES];
    loop {
        match reader.read(&mut prefix[..1]) {
            Ok(0) => return Ok(PeerFrameRead::Closed),
            Ok(1) => break,
            Ok(_) => {
                return Err(PeerFrameIoError::Io(io::Error::other(
                    "one-byte peer-frame read returned an invalid count",
                )));
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                ) =>
            {
                return Ok(PeerFrameRead::Idle);
            }
            Err(error) => return Err(PeerFrameIoError::Io(error)),
        }
    }
    reader
        .read_exact(&mut prefix[1..])
        .map_err(PeerFrameIoError::Io)?;
    let body_wire = u32::from_be_bytes(prefix);
    let body =
        usize::try_from(body_wire).map_err(|_| PeerFrameIoError::LengthUnsupported(body_wire))?;
    let complete = PEER_FRAME_LENGTH_PREFIX_BYTES
        .checked_add(body)
        .ok_or(PeerFrameIoError::LengthUnsupported(body_wire))?;
    if complete > maximum {
        return Err(PeerFrameIoError::TooLarge {
            actual: complete,
            maximum,
        });
    }
    let permit =
        memory
            .try_acquire(complete)
            .map_err(|full| PeerFrameIoError::ReceiveMemoryFull {
                required: full.required,
                maximum: full.maximum,
            })?;

    output.clear();
    output.reserve(complete);
    output.extend_from_slice(&prefix);
    output.resize(complete, 0);
    reader
        .read_exact(&mut output[PEER_FRAME_LENGTH_PREFIX_BYTES..])
        .map_err(PeerFrameIoError::Io)?;
    Ok(PeerFrameRead::Complete {
        bytes: complete,
        memory: permit,
    })
}

pub(crate) fn write_all_flush(writer: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    writer.write_all(bytes)?;
    writer.flush()
}

fn read_hello(
    reader: &mut impl Read,
    output: &mut Vec<u8>,
    client: bool,
    maximum: usize,
) -> Result<(), HandshakeIoError> {
    output.clear();
    output.reserve(maximum);
    read_append(reader, output, HANDSHAKE_MAGIC.len() + 4)?;
    if client {
        read_append(reader, output, 4)?;
    }
    read_identity(reader, output)?;
    read_identity(reader, output)?;
    read_append(reader, output, if client { 12 } else { 5 })?;
    Ok(())
}

fn read_identity(reader: &mut impl Read, output: &mut Vec<u8>) -> Result<(), HandshakeIoError> {
    let start = output.len();
    read_append(reader, output, 1)?;
    let length = usize::from(output[start]);
    if length > MAX_ID_BYTES {
        return Err(HandshakeIoError::Decode(DecodeHandshakeError::TooLong {
            actual: length,
            maximum: MAX_ID_BYTES,
        }));
    }
    read_append(reader, output, length)
}

fn read_append(
    reader: &mut impl Read,
    output: &mut Vec<u8>,
    length: usize,
) -> Result<(), HandshakeIoError> {
    let start = output.len();
    let end = start
        .checked_add(length)
        .ok_or_else(|| HandshakeIoError::Io(io::Error::other("handshake length overflow")))?;
    output.resize(end, 0);
    if let Err(error) = reader.read_exact(&mut output[start..end]) {
        output.truncate(start);
        return Err(HandshakeIoError::Io(error));
    }
    Ok(())
}

#[cfg(test)]
#[path = "io_test.rs"]
mod tests;
