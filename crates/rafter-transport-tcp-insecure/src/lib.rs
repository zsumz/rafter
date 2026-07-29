//! Blocking insecure demo-only TCP transport for Rafter peer messages.
//!
//! This crate is intentionally small: it adds a four-byte big-endian length
//! prefix around `rafter-codec` peer-message frames and a std-only TCP helper
//! that opens a connection per outbound message with bounded reconnect
//! backoff. It is for examples, local testing, and demo wiring only.
//! It owns only demo frame I/O and a minimal peer-address map.
//! It does not authenticate peers and is not production-ready.
//!
//! This is not Rafter's production transport reference: it has no persistent
//! per-peer streams, no bounded outbound queues, no write deadlines, and no
//! read timeouts. The connection-per-message shape can reorder under retries
//! and can block on a slow or dead peer. Production transports should follow
//! the delivery, ordering, and backpressure contract exposed by
//! `rafter-service`.
//!
//! This transport does not authenticate connections, prove that the remote
//! endpoint owns the node ID embedded in the peer message, or fence traffic
//! from removed members. Production transports must provide those properties
//! before passing messages into the Raft core; otherwise an unauthorized
//! higher-term message can still cause the normal Raft term update before the
//! kernel rejects the sender at the membership layer. See the production
//! boundary in the repository README for the full embedding contract.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use rafter::{Message, NodeId};
use rafter_codec::{
    decode_message, encode_message_into, DecodePeerMessageError, EncodePeerMessageError,
};

/// Default maximum accepted frame payload: 1 MiB after the length prefix.
pub const DEFAULT_MAX_FRAME_LEN: usize = 1024 * 1024;

/// Bounded reconnect policy for outbound TCP sends.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReconnectBackoff {
    /// Total connection attempts, including the initial attempt.
    pub max_attempts: usize,
    /// Delay before the first reconnect attempt.
    pub initial_delay: Duration,
    /// Maximum delay between successive reconnect attempts.
    pub max_delay: Duration,
}

impl Default for ReconnectBackoff {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(250),
        }
    }
}

impl ReconnectBackoff {
    /// Returns a policy that tries exactly once.
    #[must_use]
    pub const fn once() -> Self {
        Self {
            max_attempts: 1,
            initial_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
        }
    }
}

/// Message decoded from an accepted TCP connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReceivedPeerMessage {
    /// Claimed sender identity decoded from the peer message.
    ///
    /// This demo transport does not authenticate the claim.
    pub from: NodeId,
    /// Decoded Raft peer message.
    pub message: Message,
    /// Socket address of the accepted TCP connection.
    pub peer_addr: SocketAddr,
}

/// Errors raised while writing a length-prefixed peer-message frame.
#[derive(Debug)]
#[non_exhaustive]
pub enum WriteFrameError {
    /// The peer message could not be encoded.
    Encode(EncodePeerMessageError),
    /// The encoded frame cannot be represented by the four-byte prefix.
    FrameTooLarge {
        /// Encoded payload length in bytes.
        len: usize,
    },
    /// Writing the length prefix or payload failed.
    Io(io::Error),
}

/// Errors raised while reading a length-prefixed peer-message frame.
#[derive(Debug)]
#[non_exhaustive]
pub enum ReadFrameError {
    /// Reading the length prefix or payload failed.
    Io(io::Error),
    /// The declared payload exceeds the caller's receive bound.
    FrameTooLarge {
        /// Declared payload length in bytes.
        len: usize,
        /// Configured maximum payload length in bytes.
        max: usize,
    },
    /// The bounded payload could not be decoded as a peer message.
    Decode(DecodePeerMessageError),
}

/// Errors raised by [`InsecureTcpTransport`].
#[derive(Debug)]
#[non_exhaustive]
pub enum TcpTransportError {
    /// No destination address is configured for the named peer.
    UnknownPeer(NodeId),
    /// Binding the local listener failed.
    Bind(io::Error),
    /// Reading the bound listener's effective address failed.
    LocalAddr(io::Error),
    /// Connecting to a configured peer failed after bounded retries.
    Connect {
        /// Destination node identity.
        peer: NodeId,
        /// Final connection error.
        source: io::Error,
    },
    /// Encoding or writing an outbound frame failed.
    Write(WriteFrameError),
    /// Reading or decoding an inbound frame failed.
    Read(ReadFrameError),
    /// Accepting an inbound TCP connection failed.
    Accept(io::Error),
}

impl fmt::Display for WriteFrameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Encode(error) => write!(formatter, "failed to encode peer message: {error}"),
            Self::FrameTooLarge { len } => {
                write!(
                    formatter,
                    "encoded peer message length {len} exceeds u32::MAX"
                )
            }
            Self::Io(error) => write!(formatter, "failed to write peer message frame: {error}"),
        }
    }
}

impl Error for WriteFrameError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Encode(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::FrameTooLarge { .. } => None,
        }
    }
}

impl fmt::Display for ReadFrameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "failed to read peer message frame: {error}"),
            Self::FrameTooLarge { len, max } => {
                write!(
                    formatter,
                    "peer message frame length {len} exceeds maximum {max}"
                )
            }
            Self::Decode(error) => write!(formatter, "failed to decode peer message: {error}"),
        }
    }
}

impl Error for ReadFrameError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Decode(error) => Some(error),
            Self::FrameTooLarge { .. } => None,
        }
    }
}

impl fmt::Display for TcpTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownPeer(peer) => {
                write!(formatter, "no TCP peer address configured for {peer}")
            }
            Self::Bind(error) => write!(formatter, "failed to bind TCP listener: {error}"),
            Self::LocalAddr(error) => {
                write!(formatter, "failed to read TCP listener address: {error}")
            }
            Self::Connect { peer, source } => {
                write!(formatter, "failed to connect to TCP peer {peer}: {source}")
            }
            Self::Write(error) => write!(formatter, "failed to send TCP peer message: {error}"),
            Self::Read(error) => write!(formatter, "failed to receive TCP peer message: {error}"),
            Self::Accept(error) => {
                write!(formatter, "failed to accept TCP peer connection: {error}")
            }
        }
    }
}

impl Error for TcpTransportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Bind(error) | Self::LocalAddr(error) | Self::Accept(error) => Some(error),
            Self::Connect { source, .. } => Some(source),
            Self::Write(error) => Some(error),
            Self::Read(error) => Some(error),
            Self::UnknownPeer(_) => None,
        }
    }
}

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

/// Blocking insecure demo-only peer transport over TCP.
///
/// This type is intentionally named with the `Insecure` prefix because it does
/// not authenticate peers, validate ownership of the embedded Raft sender ID,
/// protect against replay/spoofing, or fence removed members. Use it for
/// examples, local tests, and demo wiring only.
#[derive(Debug)]
pub struct InsecureTcpTransport {
    listener: TcpListener,
    peers: Arc<RwLock<BTreeMap<NodeId, SocketAddr>>>,
    max_frame_len: usize,
    backoff: ReconnectBackoff,
}

impl InsecureTcpTransport {
    /// Binds a local listener and installs the peer address map.
    ///
    /// # Errors
    ///
    /// Returns [`TcpTransportError::Bind`] when the listener cannot bind.
    pub fn bind(
        bind_addr: impl ToSocketAddrs,
        peers: BTreeMap<NodeId, SocketAddr>,
    ) -> Result<Self, TcpTransportError> {
        let listener = TcpListener::bind(bind_addr).map_err(TcpTransportError::Bind)?;
        Ok(Self {
            listener,
            peers: Arc::new(RwLock::new(peers)),
            max_frame_len: DEFAULT_MAX_FRAME_LEN,
            backoff: ReconnectBackoff::default(),
        })
    }

    /// Overrides the maximum accepted frame payload length.
    #[must_use]
    pub const fn with_max_frame_len(mut self, max_frame_len: usize) -> Self {
        self.max_frame_len = max_frame_len;
        self
    }

    /// Overrides the outbound reconnect policy.
    #[must_use]
    pub const fn with_reconnect_backoff(mut self, backoff: ReconnectBackoff) -> Self {
        self.backoff = backoff;
        self
    }

    /// Returns the concrete listener address, including an OS-assigned port.
    ///
    /// # Errors
    ///
    /// Returns [`TcpTransportError::LocalAddr`] when the address cannot be read.
    pub fn local_addr(&self) -> Result<SocketAddr, TcpTransportError> {
        self.listener
            .local_addr()
            .map_err(TcpTransportError::LocalAddr)
    }

    /// Replaces the outbound peer address map.
    pub fn set_peers(&self, peers: BTreeMap<NodeId, SocketAddr>) {
        *self
            .peers
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = peers;
    }

    /// Sends a peer message to `peer`, retrying failed connects according to
    /// the configured backoff.
    ///
    /// This compatibility helper allocates a fresh encode buffer per send. Use
    /// [`Self::send_with_scratch`] on hot paths that can retain a reusable
    /// buffer between sends.
    ///
    /// # Errors
    ///
    /// Returns [`TcpTransportError::UnknownPeer`] when no address is configured,
    /// [`TcpTransportError::Connect`] after all connect attempts fail, or
    /// [`TcpTransportError::Write`] when the connected stream cannot be written.
    pub fn send(&self, peer: NodeId, message: &Message) -> Result<(), TcpTransportError> {
        let mut scratch = Vec::new();
        self.send_with_scratch(peer, message, &mut scratch)
    }

    /// Sends a peer message using a caller-owned reusable encode buffer.
    ///
    /// The buffer is cleared and reused by `rafter-codec`; retaining it across
    /// a send loop avoids allocating one frame buffer for every outbound
    /// message.
    ///
    /// # Errors
    ///
    /// Returns [`TcpTransportError::UnknownPeer`] when no address is configured,
    /// [`TcpTransportError::Connect`] after all connect attempts fail, or
    /// [`TcpTransportError::Write`] when the connected stream cannot be written.
    pub fn send_with_scratch(
        &self,
        peer: NodeId,
        message: &Message,
        scratch: &mut Vec<u8>,
    ) -> Result<(), TcpTransportError> {
        let addr = self.peer_address(peer)?;
        let mut stream = self.connect_with_backoff(peer, addr)?;
        write_message_frame_into(&mut stream, scratch, message)
            .map_err(TcpTransportError::Write)?;
        stream.shutdown(Shutdown::Write).ok();
        Ok(())
    }

    /// Accepts one TCP connection and reads one peer message frame from it.
    ///
    /// # Errors
    ///
    /// Returns [`TcpTransportError::Accept`] when `accept` fails or
    /// [`TcpTransportError::Read`] when frame reading or decoding fails.
    pub fn receive(&self) -> Result<ReceivedPeerMessage, TcpTransportError> {
        let (mut stream, peer_addr) = self.listener.accept().map_err(TcpTransportError::Accept)?;
        let message =
            read_message_frame(&mut stream, self.max_frame_len).map_err(TcpTransportError::Read)?;
        let from = message_sender(&message);
        Ok(ReceivedPeerMessage {
            from,
            message,
            peer_addr,
        })
    }

    fn connect_with_backoff(
        &self,
        peer: NodeId,
        addr: SocketAddr,
    ) -> Result<TcpStream, TcpTransportError> {
        connect_with_backoff_using(peer, addr, self.backoff, TcpStream::connect, thread::sleep)
    }

    fn peer_address(&self, peer: NodeId) -> Result<SocketAddr, TcpTransportError> {
        self.peers
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&peer)
            .copied()
            .ok_or(TcpTransportError::UnknownPeer(peer))
    }
}

fn connect_with_backoff_using<T>(
    peer: NodeId,
    addr: SocketAddr,
    backoff: ReconnectBackoff,
    mut connect: impl FnMut(SocketAddr) -> io::Result<T>,
    mut sleep: impl FnMut(Duration),
) -> Result<T, TcpTransportError> {
    let attempts = backoff.max_attempts.max(1);
    let mut delay = backoff.initial_delay;
    let mut last_error = None;
    for attempt in 0..attempts {
        match connect(addr) {
            Ok(stream) => return Ok(stream),
            Err(error) => {
                last_error = Some(error);
                if attempt + 1 < attempts && !delay.is_zero() {
                    sleep(delay);
                    delay = delay.saturating_mul(2).min(backoff.max_delay);
                }
            }
        }
    }
    let Some(source) = last_error else {
        return Err(TcpTransportError::Connect {
            peer,
            source: io::Error::other("no TCP connect attempts were made"),
        });
    };
    Err(TcpTransportError::Connect { peer, source })
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
