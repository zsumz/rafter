//! Connection-per-message TCP peer transport and the errors it reports.
//!
//! This module owns the listener, the peer address map, and the send and
//! receive paths that carry one frame per connection. It authenticates nobody
//! and keeps no persistent stream, queue, or deadline; framing and reconnect
//! policy belong to the sibling modules it calls.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::io;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::{Arc, RwLock};
use std::thread;

use rafter::{Message, NodeId};

use crate::backoff::connect_with_backoff_using;
use crate::{
    message_sender, read_message_frame, write_message_frame_into, ReadFrameError, ReconnectBackoff,
    WriteFrameError, DEFAULT_MAX_FRAME_LEN,
};

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
