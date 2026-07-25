//! Consumer-owned TCP link between replica processes.
//!
//! This is deployment plumbing, not a Rafter API. It carries [`Message`] values
//! between processes and does nothing else: no authentication, no peer identity
//! proof, no replay protection, no fencing of removed members. `CONTRACT.md`
//! says the same thing in the contract's voice, and the process suite is
//! labelled integration evidence because of it.
//!
//! # Shape
//!
//! Each replica binds one listener and dials each peer once. A frame is a `u32`
//! big-endian length followed by that many bytes of
//! [`peer_codec`](super::peer_codec) payload; a length above
//! [`MAX_FRAME_LEN`] is refused before a byte of it is read, so a hostile or
//! corrupt peer cannot make this process allocate arbitrarily.
//!
//! Outbound traffic is queued per peer with a **bounded** queue and dropped when
//! that queue is full. Dropping is correct: Raft's delivery contract already
//! tolerates loss, reordering, and duplication, and the alternative — blocking
//! the replica's own loop on a dead peer's socket — converts one slow peer into
//! a stalled replica. This is the one place this link is deliberately stronger
//! than the demo TCP transport Rafter ships, which sets no deadlines and can
//! block its caller.
//!
//! # Discovery
//!
//! A replica publishes its listening address into its own data directory as
//! `peer.addr`, written to a temporary name and renamed so a reader never sees
//! a half-written address. Peers resolve each other by reading that file. This
//! removes the only real source of flake a port-assigning harness would have —
//! two processes racing for the same port — and it survives restart for free,
//! because a restarted replica publishes its new port the same way.
//!
//! Filesystem discovery is deployment policy of the crudest possible kind. A
//! production composition replaces it with real service discovery and
//! authenticated identity; nothing above the link would change.

use std::{
    collections::BTreeMap,
    fs,
    io::{self, Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender, TrySendError},
        Arc,
    },
    thread,
    time::Duration,
};

use rafter::{Message, NodeId};

use super::peer_codec::{decode_message, encode_message, message_sender};

/// Largest peer frame this link will read or write.
///
/// Rafter bounds an append batch and a snapshot chunk far below this, so the
/// limit exists to refuse nonsense rather than to shape traffic.
pub const MAX_FRAME_LEN: usize = 8 * 1024 * 1024;

/// Outbound frames one peer may fall behind by before frames are dropped.
const PEER_SEND_QUEUE_LEN: usize = 256;

/// Deadline on one socket write, so a stalled peer cannot pin a sender thread.
const PEER_WRITE_TIMEOUT: Duration = Duration::from_millis(500);

/// How long a sender waits before redialing a peer it could not reach.
const PEER_REDIAL_DELAY: Duration = Duration::from_millis(20);

/// Stable name of the address a live replica publishes for its peers.
const PEER_ADDRESS_FILE: &str = "peer.addr";

/// A peer message this replica received.
#[derive(Debug)]
pub struct InboundMessage {
    /// The node the message names as its sender.
    pub from: NodeId,
    /// The message itself.
    pub message: Box<Message>,
}

/// One replica's TCP link to the rest of its cluster.
#[derive(Debug)]
pub struct PeerLink {
    local_addr: SocketAddr,
    inbound: Receiver<InboundMessage>,
    senders: BTreeMap<NodeId, SyncSender<Vec<u8>>>,
    dropped_frames: u64,
    encode_failures: u64,
    shutdown: Arc<AtomicBool>,
}

impl PeerLink {
    /// Binds a listener, starts the receive path, and dials every peer.
    ///
    /// `cluster_dir` is the root holding one `node-<id>` directory per replica;
    /// a peer's published address is read from that directory on every dial, so
    /// a restarted peer is found at its new port without any reconfiguration.
    ///
    /// # Errors
    ///
    /// Returns an error when the listener cannot bind or report its address.
    pub fn bind(
        bind_addr: &str,
        node_id: NodeId,
        peers: &[NodeId],
        cluster_dir: &Path,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind(bind_addr)?;
        let local_addr = listener.local_addr()?;
        let shutdown = Arc::new(AtomicBool::new(false));

        let (inbound_tx, inbound) = mpsc::channel();
        let accept_shutdown = Arc::clone(&shutdown);
        thread::Builder::new()
            .name(format!("peer-accept-{}", node_id.0))
            .spawn(move || accept_loop(&listener, &inbound_tx, &accept_shutdown))?;

        let mut senders = BTreeMap::new();
        for peer in peers {
            let (tx, rx) = mpsc::sync_channel(PEER_SEND_QUEUE_LEN);
            let address_path = peer_address_path(cluster_dir, *peer);
            let sender_shutdown = Arc::clone(&shutdown);
            thread::Builder::new()
                .name(format!("peer-send-{}-{}", node_id.0, peer.0))
                .spawn(move || send_loop(&rx, &address_path, &sender_shutdown))?;
            senders.insert(*peer, tx);
        }

        Ok(Self {
            local_addr,
            inbound,
            senders,
            dropped_frames: 0,
            encode_failures: 0,
            shutdown,
        })
    }

    /// Returns the address this replica listens on.
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Publishes this replica's address where its peers look for it.
    ///
    /// # Errors
    ///
    /// Returns an error when the address file cannot be staged or renamed.
    pub fn publish_address(&self, node_dir: &Path) -> io::Result<()> {
        let final_path = node_dir.join(PEER_ADDRESS_FILE);
        let staged_path = node_dir.join(format!("{PEER_ADDRESS_FILE}.{}.tmp", std::process::id()));
        fs::write(&staged_path, self.local_addr.to_string().as_bytes())?;
        fs::rename(&staged_path, &final_path)
    }

    /// Queues one message for `peer`, dropping it if that peer's queue is full.
    ///
    /// Never blocks. Rafter's delivery contract permits loss, so a dropped frame
    /// is a retransmission the protocol already knows how to arrange.
    pub fn send(&mut self, peer: NodeId, message: &Message, scratch: &mut Vec<u8>) {
        let Some(sender) = self.senders.get(&peer) else {
            return;
        };
        scratch.clear();
        if encode_message(message, scratch).is_err() {
            self.encode_failures += 1;
            return;
        }
        match sender.try_send(scratch.clone()) {
            Ok(()) => {}
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                self.dropped_frames += 1;
            }
        }
    }

    /// Returns every message that arrived since the last call.
    pub fn drain_inbound(&self) -> Vec<InboundMessage> {
        self.inbound.try_iter().collect()
    }

    /// Returns how many outbound frames were dropped by a full or dead queue.
    pub const fn dropped_frames(&self) -> u64 {
        self.dropped_frames
    }

    /// Returns how many outbound messages this link refused to encode.
    ///
    /// A nonzero count means the kernel produced a frame this consumer-owned
    /// format does not carry, which is a defect in this file rather than a
    /// network event.
    pub const fn encode_failures(&self) -> u64 {
        self.encode_failures
    }

    /// Asks every link thread to stop at its next opportunity.
    pub fn shut_down(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

fn peer_address_path(cluster_dir: &Path, peer: NodeId) -> PathBuf {
    cluster_dir
        .join(format!("node-{}", peer.0))
        .join(PEER_ADDRESS_FILE)
}

fn accept_loop(
    listener: &TcpListener,
    inbound: &mpsc::Sender<InboundMessage>,
    shutdown: &Arc<AtomicBool>,
) {
    while !shutdown.load(Ordering::Relaxed) {
        let Ok((stream, _)) = listener.accept() else {
            continue;
        };
        let inbound = inbound.clone();
        let shutdown = Arc::clone(shutdown);
        // One reader thread per connection. A replica has two peers, and each
        // dials once, so this is bounded by the cluster rather than by traffic.
        if thread::Builder::new()
            .name(String::from("peer-recv"))
            .spawn(move || receive_loop(stream, &inbound, &shutdown))
            .is_err()
        {
            return;
        }
    }
}

fn receive_loop(
    mut stream: TcpStream,
    inbound: &mpsc::Sender<InboundMessage>,
    shutdown: &Arc<AtomicBool>,
) {
    // A read deadline keeps a silent peer from pinning this thread forever
    // while still letting an idle connection live: a timeout is a normal
    // outcome here, not a failure.
    drop(stream.set_read_timeout(Some(Duration::from_millis(200))));
    let mut length = [0_u8; 4];
    let mut frame = Vec::new();
    while !shutdown.load(Ordering::Relaxed) {
        match stream.read_exact(&mut length) {
            Ok(()) => {}
            Err(error) if would_block(&error) => continue,
            Err(_) => break,
        }
        let len = u32::from_be_bytes(length) as usize;
        if len > MAX_FRAME_LEN {
            break;
        }
        frame.clear();
        frame.resize(len, 0);
        if read_exact_blocking(&mut stream, &mut frame, shutdown).is_err() {
            break;
        }
        let Ok(message) = decode_message(&frame) else {
            break;
        };
        if inbound
            .send(InboundMessage {
                from: message_sender(&message),
                message: Box::new(message),
            })
            .is_err()
        {
            break;
        }
    }
    drop(stream.shutdown(Shutdown::Both));
}

/// Reads a whole frame body, treating a read deadline as "keep waiting".
///
/// The deadline exists so this thread notices shutdown, not so a frame may
/// arrive in pieces and be abandoned half-read: abandoning one would desync the
/// stream, and the caller would then decode the next frame from the middle of
/// this one.
fn read_exact_blocking(
    stream: &mut TcpStream,
    buffer: &mut [u8],
    shutdown: &Arc<AtomicBool>,
) -> io::Result<()> {
    let mut filled = 0;
    while filled < buffer.len() {
        if shutdown.load(Ordering::Relaxed) {
            return Err(io::Error::other("link is shutting down"));
        }
        match stream.read(&mut buffer[filled..]) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
            Ok(read) => filled += read,
            Err(error) if would_block(&error) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn send_loop(frames: &Receiver<Vec<u8>>, address_path: &Path, shutdown: &Arc<AtomicBool>) {
    let mut stream: Option<TcpStream> = None;
    while !shutdown.load(Ordering::Relaxed) {
        let Ok(frame) = frames.recv_timeout(Duration::from_millis(100)) else {
            continue;
        };
        // The peer's address is re-read whenever a connection has to be made,
        // so a peer that restarted on a new port is found without anyone being
        // reconfigured.
        if stream.is_none() {
            stream = dial(address_path);
            if stream.is_none() {
                thread::sleep(PEER_REDIAL_DELAY);
                continue;
            }
        }
        let Some(open) = stream.as_mut() else {
            continue;
        };
        if write_frame(open, &frame).is_err() {
            stream = None;
        }
    }
}

fn dial(address_path: &Path) -> Option<TcpStream> {
    let published = fs::read_to_string(address_path).ok()?;
    let address: SocketAddr = published.trim().parse().ok()?;
    let stream = TcpStream::connect_timeout(&address, Duration::from_millis(200)).ok()?;
    stream.set_write_timeout(Some(PEER_WRITE_TIMEOUT)).ok()?;
    drop(stream.set_nodelay(true));
    Some(stream)
}

fn write_frame(stream: &mut TcpStream, frame: &[u8]) -> io::Result<()> {
    let length = u32::try_from(frame.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "frame exceeds the length field",
        )
    })?;
    stream.write_all(&length.to_be_bytes())?;
    stream.write_all(frame)?;
    stream.flush()
}

fn would_block(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut | io::ErrorKind::Interrupted
    )
}
