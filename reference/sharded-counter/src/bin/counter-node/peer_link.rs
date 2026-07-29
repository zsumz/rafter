//! Bounded unauthenticated integration transport for multi-group peer frames.
//!
//! The outer envelope carries group incarnation and both endpoint identities;
//! the inner Raft message uses `rafter-codec`. The connection preamble and
//! sender field are consistency checks only. Nothing here authenticates a
//! process, so this link is intentionally not production composition.

use std::{
    collections::BTreeMap,
    error::Error,
    fmt, fs,
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError},
        Arc,
    },
    thread,
    time::Duration,
};

use rafter::{Message, NodeId};
use rafter_codec::{decode_message, encode_message};
use rafter_crc32::crc32;
use rafter_reference_sharded_counter::{GroupId, GroupIncarnation};

const MAGIC: [u8; 4] = *b"RCPE";
const VERSION: u8 = 1;
const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
const OUTBOUND_PER_PEER: usize = 256;
const INBOUND_GLOBAL: usize = 4096;
const MAX_INBOUND_CONNECTIONS: usize = 64;
const CONNECT_TIMEOUT: Duration = Duration::from_millis(200);
const WRITE_TIMEOUT: Duration = Duration::from_millis(500);
const READ_TIMEOUT: Duration = Duration::from_secs(2);
const REDIAL_DELAY: Duration = Duration::from_millis(20);
const IDLE_POLL: Duration = Duration::from_millis(100);
const ADDRESS_FILE: &str = "peer.addr";

/// One peer frame as the consumer routes it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerFrame {
    pub group_id: GroupId,
    pub incarnation: GroupIncarnation,
    pub from: NodeId,
    pub to: NodeId,
    pub message: Message,
}

/// Typed outbound refusal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LinkError {
    UnknownPeer(NodeId),
    QueueFull { peer: NodeId, bound: usize },
    Unencodable,
}

impl fmt::Display for LinkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownPeer(peer) => write!(formatter, "peer {} has no route", peer.0),
            Self::QueueFull { peer, bound } => write!(
                formatter,
                "peer {} outbound queue reached its bound {bound}",
                peer.0
            ),
            Self::Unencodable => formatter.write_str("peer envelope does not fit the wire format"),
        }
    }
}

impl Error for LinkError {}

#[derive(Debug, Default)]
struct Counters {
    outbound_full: AtomicU64,
    inbound_full: AtomicU64,
    malformed: AtomicU64,
    identity_refused: AtomicU64,
    inbound_connection_full: AtomicU64,
}

/// Published link counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LinkCounters {
    pub outbound_full: u64,
    pub inbound_full: u64,
    pub malformed: u64,
    pub identity_refused: u64,
    pub inbound_connection_full: u64,
}

/// Bounded multi-group peer link.
#[derive(Debug)]
pub struct PeerLink {
    local_addr: SocketAddr,
    senders: BTreeMap<NodeId, SyncSender<PeerFrame>>,
    inbound: Receiver<PeerFrame>,
    counters: Arc<Counters>,
    shutdown: Arc<AtomicBool>,
}

impl PeerLink {
    pub fn bind(
        cluster_dir: &Path,
        node_id: NodeId,
        members: &[NodeId],
    ) -> Result<Self, io::Error> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let local_addr = listener.local_addr()?;
        let (inbound_tx, inbound) = mpsc::sync_channel(INBOUND_GLOBAL);
        let counters = Arc::new(Counters::default());
        let shutdown = Arc::new(AtomicBool::new(false));
        spawn_acceptor(
            listener,
            node_id,
            inbound_tx,
            Arc::clone(&counters),
            Arc::clone(&shutdown),
        );

        let mut senders = BTreeMap::new();
        for peer in members.iter().copied().filter(|peer| *peer != node_id) {
            let (sender, receiver) = mpsc::sync_channel(OUTBOUND_PER_PEER);
            spawn_sender(
                cluster_dir.to_path_buf(),
                node_id,
                peer,
                receiver,
                Arc::clone(&shutdown),
            );
            senders.insert(peer, sender);
        }
        Ok(Self {
            local_addr,
            senders,
            inbound,
            counters,
            shutdown,
        })
    }

    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn publish_address(&self, cluster_dir: &Path, node_id: NodeId) -> Result<(), io::Error> {
        let host_dir = cluster_dir.join(format!("host-{}", node_id.0));
        fs::create_dir_all(&host_dir)?;
        let path = host_dir.join(ADDRESS_FILE);
        let temp = host_dir.join(format!("{ADDRESS_FILE}.{}.tmp", std::process::id()));
        {
            let mut file = fs::File::create(&temp)?;
            writeln!(file, "{}", self.local_addr)?;
            file.sync_all()?;
        }
        fs::rename(temp, path)?;
        fs::File::open(host_dir)?.sync_all()
    }

    pub fn send(&self, frame: PeerFrame) -> Result<(), LinkError> {
        let peer = frame.to;
        let sender = self
            .senders
            .get(&peer)
            .ok_or(LinkError::UnknownPeer(peer))?;
        match sender.try_send(frame) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                self.counters.outbound_full.fetch_add(1, Ordering::Relaxed);
                Err(LinkError::QueueFull {
                    peer,
                    bound: OUTBOUND_PER_PEER,
                })
            }
            Err(TrySendError::Disconnected(_)) => Err(LinkError::UnknownPeer(peer)),
        }
    }

    pub fn drain_inbound(&self, limit: usize) -> Vec<PeerFrame> {
        let mut frames = Vec::new();
        for _ in 0..limit {
            match self.inbound.try_recv() {
                Ok(frame) => frames.push(frame),
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            }
        }
        frames
    }

    pub fn counters(&self) -> LinkCounters {
        LinkCounters {
            outbound_full: self.counters.outbound_full.load(Ordering::Relaxed),
            inbound_full: self.counters.inbound_full.load(Ordering::Relaxed),
            malformed: self.counters.malformed.load(Ordering::Relaxed),
            identity_refused: self.counters.identity_refused.load(Ordering::Relaxed),
            inbound_connection_full: self
                .counters
                .inbound_connection_full
                .load(Ordering::Relaxed),
        }
    }

    pub fn shut_down(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

impl Drop for PeerLink {
    fn drop(&mut self) {
        self.shut_down();
    }
}

fn spawn_acceptor(
    listener: TcpListener,
    node_id: NodeId,
    inbound: SyncSender<PeerFrame>,
    counters: Arc<Counters>,
    shutdown: Arc<AtomicBool>,
) {
    thread::spawn(move || {
        let active = Arc::new(AtomicUsize::new(0));
        listener
            .set_nonblocking(true)
            .expect("peer listener accepts nonblocking mode");
        while !shutdown.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, _)) => {
                    if active.fetch_add(1, Ordering::AcqRel) >= MAX_INBOUND_CONNECTIONS {
                        active.fetch_sub(1, Ordering::AcqRel);
                        counters
                            .inbound_connection_full
                            .fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    let inbound = inbound.clone();
                    let counters = Arc::clone(&counters);
                    let shutdown = Arc::clone(&shutdown);
                    let active = Arc::clone(&active);
                    thread::spawn(move || {
                        receive_connection(stream, node_id, &inbound, &counters, &shutdown);
                        active.fetch_sub(1, Ordering::AcqRel);
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(REDIAL_DELAY);
                }
                Err(_) => {
                    counters.malformed.fetch_add(1, Ordering::Relaxed);
                    thread::sleep(REDIAL_DELAY);
                }
            }
        }
    });
}

fn receive_connection(
    mut stream: TcpStream,
    node_id: NodeId,
    inbound: &SyncSender<PeerFrame>,
    counters: &Counters,
    shutdown: &AtomicBool,
) {
    let _ = stream.set_read_timeout(Some(READ_TIMEOUT));
    let mut preamble = [0_u8; 16];
    if stream.read_exact(&mut preamble).is_err() {
        counters.malformed.fetch_add(1, Ordering::Relaxed);
        return;
    }
    let claimed = NodeId(u64::from_be_bytes(
        preamble[..8].try_into().expect("eight-byte sender"),
    ));
    let target = NodeId(u64::from_be_bytes(
        preamble[8..].try_into().expect("eight-byte target"),
    ));
    if target != node_id {
        counters.identity_refused.fetch_add(1, Ordering::Relaxed);
        return;
    }
    while !shutdown.load(Ordering::Relaxed) {
        match read_frame(&mut stream) {
            Ok(frame) if frame.from == claimed && frame.to == node_id => {
                match inbound.try_send(frame) {
                    Ok(()) => {}
                    Err(TrySendError::Full(_)) => {
                        counters.inbound_full.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(TrySendError::Disconnected(_)) => return,
                }
            }
            Ok(_) => {
                counters.identity_refused.fetch_add(1, Ordering::Relaxed);
                return;
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::UnexpectedEof
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::WouldBlock
                        | io::ErrorKind::TimedOut
                ) =>
            {
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) {
                    continue;
                }
                return;
            }
            Err(_) => {
                counters.malformed.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
    }
}

fn spawn_sender(
    cluster_dir: PathBuf,
    node_id: NodeId,
    peer: NodeId,
    receiver: Receiver<PeerFrame>,
    shutdown: Arc<AtomicBool>,
) {
    thread::spawn(move || {
        let mut stream = None;
        while !shutdown.load(Ordering::Relaxed) {
            let frame = match receiver.recv_timeout(IDLE_POLL) {
                Ok(frame) => frame,
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => return,
            };
            let Ok(bytes) = encode_frame(&frame) else {
                continue;
            };
            for _ in 0..5 {
                if stream.is_none() {
                    stream = connect(&cluster_dir, node_id, peer).ok();
                }
                if let Some(connection) = stream.as_mut() {
                    if connection.write_all(&bytes).is_ok() {
                        break;
                    }
                }
                stream = None;
                thread::sleep(REDIAL_DELAY);
            }
        }
    });
}

fn connect(cluster_dir: &Path, node_id: NodeId, peer: NodeId) -> Result<TcpStream, io::Error> {
    let path = cluster_dir.join(format!("host-{}/{}", peer.0, ADDRESS_FILE));
    let address = fs::read_to_string(path)?;
    let address = address
        .trim()
        .parse::<SocketAddr>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let mut stream = TcpStream::connect_timeout(&address, CONNECT_TIMEOUT)?;
    stream.set_write_timeout(Some(WRITE_TIMEOUT))?;
    stream.set_read_timeout(Some(READ_TIMEOUT))?;
    stream.write_all(&node_id.0.to_be_bytes())?;
    stream.write_all(&peer.0.to_be_bytes())?;
    Ok(stream)
}

fn encode_frame(frame: &PeerFrame) -> Result<Vec<u8>, LinkError> {
    let message = encode_message(&frame.message).map_err(|_| LinkError::Unencodable)?;
    let mut body = Vec::with_capacity(4 + 1 + 4 + 4 + 8 + 8 + 4 + message.len() + 4);
    body.extend_from_slice(&MAGIC);
    body.push(VERSION);
    body.extend_from_slice(&frame.group_id.get().to_be_bytes());
    body.extend_from_slice(&frame.incarnation.get().to_be_bytes());
    body.extend_from_slice(&frame.from.0.to_be_bytes());
    body.extend_from_slice(&frame.to.0.to_be_bytes());
    let message_len = u32::try_from(message.len()).map_err(|_| LinkError::Unencodable)?;
    body.extend_from_slice(&message_len.to_be_bytes());
    body.extend_from_slice(&message);
    body.extend_from_slice(&crc32(&body).to_be_bytes());
    if body.len() > MAX_FRAME_BYTES {
        return Err(LinkError::Unencodable);
    }
    let body_len = u32::try_from(body.len()).map_err(|_| LinkError::Unencodable)?;
    let mut framed = Vec::with_capacity(4 + body.len());
    framed.extend_from_slice(&body_len.to_be_bytes());
    framed.extend_from_slice(&body);
    Ok(framed)
}

fn read_frame(reader: &mut impl Read) -> Result<PeerFrame, io::Error> {
    let mut len = [0_u8; 4];
    reader.read_exact(&mut len)?;
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "peer frame exceeds its bound",
        ));
    }
    let mut body = vec![0_u8; len];
    reader.read_exact(&mut body)?;
    if body.len() < 4 + 1 + 4 + 4 + 8 + 8 + 4 + 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "short peer frame",
        ));
    }
    let checksum_at = body.len() - 4;
    let expected = u32::from_be_bytes(body[checksum_at..].try_into().expect("four-byte checksum"));
    if crc32(&body[..checksum_at]) != expected {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "peer frame checksum mismatch",
        ));
    }
    let mut cursor = 0;
    if body[cursor..cursor + 4] != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "wrong peer magic",
        ));
    }
    cursor += 4;
    if body[cursor] != VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported peer version",
        ));
    }
    cursor += 1;
    let group_id = GroupId::new(take_u32(&body, &mut cursor)?);
    let incarnation = GroupIncarnation::new(take_u32(&body, &mut cursor)?)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "zero incarnation"))?;
    let from = NodeId(take_u64(&body, &mut cursor)?);
    let to = NodeId(take_u64(&body, &mut cursor)?);
    let message_len = take_u32(&body, &mut cursor)? as usize;
    let end = cursor
        .checked_add(message_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "message length overflow"))?;
    if end != checksum_at {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "peer message length mismatch",
        ));
    }
    let message = decode_message(&body[cursor..end])
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(PeerFrame {
        group_id,
        incarnation,
        from,
        to,
        message,
    })
}

fn take_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, io::Error> {
    let end = cursor
        .checked_add(4)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "field overflow"))?;
    let value = bytes
        .get(*cursor..end)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated u32"))?;
    *cursor = end;
    Ok(u32::from_be_bytes(
        value.try_into().expect("four-byte field"),
    ))
}

fn take_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, io::Error> {
    let end = cursor
        .checked_add(8)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "field overflow"))?;
    let value = bytes
        .get(*cursor..end)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated u64"))?;
    *cursor = end;
    Ok(u64::from_be_bytes(
        value.try_into().expect("eight-byte field"),
    ))
}
