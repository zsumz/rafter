//! Consumer-owned TCP link between lock replica processes.
//!
//! This is deployment plumbing, not a Rafter API. `rafter-service` asks an
//! embedder for a [`RaftTransport`] that moves a [`PeerEnvelope`] to a peer and
//! an [`AuthenticatedPeerValidator`] that decides which Raft replica an
//! authenticated principal is allowed to be. This module supplies both over
//! TCP, and the bytes on the wire are `rafter-transport-tcp-insecure`'s
//! published frame encoding.
//!
//! # What the principal here actually is
//!
//! Nothing. The frame carries no connection-level identity, so the principal
//! this module hands the driver is built from the sender field *inside the
//! frame it just decoded*. [`PeerDirectory::node_for_authenticated_peer`]
//! therefore always agrees with the message, and the mismatch check that exists
//! in the validator contract can never fire here. Two of the validator's four
//! questions are still answered for real — an unrouted group is refused, and a
//! fenced member is refused — and the identity question is not answered at all.
//!
//! That is stated rather than dressed up because dressing it up is the failure
//! mode: a principal derived from the claim it is supposed to check looks like
//! authentication in a stack trace and is not. `CONTRACT.md` records it as an
//! open residual of the integration composition, and
//! `tests/process_cluster.rs` has a test that demonstrates the equivalent hole
//! on the client side rather than only asserting it in prose.
//!
//! # Shape
//!
//! Each replica binds one listener and dials each peer once. A frame is a `u32`
//! big-endian length followed by that many bytes of `rafter-codec` peer
//! message, written by [`write_message_frame_into`] and read by
//! [`read_message_frame`]; a length above [`MAX_FRAME_LEN`] is refused before a
//! byte of the body is read, so a corrupt peer cannot make this process
//! allocate arbitrarily.
//!
//! Outbound traffic is queued per peer with a **bounded** queue and dropped
//! when that queue is full. Dropping is correct and it is what the layer above
//! already expects: `rafter-service` counts a refused send and lets the
//! protocol re-send, because a write must not fail on an undeliverable
//! heartbeat. The alternative — blocking inside [`RaftTransport::send`], which
//! the driver calls while it holds the group — would convert one slow peer into
//! a stalled replica.
//!
//! The connection-per-message shape of the crate's own
//! [`InsecureTcpTransport`](rafter_transport_tcp_insecure::InsecureTcpTransport)
//! is deliberately not used. This link keeps one connection per peer and reuses
//! it. At a 20 ms tick a three-replica cluster offers a few hundred frames a
//! second, and a connection each would leave that many sockets a second in
//! `TIME_WAIT` for minutes — a port-exhaustion flake that arrives only under
//! load, which is the kind of fixture this suite is meant not to have. Only the
//! crate's frame codec is used, which is the part that is a wire format rather
//! than a connection policy.
//!
//! # Discovery
//!
//! A replica publishes its listening address into its own data directory as
//! `peer.addr`, written to a temporary name and renamed so a reader never sees
//! a half-written address. Peers resolve each other by reading that file on
//! every dial, so a replica that restarted on a fresh ephemeral port is found
//! without anyone being reconfigured, and no harness ever has to pick a port
//! number two processes could race for.
//!
//! Filesystem discovery is deployment policy of the crudest possible kind. A
//! production composition replaces it with real service discovery and
//! authenticated identity; nothing above the link would change.
//!
//! A dialer opens each connection by writing the node id it believes it
//! reached, and an acceptor that is not that node closes the connection. That
//! is **not** authentication and it is not what it is for. Ephemeral ports are
//! recycled, and a restarting replica can bind the port a peer's stale
//! `peer.addr` still names — after which the dial succeeds, the connection
//! sticks, and one replica's frames are delivered to another for the rest of
//! the run. The address file is re-read on every dial, which fixes the case
//! where the dial fails and not the case where it wrongly succeeds. The
//! preamble fixes that one. It stops a mistake, not an adversary: the id in it
//! is as unproven as the sender field inside every frame.
//!
//! # Threads that outlive their usefulness
//!
//! The accept loop blocks in `accept`, and each connection's reader blocks in
//! `read_message_frame`. Neither takes a deadline, and that is a choice: a
//! deadline mid-frame would leave `read_exact` having consumed part of a length
//! prefix or a body, and the next read would decode from the middle of a frame.
//! The cost is that a reader whose peer vanished without closing the socket
//! blocks until the process exits. This process's only shutdown is exit, so
//! those threads are reaped by it; the count is bounded by the number of times
//! a peer redialed, not by traffic.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt, fs,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, Receiver, SyncSender, TrySendError},
        Arc, Mutex, MutexGuard, PoisonError,
    },
    thread,
    time::Duration,
};

use rafter::NodeId;
use rafter_service::{
    AuthenticatedPeerEnvelope, AuthenticatedPeerValidator, PeerEnvelope, PeerSet, RaftTransport,
    SnapshotChunkEnvelope,
};
use rafter_transport_tcp_insecure::{message_sender, read_message_frame, write_message_frame_into};

use super::replica::{LockGroupId, GROUP_ID};

/// Largest peer frame this link will read.
///
/// Rafter bounds an append batch and a snapshot chunk far below this, so the
/// limit exists to refuse nonsense rather than to shape traffic.
pub const MAX_FRAME_LEN: usize = 8 * 1024 * 1024;

/// Outbound frames one peer may fall behind by before frames are dropped.
const PEER_SEND_QUEUE_LEN: usize = 256;

/// Deadline on one socket write, so a stalled peer cannot pin a sender thread.
const PEER_WRITE_TIMEOUT: Duration = Duration::from_millis(500);

/// Deadline on one dial, for the same reason.
const PEER_CONNECT_TIMEOUT: Duration = Duration::from_millis(200);

/// How long a sender waits before redialing a peer it could not reach.
const PEER_REDIAL_DELAY: Duration = Duration::from_millis(20);

/// How long a sender blocks for an outbound frame before rechecking shutdown.
const SENDER_IDLE_POLL: Duration = Duration::from_millis(100);

/// Stable name of the address a live replica publishes for its peers.
const PEER_ADDRESS_FILE: &str = "peer.addr";

/// Transport identity of one replica, as this deployment spells it.
///
/// A principal is deliberately a distinct type from [`NodeId`] because
/// `rafter-service` keeps proving who is on a connection separate from deciding
/// which replica that party may be. This deployment proves nothing, so the
/// distinction is structural here rather than earned; see the module docs.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct PeerPrincipal(String);

impl PeerPrincipal {
    /// Returns the principal this deployment issues to one replica.
    pub fn for_node(node_id: NodeId) -> Self {
        Self(format!("replica-{}", node_id.0))
    }
}

impl fmt::Display for PeerPrincipal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Why this link refused an outbound frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LinkError {
    /// The frame names a group this deployment does not route.
    UnknownGroup,
    /// The frame names a replica this process has no queue for.
    UnknownPeer { peer: NodeId },
    /// The bounded outbound queue is full, so the frame fails closed.
    QueueFull { peer: NodeId, limit: usize },
    /// The kernel produced a message this wire format does not carry.
    Unencodable { peer: NodeId },
    /// A leader snapshot chunk directive reached the transport.
    ///
    /// This deployment's runtime is a `DurableRaftNode`, which owns its
    /// snapshot store and resolves chunk directives into ordinary
    /// `InstallSnapshotChunk` frames inside `step`. A directive arriving here
    /// would mean the runtime and the driver disagree about who owns the
    /// payload, so the honest body is a refusal rather than a second snapshot
    /// store bolted onto a link. The driver counts it and the protocol
    /// re-sends.
    UnresolvedSnapshotChunk { to: NodeId },
}

impl fmt::Display for LinkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownGroup => formatter.write_str("frame names an unrouted group"),
            Self::UnknownPeer { peer } => write!(formatter, "peer {} has no queue", peer.0),
            Self::QueueFull { peer, limit } => write!(
                formatter,
                "peer {} is {limit} frames behind, so this frame was dropped",
                peer.0
            ),
            Self::Unencodable { peer } => write!(
                formatter,
                "a message for peer {} does not fit this wire format",
                peer.0
            ),
            Self::UnresolvedSnapshotChunk { to } => write!(
                formatter,
                "a snapshot chunk directive for {} reached a link that resolves none",
                to.0
            ),
        }
    }
}

impl Error for LinkError {}

/// Counters this link publishes when the process stops.
#[derive(Debug, Default)]
struct Counters {
    dropped: AtomicU64,
    unencodable: AtomicU64,
    refused_chunks: AtomicU64,
}

/// One replica's outbound half, owned by the managed driver.
#[derive(Clone, Debug)]
pub struct TcpPeerTransport {
    senders: Arc<BTreeMap<NodeId, SyncSender<Vec<u8>>>>,
    directory: PeerDirectory,
    counters: Arc<Counters>,
}

impl RaftTransport<LockGroupId> for TcpPeerTransport {
    type PeerPrincipal = PeerPrincipal;
    type Error = LinkError;

    /// Queues one frame for a peer, never blocking.
    ///
    /// The driver calls this while it holds the group, and it counts a refusal
    /// rather than propagating it, so the only wrong answer here is one that
    /// waits.
    fn send(&self, envelope: PeerEnvelope<LockGroupId>) -> Result<(), Self::Error> {
        if envelope.group_id != GROUP_ID {
            return Err(LinkError::UnknownGroup);
        }
        let peer = envelope.to;
        let Some(sender) = self.senders.get(&peer) else {
            return Err(LinkError::UnknownPeer { peer });
        };
        // A fresh buffer per frame. The reusable-scratch variant of the codec
        // needs `&mut`, which this `&self` method cannot hold without interior
        // mutability, and a lock here would be a lock inside the driver's lock.
        let mut frame = Vec::new();
        let mut scratch = Vec::new();
        if write_message_frame_into(&mut frame, &mut scratch, &envelope.message).is_err() {
            self.counters.unencodable.fetch_add(1, Ordering::Relaxed);
            return Err(LinkError::Unencodable { peer });
        }
        match sender.try_send(frame) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                self.counters.dropped.fetch_add(1, Ordering::Relaxed);
                Err(LinkError::QueueFull {
                    peer,
                    limit: PEER_SEND_QUEUE_LEN,
                })
            }
        }
    }

    fn send_snapshot_chunk(
        &self,
        envelope: SnapshotChunkEnvelope<LockGroupId>,
    ) -> Result<(), Self::Error> {
        if envelope.group_id != GROUP_ID {
            return Err(LinkError::UnknownGroup);
        }
        self.counters.refused_chunks.fetch_add(1, Ordering::Relaxed);
        Err(LinkError::UnresolvedSnapshotChunk { to: envelope.to })
    }

    fn update_peers(
        &self,
        group_id: &LockGroupId,
        peers: PeerSet<Self::PeerPrincipal>,
    ) -> Result<(), Self::Error> {
        if *group_id != GROUP_ID {
            return Err(LinkError::UnknownGroup);
        }
        self.directory.replace_authorized(peers.into_peers());
        Ok(())
    }

    fn fence_peer(
        &self,
        group_id: &LockGroupId,
        peer: Self::PeerPrincipal,
    ) -> Result<(), Self::Error> {
        if *group_id != GROUP_ID {
            return Err(LinkError::UnknownGroup);
        }
        self.directory.fence(&peer);
        Ok(())
    }
}

/// One replica's view of which principals may speak to it.
#[derive(Clone, Debug, Default)]
pub struct PeerDirectory {
    shared: Arc<Mutex<DirectoryState>>,
}

#[derive(Debug, Default)]
struct DirectoryState {
    /// Every principal this deployment can name, and the replica it is.
    known: BTreeMap<PeerPrincipal, NodeId>,
    /// The principals currently allowed to speak, set through `update_peers`.
    authorized: BTreeSet<PeerPrincipal>,
    /// Principals refused regardless of authorization.
    fenced: BTreeSet<PeerPrincipal>,
}

impl PeerDirectory {
    /// Builds a directory that can name every replica and authorizes none.
    ///
    /// Naming is the deployment's knowledge and authorizing is the cluster's.
    /// The driver publishes the group's membership as a peer set when it adopts
    /// the group, before it serves anything, which is what fills the other
    /// half; pre-authorizing here would hide whether that happened.
    pub fn new(all_nodes: &[NodeId]) -> Self {
        let directory = Self::default();
        let mut state = lock(&directory.shared);
        for node_id in all_nodes {
            state
                .known
                .insert(PeerPrincipal::for_node(*node_id), *node_id);
        }
        drop(state);
        directory
    }

    fn replace_authorized(&self, peers: Vec<PeerPrincipal>) {
        lock(&self.shared).authorized = peers.into_iter().collect();
    }

    fn fence(&self, peer: &PeerPrincipal) {
        lock(&self.shared).fenced.insert(peer.clone());
    }
}

impl AuthenticatedPeerValidator<LockGroupId, PeerPrincipal> for PeerDirectory {
    fn is_known_group(&self, group_id: &LockGroupId) -> bool {
        *group_id == GROUP_ID
    }

    /// Maps a principal to a replica.
    ///
    /// This is a real lookup against a real table, and it still proves nothing,
    /// because the principal it is given was built from the frame's own sender
    /// field. See the module docs.
    fn node_for_authenticated_peer(
        &self,
        _group_id: &LockGroupId,
        peer: &PeerPrincipal,
    ) -> Option<NodeId> {
        lock(&self.shared).known.get(peer).copied()
    }

    fn principal_for_node(
        &self,
        _group_id: &LockGroupId,
        node_id: NodeId,
    ) -> Option<PeerPrincipal> {
        let state = lock(&self.shared);
        state
            .known
            .iter()
            .find_map(|(principal, mapped)| (*mapped == node_id).then(|| principal.clone()))
    }

    fn is_authorized_peer(&self, _group_id: &LockGroupId, node_id: NodeId) -> bool {
        let state = lock(&self.shared);
        state
            .authorized
            .iter()
            .any(|peer| state.known.get(peer) == Some(&node_id))
    }

    fn is_fenced_peer(&self, _group_id: &LockGroupId, node_id: NodeId) -> bool {
        let state = lock(&self.shared);
        state
            .fenced
            .iter()
            .any(|peer| state.known.get(peer) == Some(&node_id))
    }
}

/// One replica's TCP link: a bound listener, a sender per peer, and a queue of
/// arrived envelopes for the process loop to deliver.
#[derive(Debug)]
pub struct PeerLink {
    local_addr: SocketAddr,
    inbound: Receiver<AuthenticatedPeerEnvelope<LockGroupId, PeerPrincipal>>,
    transport: TcpPeerTransport,
    directory: PeerDirectory,
    counters: Arc<Counters>,
    shutdown: Arc<AtomicBool>,
}

impl PeerLink {
    /// Binds a listener, starts the receive path, and prepares a sender per peer.
    ///
    /// `cluster_dir` is the root holding one `node-<id>` directory per replica;
    /// a peer's published address is read from that directory on every dial.
    ///
    /// # Errors
    ///
    /// Returns an error when the listener cannot bind, cannot report its
    /// address, or a link thread cannot be started.
    pub fn bind(
        bind_addr: &str,
        node_id: NodeId,
        members: &[NodeId],
        cluster_dir: &Path,
    ) -> std::io::Result<Self> {
        let listener = TcpListener::bind(bind_addr)?;
        let local_addr = listener.local_addr()?;
        let shutdown = Arc::new(AtomicBool::new(false));
        let counters = Arc::new(Counters::default());
        let directory = PeerDirectory::new(members);

        let (inbound_tx, inbound) = mpsc::channel();
        thread::Builder::new()
            .name(format!("peer-accept-{}", node_id.0))
            .spawn(move || accept_loop(&listener, node_id, &inbound_tx))?;

        let mut senders = BTreeMap::new();
        for peer in members.iter().copied().filter(|peer| *peer != node_id) {
            let (tx, rx) = mpsc::sync_channel(PEER_SEND_QUEUE_LEN);
            let address_path = peer_address_path(cluster_dir, peer);
            let sender_shutdown = Arc::clone(&shutdown);
            thread::Builder::new()
                .name(format!("peer-send-{}-{}", node_id.0, peer.0))
                .spawn(move || send_loop(&rx, &address_path, peer, &sender_shutdown))?;
            senders.insert(peer, tx);
        }

        Ok(Self {
            local_addr,
            inbound,
            transport: TcpPeerTransport {
                senders: Arc::new(senders),
                directory: directory.clone(),
                counters: Arc::clone(&counters),
            },
            directory,
            counters,
            shutdown,
        })
    }

    /// Returns the address this replica listens on.
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Returns the outbound half the managed driver takes ownership of.
    pub fn transport(&self) -> TcpPeerTransport {
        self.transport.clone()
    }

    /// Returns the validator the managed driver takes ownership of.
    pub fn validator(&self) -> PeerDirectory {
        self.directory.clone()
    }

    /// Publishes this replica's address where its peers look for it.
    ///
    /// Staged under a process-unique name and renamed, so a peer reading
    /// concurrently sees either the old address or the new one and never half
    /// of either.
    ///
    /// # Errors
    ///
    /// Returns an error when the address file cannot be staged or renamed.
    pub fn publish_address(&self, node_dir: &Path) -> std::io::Result<()> {
        let final_path = node_dir.join(PEER_ADDRESS_FILE);
        let staged_path = node_dir.join(format!("{PEER_ADDRESS_FILE}.{}.tmp", std::process::id()));
        fs::write(&staged_path, self.local_addr.to_string().as_bytes())?;
        fs::rename(&staged_path, &final_path)
    }

    /// Returns every envelope that arrived since the last call.
    pub fn drain_inbound(&self) -> Vec<AuthenticatedPeerEnvelope<LockGroupId, PeerPrincipal>> {
        self.inbound.try_iter().collect()
    }

    /// Returns the diagnostic counters, in the order the `LINK` line prints them.
    pub fn counts(&self) -> (u64, u64, u64) {
        (
            self.counters.dropped.load(Ordering::Relaxed),
            self.counters.unencodable.load(Ordering::Relaxed),
            self.counters.refused_chunks.load(Ordering::Relaxed),
        )
    }

    /// Asks every sender thread to stop at its next opportunity.
    ///
    /// The accept loop and the per-connection readers are not asked, because
    /// they are blocked in a call that takes no deadline; the module docs say
    /// why, and process exit is what reaps them.
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
    local_node_id: NodeId,
    inbound: &mpsc::Sender<AuthenticatedPeerEnvelope<LockGroupId, PeerPrincipal>>,
) {
    while let Ok((stream, _)) = listener.accept() {
        let inbound = inbound.clone();
        // One reader thread per connection. Each peer dials once and reuses its
        // connection, so this is bounded by the cluster and by redials rather
        // than by traffic.
        if thread::Builder::new()
            .name(String::from("peer-recv"))
            .spawn(move || receive_loop(stream, local_node_id, &inbound))
            .is_err()
        {
            return;
        }
    }
}

fn receive_loop(
    mut stream: TcpStream,
    local_node_id: NodeId,
    inbound: &mpsc::Sender<AuthenticatedPeerEnvelope<LockGroupId, PeerPrincipal>>,
) {
    // The dialer names the replica it believes it reached. A connection that
    // named somebody else found a recycled port, and every frame on it belongs
    // to a different replica.
    let mut preamble = [0_u8; 8];
    if stream.read_exact(&mut preamble).is_err() {
        return;
    }
    if u64::from_be_bytes(preamble) != local_node_id.0 {
        return;
    }
    while let Ok(message) = read_message_frame(&mut stream, MAX_FRAME_LEN) {
        let from = message_sender(&message);
        // The principal is the frame's own claim, re-badged. Nothing on this
        // connection established it; the module docs are explicit about it and
        // so is `CONTRACT.md`.
        let envelope = AuthenticatedPeerEnvelope {
            group_id: GROUP_ID,
            authenticated_peer: PeerPrincipal::for_node(from),
            raft_from: from,
            raft_to: local_node_id,
            message,
        };
        if inbound.send(envelope).is_err() {
            return;
        }
    }
}

fn send_loop(
    frames: &Receiver<Vec<u8>>,
    address_path: &Path,
    peer: NodeId,
    shutdown: &Arc<AtomicBool>,
) {
    let mut stream: Option<TcpStream> = None;
    while !shutdown.load(Ordering::Relaxed) {
        let Ok(frame) = frames.recv_timeout(SENDER_IDLE_POLL) else {
            continue;
        };
        // The peer's address is re-read whenever a connection has to be made,
        // so a peer that restarted on a new port is found without any
        // reconfiguration.
        if stream.is_none() {
            stream = dial(address_path, peer);
            if stream.is_none() {
                thread::sleep(PEER_REDIAL_DELAY);
                continue;
            }
        }
        let Some(open) = stream.as_mut() else {
            continue;
        };
        if open.write_all(&frame).is_err() || open.flush().is_err() {
            stream = None;
        }
    }
}

fn dial(address_path: &Path, peer: NodeId) -> Option<TcpStream> {
    let published = fs::read_to_string(address_path).ok()?;
    let address: SocketAddr = published.trim().parse().ok()?;
    let mut stream = TcpStream::connect_timeout(&address, PEER_CONNECT_TIMEOUT).ok()?;
    stream.set_write_timeout(Some(PEER_WRITE_TIMEOUT)).ok()?;
    drop(stream.set_nodelay(true));
    // Name the replica this address was published by. A recycled port answers
    // the connect and then reads an id that is not its own, and closes.
    stream.write_all(&peer.0.to_be_bytes()).ok()?;
    stream.flush().ok()?;
    Some(stream)
}

fn lock<T>(shared: &Mutex<T>) -> MutexGuard<'_, T> {
    shared.lock().unwrap_or_else(PoisonError::into_inner)
}
