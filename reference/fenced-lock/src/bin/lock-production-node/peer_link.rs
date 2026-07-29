//! Mutually authenticated, bounded peer link for the production fixture.
//!
//! Rustls owns TLS 1.2/1.3, certificate-chain validation, encryption, and
//! mutual authentication. This module maps the authenticated leaf certificate
//! bytes to one configured `NodeId`, then separately requires the outer sender,
//! Raft message sender, and authenticated identity to agree before a frame can
//! enter the managed driver.
//!
//! The outer envelope adds a durable connection session and sequence number.
//! [`TransportReplayStore`] publishes acceptance before a frame reaches the
//! driver, so old connections and accepted duplicates remain refused across a
//! process restart. Raft's duplicate tolerance is not used as replay policy.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt, fs,
    fs::File,
    io::{BufReader, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc::{self, Receiver, SyncSender, TrySendError},
        Arc, Mutex, MutexGuard, PoisonError,
    },
    thread,
    time::Duration,
};

use rafter::{Message, NodeConfig, NodeId};
use rafter_codec::{decode_message, encode_message, max_receive_frame_bytes};
use rafter_reference_fenced_lock::production::{ReplayDecision, TransportReplayStore};
use rafter_service::{
    AuthenticatedPeerEnvelope, AuthenticatedPeerValidator, PeerEnvelope, PeerPolicy, RaftTransport,
    SnapshotChunkEnvelope,
};
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
    server::WebPkiClientVerifier,
    ClientConfig, ClientConnection, RootCertStore, ServerConfig, ServerConnection, StreamOwned,
};

use super::replica::{LockGroupId, GROUP_ID};

const OUTER_MAGIC: &[u8; 4] = b"RFTP";
const OUTER_VERSION: u8 = 1;
const OUTER_HEADER_BYTES: usize = 49;
const LENGTH_PREFIX_BYTES: usize = 4;

/// Outbound frames one peer may lag before nonblocking admission refuses.
pub const PEER_SEND_QUEUE_LEN: usize = 256;
/// Accepted frames one peer may hold in the process queue.
pub const PEER_INBOUND_QUEUE_LEN: usize = 128;
/// Accepted frames all peers may hold together.
pub const GLOBAL_INBOUND_QUEUE_LEN: usize = 512;
/// Concurrent authenticated or handshaking peer connections.
pub const MAX_PEER_CONNECTIONS: usize = 16;

const PEER_WRITE_TIMEOUT: Duration = Duration::from_millis(500);
const PEER_READ_TIMEOUT: Duration = Duration::from_secs(2);
const PEER_CONNECT_TIMEOUT: Duration = Duration::from_millis(200);
const PEER_REDIAL_DELAY: Duration = Duration::from_millis(20);
const SENDER_IDLE_POLL: Duration = Duration::from_millis(100);
const PEER_ADDRESS_FILE: &str = "peer.production.addr";
const TLS_SERVER_NAME: &str = "rafter-peer";

/// Returns the exact accepted outer-frame body bound.
///
/// The inner term comes from `rafter-codec`'s receive-limit arithmetic applied
/// to the exact `NodeConfig` defaults this fixture uses; the outer term is this
/// module's fixed authenticated-envelope header.
#[must_use]
pub fn max_frame_body_bytes() -> usize {
    let config = NodeConfig::new(NodeId(0), Vec::new(), 1)
        .expect("a one-node default config is structurally valid");
    OUTER_HEADER_BYTES + max_receive_frame_bytes(config.max_append_entries_bytes())
}

/// Full frame bytes including the length prefix.
#[must_use]
pub fn max_frame_bytes() -> usize {
    LENGTH_PREFIX_BYTES + max_frame_body_bytes()
}

/// Paths and certificate-to-node mapping for one mTLS endpoint.
#[derive(Clone, Debug)]
pub struct PeerTlsPaths {
    pub ca: PathBuf,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub peer_certificates: BTreeMap<NodeId, PathBuf>,
}

/// Loaded TLS endpoint and authenticated principal map.
#[derive(Clone)]
pub struct PeerTlsConfig {
    server: Arc<ServerConfig>,
    client: Arc<ClientConfig>,
    node_by_certificate: Arc<BTreeMap<Vec<u8>, NodeId>>,
}

impl fmt::Debug for PeerTlsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PeerTlsConfig")
            .field("configured_certificates", &self.node_by_certificate.len())
            .finish_non_exhaustive()
    }
}

impl PeerTlsConfig {
    /// Loads a test/deployment CA, local identity, and exact peer map.
    ///
    /// # Errors
    ///
    /// Returns an error when PEM parsing, key validation, trust configuration,
    /// or the certificate map is invalid.
    pub fn load(local_node: NodeId, paths: &PeerTlsPaths) -> Result<Self, String> {
        let roots = load_roots(&paths.ca)?;
        let certificate = load_certificates(&paths.certificate)?;
        let key = load_private_key(&paths.private_key)?;
        let client_verifier = WebPkiClientVerifier::builder(Arc::new(roots.clone()))
            .build()
            .map_err(|error| format!("could not build the client certificate verifier: {error}"))?;
        let server = ServerConfig::builder()
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(certificate.clone(), key.clone_key())
            .map_err(|error| format!("could not configure the TLS server identity: {error}"))?;
        let client = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(certificate.clone(), key)
            .map_err(|error| format!("could not configure the TLS client identity: {error}"))?;

        let mut node_by_certificate = BTreeMap::new();
        for (node_id, path) in &paths.peer_certificates {
            let peer = load_certificates(path)?;
            let leaf = peer
                .first()
                .ok_or_else(|| format!("peer certificate {} is empty", path.display()))?;
            if node_by_certificate
                .insert(leaf.as_ref().to_vec(), *node_id)
                .is_some()
            {
                return Err(format!(
                    "the same peer certificate is mapped to more than one node; latest is {}",
                    node_id.0
                ));
            }
        }
        let local_leaf = certificate
            .first()
            .ok_or_else(|| "the local certificate chain is empty".to_owned())?;
        if node_by_certificate.get(local_leaf.as_ref()) != Some(&local_node) {
            return Err(format!(
                "the local certificate does not map to configured node {}",
                local_node.0
            ));
        }
        Ok(Self {
            server: Arc::new(server),
            client: Arc::new(client),
            node_by_certificate: Arc::new(node_by_certificate),
        })
    }

    fn authenticated_node(&self, certificates: Option<&[CertificateDer<'_>]>) -> Option<NodeId> {
        certificates
            .and_then(|chain| chain.first())
            .and_then(|leaf| self.node_by_certificate.get(leaf.as_ref()).copied())
    }

    fn configured_nodes(&self) -> Vec<NodeId> {
        self.node_by_certificate.values().copied().collect()
    }
}

fn load_roots(path: &Path) -> Result<RootCertStore, String> {
    let certificates = load_certificates(path)?;
    let mut roots = RootCertStore::empty();
    for certificate in certificates {
        roots
            .add(certificate)
            .map_err(|error| format!("CA certificate {} is invalid: {error}", path.display()))?;
    }
    Ok(roots)
}

fn load_certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, String> {
    let file =
        File::open(path).map_err(|error| format!("could not open {}: {error}", path.display()))?;
    let mut reader = BufReader::new(file);
    let certificates = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            format!(
                "could not parse certificates in {}: {error}",
                path.display()
            )
        })?;
    if certificates.is_empty() {
        return Err(format!("{} contains no certificates", path.display()));
    }
    Ok(certificates)
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, String> {
    let file =
        File::open(path).map_err(|error| format!("could not open {}: {error}", path.display()))?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|error| format!("could not parse private key {}: {error}", path.display()))?
        .ok_or_else(|| format!("{} contains no private key", path.display()))
}

/// Certificate-authenticated identity of one replica.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct PeerPrincipal(NodeId);

impl PeerPrincipal {
    pub const fn for_node(node_id: NodeId) -> Self {
        Self(node_id)
    }
}

impl fmt::Display for PeerPrincipal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "mtls-node-{}", self.0 .0)
    }
}

/// Why the managed transport refused an outbound frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LinkError {
    UnknownGroup,
    UnknownPeer { peer: NodeId },
    QueueFull { peer: NodeId, limit: usize },
    Unencodable { peer: NodeId },
    UnresolvedSnapshotChunk { to: NodeId },
}

impl fmt::Display for LinkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownGroup => formatter.write_str("frame names an unrouted group"),
            Self::UnknownPeer { peer } => write!(formatter, "peer {} has no queue", peer.0),
            Self::QueueFull { peer, limit } => write!(
                formatter,
                "peer {} is {limit} frames behind, so admission refused",
                peer.0
            ),
            Self::Unencodable { peer } => {
                write!(formatter, "message for peer {} cannot be encoded", peer.0)
            }
            Self::UnresolvedSnapshotChunk { to } => write!(
                formatter,
                "snapshot chunk directive for {} reached a link that resolves none",
                to.0
            ),
        }
    }
}

impl Error for LinkError {}

#[derive(Debug, Default)]
struct Counters {
    dropped: AtomicU64,
    unencodable: AtomicU64,
    refused_chunks: AtomicU64,
    authenticated_connections: AtomicU64,
    authentication_failed: AtomicU64,
    unknown_certificate: AtomicU64,
    identity_mismatch: AtomicU64,
    unauthorized_peer: AtomicU64,
    replay_duplicate: AtomicU64,
    replay_stale_session: AtomicU64,
    replay_outside_window: AtomicU64,
    malformed_frame: AtomicU64,
    inbound_peer_full: AtomicU64,
    inbound_global_full: AtomicU64,
    connection_full: AtomicU64,
}

/// Stable structured link diagnostics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LinkDiagnostics {
    pub authenticated_connections: u64,
    pub authentication_failed: u64,
    pub unknown_certificate: u64,
    pub identity_mismatch: u64,
    pub unauthorized_peer: u64,
    pub replay_duplicate: u64,
    pub replay_stale_session: u64,
    pub replay_outside_window: u64,
    pub malformed_frame: u64,
    pub inbound_peer_full: u64,
    pub inbound_global_full: u64,
    pub connection_full: u64,
}

/// Outbound half owned by the managed driver.
#[derive(Clone, Debug)]
pub struct TcpPeerTransport {
    senders: Arc<BTreeMap<NodeId, SyncSender<Vec<u8>>>>,
    outbound_by_peer: Arc<BTreeMap<NodeId, AtomicUsize>>,
    directory: PeerDirectory,
    counters: Arc<Counters>,
}

impl RaftTransport<LockGroupId> for TcpPeerTransport {
    type PeerPrincipal = PeerPrincipal;
    type Error = LinkError;

    fn send(&self, envelope: PeerEnvelope<LockGroupId>) -> Result<(), Self::Error> {
        if envelope.group_id != GROUP_ID {
            return Err(LinkError::UnknownGroup);
        }
        let peer = envelope.to;
        let Some(sender) = self.senders.get(&peer) else {
            return Err(LinkError::UnknownPeer { peer });
        };
        let depth = self
            .outbound_by_peer
            .get(&peer)
            .expect("every sender has a depth counter");
        let payload = encode_message(&envelope.message).map_err(|_| {
            self.counters.unencodable.fetch_add(1, Ordering::Relaxed);
            LinkError::Unencodable { peer }
        })?;
        depth.fetch_add(1, Ordering::Relaxed);
        match sender.try_send(payload) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                depth.fetch_sub(1, Ordering::Relaxed);
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
        policy: PeerPolicy<Self::PeerPrincipal>,
    ) -> Result<(), Self::Error> {
        if *group_id != GROUP_ID {
            return Err(LinkError::UnknownGroup);
        }
        self.directory
            .install_policy(policy.retirement_floor(), policy.into_peers());
        Ok(())
    }
}

/// Authenticated principal and current Raft authorization directory.
#[derive(Clone, Debug, Default)]
pub struct PeerDirectory {
    shared: Arc<Mutex<DirectoryState>>,
}

#[derive(Debug, Default)]
struct DirectoryState {
    known: BTreeMap<PeerPrincipal, NodeId>,
    authorized: BTreeSet<PeerPrincipal>,
    retirement_floor: Option<NodeId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Authorization {
    Authorized,
    Retired,
    Unknown,
}

impl PeerDirectory {
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

    fn install_policy(&self, retirement_floor: Option<NodeId>, peers: Vec<PeerPrincipal>) {
        let mut state = lock(&self.shared);
        state.authorized = peers.into_iter().collect();
        state.retirement_floor = match (state.retirement_floor, retirement_floor) {
            (Some(held), Some(published)) => Some(held.max(published)),
            (held, None) => held,
            (None, published) => published,
        };
    }

    fn authorization(&self, node_id: NodeId) -> Authorization {
        let state = lock(&self.shared);
        let principal = PeerPrincipal::for_node(node_id);
        if state.authorized.contains(&principal) {
            Authorization::Authorized
        } else if state.retirement_floor.is_some_and(|floor| node_id <= floor) {
            Authorization::Retired
        } else {
            Authorization::Unknown
        }
    }
}

impl AuthenticatedPeerValidator<LockGroupId, PeerPrincipal> for PeerDirectory {
    fn is_known_group(&self, group_id: &LockGroupId) -> bool {
        *group_id == GROUP_ID
    }

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
        self.authorization(node_id) == Authorization::Authorized
    }

    fn is_retired_peer(&self, _group_id: &LockGroupId, node_id: NodeId) -> bool {
        self.authorization(node_id) == Authorization::Retired
    }
}

#[derive(Debug)]
struct QueuedEnvelope {
    peer: NodeId,
    envelope: AuthenticatedPeerEnvelope<LockGroupId, PeerPrincipal>,
}

/// Bound listener, per-peer senders, and bounded authenticated inbound queue.
#[derive(Debug)]
pub struct PeerLink {
    local_addr: SocketAddr,
    inbound: Receiver<QueuedEnvelope>,
    inbound_by_peer: Arc<BTreeMap<NodeId, AtomicUsize>>,
    outbound_by_peer: Arc<BTreeMap<NodeId, AtomicUsize>>,
    transport: TcpPeerTransport,
    directory: PeerDirectory,
    counters: Arc<Counters>,
    shutdown: Arc<AtomicBool>,
    replay: TransportReplayStore,
}

impl PeerLink {
    /// Binds the authenticated link and starts bounded connection/sender loops.
    ///
    /// # Errors
    ///
    /// Returns a reason when replay metadata is unavailable, TLS configuration
    /// is inconsistent, the listener cannot bind, or a bounded worker cannot be
    /// created.
    pub fn bind(
        bind_addr: &str,
        node_id: NodeId,
        cluster_dir: &Path,
        node_dir: &Path,
        tls: &PeerTlsConfig,
    ) -> Result<Self, String> {
        let all_nodes = tls.configured_nodes();
        if !all_nodes.contains(&node_id) {
            return Err(format!("TLS principal map omits local node {}", node_id.0));
        }
        let replay =
            TransportReplayStore::open(node_dir, GROUP_ID.0).map_err(|error| error.to_string())?;
        let listener = TcpListener::bind(bind_addr)
            .map_err(|error| format!("could not bind authenticated peer listener: {error}"))?;
        let local_addr = listener
            .local_addr()
            .map_err(|error| format!("could not read authenticated peer address: {error}"))?;
        let shutdown = Arc::new(AtomicBool::new(false));
        let counters = Arc::new(Counters::default());
        let directory = PeerDirectory::new(&all_nodes);
        let inbound_by_peer = Arc::new(
            all_nodes
                .iter()
                .copied()
                .filter(|peer| *peer != node_id)
                .map(|peer| (peer, AtomicUsize::new(0)))
                .collect(),
        );
        let outbound_by_peer = Arc::new(
            all_nodes
                .iter()
                .copied()
                .filter(|peer| *peer != node_id)
                .map(|peer| (peer, AtomicUsize::new(0)))
                .collect(),
        );
        let (inbound_tx, inbound) = mpsc::sync_channel(GLOBAL_INBOUND_QUEUE_LEN);
        let active_connections = Arc::new(AtomicUsize::new(0));
        {
            let tls = tls.clone();
            let counters = Arc::clone(&counters);
            let directory = directory.clone();
            let replay = replay.clone();
            let inbound_by_peer = Arc::clone(&inbound_by_peer);
            thread::Builder::new()
                .name(format!("production-peer-accept-{}", node_id.0))
                .spawn(move || {
                    accept_loop(
                        &listener,
                        node_id,
                        &inbound_tx,
                        &inbound_by_peer,
                        &tls,
                        &directory,
                        &replay,
                        &counters,
                        &active_connections,
                    );
                })
                .map_err(|error| format!("could not spawn peer acceptor: {error}"))?;
        }

        let mut senders = BTreeMap::new();
        for peer in all_nodes.iter().copied().filter(|peer| *peer != node_id) {
            let (tx, rx) = mpsc::sync_channel(PEER_SEND_QUEUE_LEN);
            let address_path = peer_address_path(cluster_dir, peer);
            let sender_shutdown = Arc::clone(&shutdown);
            let sender_tls = tls.clone();
            let sender_replay = replay.clone();
            let sender_counters = Arc::clone(&counters);
            let sender_depths = Arc::clone(&outbound_by_peer);
            thread::Builder::new()
                .name(format!("production-peer-send-{}-{}", node_id.0, peer.0))
                .spawn(move || {
                    send_loop(
                        &rx,
                        &address_path,
                        node_id,
                        peer,
                        &sender_tls,
                        &sender_replay,
                        &sender_counters,
                        &sender_depths,
                        &sender_shutdown,
                    );
                })
                .map_err(|error| format!("could not spawn peer sender: {error}"))?;
            senders.insert(peer, tx);
        }

        Ok(Self {
            local_addr,
            inbound,
            inbound_by_peer,
            outbound_by_peer: Arc::clone(&outbound_by_peer),
            transport: TcpPeerTransport {
                senders: Arc::new(senders),
                outbound_by_peer,
                directory: directory.clone(),
                counters: Arc::clone(&counters),
            },
            directory,
            counters,
            shutdown,
            replay,
        })
    }

    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn transport(&self) -> TcpPeerTransport {
        self.transport.clone()
    }

    pub fn validator(&self) -> PeerDirectory {
        self.directory.clone()
    }

    pub fn publish_address(&self, node_dir: &Path) -> std::io::Result<()> {
        let final_path = node_dir.join(PEER_ADDRESS_FILE);
        let staged = node_dir.join(format!("{PEER_ADDRESS_FILE}.{}.tmp", std::process::id()));
        fs::write(&staged, self.local_addr.to_string().as_bytes())?;
        fs::rename(staged, final_path)
    }

    pub fn drain_inbound(&self) -> Vec<AuthenticatedPeerEnvelope<LockGroupId, PeerPrincipal>> {
        self.inbound
            .try_iter()
            .map(|queued| {
                if let Some(depth) = self.inbound_by_peer.get(&queued.peer) {
                    depth.fetch_sub(1, Ordering::Relaxed);
                }
                queued.envelope
            })
            .collect()
    }

    pub fn counts(&self) -> (u64, u64, u64) {
        (
            self.counters.dropped.load(Ordering::Relaxed),
            self.counters.unencodable.load(Ordering::Relaxed),
            self.counters.refused_chunks.load(Ordering::Relaxed),
        )
    }

    pub fn diagnostics(&self) -> LinkDiagnostics {
        LinkDiagnostics {
            authenticated_connections: self
                .counters
                .authenticated_connections
                .load(Ordering::Relaxed),
            authentication_failed: self.counters.authentication_failed.load(Ordering::Relaxed),
            unknown_certificate: self.counters.unknown_certificate.load(Ordering::Relaxed),
            identity_mismatch: self.counters.identity_mismatch.load(Ordering::Relaxed),
            unauthorized_peer: self.counters.unauthorized_peer.load(Ordering::Relaxed),
            replay_duplicate: self.counters.replay_duplicate.load(Ordering::Relaxed),
            replay_stale_session: self.counters.replay_stale_session.load(Ordering::Relaxed),
            replay_outside_window: self.counters.replay_outside_window.load(Ordering::Relaxed),
            malformed_frame: self.counters.malformed_frame.load(Ordering::Relaxed),
            inbound_peer_full: self.counters.inbound_peer_full.load(Ordering::Relaxed),
            inbound_global_full: self.counters.inbound_global_full.load(Ordering::Relaxed),
            connection_full: self.counters.connection_full.load(Ordering::Relaxed),
        }
    }

    pub fn terminal_failure(&self) -> Option<String> {
        self.replay.terminal_failure()
    }

    pub fn replay_peer_windows(&self) -> usize {
        self.replay.peer_windows()
    }

    pub fn queue_depths(&self) -> (usize, usize) {
        let outbound = self
            .outbound_by_peer
            .values()
            .map(|depth| depth.load(Ordering::Relaxed))
            .sum();
        let inbound = self
            .inbound_by_peer
            .values()
            .map(|depth| depth.load(Ordering::Relaxed))
            .sum();
        (outbound, inbound)
    }

    pub fn shut_down(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

fn peer_address_path(cluster_dir: &Path, peer: NodeId) -> PathBuf {
    cluster_dir
        .join(format!("node-{}", peer.0))
        .join(PEER_ADDRESS_FILE)
}

#[allow(clippy::too_many_arguments)]
fn accept_loop(
    listener: &TcpListener,
    local_node: NodeId,
    inbound: &SyncSender<QueuedEnvelope>,
    inbound_by_peer: &Arc<BTreeMap<NodeId, AtomicUsize>>,
    tls: &PeerTlsConfig,
    directory: &PeerDirectory,
    replay: &TransportReplayStore,
    counters: &Arc<Counters>,
    active_connections: &Arc<AtomicUsize>,
) {
    while let Ok((stream, _)) = listener.accept() {
        let held = active_connections.fetch_add(1, Ordering::Relaxed);
        if held >= MAX_PEER_CONNECTIONS {
            active_connections.fetch_sub(1, Ordering::Relaxed);
            counters.connection_full.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        let inbound = SyncSender::clone(inbound);
        let inbound_by_peer = Arc::clone(inbound_by_peer);
        let tls = tls.clone();
        let directory = directory.clone();
        let replay = replay.clone();
        let counters = Arc::clone(counters);
        let active = Arc::clone(active_connections);
        if thread::Builder::new()
            .name(String::from("production-peer-recv"))
            .spawn(move || {
                let _guard = ConnectionGuard(active);
                receive_loop(
                    stream,
                    local_node,
                    &inbound,
                    &inbound_by_peer,
                    &tls,
                    &directory,
                    &replay,
                    &counters,
                );
            })
            .is_err()
        {
            active_connections.fetch_sub(1, Ordering::Relaxed);
            return;
        }
    }
}

struct ConnectionGuard(Arc<AtomicUsize>);

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

#[allow(clippy::too_many_arguments)]
fn receive_loop(
    socket: TcpStream,
    local_node: NodeId,
    inbound: &SyncSender<QueuedEnvelope>,
    inbound_by_peer: &BTreeMap<NodeId, AtomicUsize>,
    tls: &PeerTlsConfig,
    directory: &PeerDirectory,
    replay: &TransportReplayStore,
    counters: &Counters,
) {
    drop(socket.set_read_timeout(Some(PEER_READ_TIMEOUT)));
    drop(socket.set_write_timeout(Some(PEER_WRITE_TIMEOUT)));
    let Ok(connection) = ServerConnection::new(Arc::clone(&tls.server)) else {
        counters
            .authentication_failed
            .fetch_add(1, Ordering::Relaxed);
        return;
    };
    let mut stream = StreamOwned::new(connection, socket);
    if complete_server_handshake(&mut stream).is_err() {
        counters
            .authentication_failed
            .fetch_add(1, Ordering::Relaxed);
        return;
    }
    let Some(authenticated) = tls.authenticated_node(stream.conn.peer_certificates()) else {
        counters.unknown_certificate.fetch_add(1, Ordering::Relaxed);
        return;
    };
    counters
        .authenticated_connections
        .fetch_add(1, Ordering::Relaxed);
    while let Ok(frame) = read_outer_frame(&mut stream) {
        if frame.group_id != GROUP_ID.0 || frame.to != local_node {
            counters.malformed_frame.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let embedded_sender = message_sender(&frame.message);
        if frame.from != authenticated || embedded_sender != authenticated {
            counters.identity_mismatch.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if directory.authorization(authenticated) != Authorization::Authorized {
            counters.unauthorized_peer.fetch_add(1, Ordering::Relaxed);
            return;
        }
        match replay.admit(authenticated, frame.session, frame.sequence) {
            Ok(ReplayDecision::Accepted) => {}
            Ok(ReplayDecision::Duplicate) => {
                counters.replay_duplicate.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            Ok(ReplayDecision::StaleSession) => {
                counters
                    .replay_stale_session
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
            Ok(ReplayDecision::OutsideWindow | ReplayDecision::InvalidSequence) => {
                counters
                    .replay_outside_window
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
            Err(_) => return,
        }
        let Some(depth) = inbound_by_peer.get(&authenticated) else {
            counters.unauthorized_peer.fetch_add(1, Ordering::Relaxed);
            return;
        };
        let previous = depth.fetch_add(1, Ordering::Relaxed);
        if previous >= PEER_INBOUND_QUEUE_LEN {
            depth.fetch_sub(1, Ordering::Relaxed);
            counters.inbound_peer_full.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        let queued = QueuedEnvelope {
            peer: authenticated,
            envelope: AuthenticatedPeerEnvelope {
                group_id: GROUP_ID,
                authenticated_peer: PeerPrincipal::for_node(authenticated),
                raft_from: authenticated,
                raft_to: local_node,
                message: frame.message,
            },
        };
        match inbound.try_send(queued) {
            Ok(()) => {}
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                depth.fetch_sub(1, Ordering::Relaxed);
                counters.inbound_global_full.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn send_loop(
    frames: &Receiver<Vec<u8>>,
    address_path: &Path,
    local_node: NodeId,
    peer: NodeId,
    tls: &PeerTlsConfig,
    replay: &TransportReplayStore,
    counters: &Arc<Counters>,
    outbound_by_peer: &Arc<BTreeMap<NodeId, AtomicUsize>>,
    shutdown: &Arc<AtomicBool>,
) {
    let mut stream: Option<OutboundConnection> = None;
    while !shutdown.load(Ordering::Relaxed) {
        let Ok(payload) = frames.recv_timeout(SENDER_IDLE_POLL) else {
            continue;
        };
        outbound_by_peer
            .get(&peer)
            .expect("sender has a depth counter")
            .fetch_sub(1, Ordering::Relaxed);
        if stream.is_none() {
            stream = dial(address_path, peer, tls, replay, counters.as_ref());
            if stream.is_none() {
                thread::sleep(PEER_REDIAL_DELAY);
                continue;
            }
        }
        let Some(open) = stream.as_mut() else {
            continue;
        };
        let Some(sequence) = open.sequence.checked_add(1) else {
            stream = None;
            continue;
        };
        open.sequence = sequence;
        let Ok(frame) = write_outer_frame(local_node, peer, open.session, sequence, &payload)
        else {
            counters.unencodable.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        if open.stream.write_all(&frame).is_err() || open.stream.flush().is_err() {
            stream = None;
        }
    }
}

struct OutboundConnection {
    stream: StreamOwned<ClientConnection, TcpStream>,
    session: u64,
    sequence: u64,
}

fn dial(
    address_path: &Path,
    peer: NodeId,
    tls: &PeerTlsConfig,
    replay: &TransportReplayStore,
    counters: &Counters,
) -> Option<OutboundConnection> {
    let published = fs::read_to_string(address_path).ok()?;
    let address: SocketAddr = published.trim().parse().ok()?;
    let socket = TcpStream::connect_timeout(&address, PEER_CONNECT_TIMEOUT).ok()?;
    socket.set_write_timeout(Some(PEER_WRITE_TIMEOUT)).ok()?;
    socket.set_read_timeout(Some(PEER_READ_TIMEOUT)).ok()?;
    drop(socket.set_nodelay(true));
    let server_name = ServerName::try_from(TLS_SERVER_NAME).ok()?;
    let connection = ClientConnection::new(Arc::clone(&tls.client), server_name).ok()?;
    let mut stream = StreamOwned::new(connection, socket);
    if complete_client_handshake(&mut stream).is_err() {
        counters
            .authentication_failed
            .fetch_add(1, Ordering::Relaxed);
        return None;
    }
    if tls.authenticated_node(stream.conn.peer_certificates()) != Some(peer) {
        counters.identity_mismatch.fetch_add(1, Ordering::Relaxed);
        return None;
    }
    let session = replay.allocate_session().ok()?;
    Some(OutboundConnection {
        stream,
        session,
        sequence: 0,
    })
}

fn complete_server_handshake(
    stream: &mut StreamOwned<ServerConnection, TcpStream>,
) -> std::io::Result<()> {
    while stream.conn.is_handshaking() {
        stream.conn.complete_io(&mut stream.sock)?;
    }
    Ok(())
}

fn complete_client_handshake(
    stream: &mut StreamOwned<ClientConnection, TcpStream>,
) -> std::io::Result<()> {
    while stream.conn.is_handshaking() {
        stream.conn.complete_io(&mut stream.sock)?;
    }
    Ok(())
}

struct OuterFrame {
    group_id: u64,
    from: NodeId,
    to: NodeId,
    session: u64,
    sequence: u64,
    message: Message,
}

fn write_outer_frame(
    from: NodeId,
    to: NodeId,
    session: u64,
    sequence: u64,
    payload: &[u8],
) -> Result<Vec<u8>, ()> {
    let body_len = OUTER_HEADER_BYTES.checked_add(payload.len()).ok_or(())?;
    if body_len > max_frame_body_bytes() {
        return Err(());
    }
    let body_len = u32::try_from(body_len).map_err(|_| ())?;
    let payload_len = u32::try_from(payload.len()).map_err(|_| ())?;
    let mut frame = Vec::with_capacity(LENGTH_PREFIX_BYTES + body_len as usize);
    frame.extend_from_slice(&body_len.to_be_bytes());
    frame.extend_from_slice(OUTER_MAGIC);
    frame.push(OUTER_VERSION);
    frame.extend_from_slice(&GROUP_ID.0.to_be_bytes());
    frame.extend_from_slice(&from.0.to_be_bytes());
    frame.extend_from_slice(&to.0.to_be_bytes());
    frame.extend_from_slice(&session.to_be_bytes());
    frame.extend_from_slice(&sequence.to_be_bytes());
    frame.extend_from_slice(&payload_len.to_be_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

fn read_outer_frame(reader: &mut impl Read) -> Result<OuterFrame, ()> {
    let mut length = [0_u8; 4];
    reader.read_exact(&mut length).map_err(|_| ())?;
    let length = u32::from_be_bytes(length) as usize;
    if !(OUTER_HEADER_BYTES..=max_frame_body_bytes()).contains(&length) {
        return Err(());
    }
    let mut body = vec![0_u8; length];
    reader.read_exact(&mut body).map_err(|_| ())?;
    if &body[..4] != OUTER_MAGIC || body[4] != OUTER_VERSION {
        return Err(());
    }
    let group_id = read_u64(&body[5..13]);
    let from = NodeId(read_u64(&body[13..21]));
    let to = NodeId(read_u64(&body[21..29]));
    let session = read_u64(&body[29..37]);
    let sequence = read_u64(&body[37..45]);
    let payload_len = u32::from_be_bytes(body[45..49].try_into().map_err(|_| ())?) as usize;
    if payload_len != body.len() - OUTER_HEADER_BYTES {
        return Err(());
    }
    let message = decode_message(&body[OUTER_HEADER_BYTES..]).map_err(|_| ())?;
    Ok(OuterFrame {
        group_id,
        from,
        to,
        session,
        sequence,
        message,
    })
}

fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(bytes.try_into().expect("caller provides eight bytes"))
}

const fn message_sender(message: &Message) -> NodeId {
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

fn lock<T>(shared: &Mutex<T>) -> MutexGuard<'_, T> {
    shared.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use rafter::{RequestVote, Term};

    use super::*;

    #[test]
    fn outer_frame_round_trips_at_the_exact_bound_contract() {
        let message = Message::RequestVote(RequestVote {
            term: Term(4),
            candidate_id: NodeId(2),
            last_log_index: rafter::LogIndex(8),
            last_log_term: Term(3),
        });
        let payload = encode_message(&message).expect("message encodes");
        let encoded =
            write_outer_frame(NodeId(2), NodeId(1), 7, 9, &payload).expect("outer frame encodes");
        let decoded = read_outer_frame(&mut encoded.as_slice()).expect("outer frame decodes");
        assert_eq!(
            (
                decoded.group_id,
                decoded.from,
                decoded.to,
                decoded.session,
                decoded.sequence,
                decoded.message
            ),
            (1, NodeId(2), NodeId(1), 7, 9, message)
        );
        assert_eq!(max_frame_bytes(), 2_163_089);
        assert_eq!(rafter_reference_fenced_lock::production::REPLAY_WINDOW, 64);
    }

    #[test]
    fn oversized_frame_is_refused_before_allocation() {
        let encoded = u32::try_from(max_frame_body_bytes() + 1)
            .expect("bound fits u32")
            .to_be_bytes();
        let mut prefix = encoded.as_slice();
        assert!(read_outer_frame(&mut prefix).is_err());
    }
}
