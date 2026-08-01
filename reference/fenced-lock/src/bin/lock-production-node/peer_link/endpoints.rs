//! Caller-owned filesystem discovery feeding the public endpoint book.

use std::{
    collections::BTreeMap,
    fs,
    io::Write as _,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, MutexGuard, PoisonError,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use rafter::NodeId;
use rafter_transport_tls::{EndpointBook, PeerEndpoint, PeerId, TlsServerName};

const PEER_ADDRESS_FILE: &str = "peer.production.addr";
const DISCOVERY_POLL: Duration = Duration::from_millis(20);
const PLACEHOLDER_PORT: u16 = 1;

pub(super) struct EndpointSupervisor {
    shutdown: Arc<AtomicBool>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for EndpointSupervisor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EndpointSupervisor")
            .field("stopping", &self.shutdown.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl EndpointSupervisor {
    pub(super) fn start(
        cluster_dir: &Path,
        peers: BTreeMap<NodeId, PeerId>,
        endpoints: EndpointBook,
        server_name: TlsServerName,
        failure: Arc<Mutex<Option<String>>>,
    ) -> Result<Self, String> {
        let shutdown = Arc::new(AtomicBool::new(false));
        let worker_shutdown = Arc::clone(&shutdown);
        let cluster_dir = cluster_dir.to_path_buf();
        let worker = thread::Builder::new()
            .name(String::from("production-peer-endpoints"))
            .spawn(move || {
                discover_loop(
                    &cluster_dir,
                    &peers,
                    &endpoints,
                    &server_name,
                    &worker_shutdown,
                    &failure,
                );
            })
            .map_err(|error| format!("could not spawn endpoint supervisor: {error}"))?;
        Ok(Self {
            shutdown,
            worker: Mutex::new(Some(worker)),
        })
    }

    pub(super) fn stop(&self) -> Result<(), String> {
        self.shutdown.store(true, Ordering::Relaxed);
        let Some(worker) = lock(&self.worker).take() else {
            return Ok(());
        };
        worker
            .join()
            .map_err(|_| String::from("endpoint supervisor panicked"))
    }
}

pub(super) fn remote_peers(
    local_node: NodeId,
    peers: &BTreeMap<NodeId, PeerId>,
) -> BTreeMap<NodeId, PeerId> {
    peers
        .iter()
        .filter(|(candidate, _)| **candidate != local_node)
        .map(|(candidate, peer)| (*candidate, peer.clone()))
        .collect()
}

pub(super) fn install_placeholders(
    endpoints: &EndpointBook,
    peers: &BTreeMap<NodeId, PeerId>,
    server_name: &TlsServerName,
) -> Result<(), String> {
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), PLACEHOLDER_PORT);
    for peer_id in peers.values() {
        endpoints
            .replace(
                peer_id.clone(),
                vec![PeerEndpoint::new(address, server_name.clone())],
            )
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

pub(super) fn publish_address(node_dir: &Path, address: SocketAddr) -> std::io::Result<()> {
    let final_path = node_dir.join(PEER_ADDRESS_FILE);
    let staged = node_dir.join(format!(".{PEER_ADDRESS_FILE}.{}.tmp", std::process::id()));
    let mut file = fs::File::create(&staged)?;
    file.write_all(address.to_string().as_bytes())?;
    file.sync_all()?;
    fs::rename(&staged, &final_path)?;
    fs::File::open(node_dir)?.sync_all()
}

fn discover_loop(
    cluster_dir: &Path,
    peers: &BTreeMap<NodeId, PeerId>,
    endpoints: &EndpointBook,
    server_name: &TlsServerName,
    shutdown: &AtomicBool,
    failure: &Mutex<Option<String>>,
) {
    let mut observed = BTreeMap::new();
    while !shutdown.load(Ordering::Relaxed) {
        for (node_id, peer_id) in peers {
            let path = peer_address_path(cluster_dir, *node_id);
            let Some(address) = read_address(&path) else {
                continue;
            };
            if observed.get(node_id) == Some(&address) {
                continue;
            }
            let replacement = vec![PeerEndpoint::new(address, server_name.clone())];
            if let Err(error) = endpoints.replace(peer_id.clone(), replacement) {
                latch(
                    failure,
                    format!("could not install {}: {error}", path.display()),
                );
                return;
            }
            observed.insert(*node_id, address);
        }
        thread::sleep(DISCOVERY_POLL);
    }
}

fn peer_address_path(cluster_dir: &Path, peer: NodeId) -> PathBuf {
    cluster_dir
        .join(format!("node-{}", peer.0))
        .join(PEER_ADDRESS_FILE)
}

fn read_address(path: &Path) -> Option<SocketAddr> {
    fs::read_to_string(path).ok()?.trim().parse().ok()
}

fn latch(failure: &Mutex<Option<String>>, detail: String) {
    lock(failure).get_or_insert(detail);
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
