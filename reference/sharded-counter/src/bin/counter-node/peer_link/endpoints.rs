//! Caller-owned filesystem discovery feeding a bounded `EndpointBook`.

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

const PEER_ADDRESS_FILE: &str = "peer.tls.addr";
const DISCOVERY_POLL: Duration = Duration::from_millis(20);
const PLACEHOLDER_PORT: u16 = 1;

pub(super) struct EndpointLifecycle {
    cluster_dir: PathBuf,
    peers: BTreeMap<NodeId, PeerId>,
    endpoints: EndpointBook,
    server_name: TlsServerName,
    supervisor: Mutex<Option<EndpointSupervisor>>,
}

impl EndpointLifecycle {
    pub(super) fn new(
        cluster_dir: &Path,
        peers: BTreeMap<NodeId, PeerId>,
        endpoints: EndpointBook,
        server_name: TlsServerName,
    ) -> Self {
        Self {
            cluster_dir: cluster_dir.to_path_buf(),
            peers,
            endpoints,
            server_name,
            supervisor: Mutex::new(None),
        }
    }

    pub(super) fn start(&self, failure: Arc<Mutex<Option<String>>>) -> Result<(), String> {
        let mut supervisor = lock(&self.supervisor);
        if supervisor.is_some() {
            return Ok(());
        }
        *supervisor = Some(EndpointSupervisor::start(
            &self.cluster_dir,
            self.peers.clone(),
            self.endpoints.clone(),
            self.server_name.clone(),
            failure,
        )?);
        Ok(())
    }

    pub(super) fn stop(&self) -> Result<(), String> {
        let Some(supervisor) = lock(&self.supervisor).take() else {
            return Ok(());
        };
        supervisor.stop()
    }
}

struct EndpointSupervisor {
    shutdown: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl EndpointSupervisor {
    fn start(
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
            .name("counter-peer-endpoints".to_string())
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
            worker: Some(worker),
        })
    }

    fn stop(mut self) -> Result<(), String> {
        self.shutdown.store(true, Ordering::Relaxed);
        let Some(worker) = self.worker.take() else {
            return Ok(());
        };
        worker
            .join()
            .map_err(|_| "endpoint supervisor panicked".to_string())
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
    for peer in peers.values() {
        endpoints
            .replace(
                peer.clone(),
                vec![PeerEndpoint::new(address, server_name.clone())],
            )
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

pub(super) fn publish_address(host_dir: &Path, address: SocketAddr) -> std::io::Result<()> {
    let final_path = host_dir.join(PEER_ADDRESS_FILE);
    let staged = host_dir.join(format!(".{PEER_ADDRESS_FILE}.{}.tmp", std::process::id()));
    let mut file = fs::File::create(&staged)?;
    file.write_all(address.to_string().as_bytes())?;
    file.sync_all()?;
    fs::rename(staged, final_path)?;
    fs::File::open(host_dir)?.sync_all()
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
                lock(failure).get_or_insert_with(|| {
                    format!("could not install {}: {error}", path.display())
                });
                return;
            }
            observed.insert(*node_id, address);
        }
        thread::sleep(DISCOVERY_POLL);
    }
}

fn peer_address_path(cluster_dir: &Path, peer: NodeId) -> PathBuf {
    cluster_dir
        .join(format!("host-{}", peer.0))
        .join(PEER_ADDRESS_FILE)
}

fn read_address(path: &Path) -> Option<SocketAddr> {
    fs::read_to_string(path).ok()?.trim().parse().ok()
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
