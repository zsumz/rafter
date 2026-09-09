//! One counter replica per process; HTTP clients and authenticated Raft peers.
mod checkpoint;
mod counter;
mod disk;
mod http;
mod peers;

use counter::Counter;
use rafter::{NodeConfig, NodeId};
use rafter_app::{group::RaftGroup, state_machine::ReplicatedStateMachine};
use rafter_runtime::DurableRaftNode;
use rafter_service::{
    DriverServiceState, InboundEnvelopeError, PeerControlPlaneCheckpoint, TransportDriverOptions,
    TransportRaftDriver,
};
use rafter_storage::{
    FileRaftHardStateStore, FileRaftLogSegment, FileRaftNodeStores, FileRaftSnapshotStore,
};
use rafter_transport_tls::{
    FileTransportSessionStore, SessionStoreLimits, TlsPeerDirectory, TlsSender,
};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub const GROUP: u64 = 1;
pub const MEMBERS: [u64; 3] = [1, 2, 3];
pub type Runtime =
    DurableRaftNode<FileRaftHardStateStore, FileRaftLogSegment, FileRaftSnapshotStore>;
pub type Driver = TransportRaftDriver<
    u64,
    Counter,
    Runtime,
    TlsSender<u64, peers::GroupCodec>,
    TlsPeerDirectory<u64>,
>;

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [command] if command == "init" => initialize(),
        [command, node] if command == "node" => {
            let id = node.parse()?;
            if !MEMBERS.contains(&id) {
                return Err("node must be 1, 2, or 3".into());
            }
            run(id)
        }
        _ => Err("usage: rafter-counter init | node <1|2|3>".into()),
    }
}

fn node_directory(id: u64) -> PathBuf {
    PathBuf::from(format!("data/node-{id}"))
}

fn initialize() -> Result<()> {
    // A separate init command cannot silently replace a damaged existing cluster.
    fs::create_dir("data")?;
    for id in MEMBERS {
        let directory = node_directory(id);
        fs::create_dir(&directory)?;
        fs::create_dir(directory.join("raft"))?;
        let stores = FileRaftNodeStores::open(directory.join("raft"))?;
        disk::save(&directory.join("counter"), &counter::State::default())?;
        checkpoint::save(
            &directory.join("checkpoint"),
            id,
            PeerControlPlaneCheckpoint::empty(GROUP),
        )?;
        let sessions = FileTransportSessionStore::create_new(
            directory.join("sessions"),
            peers::cluster(),
            peers::principal(id),
            SessionStoreLimits::default(),
        )?;
        drop((sessions, stores));
        disk::save(&directory.join("identity"), &id)?;
    }
    fs::File::open("data")?.sync_all()?;
    fs::File::open(".")?.sync_all()?;
    println!("Created data for nodes 1, 2, and 3. Run init only once.");
    Ok(())
}

fn run(id: u64) -> Result<()> {
    let directory = node_directory(id);
    let stored_id: u64 = disk::load(&directory.join("identity"))?;
    if stored_id != id {
        return Err("data directory belongs to another node".into());
    }
    // Acquire exclusive directory ownership before opening any application files.
    let (hard_state, log, snapshots) =
        FileRaftNodeStores::open(directory.join("raft"))?.into_parts();
    let app = Counter::open(&directory)?;
    let applied = app.applied_index()?;
    let config = NodeConfig::new(
        NodeId(id),
        MEMBERS
            .into_iter()
            .filter(|peer| *peer != id)
            .map(NodeId)
            .collect(),
        15 + id * 5,
    )?
    .with_heartbeat_interval_ticks(3);
    let recovered = DurableRaftNode::recover_with_storage_and_snapshot_store_applied_through(
        config, hard_state, log, snapshots, applied,
    )?;
    let (raft, outputs) = recovered.into_parts();
    let group = RaftGroup::with_applied_index(GROUP, NodeId(id), raft, app, applied);
    let (transport, validator) = peers::open(id, &directory)?;
    let driver = Driver::with_control_plane_checkpoint(
        group,
        outputs,
        transport.sender(),
        validator,
        TransportDriverOptions::default(),
        checkpoint::load(&directory.join("checkpoint"), id)?,
    )?;
    checkpoint::save(
        &directory.join("checkpoint"),
        id,
        driver.control_plane_checkpoint(),
    )?;
    transport.start()?;

    let address = peers::address("COUNTER_HTTP_BASE", 8000, id)?;
    let server = tiny_http::Server::http(address)?;
    let stop = Arc::new(AtomicBool::new(false));
    let connections = Arc::new(AtomicUsize::new(0));
    let worker = {
        let (driver, stop, connections) = (driver.clone(), stop.clone(), connections.clone());
        thread::spawn(move || http::serve(server, driver, stop, connections))
    };
    println!(
        "Node {id}: http://{address} (peer TLS on {})",
        transport.local_addr()
    );
    let outcome = drive(&directory, id, &driver, &transport, &stop, &connections);
    stop.store(true, Ordering::Release);
    let http_outcome = worker.join().map_err(|_| "HTTP worker panicked")?;
    transport.join()?;
    outcome?;
    http_outcome
}

fn drive(
    directory: &Path,
    id: u64,
    driver: &Driver,
    transport: &peers::Transport,
    stop: &AtomicBool,
    connections: &AtomicUsize,
) -> Result<()> {
    let inbound = transport.inbound();
    let mut next_tick = Instant::now();
    let mut checkpoint_epoch = driver.control_plane_checkpoint_epoch();
    while !stop.load(Ordering::Acquire) {
        let stepped = (|| -> Result<()> {
            for envelope in inbound.drain(64)? {
                if let Err(InboundEnvelopeError::Driver { source }) = driver.deliver(envelope) {
                    return Err(source.into());
                }
            }
            if Instant::now() >= next_tick {
                driver.tick()?;
                next_tick = Instant::now() + Duration::from_millis(20);
            }
            driver.drive_pending_reads()?;
            Ok(())
        })();
        // Also save a terminal checkpoint if stepping discovered a contradiction.
        let epoch = driver.control_plane_checkpoint_epoch();
        if epoch != checkpoint_epoch {
            checkpoint::save(
                &directory.join("checkpoint"),
                id,
                driver.control_plane_checkpoint(),
            )?;
            checkpoint_epoch = epoch;
        }
        stepped?;
        if driver.service_state() != DriverServiceState::Serving {
            return Err(format!("driver stopped serving: {:?}", driver.service_state()).into());
        }
        if let Some(error) = transport.terminal_failure() {
            return Err(error.into());
        }
        let diagnostics = transport.diagnostics();
        connections.store(
            diagnostics.active_inbound_connections + diagnostics.active_outbound_connections,
            Ordering::Release,
        );
        thread::sleep(Duration::from_millis(2));
    }
    Ok(())
}
