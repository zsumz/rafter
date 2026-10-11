//! One counter replica per process; HTTP clients and authenticated Raft peers.
mod application;
mod checkpoint;
mod counter;
mod disk;
mod http;
mod owner;
mod peers;
mod policy;

use counter::Counter;
use rafter::{NodeConfig, NodeId};
use rafter_app::state_machine::ApplyResult;
use rafter_runtime::{DurableRaftNode, DurableRaftNodeStorage};
use rafter_service::{
    durable::{DurableRaftDriver, DurableRaftHandle},
    PeerControlPlaneCheckpoint,
};
use rafter_storage::{
    FileRaftHardStateStore, FileRaftLogSegment, FileRaftNodeStores, FileRaftSnapshotStore,
};
use rafter_transport_tls::{FileTransportSessionStore, SessionStoreLimits, TlsSender};
use std::{
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread,
};

struct StopOnExit<'a>(&'a AtomicBool);
impl Drop for StopOnExit<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub const GROUP: u64 = 1;
pub const MEMBERS: [u64; 3] = [1, 2, 3];
pub type Runtime =
    DurableRaftNode<FileRaftHardStateStore, FileRaftLogSegment, FileRaftSnapshotStore>;
pub type Driver = DurableRaftDriver<
    u64,
    FileRaftHardStateStore,
    FileRaftLogSegment,
    FileRaftSnapshotStore,
    application::Entry,
    Counter,
    TlsSender<u64, peers::GroupCodec>,
    u64,
>;
pub type Handle = DurableRaftHandle<u64, ApplyResult<u64>, u64>;

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
    let (hard_state_store, log_segment, snapshot_store) =
        FileRaftNodeStores::open(directory.join("raft"))?.into_parts();
    let app = Counter::open(&directory)?;
    let checkpoint = checkpoint::load(&directory.join("checkpoint"), id)?;
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
    let (transport, validator) = peers::open(id, &directory)?;
    let mut driver = owner::recover(
        config,
        DurableRaftNodeStorage {
            hard_state_store,
            log_segment,
            snapshot_store,
        },
        app,
        transport.sender(),
        checkpoint,
    )?;
    checkpoint::save(
        &directory.join("checkpoint"),
        id,
        policy::observed(
            driver
                .ready_node()
                .ok_or("recovery lost node ownership")?
                .commit_index(),
        ),
    )?;
    peers::authorize(id, &transport)?;
    transport.start()?;

    let address = peers::address("COUNTER_HTTP_BASE", 8000, id)?;
    let server = tiny_http::Server::http(address)?;
    let stop = Arc::new(AtomicBool::new(false));
    let ready = Arc::new(AtomicBool::new(true));
    let connections = Arc::new(AtomicUsize::new(0));
    let worker = {
        let (handle, stop, ready, connections) = (
            driver.handle(),
            stop.clone(),
            ready.clone(),
            connections.clone(),
        );
        thread::spawn(move || {
            // Close the owner even if the HTTP worker unwinds before returning.
            let _stop_on_exit = StopOnExit(&stop);
            http::serve(server, handle, &stop, &ready, &connections)
        })
    };
    println!(
        "Node {id}: http://{address} (peer TLS on {})",
        transport.local_addr()
    );
    let outcome = owner::drive(
        &mut driver,
        &transport,
        &validator,
        &stop,
        &ready,
        &connections,
    );
    ready.store(false, Ordering::Release);
    stop.store(true, Ordering::Release);
    // Release storage before joining an HTTP worker that may still await a result.
    drop(driver);
    let http_outcome = worker.join().map_err(|_| "HTTP worker panicked");
    let transport_outcome = transport.join();
    outcome?;
    http_outcome??;
    transport_outcome?;
    Ok(())
}
