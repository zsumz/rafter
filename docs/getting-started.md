# Getting started

Let's build a counter shared by **three separate server processes**. You can add
to it, read it from any server, and restart the servers without losing completed
writes. The servers talk to each other over TLS.

This is one complete application. All the files are below; you don't need to
borrow code from another example.

## 1. Create the project folder

You'll need **Rust with rustup, OpenSSL, curl, and a C compiler** on macOS or
Linux. The project selects Rust 1.88, which rustup can install for you.

```sh
mkdir -p rafter-counter/src
cd rafter-counter
```

Keep all the following files in this folder. You can also copy the
[complete starter folder](./examples/counter) if you already have a Rafter checkout.
It builds independently of that checkout.

## 2. Add the application files

Expand each filename and copy its complete contents. The small counter is the
application you own; the other files connect it to Rafter, disk storage, and TLS.

Cargo downloads the Rafter libraries from a pinned revision, so this example
uses the same APIs each time. You don't need to clone the library separately.

<details>
<summary><strong>Cargo.toml</strong> — Dependencies</summary>

<!-- counter-file: Cargo.toml -->
```toml
[package]
name = "rafter-counter"
version = "0.1.0"
edition = "2021"
publish = false
rust-version = "1.88"

[workspace]

[dependencies]
rafter = { git = "https://github.com/zsumz/rafter", rev = "518aefd7" }
rafter-app = { git = "https://github.com/zsumz/rafter", rev = "518aefd7" }
rafter-storage = { git = "https://github.com/zsumz/rafter", rev = "518aefd7" }
rafter-runtime = { git = "https://github.com/zsumz/rafter", rev = "518aefd7" }
rafter-service = { git = "https://github.com/zsumz/rafter", rev = "518aefd7" }
rafter-transport-tls = { git = "https://github.com/zsumz/rafter", rev = "518aefd7" }
rafter-crc32 = { git = "https://github.com/zsumz/rafter", rev = "518aefd7" }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tiny_http = "0.12"
```

</details>

<details>
<summary><strong>rust-toolchain.toml</strong> — Use the tested Rust version</summary>

<!-- counter-file: rust-toolchain.toml -->
```toml
[toolchain]
channel = "1.88.0"
profile = "minimal"
components = ["rustfmt", "clippy"]
```

</details>

<details>
<summary><strong>.gitignore</strong> — Keep generated data and keys out of Git</summary>

<!-- counter-file: .gitignore -->
```gitignore
/target/
/data/
/certs/
```

</details>

<details>
<summary><strong>certs.sh</strong> — Generate local certificates</summary>

<!-- counter-file: certs.sh -->
```sh
#!/bin/sh
# Create credentials for this local exercise. Never replace an existing set.
set -eu
umask 077
mkdir certs
cat > certs/ca.cnf <<'CONFIG'
[req]
prompt = no
distinguished_name = dn
x509_extensions = ca
[dn]
CN = Rafter counter local CA
[ca]
basicConstraints = critical,CA:TRUE
keyUsage = critical,keyCertSign,cRLSign
CONFIG
openssl req -x509 -newkey rsa:2048 -nodes -days 30 \
  -config certs/ca.cnf -keyout certs/ca-key.pem -out certs/ca.pem
for id in 1 2 3; do
  cat > "certs/node-$id.cnf" <<CONFIG
[req]
prompt = no
distinguished_name = dn
[dn]
CN = node-$id
[peer]
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth,clientAuth
subjectAltName = DNS:node-$id
CONFIG
  openssl req -new -newkey rsa:2048 -nodes \
    -config "certs/node-$id.cnf" -keyout "certs/node-$id-key.pem" -out "certs/node-$id.csr"
  openssl x509 -req -days 30 -in "certs/node-$id.csr" \
    -CA certs/ca.pem -CAkey certs/ca-key.pem -set_serial "$id" \
    -extfile "certs/node-$id.cnf" -extensions peer -out "certs/node-$id.pem"
done
printf '\nCreated local TLS certificates in certs/.\n'
```

</details>

<details>
<summary><strong>src/main.rs</strong> — Start and run a server</summary>

<!-- counter-file: src/main.rs -->
```rust
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
```

</details>

<details>
<summary><strong>src/counter.rs</strong> — Define and save the counter</summary>

<!-- counter-file: src/counter.rs -->
```rust
//! The application's counter and the last command included in its saved value.
use crate::disk;
use rafter::{LogIndex, SnapshotChunkRequest, SnapshotChunkSource};
use rafter_app::state_machine::{
    ApplicationSnapshot, ApplicationSnapshotError, ApplyBatch, ApplyResult, ReadBarrier,
    ReplicatedStateMachine, SnapshotSupport,
};
use rafter_storage::FileRaftSnapshotStore;
use serde::{Deserialize, Serialize};
use std::{
    io,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct State {
    pub applied: u64,
    pub value: u64,
}

#[derive(Debug)]
pub struct Counter {
    state: State,
    file: PathBuf,
    snapshots: PathBuf,
}

impl Counter {
    pub fn open(directory: &Path) -> io::Result<Self> {
        let file = directory.join("counter");
        Ok(Self {
            state: disk::load(&file)?,
            file,
            snapshots: directory.join("raft/snapshots"),
        })
    }

    pub fn value(&self) -> u64 {
        self.state.value
    }
}

impl ReplicatedStateMachine for Counter {
    type Command = u64;
    type CommandResult = u64;
    type Query = ();
    type QueryResult = u64;
    type Error = io::Error;
    const SNAPSHOT_SUPPORT: SnapshotSupport = SnapshotSupport::Supported;

    fn applied_index(&self) -> io::Result<LogIndex> {
        Ok(LogIndex(self.state.applied))
    }

    fn encode_command(&self, amount: &u64) -> io::Result<Vec<u8>> {
        Ok(amount.to_be_bytes().to_vec())
    }

    fn decode_command(&self, bytes: &[u8]) -> io::Result<u64> {
        Ok(u64::from_be_bytes(bytes.try_into().map_err(|_| {
            disk::invalid("an increment must be eight bytes")
        })?))
    }

    fn apply_batch(&mut self, batch: ApplyBatch<u64>) -> io::Result<Vec<ApplyResult<u64>>> {
        let mut next = self.state.clone();
        let mut results = Vec::new();
        for entry in batch.entries {
            if entry.index.0 <= next.applied {
                return Err(disk::invalid("command was already applied"));
            }
            // This counter stops at u64::MAX rather than wrapping back to zero.
            next.value = next.value.saturating_add(entry.command);
            next.applied = entry.index.0;
            results.push(ApplyResult {
                index: entry.index,
                term: entry.term,
                result: next.value,
                local_proposal_id: entry.local_proposal_id,
            });
        }
        if !results.is_empty() {
            // Publish data and progress together, before acknowledging the write.
            disk::save(&self.file, &next)?;
            self.state = next;
        }
        Ok(results)
    }

    fn read(&self, _: (), barrier: ReadBarrier) -> io::Result<u64> {
        if self.state.applied < barrier.required_applied_index.0 {
            return Err(disk::invalid("counter has not caught up yet"));
        }
        Ok(self.state.value)
    }

    fn build_snapshot(
        &mut self,
        at: LogIndex,
    ) -> Result<ApplicationSnapshot, ApplicationSnapshotError<io::Error>> {
        if at.0 != self.state.applied {
            return Err(disk::invalid("snapshot must match saved progress").into());
        }
        Ok(ApplicationSnapshot {
            applied_index: at,
            payload: serde_json::to_vec(&self.state).map_err(io::Error::from)?,
            raft_snapshot: None,
        })
    }

    fn install_snapshot(
        &mut self,
        snapshot: ApplicationSnapshot,
    ) -> Result<(), ApplicationSnapshotError<io::Error>> {
        if snapshot.applied_index.0 < self.state.applied {
            return Err(disk::invalid("snapshot would move the counter backwards").into());
        }
        let payload = if let Some(descriptor) = &snapshot.raft_snapshot {
            if descriptor.application_payload_len > 65_536 {
                return Err(disk::invalid("counter snapshot is too large").into());
            }
            let store = FileRaftSnapshotStore::open(&self.snapshots)
                .map_err(|error| disk::invalid(&error.to_string()))?;
            store
                .snapshot_chunk(SnapshotChunkRequest {
                    transfer_id: descriptor.transfer_id(),
                    metadata: &descriptor.metadata,
                    total_payload_len: descriptor.application_payload_len,
                    application_payload_crc32: descriptor.application_payload_crc32,
                    offset: 0,
                    len: descriptor.application_payload_len as u32,
                })
                .ok_or_else(|| disk::invalid("snapshot payload is unavailable"))?
        } else {
            snapshot.payload
        };
        let next: State = serde_json::from_slice(&payload).map_err(io::Error::from)?;
        if next.applied != snapshot.applied_index.0 {
            return Err(disk::invalid("snapshot progress does not match its data").into());
        }
        if next.applied == self.state.applied && next.value != self.state.value {
            return Err(disk::invalid("snapshot conflicts with already saved data").into());
        }
        disk::save(&self.file, &next)?;
        self.state = next;
        Ok(())
    }
}
```

</details>

<details>
<summary><strong>src/peers.rs</strong> — Connect the servers with TLS</summary>

<!-- counter-file: src/peers.rs -->
```rust
//! Three fixed peers, each with its own TLS identity and on-disk sessions.
use crate::{disk, Result, GROUP, MEMBERS};
use rafter::NodeId;
use rafter_transport_tls::{
    CertificateDirectory, ClusterId, EndpointBook, FileTransportSessionStore, GroupIdCodec,
    PeerEndpoint, PeerId, TlsIdentity, TlsPeerDirectory, TlsPeerTransport, TlsServerName,
    TransportConfig, TransportLimits, TransportTimeouts,
};
use std::{io, path::Path};

pub type Transport = TlsPeerTransport<u64, GroupCodec>;

pub fn cluster() -> ClusterId {
    ClusterId::new("rafter-counter-v1").unwrap()
}
pub fn principal(id: u64) -> PeerId {
    PeerId::new(&format!("node-{id}")).unwrap()
}

pub fn address(variable: &str, default: u16, id: u64) -> Result<std::net::SocketAddr> {
    let base: u16 = std::env::var(variable)
        .unwrap_or_else(|_| default.to_string())
        .parse()?;
    let port = base
        .checked_add(u16::try_from(id)?)
        .ok_or("port is out of range")?;
    Ok(([127, 0, 0, 1], port).into())
}

pub fn open(id: u64, directory: &Path) -> Result<(Transport, TlsPeerDirectory<u64>)> {
    let identity = TlsIdentity::from_pem_files(
        format!("certs/node-{id}.pem"),
        format!("certs/node-{id}-key.pem"),
        "certs/ca.pem",
    )?;
    let limits = TransportLimits::default();
    let directory_map = TlsPeerDirectory::new(limits.directory());
    let endpoints = EndpointBook::new(limits.endpoints());
    let mut certificates = CertificateDirectory::builder();
    for peer in MEMBERS {
        certificates = certificates
            .map_pem_certificate_file(format!("certs/node-{peer}.pem"), principal(peer))?;
        directory_map.bind(GROUP, NodeId(peer), principal(peer))?;
        if peer != id {
            endpoints.replace(
                principal(peer),
                vec![PeerEndpoint::new(
                    address("COUNTER_PEER_BASE", 7000, peer)?,
                    TlsServerName::new(&format!("node-{peer}"))?,
                )],
            )?;
        }
    }
    let certificates = certificates.build();
    identity.validate_local_peer(&principal(id), &certificates)?;
    let sessions = FileTransportSessionStore::open_existing(
        directory.join("sessions"),
        &cluster(),
        &principal(id),
    )?;
    let config = TransportConfig::new(
        cluster(),
        principal(id),
        address("COUNTER_PEER_BASE", 7000, id)?,
        limits,
        TransportTimeouts::default(),
    );
    let transport = TlsPeerTransport::builder(config, GroupCodec)
        .identity(identity)
        .certificates(certificates)
        .directory(directory_map.clone())
        .endpoints(endpoints)
        .session_store(sessions)
        .bind_paused()?;
    Ok((transport, directory_map))
}

#[derive(Clone, Copy, Debug)]
pub struct GroupCodec;
impl GroupIdCodec<u64> for GroupCodec {
    type Error = io::Error;
    fn max_encoded_len(&self) -> usize {
        8
    }
    fn max_decoded_heap_bytes(&self) -> usize {
        0
    }
    fn encode(&self, group: &u64, output: &mut Vec<u8>) -> io::Result<()> {
        output.clear();
        output.extend_from_slice(&group.to_be_bytes());
        Ok(())
    }
    fn decode(&self, bytes: &[u8]) -> io::Result<u64> {
        Ok(u64::from_be_bytes(bytes.try_into().map_err(|_| {
            disk::invalid("group ID must be eight bytes")
        })?))
    }
}
```

</details>

<details>
<summary><strong>src/http.rs</strong> — Handle curl requests</summary>

<!-- counter-file: src/http.rs -->
```rust
//! A small loopback-only client API, independent of the TLS peer connections.
use crate::{peers, Driver, Result};
use rafter::Role;
use rafter_service::{DriverServiceState, ReadOptions, WriteOptions};
use serde_json::json;
use std::{
    future::Future,
    pin::pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Wake, Waker},
    thread,
    time::{Duration, Instant},
};
use tiny_http::{Header, Method, Request, Response, Server};

pub fn serve(
    server: Server,
    driver: Driver,
    stop: Arc<AtomicBool>,
    connections: Arc<AtomicUsize>,
) -> Result<()> {
    while !stop.load(Ordering::Acquire) {
        let Some(request) = server.recv_timeout(Duration::from_millis(50))? else {
            continue;
        };
        // One request at a time keeps the tutorial's client work bounded.
        if let Err(error) = respond(request, &driver, &stop, connections.load(Ordering::Acquire)) {
            eprintln!("client connection closed: {error}");
        }
    }
    Ok(())
}

fn respond(request: Request, driver: &Driver, stop: &AtomicBool, connections: usize) -> Result<()> {
    let (metrics, value, caught_up) = driver.with_group(|group| {
        let metrics = group.metrics();
        let caught_up = metrics.applied_index >= group.committed_application_index()
            && metrics.fatal_state == rafter_app::group::GroupFatalState::Healthy;
        (metrics, group.state_machine().value(), caught_up)
    })?;
    let ready = caught_up
        && !driver.peer_policy_is_stale()
        && driver.service_state() == DriverServiceState::Serving;
    if request.method() == &Method::Get && request.url() == "/status" {
        return send(
            request,
            200,
            json!({"node": metrics.node_id.0,
            "role": format!("{:?}", metrics.role), "leader": metrics.leader_hint.map(|id| id.0),
            "ready": ready, "local_value": value, "applied": metrics.applied_index.0,
            "tls_connections": connections}),
        );
    }
    if request.method() == &Method::Post && request.url() == "/stop" {
        stop.store(true, Ordering::Release);
        return send(request, 200, json!({"stopping": metrics.node_id.0}));
    }
    let increment = request
        .url()
        .strip_prefix("/add/")
        .and_then(|text| text.parse::<u64>().ok());
    let writing = request.method() == &Method::Post && increment.is_some();
    let reading = request.method() == &Method::Get && request.url() == "/value";
    if !writing && !reading {
        return send(
            request,
            404,
            json!({"error": "use GET /value, GET /status, or POST /add/<amount>"}),
        );
    }
    if !ready {
        return send(request, 503, json!({"error": "node is recovering"}));
    }
    if metrics.role != Role::Leader {
        if let Some(leader) = metrics
            .leader_hint
            .filter(|leader| *leader != metrics.node_id)
        {
            let location = format!(
                "http://{}{}",
                peers::address("COUNTER_HTTP_BASE", 8000, leader.0)?,
                request.url()
            );
            let header =
                Header::from_bytes("Location", location).map_err(|_| "invalid redirect")?;
            request.respond(Response::empty(307).with_header(header))?;
            return Ok(());
        }
        return send(
            request,
            503,
            json!({"error": "waiting for a leader; try again shortly"}),
        );
    }
    let result = if writing {
        (|| -> Result<u64> {
            let (id, future) = driver.begin_write(increment.unwrap(), WriteOptions::default())?;
            let result = wait(future).map(|receipt| receipt.result);
            if result.is_err() {
                let _ = driver.abandon_write(id);
            }
            result
        })()
    } else {
        (|| -> Result<u64> {
            let (id, future) = driver.begin_read((), ReadOptions::default())?;
            let result = wait(future).map(|receipt| receipt.result);
            if result.is_err() {
                let _ = driver.abandon_read(id);
            }
            result
        })()
    };
    match result {
        Ok(value) => send(request, 200, json!({"value": value})),
        Err(error) => send(
            request,
            503,
            json!({"error": error.to_string(),
            "write_may_have_committed": writing}),
        ),
    }
}

fn send(request: Request, status: u16, body: serde_json::Value) -> Result<()> {
    let header = Header::from_bytes("Content-Type", "application/json").unwrap();
    request.respond(
        Response::from_string(format!("{body}\n"))
            .with_status_code(status)
            .with_header(header),
    )?;
    Ok(())
}

struct WakeThread(thread::Thread);
impl Wake for WakeThread {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

fn wait<T, E: std::error::Error + Send + Sync + 'static>(
    future: impl Future<Output = std::result::Result<T, E>>,
) -> Result<T> {
    let waker = Waker::from(Arc::new(WakeThread(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Poll::Ready(result) = future.as_mut().poll(&mut context) {
            return Ok(result?);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err("request timed out; a submitted write may still commit".into());
        }
        thread::park_timeout(remaining);
    }
}
```

</details>

<details>
<summary><strong>src/disk.rs</strong> — Save complete records to disk</summary>

<!-- counter-file: src/disk.rs -->
```rust
//! Small, checksummed records, published atomically while the node owns its files.
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::Path,
};

pub fn save<T: Serialize>(path: &Path, value: &T) -> io::Result<()> {
    let payload = serde_json::to_vec(value)?;
    let temporary = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(b"RCT1")?;
    file.write_all(&rafter_crc32::crc32(&payload).to_be_bytes())?;
    file.write_all(&payload)?;
    file.sync_all()?;
    drop(file);
    fs::rename(temporary, path)?;
    File::open(path.parent().expect("record has a parent"))?.sync_all()
}

pub fn load<T: DeserializeOwned>(path: &Path) -> io::Result<T> {
    let mut bytes = Vec::new();
    File::open(path)?.take(65_537).read_to_end(&mut bytes)?;
    if bytes.len() < 8 || bytes.len() > 65_536 || &bytes[..4] != b"RCT1" {
        return Err(invalid("invalid counter record"));
    }
    let checksum = u32::from_be_bytes(bytes[4..8].try_into().unwrap());
    if rafter_crc32::crc32(&bytes[8..]) != checksum {
        return Err(invalid("counter record checksum mismatch"));
    }
    serde_json::from_slice(&bytes[8..]).map_err(io::Error::from)
}

pub fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
```

</details>

<details>
<summary><strong>src/checkpoint.rs</strong> — Keep the server membership record</summary>

<!-- counter-file: src/checkpoint.rs -->
```rust
//! Preserve the service's membership record across restarts.
use crate::{disk, GROUP};
use rafter::{LogIndex, NodeId};
use rafter_service::{CurrentCommittedState, PeerControlPlaneCheckpoint};
use serde::{Deserialize, Serialize};
use std::{io, path::Path};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    node: u64,
    group: u64,
    high_water: Option<u64>,
    current: Option<(u64, Vec<u64>)>,
    contradicted: Option<u64>,
}

pub fn save(path: &Path, node: u64, checkpoint: PeerControlPlaneCheckpoint<u64>) -> io::Result<()> {
    disk::save(
        path,
        &Record {
            node,
            group: checkpoint.group,
            high_water: checkpoint.committed_id_high_water.map(|id| id.0),
            current: checkpoint.current_committed.map(|state| {
                (
                    state.through.0,
                    state.membership.into_iter().map(|id| id.0).collect(),
                )
            }),
            contradicted: checkpoint.contradicted_at.map(|index| index.0),
        },
    )
}

pub fn load(path: &Path, node: u64) -> io::Result<PeerControlPlaneCheckpoint<u64>> {
    let record: Record = disk::load(path)?;
    if record.node != node || record.group != GROUP {
        return Err(disk::invalid(
            "this checkpoint belongs to another node or group",
        ));
    }
    let mut checkpoint = PeerControlPlaneCheckpoint::empty(GROUP);
    checkpoint.committed_id_high_water = record.high_water.map(NodeId);
    checkpoint.current_committed = record.current.map(|(through, members)| {
        CurrentCommittedState::new(LogIndex(through), members.into_iter().map(NodeId).collect())
    });
    checkpoint.contradicted_at = record.contradicted.map(LogIndex);
    Ok(checkpoint)
}
```

</details>

## 3. Prepare the certificates and data

Run these once, from your `rafter-counter` folder:

```sh
sh certs.sh
cargo build
cargo run -- init
```

The certificate script prints OpenSSL's progress, then `Created local TLS
certificates in certs/.` The last command prints:

```text
Created data for nodes 1, 2, and 3. Run init only once.
```

Each server now has its own certificate and data directory:

| Server | Saved data | Client API | TLS peer connection |
| --- | --- | --- | --- |
| 1 | `data/node-1` | `127.0.0.1:8001` | `127.0.0.1:7001` |
| 2 | `data/node-2` | `127.0.0.1:8002` | `127.0.0.1:7002` |
| 3 | `data/node-3` | `127.0.0.1:8003` | `127.0.0.1:7003` |

The setup commands refuse to replace existing folders. Keep these same files
when restarting; creating a new cluster is a separate operation.

## 4. Start the three servers

Open three terminals in the same `rafter-counter` folder. Leave each command
running.

**Terminal 1**

```sh
cargo run -- node 1
```

**Terminal 2**

```sh
cargo run -- node 2
```

**Terminal 3**

```sh
cargo run -- node 3
```

Each prints its client and peer addresses. For example:

```text
Node 1: http://127.0.0.1:8001 (peer TLS on 127.0.0.1:7001)
```

The servers choose a **leader** to coordinate writes. Give them a moment to
connect before making your first request.

## 5. Add and read a value

Use a fourth terminal. Add five to the counter:

```sh
curl --location --fail -X POST http://127.0.0.1:8001/add/5
```

**Expected result:**

```json
{"value":5}
```

Now read it through a different server:

```sh
curl --location --fail http://127.0.0.1:8002/value
```

**Expected result:** `{"value":5}`.

Add three more through server 3:

```sh
curl --location --fail -X POST http://127.0.0.1:8003/add/3
```

**Expected result:** `{"value":8}`.

`--location` lets curl follow a server's redirect to the current leader. A
successful write means the command was agreed and the leader saved the new
counter value. Reads use Rafter's linearizable read API, so they account for
writes completed before the read began.

To see a server's role, local saved value, and authenticated TLS connections:

```sh
curl --fail http://127.0.0.1:8001/status
```

`/status` is a local diagnostic view. Use `/value` when you need the agreed value.

## 6. Restart and keep your data

Press **Ctrl+C in all three server terminals**. Restart them with the same
commands from step 4, in the same folder. Keep `data/` and `certs/` as they are;
you don't need to run setup again.

Once the servers have chosen a leader, read the counter:

```sh
curl --location --fail http://127.0.0.1:8001/value
```

**Expected result:** `{"value":8}`.

The app saves both the value and which commands are already included. On
restart, it resumes from that point instead of adding those commands twice.

You can also stop just one server and continue using the other two. If the
leader stops, allow a moment for a new leader to be chosen. Three servers need
two available to agree on new writes.

<details>
<summary><strong>If a request fails</strong></summary>

Use `/status` to check that at least two servers are running and have connected.
A request made during startup or an election can return `503` while the group
chooses a leader.

A write that times out may still complete. Don't automatically repeat an
increment after an uncertain result: it could add the amount twice. A real
client can attach its own request IDs and have the application remember results
for safe retries.

Missing or corrupt saved files stop a server from starting. The app does not
silently reset a counter it cannot recover.

</details>

## The layers you just used

| Part | Who provides it? |
| --- | --- |
| Agree on the order of commands | `rafter` |
| Save and recover Raft's state | `rafter-storage` and `rafter-runtime` |
| Apply commands to the counter | `rafter-app` plus your `Counter` type |
| Track writes and serve consistent reads | `rafter-service` |
| Encrypt and authenticate server connections | `rafter-transport-tls` |
| Counter behavior, saved counter value, and HTTP API | This application |

For a smaller stack, use just `rafter` and supply your own storage, network, and
processing loop. You can also replace one component at a time. Add
`rafter-multiraft` when you need many independent groups in one process.

This starter uses three fixed members and keeps its Raft log without automatic
compaction. Its HTTP client API is local-only; TLS protects the peer connections.
The generated certificates are for this local exercise and expire after 30 days.
For deployment choices, continue with the [architecture guide](./architecture.md)
and [TLS configuration](../crates/rafter-transport-tls/README.md).
