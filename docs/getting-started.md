# Getting started

Rafter gives you the pieces to keep application state in agreement across
servers. You choose how much of the stack to use, and your code defines what
that shared state means.

We'll build a counter that three servers share. A client sends **add five**;
Rafter gets the servers to agree on that command; your code adds five and saves
the result. That separation is the basis of the whole stack.

The small code excerpts below come from one [standalone counter](./examples/counter).
Read them as a tour of the integration points. The complete files and commands
to run it are at the end.

## 1. Choose where to start

Start with the part of your system you want Rafter to handle:

| You want… | Use… | You provide… |
| --- | --- | --- |
| Just the Raft algorithm | `rafter` | Storage, networking, a loop to drive it, and application behavior |
| A durable group inside your own host | `rafter-storage` + `rafter-runtime` + `rafter-app` | Application behavior and storage, message routing, and the host's execution loop |
| The supplied peer transport and write/read calls too | Add `rafter-service` + `rafter-transport-tls` | Application behavior and storage, peer configuration, and your client-facing API and execution loop |

Each higher layer uses the layers below it. With the full stack, your loop
calls the service driver, which handles the lower-level steps.

At the smallest layer, **`rafter::Node` takes inputs and returns work to do**:

```rust
let outputs = node.step(input);
```

An input might be a tick, a peer message, or a proposed command. Outputs can ask
you to save Raft state, send a message, or apply a committed command. This core
performs no IO: your host must do that work in the right order. For example, a
vote must be saved before sending the message that grants it.

Choose the core alone when you want to implement those connections yourself.
For our counter, we'll use the supplied storage, runtime, application, service,
and TLS layers. Each takes over a piece of that work. None chooses what adding
five means or what API your clients use.

## 2. Define your application's behavior

**`rafter-app` provides the `ReplicatedStateMachine` trait. You implement it.**
A state machine is simply your data plus the rules for changing and reading it.

Our counter's implementation chooses these types:

<!-- counter-snippet: src/counter.rs -->
```rust
type Command = u64;
type CommandResult = u64;
type Query = ();
type QueryResult = u64;
```

A command is the amount to add. Its result is the new value. A query takes no
arguments (`()`) and returns the current value. A different application could
use a command enum such as `CreateJob` and `FinishJob`, with its own result types.

The trait gives you three jobs:

| Your methods | What you implement |
| --- | --- |
| `encode_command` / `decode_command` | Turn a command into bytes every replica interprets the same way |
| `apply_batch` / `read` | Change or read your application state |
| `applied_index` / `build_snapshot` / `install_snapshot` | Report saved progress and save or restore a complete application image |

**Rafter decides which commands are committed and their order. Your
`apply_batch` decides what those commands do.** Here is the counter's update
inside that method:

<!-- counter-snippet: src/counter.rs -->
```rust
next.value = next.value.saturating_add(entry.command);
next.applied = entry.index.0;
```

`entry.command` is the amount. `entry.index` identifies its position in the Raft
log. The counter stops at `u64::MAX`; another overflow policy would be an
application decision. Given the same starting state and commands, every replica
must produce the same result. Put timestamps or random choices in the command
when you need them, so replicas use the same values.

The method saves the new state before reporting success:

<!-- counter-snippet: src/counter.rs -->
```rust
disk::save(&self.file, &next)?;
self.state = next;
```

`disk::save` is **our application's helper**, not a Rafter API. It saves the
counter value and applied index together. If the server restarts with value
`5` and that command's index, it knows the increment is already included.
Saving only the value would leave recovery unable to tell whether to add it again.

You can use a database transaction instead of this helper. The requirement is
the same: when `apply_batch` succeeds, both the effects and their applied index
must survive a restart. Return one `ApplyResult` for each command in the batch,
in order. Rafter uses those results to complete the corresponding requests.
For a richer application, put ordinary rejections such as "job already finished"
in `CommandResult`; an `apply_batch` error means the group cannot safely continue.

## 3. Add durable Raft storage and connect your state machine

There are **two kinds of saved state** in this application:

| State | Who saves it? |
| --- | --- |
| Raft's votes, command log, and snapshot records | `rafter-storage`, driven by `rafter-runtime` |
| The counter value and which commands it includes | Your `Counter` implementation |

**`rafter-storage` supplies the file stores.** Open the bundle for this server:

<!-- counter-snippet: src/main.rs -->
```rust
let (hard_state, log, snapshots) =
    FileRaftNodeStores::open(directory.join("raft"))?.into_parts();
let app = Counter::open(&directory)?;
let applied = app.applied_index()?;
```

`FileRaftNodeStores` also locks the directory against another live writer.
`Counter::open` is our code loading the saved application state. Each server has
its own directory; the example's separate `init` command creates it once.

**`rafter-runtime` decides when Raft's storage must be written.** Its
`DurableRaftNode` saves required changes before releasing messages or commands
for the next layer to act on. You do not need to implement that ordering yourself.
Here, "runtime" refers to that durability work; your host still chooses the
threads or async runtime that execute it.

On startup, recover it using the counter's saved progress:

<!-- counter-snippet: src/main.rs -->
```rust
let recovered = DurableRaftNode::recover_with_storage_and_snapshot_store_applied_through(
    config, hard_state, log, snapshots, applied,
)?;
let (raft, outputs) = recovered.into_parts();
let group = RaftGroup::with_applied_index(GROUP, NodeId(id), raft, app, applied);
```

`config` contains this server's node ID, its peers, and timing settings.
`GROUP` identifies the one shared counter; `id` identifies this replica.

The last line is **`rafter-app` joining the durable node to your state machine**.
`RaftGroup` delivers committed commands to `apply_batch`, coordinates reads, and
matches application results to proposals. It uses the same `applied` value as
recovery so commands already saved by your application are not applied twice.
Keep `outputs`: recovery can produce work too. We'll give it to the service
when we connect the group in step 5.

At this point, you have a durable application group. If you already have a
network and execution loop, you can drive `RaftGroup` directly and route its
outgoing messages yourself. The next two crates provide another way to do that.

## 4. Use Rafter's TLS transport

**`rafter-transport-tls` opens peer connections, encrypts traffic, authenticates
peers, and handles bounded queues and reconnects.** You configure who those
peers are and where to reach them.

Our `peers::open` helper prepares that configuration, then uses the public builder:

<!-- counter-snippet: src/peers.rs -->
```rust
let transport = TlsPeerTransport::builder(config, GroupCodec)
    .identity(identity)
    .certificates(certificates)
    .directory(directory_map.clone())
    .endpoints(endpoints)
    .session_store(sessions)
    .bind_paused()?;
```

Here is what your application supplies to that builder:

| Input | In this counter |
| --- | --- |
| `config` | Cluster and local peer IDs, a listening address, limits, and timeouts |
| `GroupCodec` | A small implementation that encodes our numeric group ID |
| `identity` | This server's certificate, private key, and trusted CA |
| `certificates` | Which certificates belong to each allowed peer |
| `directory_map` | Which Raft node ID belongs to each peer in the group |
| `endpoints` | The network addresses and TLS server names of the other servers |
| `sessions` | A `FileTransportSessionStore` reopened from this server's saved files |

Rafter supplies the session store implementation. Your application creates it
once and reopens it on restart; this keeps connection history intact. Likewise,
Rafter checks certificates, but your deployment issues and rotates them.

`bind_paused` prepares the transport without activating traffic. We'll attach the
group's peer policy before starting it. These are **server-to-server connections**;
your HTTP or other client-facing API remains a separate choice.

## 5. Connect the service and submit requests

**`rafter-service` connects the group to a transport and gives callers futures
for write and read results.** It lets request handling wait for a result while
the server continues driving Raft.

In our example, `Driver` is a type alias for `TransportRaftDriver` using our
counter, durable runtime, TLS sender, and peer validator. We connect them here:

<!-- counter-snippet: src/main.rs -->
```rust
let (transport, validator) = peers::open(id, &directory)?;
let driver = Driver::with_control_plane_checkpoint(
    group,
    outputs,
    transport.sender(),
    validator,
    TransportDriverOptions::default(),
    checkpoint::load(&directory.join("checkpoint"), id)?,
)?;
```

This passes on the recovery outputs from step 3 and connects the sender and
validator from step 4. The checkpoint is the service's saved membership record.
`checkpoint::load` and `checkpoint::save` are application helpers: Rafter defines
the record and its rules, while we choose how to keep it on disk.

Save the resulting record, then start peer traffic:

<!-- counter-snippet: src/main.rs -->
```rust
checkpoint::save(
    &directory.join("checkpoint"),
    id,
    driver.control_plane_checkpoint(),
)?;
transport.start()?;
```

Our HTTP handler parses `/add/5` into an increment and submits it on the leader:

<!-- counter-snippet: src/http.rs -->
```rust
let (id, future) = driver.begin_write(increment.unwrap(), WriteOptions::default())?;
let result = wait(future).map(|receipt| receipt.result);
```

`begin_write` returns a request ID and a future. A successful receipt contains
the result from your `apply_batch`. The `wait` here is our helper for polling a
future on the HTTP thread; an async caller can await it instead. **The Raft loop
must keep running elsewhere while the request waits.** We'll cover that next.

Reads have a corresponding API:

<!-- counter-snippet: src/http.rs -->
```rust
let (id, future) = driver.begin_read((), ReadOptions::default())?;
let result = wait(future).map(|receipt| receipt.result);
```

The default read checks leadership and waits for the necessary commands to be
applied before calling your `read` method. This is a *linearizable read*: it
accounts for writes completed before the read began. Reading the counter's
local value directly may return stale data on a follower.

**Your API handles client concerns.** This example chooses HTTP, redirects
followers to the leader, and sets a request timeout. On timeout it abandons the
waiter, not the command: a submitted increment may still commit. Safe retries
need application request IDs and remembered results; Raft alone does not make
repeating `/add/5` harmless.

## 6. Keep the server moving

**You own the execution loop.** Rafter's TLS transport runs its connection
workers, but your host still delivers incoming envelopes to the driver, supplies
ticks, and advances pending reads. Our loop's central work is:

<!-- counter-snippet: src/main.rs -->
```rust
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
```

`inbound` comes from `transport.inbound()`. Delivering an envelope or ticking the
driver steps the group, performs required persistence, applies committed work,
and routes outgoing messages through the configured sender. The example chooses
a 20 ms tick interval; the core itself never reads a clock.

The complete loop also saves checkpoint changes and stops on terminal failures.
Startup checks saved state before serving. The HTTP handler waits on a separate
thread so a waiting client cannot prevent the replicas from reaching agreement.
You can organize those tasks differently in your own host.

## Follow one write through the stack

With all the pieces connected, `/add/5` follows this path:

1. **Your HTTP handler** parses the amount and calls `begin_write` on the leader.
2. **`rafter`** orders the command and replicates it. **`rafter-runtime`** saves
   required Raft state through **`rafter-storage`** before releasing outputs;
   the service routes peer traffic through **`rafter-transport-tls`**.
3. Once a majority has durably accepted the command, **`rafter-app`** calls your
   `apply_batch` when that command is ready to be applied in order.
4. **Your counter** updates its value and saves it together with the applied index.
5. **`rafter-service`** completes the write's future with the application result.
   **Your HTTP handler** turns it into `{"value":5}`.

Each replica applies committed commands to its own application state. A
successful write does not require waiting for the slowest replica, so one
follower can catch up later.

## Make it your application

To build a job queue, replace the counter's command, result, and query types;
implement the job transitions in `apply_batch`; and save your jobs and applied
index together. Then expose the API your clients need. The durable Raft and TLS
connections can stay in place.

You can also change one infrastructure choice at a time:

| You want… | What to change |
| --- | --- |
| Your own storage engine for Raft records | Implement the `rafter-storage` traits and pass your stores to `DurableRaftNode` |
| Your own peer transport | Implement the `rafter-service` transport and peer-validation contracts instead of using the TLS sender and directory |
| Many independent groups in one host | Consider `rafter-multiraft` for scheduling those groups |

Using your own application database is a separate decision from replacing Raft's
stores. You can keep Rafter's file stores and save your application state in your
database, or supply both.

A **snapshot** is a saved application image at a particular applied index.
Your implementation defines that image and restores it; you also decide when
to create one and compact old log entries. The counter implements snapshot
methods, but this small starter keeps its log and does not schedule compaction
or configure outbound snapshot transfer. Its three members are fixed.

You also own deployment choices: peer addresses, certificates, membership
changes, client authentication, and when a recovered server may accept requests.
The [architecture guide](./architecture.md) and
[TLS configuration guide](../crates/rafter-transport-tls/README.md) expand those
contracts when you need them.

## Run the counter and explore

The [standalone project](./examples/counter) contains all the files used above.
It has its own Cargo workspace and pinned Rafter Git dependencies. You can run
it from that folder or move the folder outside the Rafter checkout.

On macOS or Linux, install Rust/rustup, OpenSSL, curl, and a C compiler. The
project selects Rust 1.88. Run once from the counter folder:

```sh
sh certs.sh
cargo build
cargo run -- init
```

Start `cargo run -- node 1`, `cargo run -- node 2`, and `cargo run -- node 3` in
three terminals in that same folder. After they choose a leader, use a fourth:

```sh
curl --location --fail -X POST http://127.0.0.1:8001/add/5
curl --location --fail http://127.0.0.1:8002/value
```

Both return `{"value":5}`. Stop all three with Ctrl+C, restart them with the same
node commands, and read again: the value is still `5`. Keep `data/` and `certs/`;
run initialization only for a new cluster. A write made during startup can fail
while the servers choose a leader; an uncertain write must not be blindly retried.

To connect the behavior to the code, try these in order:

1. Change the amount in `/add/5`. It becomes `entry.command` in `apply_batch`.
2. Stop one server and write through a running server. Two replicas can still
   agree. Restart the third and watch its `/status` value catch up.
3. Replace the counter's addition with another deterministic rule. Start a
   fresh exercise with empty data and the same new code on every replica, so old
   log entries are not interpreted with new meanings.

The client HTTP listeners are local-only. TLS protects the peer connections,
and the exercise's generated certificates expire after 30 days.

<details>
<summary><strong>Complete application files</strong> — Optional source reference</summary>

These are the full files behind the excerpts. To assemble the project manually,
create a `rafter-counter/src` folder and save each file at the path shown. You
can also use the standalone project directly.

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

</details>
