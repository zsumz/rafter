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

The counter uses `src/application.rs` to adapt committed bytes to the bounded
application worker. It decodes to a fixed-size entry, preserves the runtime-verified
protocol-only prefix, and delegates durability to the same `apply_batch`. Worker
tests cover retained credits, durable results, and failed publication.

## 3. Add durable storage and an application owner

There are two kinds of saved state: Rafter's votes, log, and snapshots, and your
application's value plus applied index. Open the supplied file-store bundle before
opening application files; the bundle also locks the Raft directory against a
second live writer:

<!-- counter-snippet: src/main.rs -->
```rust
let (hard_state_store, log_segment, snapshot_store) =
    FileRaftNodeStores::open(directory.join("raft"))?.into_parts();
let app = Counter::open(&directory)?;
```

The example enables `rafter-service`'s optional `durable` feature. Its
`DurableClientOwner` recovers Raft from the application's saved floor, then starts
bounded persistence and application workers. Recovery outputs stay with that
owner until replay or snapshot installation finishes. Already saved increments
are not applied again.

Our `owner::recover` helper creates it through this public constructor:

<!-- counter-snippet: src/owner.rs -->
```rust
let owner = DurableClientOwner::recover_with_reads(
    config,
    storage,
    app,
    options,
    Entry::try_from,
    |counter, _query| counter.value(),
    move || wake.unpark(),
)?;
```

`options` bounds the work queues; `Entry::try_from` decodes one committed command.
The query callback borrows a verified immutable counter and returns an owned
number. `wake` unparks the owner thread after worker or client activity. The
example reserves eight writes and eight reads independently; dropping an accepted
write's waiter does not prematurely release its admission credit.

This is one way to compose the layers. A host wanting synchronous application
execution can instead combine `DurableRaftNode`, `RaftGroup`, and
`TransportRaftDriver`. The durable owner keeps application execution on its own
worker; ordinary consensus inputs and local snapshot maintenance can still do
synchronous storage I/O on the owner thread.

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

## 5. Attach the service and submit requests

`DurableRaftDriver` connects the unique owner to the existing TLS sender and
exposes a cloneable `RaftHandle`. Cloning a handle does not retain the backend.
The main thread owns the driver; the HTTP thread receives only its handle.

The example recovers and checks its fixed membership before publishing peer
policy or starting traffic. `policy.rs` accepts only the original voters 1, 2,
and 3, with no learners or joint configuration. Its saved checkpoint must agree:
a larger retirement floor, removed voter, contradictory record, or record ahead
of Raft causes startup to fail. An empty checkpoint is valid only beside pristine
Raft storage. Every later configuration crossing and snapshot boundary is checked.
This is an application restriction, not a replacement for the library's dynamic
membership machinery.

After recovery, the application saves the validated observation, then activates
its peer policy and transport:

<!-- counter-snippet: src/main.rs -->
```rust
peers::authorize(id, &transport)?;
transport.start()?;
```

A write encodes the increment before entering the client queue:

<!-- counter-snippet: src/http.rs -->
```rust
let command = increment.unwrap().to_be_bytes().to_vec();
wait(handle.write(command)).map(|receipt| receipt.result.result)
```

The receipt contains the durable application result. `wait` is our helper for
polling a future on the HTTP thread; an async caller can await it instead. The
owner must keep running while the request waits.

A linearizable read obtains a real quorum grant and waits for its application
floor before the query callback runs:

<!-- counter-snippet: src/http.rs -->
```rust
wait(handle.read(Vec::new(), ReadConsistency::Linearizable)).map(|receipt| receipt.result)
```

The diagnostic `/status` endpoint uses a local read, which may be stale on a
follower. Its `ready` field describes admission after recovery, not a quorum
proof or a promise that the replica has caught up to the leader.

For operations, `/metrics` observes the owner without querying the application.
Commit, applied and snapshot indexes, pending writes and reserved reads are the
last published progress; they can stay stale during synchronous storage I/O.
`retained_writes`, `retained_reads` and their `_bytes` fields read current admission
credits, including canceled work still owned by the driver and uncollected results.
`write_refusals` and `read_refusals` count budget refusals since owner construction
and reset on restart. These counters remain accessible while storage is blocked.
They are independent observations, not an atomic snapshot, a capacity reservation
or a whole-host memory bound. None proves a quorum read. Commit minus applied is
not an application backlog count because the Raft log also contains protocol entries.

The HTTP API redirects followers and bounds request waiting. Its error response
sets `write_may_have_committed` to false only when the managed write error proves
the command was not appended. On timeout, dropping the future abandons the
waiter; a submitted increment may still commit. Safe
write retries need application request IDs and remembered outcomes. Raft alone
does not make repeating `/add/5` harmless.

## 6. Keep the server moving and drain it on shutdown

The main loop calls `poll_next` through a budget of 64 events per turn. It handles
snapshot installation atomically, consumes application completions, and checks
configuration events. The driver routes outbound messages through the existing
TLS sender. The host still delivers authenticated inbound messages and supplies
clock ticks:

<!-- counter-snippet: src/owner.rs -->
```rust
if driver.state() == ServiceState::Ready && Instant::now() >= next_tick {
    step(driver, Input::Tick)?;
    next_tick = Instant::now() + Duration::from_millis(20);
}
```

Only take an inbound envelope when the owner can accept it. Check the group,
then use `validate_inbound_peer_envelope` to verify the destination, authenticated
principal, sender identity, and current directory authorization before stepping
Raft. A refused input caused by concurrent shutdown is an ordinary lifecycle
transition; other errors stop the process and require recovery.

Worker wakeups are retained by `unpark`; a bounded park also lets the loop check
incoming transport work and the next tick. When the event budget is exhausted,
it schedules another turn immediately.

Shutdown closes admission and waits for actual backend release:

<!-- counter-snippet: src/http.rs -->
```rust
wait(handle.shutdown())?;
stop.store(true, Ordering::Release);
```

The owner keeps polling until `Stopped`, even while the HTTP thread is waiting.
On fatal error, the main thread drops the owner before joining HTTP so retained
clients receive terminal outcomes after storage is released. A drop guard also
requests the drain if the HTTP worker panics before returning. The main thread
then joins the transport workers and reports the worker failure. A client handle
cannot keep the file locks alive.

## Follow one write through the stack

With all the pieces connected, `/add/5` follows this path:

1. **Your HTTP handler** parses the amount and calls `handle.write` on the leader.
2. **`rafter`** orders the command and replicates it. **`rafter-runtime`** saves
   required Raft state through **`rafter-storage`** before releasing outputs;
   the service routes peer traffic through **`rafter-transport-tls`**.
3. Once a majority has durably accepted the command, the **application worker**
   uses your adapter to call `apply_batch` in committed order.
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
or configure outbound snapshot transfer. Its three members are fixed, and
configuration changes are refused. The owner handles a recovered snapshot by
installing its exact saved bytes before resuming admission.

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
rafter = { git = "https://github.com/zsumz/rafter", rev = "90a5de9b8405f9eaa3b698a100f0d203ff8c90c6" }
rafter-app = { git = "https://github.com/zsumz/rafter", rev = "90a5de9b8405f9eaa3b698a100f0d203ff8c90c6" }
rafter-storage = { git = "https://github.com/zsumz/rafter", rev = "90a5de9b8405f9eaa3b698a100f0d203ff8c90c6" }
rafter-runtime = { git = "https://github.com/zsumz/rafter", rev = "90a5de9b8405f9eaa3b698a100f0d203ff8c90c6" }
rafter-service = { git = "https://github.com/zsumz/rafter", rev = "90a5de9b8405f9eaa3b698a100f0d203ff8c90c6", features = ["durable"] }
rafter-transport-tls = { git = "https://github.com/zsumz/rafter", rev = "90a5de9b8405f9eaa3b698a100f0d203ff8c90c6" }
rafter-crc32 = { git = "https://github.com/zsumz/rafter", rev = "90a5de9b8405f9eaa3b698a100f0d203ff8c90c6" }
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
subjectKeyIdentifier = hash
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
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always
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
    durable::{DurableCommandSender, DurableRaftDriver, DurableRaftHandle},
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
pub type Sender = DurableCommandSender<u64, ApplyResult<u64>, u64>;

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
        let (handle, sender, stop, ready, connections) = (
            driver.handle(),
            driver.sender(),
            stop.clone(),
            ready.clone(),
            connections.clone(),
        );
        thread::spawn(move || {
            // Close the owner even if the HTTP worker unwinds before returning.
            let _stop_on_exit = StopOnExit(&stop);
            http::serve(server, handle, sender, &stop, &ready, &connections)
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
```

</details>

<details>
<summary><strong>src/owner.rs</strong> — Own and drive the durable backend</summary>

<!-- counter-file: src/owner.rs -->
```rust
//! The main thread uniquely owns both durable workers and drives bounded turns.
use crate::{
    application::Entry, counter::Counter, disk, peers, policy, Driver, Result, Runtime, GROUP,
};
use rafter::{Input, NodeConfig, Output};
use rafter_app::state_machine::{ApplicationSnapshot, ReplicatedStateMachine};
use rafter_runtime::{
    application::ApplicationWorkerOptions,
    service::{ServiceEvent, ServiceOptions, ServicePoll, ServiceState, ServiceStepError},
    DurableRaftNodeStorage,
};
use rafter_service::{
    durable::{
        DurableClientOptions, DurableClientOwner, DurableClientStepError, DurableReadLimits,
        DurableWriteLimits,
    },
    validate_inbound_peer_envelope, DriverCommandSender, PeerControlPlaneCheckpoint,
};
use rafter_storage::{FileRaftHardStateStore, FileRaftLogSegment, FileRaftSnapshotStore};
use rafter_transport_tls::{TlsPeerDirectory, TlsSender};
use std::{
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    thread,
    time::{Duration, Instant},
};

pub fn recover(
    config: NodeConfig,
    storage: DurableRaftNodeStorage<
        FileRaftHardStateStore,
        FileRaftLogSegment,
        FileRaftSnapshotStore,
    >,
    app: Counter,
    sender: TlsSender<u64, peers::GroupCodec>,
    checkpoint: PeerControlPlaneCheckpoint<u64>,
) -> Result<Driver> {
    let mut service = ServiceOptions::new();
    service.application = ApplicationWorkerOptions::new().with_limits(64, 16_384, 16, 4096);
    service.max_proposals = 8;
    service.max_proposal_bytes = 64;
    let options = DurableClientOptions::new(
        service,
        DurableWriteLimits {
            writes: 8.try_into()?,
            payload_bytes: 64.try_into()?,
        },
        DurableReadLimits {
            reads: 8.try_into()?,
            payload_bytes: 8.try_into()?,
        },
    );
    let wake = thread::current();
    let owner = DurableClientOwner::recover_with_reads(
        config,
        storage,
        app,
        options,
        Entry::try_from,
        |counter, _query| counter.value(),
        move || wake.unpark(),
    )?;
    let node = owner
        .ready_node()
        .ok_or("recovery has no observable node")?;
    validate_node(node)?;
    policy::restore(
        &checkpoint,
        node.commit_index(),
        node.last_log_index(),
        node.current_term(),
    )?;
    let mut driver =
        Driver::attach(GROUP, owner, sender).map_err(|error| disk::invalid(&error.to_string()))?;
    let deadline = Instant::now() + Duration::from_secs(30);
    while driver.state() != ServiceState::Ready {
        turn(&mut driver)?;
        if Instant::now() >= deadline {
            return Err("counter recovery timed out".into());
        }
        thread::park_timeout(Duration::from_millis(1));
    }
    Ok(driver)
}

fn validate_node(node: &Runtime) -> Result<()> {
    policy::membership(&node.effective_membership())?;
    policy::membership(&node.committed_membership())?;
    if let Some(membership) = node.snapshot_committed_membership() {
        policy::membership(&membership)?;
    }
    Ok(())
}

fn turn(driver: &mut Driver) -> Result<bool> {
    for _ in 0..64 {
        if let Some(node) = driver.ready_node() {
            validate_node(node)?;
        }
        match driver.poll_next()? {
            ServicePoll::Pending => return Ok(false),
            ServicePoll::Event(ServiceEvent::InstallSnapshot {
                snapshot,
                mut application,
            }) => {
                policy::membership(
                    snapshot
                        .metadata
                        .committed_membership()
                        .ok_or("counter snapshot has no committed membership")?,
                )?;
                application.install_snapshot(ApplicationSnapshot {
                    applied_index: snapshot.metadata.last_included_index,
                    payload: Vec::new(),
                    raft_snapshot: Some(snapshot),
                })?;
                driver.resume_snapshot(application)?;
            }
            ServicePoll::Event(ServiceEvent::Output(Output::ConfigurationCommitted {
                previous,
                configuration,
                ..
            })) => {
                // Check every crossing, including an intermediate removal that
                // a later endpoint in the same Raft step would otherwise hide.
                policy::membership(&previous)?;
                policy::membership(&configuration.membership_config())?;
            }
            ServicePoll::Event(ServiceEvent::ApplicationFailed(failure)) => {
                return Err(format!("counter application failed: {:?}", failure.kind()).into());
            }
            ServicePoll::Progress | ServicePoll::Event(_) => {}
            _ => return Err("unsupported durable service poll state".into()),
        }
    }
    Ok(true)
}

fn step(driver: &mut Driver, input: Input) -> Result<()> {
    match driver.try_step(input) {
        Ok(()) => Ok(()),
        // HTTP may close admission between the owner's Ready check and input.
        // A refused tick/peer message during shutdown needs no further delivery.
        Err(DurableClientStepError::Service(ServiceStepError::Unavailable {
            state: ServiceState::Draining,
            ..
        })) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub fn drive(
    driver: &mut Driver,
    transport: &peers::Transport,
    validator: &TlsPeerDirectory<u64>,
    stop: &AtomicBool,
    ready: &AtomicBool,
    connections: &AtomicUsize,
) -> Result<()> {
    let inbound = transport.inbound();
    let mut next_tick = Instant::now();
    let mut shutdown_requested = false;
    let mut draining_reported = false;
    loop {
        if stop.load(Ordering::Acquire) && !shutdown_requested {
            // The sender requests shutdown immediately; canceling its waiter
            // does not cancel the drain. Continue driving until Stopped.
            drop(driver.sender().shutdown(GROUP));
            shutdown_requested = true;
        }
        let reschedule = turn(driver)?;
        if driver.state() == ServiceState::Draining && !draining_reported {
            eprintln!(
                "Node {}: draining durable work",
                driver.handle().metrics()?.current().node_id.0
            );
            draining_reported = true;
        }
        if driver.state() == ServiceState::Stopped {
            return Ok(());
        }
        if driver.state() == ServiceState::Ready && Instant::now() >= next_tick {
            step(driver, Input::Tick)?;
            next_tick = Instant::now() + Duration::from_millis(20);
        }
        for _ in 0..64 {
            if driver.state() != ServiceState::Ready {
                break;
            }
            let Some(envelope) = inbound.drain(1)?.pop() else {
                break;
            };
            if envelope.group_id != GROUP {
                continue;
            }
            let id = driver
                .ready_node()
                .ok_or("ready driver lost node ownership")?
                .id();
            if let Ok(envelope) = validate_inbound_peer_envelope(envelope, id, validator) {
                step(
                    driver,
                    Input::Message {
                        from: envelope.from,
                        message: envelope.message,
                    },
                )?;
            }
        }
        ready.store(
            matches!(driver.state(), ServiceState::Ready | ServiceState::Busy),
            Ordering::Release,
        );
        if let Some(error) = transport.terminal_failure() {
            return Err(error.into());
        }
        let diagnostics = transport.diagnostics();
        connections.store(
            diagnostics.active_inbound_connections + diagnostics.active_outbound_connections,
            Ordering::Release,
        );
        if !reschedule {
            thread::park_timeout(Duration::from_millis(2));
        }
    }
}
```

</details>

<details>
<summary><strong>src/policy.rs</strong> — Enforce the fixed membership across recovery</summary>

<!-- counter-file: src/policy.rs -->
```rust
//! This example accepts only its original three voters, with no learners.
use crate::{disk, GROUP, MEMBERS};
use rafter::{LogIndex, MembershipConfig, NodeId, Term};
use rafter_service::{CurrentCommittedState, PeerControlPlaneCheckpoint};
use std::io;

pub fn membership(configuration: &MembershipConfig) -> io::Result<()> {
    match configuration {
        MembershipConfig::Stable(set)
            if set.voters() == MEMBERS.map(NodeId) && set.learners().is_empty() =>
        {
            Ok(())
        }
        _ => Err(disk::invalid(
            "counter requires the original three voters; membership changes are unsupported",
        )),
    }
}

pub fn restore(
    checkpoint: &PeerControlPlaneCheckpoint<u64>,
    commit: LogIndex,
    last: LogIndex,
    term: Term,
) -> io::Result<()> {
    if checkpoint.group != GROUP || checkpoint.contradicted_at.is_some() {
        return Err(disk::invalid("foreign or contradictory counter checkpoint"));
    }
    match (
        &checkpoint.current_committed,
        checkpoint.committed_id_high_water,
    ) {
        (None, None) if commit == LogIndex::ZERO && last == LogIndex::ZERO && term == Term(0) => {
            Ok(())
        }
        (Some(current), Some(NodeId(3)))
            if current.through <= commit
                && current.membership == MEMBERS.into_iter().map(NodeId).collect() =>
        {
            Ok(())
        }
        _ => Err(disk::invalid(
            "counter checkpoint would forget membership or retirement history",
        )),
    }
}

pub fn observed(through: LogIndex) -> PeerControlPlaneCheckpoint<u64> {
    let mut checkpoint = PeerControlPlaneCheckpoint::empty(GROUP);
    checkpoint.committed_id_high_water = Some(NodeId(3));
    checkpoint.current_committed = Some(CurrentCommittedState::new(
        through,
        MEMBERS.into_iter().map(NodeId).collect(),
    ));
    checkpoint
}
```

</details>

<details>
<summary><strong>src/application.rs</strong> — Adapt committed commands to the application worker</summary>

<!-- counter-file: src/application.rs -->
```rust
//! Move committed counter commands through the bounded application worker.
use crate::{counter::Counter, disk};
use rafter::LogIndex;
use rafter_app::state_machine::{ApplyBatch, ApplyEntry, ApplyResult, ReplicatedStateMachine};
use rafter_runtime::{
    application::{ApplicationEntry, DurableApplication},
    service::{CommittedCommand, ServiceAdapterError},
};
use std::{io, mem::size_of};

#[derive(Debug)]
pub struct Entry {
    first: LogIndex,
    command: ApplyEntry<u64>,
}

impl TryFrom<CommittedCommand> for Entry {
    type Error = ServiceAdapterError;

    fn try_from(command: CommittedCommand) -> Result<Self, Self::Error> {
        // Decode to a scalar; do not retain an arbitrarily large backing frame.
        let amount = u64::from_be_bytes(
            command
                .payload
                .as_ref()
                .try_into()
                .map_err(|_| disk::invalid("an increment must be eight bytes"))?,
        );
        Ok(Self {
            first: command.first_index,
            command: ApplyEntry {
                index: command.index,
                term: command.term,
                command: amount,
                local_proposal_id: command.local_proposal_id,
            },
        })
    }
}

impl ApplicationEntry for Entry {
    fn first_log_index(&self) -> LogIndex {
        self.first
    }
    fn log_index(&self) -> LogIndex {
        self.command.index
    }
    fn retained_bytes(&self) -> usize {
        // Entry, temporary batch copy, and eventual result are all fixed size.
        size_of::<Self>() + size_of::<ApplyEntry<u64>>() + size_of::<ApplyResult<u64>>()
    }
}

impl DurableApplication<Entry> for Counter {
    type Outcome = ApplyResult<u64>;
    type Error = io::Error;

    fn applied_through(&self) -> LogIndex {
        // This implementation's floor is the atomically saved in-memory state.
        self.applied_index().expect("counter floor is infallible")
    }
    fn apply(&mut self, entries: &[Entry]) -> io::Result<Vec<Self::Outcome>> {
        self.apply_batch(ApplyBatch {
            entries: entries.iter().map(|entry| entry.command.clone()).collect(),
        })
    }
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
        let mut results = Vec::with_capacity(batch.entries.len());
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
use rafter_service::{PeerPolicy, RaftTransport};
use rafter_transport_tls::{
    CertificateDirectory, ClusterId, EndpointBook, FileTransportSessionStore, GroupIdCodec,
    PeerEndpoint, PeerId, TlsIdentity, TlsPeerDirectory, TlsPeerTransport, TlsServerName,
    TransportConfig, TransportLimits, TransportTimeouts,
};
use std::{io, path::Path};

pub type Transport = TlsPeerTransport<u64, GroupCodec>;

pub fn authorize(id: u64, transport: &Transport) -> Result<()> {
    transport.sender().update_peers(
        &GROUP,
        PeerPolicy::new(
            MEMBERS
                .into_iter()
                .filter(|peer| *peer != id)
                .map(principal)
                .collect(),
            Some(NodeId(3)),
        ),
    )?;
    Ok(())
}

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
use crate::{peers, Handle, Result, Sender};
use rafter::Role;
use rafter_service::{ReadConsistency, WriteError};
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
    handle: Handle,
    sender: Sender,
    stop: &AtomicBool,
    ready: &AtomicBool,
    connections: &AtomicUsize,
) -> Result<()> {
    while !stop.load(Ordering::Acquire) {
        let Some(request) = server.recv_timeout(Duration::from_millis(50))? else {
            continue;
        };
        // One request at a time keeps the tutorial's client work bounded.
        if let Err(error) = respond(
            request,
            &handle,
            &sender,
            stop,
            ready.load(Ordering::Acquire),
            connections.load(Ordering::Acquire),
        ) {
            eprintln!("client connection closed: {error}");
        }
    }
    Ok(())
}

fn respond(
    request: Request,
    handle: &Handle,
    sender: &Sender,
    stop: &AtomicBool,
    ready: bool,
    connections: usize,
) -> Result<()> {
    let metrics = handle.metrics()?.current();
    if request.method() == &Method::Post && request.url() == "/stop" {
        // The owner keeps driving while this waits for actual backend release.
        wait(handle.shutdown())?;
        stop.store(true, Ordering::Release);
        return send(request, 200, json!({"stopping": metrics.node_id.0}));
    }
    if request.method() == &Method::Get && request.url() == "/metrics" {
        // Credits stay observable even while published progress is stale.
        let retained = sender.retained();
        let refusals = sender.backpressure_refusals();
        return send(
            request,
            200,
            json!({
                "node": metrics.node_id.0, "role": format!("{:?}", metrics.role),
                "term": metrics.term.0, "ready": ready,
                "commit": metrics.commit_index.0, "applied": metrics.applied_index.0,
                "snapshot": metrics.snapshot_index.0,
                "pending_writes": metrics.pending_proposals,
                "pending_reads": metrics.pending_query_reads,
                "reserved_reads": metrics.reserved_reads,
                "tls_connections": connections,
                "retained_writes": retained.writes.writes,
                "retained_write_bytes": retained.writes.payload_bytes,
                "write_refusals": refusals.writes,
                "retained_reads": retained.reads.map(|reads| reads.reads),
                "retained_read_bytes": retained.reads.map(|reads| reads.payload_bytes),
                "read_refusals": refusals.reads,
            }),
        );
    }
    if request.method() == &Method::Get && request.url() == "/status" {
        let value = wait(handle.read(Vec::new(), ReadConsistency::Local))?.result;
        return send(
            request,
            200,
            json!({"node": metrics.node_id.0,
            "role": format!("{:?}", metrics.role), "leader": metrics.leader_hint.map(|id| id.0),
            "ready": ready, "local_value": value, "applied": metrics.applied_index.0,
            "tls_connections": connections}),
        );
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
        let command = increment.unwrap().to_be_bytes().to_vec();
        wait(handle.write(command)).map(|receipt| receipt.result.result)
    } else {
        wait(handle.read(Vec::new(), ReadConsistency::Linearizable)).map(|receipt| receipt.result)
    };
    match result {
        Ok(value) => send(request, 200, json!({"value": value})),
        Err(error) => {
            // A deadline alone proves nothing about a submitted command.
            // Only the managed write error can prove a pre-append refusal.
            let uncertain = writing
                && error
                    .downcast_ref::<WriteError>()
                    .is_none_or(|write| write.fate().may_commit());
            send(
                request,
                503,
                json!({"error": error.to_string(),
                "write_may_have_committed": uncertain}),
            )
        }
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
use std::{collections::BTreeSet, io, path::Path};

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
    checkpoint.current_committed = if let Some((through, members)) = record.current {
        let membership: BTreeSet<_> = members.iter().copied().map(NodeId).collect();
        if membership.len() != members.len() {
            return Err(disk::invalid("duplicate checkpoint member"));
        }
        Some(CurrentCommittedState::new(LogIndex(through), membership))
    } else {
        None
    };
    checkpoint.contradicted_at = record.contradicted.map(LogIndex);
    Ok(checkpoint)
}
```

</details>

</details>
