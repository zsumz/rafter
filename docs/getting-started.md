# Getting started

Rafter lets you build a replicated application a layer at a time. You can use
only the Raft protocol, or combine the supplied storage, runtime, application,
service, and TLS transport crates. You can also replace individual pieces.

A **state machine** is your application's data and the rules for changing it.
For a key-value store, a command might mean “set `alpha` to `one`.” Raft puts
commands in an agreed order; your state machine applies them in that order.

## Choose a starting point

| Start here | Rafter provides | You provide |
| --- | --- | --- |
| [Minimal: the kernel](#minimal-the-kernel) | Elections, replication, and commitment through `rafter`. | Storage and recovery, networking, ticks, application state, and the loop connecting them. |
| [Full stack: the supplied components](#full-stack-the-supplied-components) | Durable Raft storage and runtime, state-machine integration, managed write/read handles, and authenticated TLS peer connections. | Application behavior and durable application state, configuration and credentials, the service loop, and a client-facing API. |

Choose the full stack when you want Rafter to handle most of the consensus
plumbing. Choose the kernel when you already have storage and networking to
integrate. Both use the same protocol core.

“Full stack” here means a composition of libraries in your application. You
still build and run the application; the crates do not start a generic server.

## Before you start

Run the commands below from the root of a Rafter checkout. Rafter supports Rust
1.88; the TLS reference check also needs the `rustfmt` and `clippy` components,
a native C toolchain for its cryptography dependency, and permission to start
local processes and listen on loopback ports.

The examples use the code in your checkout. For your own application, keep its
Rafter dependencies on the same checkout or a compatible set of published
versions. The dependency snippets below assume your application and the
`rafter` checkout are sibling directories.

## Minimal: the kernel

Your application can depend on just one crate:

```toml
[dependencies]
rafter = { path = "../rafter/crates/rafter" }
```

Try the complete kernel example:

```sh
cargo run -p rafter --example pure_raft
```

It creates three nodes in one process, elects node 1, proposes
`set account:7 balance=42`, and prints that all three nodes applied the command.
The “network” is an in-memory queue and time advances through explicit ticks.
This is a protocol demonstration with no durable storage.

Read [the example](../crates/rafter/examples/pure_raft.rs) from `main` downward.
The integration has four parts:

1. Create a `Node` with a unique `NodeId`, its peers, and an election timeout.
2. Call `Node::step` with ticks, received messages, or client proposals.
3. Deliver `Output::Send` messages to the addressed peers.
4. Apply `Output::Apply` commands to your application state, in order.

The kernel treats command bytes as opaque. The example prints them; a real KV
application would decode the command and update its map. A proposal being
locally appended is not yet a committed write. To associate a request with its
outcome, see [tracked proposals](../crates/rafter/examples/tracked_proposal.rs).

A **linearizable read** acts like a read from a single up-to-date copy of the
data. Obtain a Raft read barrier and wait until your state machine has applied
through it before reading the value. Reading the local map alone can return
stale data.

### Making the minimal path durable

Once data must survive a restart, the host must persist Raft's required state
before releasing the resulting messages or application work. On recovery, it
must also know which commands the application has already durably applied.

You can implement that integration yourself, or add `rafter-runtime` with
`rafter-storage`. The runtime enforces the persist-before-output ordering, and
the storage crate supplies both file stores and traits for custom backends.
See the [recovery contract](./architecture.md#recovery-and-snapshots) and the
[file storage layout](../crates/rafter-storage/README.md#standard-file-layout).

## Full stack: the supplied components

For one durable Raft group with managed handles and TLS peers, use these crates:

```toml
[dependencies]
rafter = { path = "../rafter/crates/rafter" }
rafter-storage = { path = "../rafter/crates/rafter-storage" }
rafter-runtime = { path = "../rafter/crates/rafter-runtime" }
rafter-app = { path = "../rafter/crates/rafter-app" }
rafter-service = { path = "../rafter/crates/rafter-service" }
rafter-transport-tls = { path = "../rafter/crates/rafter-transport-tls" }
```

The main pieces fit together like this:

```text
Your client API
    |
rafter-service: write/read handles and managed driver <--> TLS peer transport
    |
rafter-app: RaftGroup + your state machine
    |
rafter-runtime: durable Raft execution
    |
rafter + rafter-storage: protocol and persisted Raft state
```

### 1. Learn the write and read API

Start with the small managed KV example:

```sh
cargo run -p rafter-service --example replicated_kv_service
```

It writes `alpha=one`, reads it with linearizable consistency, prints metrics,
and shuts down. The client-facing calls look like this inside an async function,
after obtaining a `RaftHandle` from the driver:

```rust
let write = raft.write(("alpha".to_owned(), "one".to_owned())).await?;
let read = raft
    .read("alpha".to_owned(), rafter_service::ReadConsistency::Linearizable)
    .await?;
assert_eq!(read.result, Some("one".to_owned()));
```

This example uses in-memory storage and delivery so you can see the API with
little setup. It does not yet use TLS or survive a process restart.

Your KV type implements `ReplicatedStateMachine`: encode/decode commands, apply
committed batches, answer queries, and report its applied index. See the
[complete KV implementation](../crates/rafter-service/examples/replicated_kv_service.rs).

For durable state, save the application data and its **applied index** together.
That index says “these commands are already reflected in the saved data.” Saving
it ahead of the data could cause recovery to skip a command whose effects were
lost. Rafter persists its log; your application defines how to persist its own
state. The [KV state file example](../crates/rafter-runtime/examples/replicated_kv/app_state.rs)
shows one simple way to store the map and index as one durable record.

### 2. Run a complete composition with TLS and recovery

The repository's fenced-lock reference application combines the supplied layers
with durable application storage and an actual service loop. It manages locks
instead of KV pairs, but its startup, transport, and recovery wiring show how to
assemble the stack for your own state machine.

Run its local process suite:

```sh
scripts/reference-source-check -- \
  -p rafter-reference-fenced-lock --test process_production \
  -- --ignored --test-threads=1
```

This command also checks formatting, lints, and builds the reference workspace's
docs. It deliberately patches the reference dependencies to this checkout;
running Cargo directly inside `reference/` would instead resolve its published
dependencies. The first build takes longer than the small examples above.

The suite starts local replica processes using dedicated test certificates and
temporary directories. Its five tests cover authenticated peer connections,
committed lock operations and reads, restart recovery, membership replacement,
and connection limits. Successful output includes `5 passed` and
`reference source mode passed`. The harness stops its processes and cleans up
the temporary data.

In the first test, a client opens a session, acquires a lock, and queries the
current lock token. In the recovery test, a replica restarts from its durable
state; missing or corrupt recovery metadata prevents it from serving requests.
Read the [scenarios](../reference/fenced-lock/tests/process_production.rs) beside
the [process harness](../reference/fenced-lock/tests/support/production_process.rs)
to follow the requests.

The test certificates are for local exercises. The supplied TLS crate protects
**peer traffic**; the reference application's small client listener is separate
and needs its own access and transport policy in a deployment.

### 3. Assemble your application's startup

Use the [reference process](../reference/fenced-lock/src/bin/lock-production-node/main.rs)
and its [replica setup](../reference/fenced-lock/src/bin/lock-node/replica.rs) as
the worked recipe:

1. Configure stable replica identities, group membership, peer addresses, TLS
   credentials, and resource limits. Build the TLS runtime paused, so workers
   cannot start exchanging messages before recovery is ready.
2. Acquire exclusive ownership of the replica's directory. Open the file-backed
   Raft stores and your application store; read the application's durable
   applied index.
3. Recover `DurableRaftNode` through that index and construct `RaftGroup` at the
   same index. Retain the recovery outputs for the managed driver.
4. Construct the driver with the group, recovery outputs, TLS sender and peer
   validator, and restored control-plane checkpoint (membership and retired
   identities). This lets it handle recovered snapshots, entries, and outgoing
   work in order. Persist checkpoint changes and reopen the durable TLS session
   state before starting workers.
5. Start transport workers and drive ticks, inbound authenticated messages, and
   pending read barriers. Serve client requests only after recovery is complete
   and the application has applied all known committed application commands.

The [TLS adapter](../reference/fenced-lock/src/bin/lock-production-node/peer_link/mod.rs)
shows how `TlsPeerTransport` takes credentials, a certificate-to-peer mapping,
a per-group peer directory, an endpoint book, and a durable session store. Its
sender implements the service transport trait; its inbound queue supplies
authenticated envelopes to the driver.

The service manages request tracking and read/write results, while your event
loop keeps the group progressing. Async handles do not replace that loop. In
particular, the reference calls `tick`, `deliver`, and `drive_reads`; omitting
read progress can leave a linearizable query waiting indefinitely.

On restart, reopen the same stores and identity. Keep application data, its
applied index, Raft files, the service checkpoint, and TLS session metadata as
part of the recovery design. For readiness, compare the application's applied
index with the **committed application index**: Raft also commits internal
entries that never become application commands.

### What remains application-specific

You define your commands, queries, durable state, and snapshot format. If you
use snapshots over TLS, wire a `SnapshotChunkResolver` into the transport and
implement application snapshot support; a TLS connection alone does not supply
the snapshot bytes. See [snapshot transport](../crates/rafter-transport-tls/README.md#nonblocking-service-boundary).

You also choose certificate issuance and rotation, peer discovery, membership
operations, client API and retry behavior, and deployment. Rafter provides the
building blocks those decisions use. The
[full composition contract](./reference-consumers.md#production-composition)
explains the reference application's choices in more detail.

## Add or replace layers as needed

| If you need… | Use… |
| --- | --- |
| Your own network protocol | The service transport and authenticated-envelope traits instead of the TLS implementation. |
| Your own storage engine | The storage traits with `rafter-runtime`. |
| Direct embedded calls without managed handles | `rafter-app` and its `RaftGroup`. |
| Many Raft groups in one process | `rafter-multiraft`, after you have a working single-group composition. |

Start with one group and a small command. Get a write, a linearizable read, and
recovery working before adding sharding or membership automation. Continue with
the [architecture guide](./architecture.md), [crate map](../README.md#crates),
or [reference consumers](./reference-consumers.md) when you need the deeper
contracts.
