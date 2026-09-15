<p align="center">
  <img src="./rafter-logo.svg" alt="rafter" width="720">
</p>

<p align="center">
  <strong>A deterministic Raft stack for systems that own their runtime.</strong>
</p>

<p align="center">
  Rafter gives you a sans-IO protocol core, durable storage/runtime layers,
  simulation tools, and small embedding crates without taking over your
  transport, task model, or application state machine.
</p>

<p align="center">
  <a href="#model">Model</a>
  <span> · </span>
  <a href="#api-layers">API Layers</a>
  <span> · </span>
  <a href="#example">Example</a>
  <span> · </span>
  <a href="#crates">Crates</a>
  <span> · </span>
  <a href="#reference-consumers">Reference Consumers</a>
  <span> · </span>
  <a href="#testing">Testing</a>
  <span> · </span>
  <a href="#benchmarks">Benchmarks</a>
</p>

<br />

## Model

The kernel turns ticks, peer messages, and client requests into explicit outputs.
It performs no IO. A durable embedding handles each step in this order:

```txt
input -> Raft step -> persist required changes -> send messages / apply entries
```

`rafter-runtime` enforces this persist-before-output boundary. Pair it with the
file-backed stores in `rafter-storage` or implement the storage traits yourself.
Your application defines how committed entries affect its state and how that
state is recovered. See the [architecture guide](./docs/architecture.md) for the
runtime contract and recovery.

## API Layers

Start with the kernel for full control, or choose a higher layer for more
integration. Follow the [getting started guide](./docs/getting-started.md) for
a step-by-step walkthrough.

| Layer | Reach for it when |
| --- | --- |
| `rafter` | You want the protocol kernel and will supply storage, transport, and execution. |
| `rafter-runtime` | You want a durable persist-before-output node. |
| `rafter-app` | You want an embedded replicated state machine. |
| `rafter-service` | You want async handles and transport traits. |
| `rafter-multiraft` | You want many caller-defined Raft groups in one host. |

Storage and transport are separate choices. `rafter-storage` provides file-backed
stores and traits for custom backends. `rafter-transport-tls` provides mutually
authenticated peer connections through the service layer, whose transport traits
also support your own implementation.

## Example

```rust
use rafter::{Input, Node, NodeConfig, NodeId, Output};

let config = NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 10).expect("valid raft config");
let mut node = Node::new(config);

for output in node.step(Input::Tick) {
    match output {
        Output::Send { to, message } => {
            // route through your transport
            let _ = (to, message);
        }
        Output::Apply { index, payload, .. } => {
            // apply to your state machine
            let _ = (index, payload);
        }
        _ => {}
    }
}
```

## Crates

| Need | Crate |
| --- | --- |
| Protocol kernel | [`rafter`](./crates/rafter/README.md) |
| Durable node | [`rafter-runtime-api`](./crates/rafter-runtime-api/README.md), [`rafter-storage`](./crates/rafter-storage/README.md), [`rafter-runtime`](./crates/rafter-runtime/README.md) |
| Embedded state machine | [`rafter-app`](./crates/rafter-app/README.md) |
| Async managed handle | [`rafter-service`](./crates/rafter-service/README.md) |
| Many Raft groups | [`rafter-multiraft`](./crates/rafter-multiraft/README.md) |
| Wire format and integrity | [`rafter-codec`](./crates/rafter-codec/README.md), [`rafter-crc32`](./crates/rafter-crc32/README.md) |
| Peer transport | [`rafter-transport-tls`](./crates/rafter-transport-tls/README.md), [`rafter-transport-tcp-insecure`](./crates/rafter-transport-tcp-insecure/README.md) (development only) |
| Simulation and workload testing | [`rafter-sim`](./crates/rafter-sim/README.md), [`rafter-maelstrom`](./crates/rafter-maelstrom/README.md) |
| Repository verification | `rafter-invariants`, `rafter-invariant-test`, `rafter-invariant-test-macros` — [verification guide](./docs/raft-invariants.md) |

## Reference Consumers

Three independent applications exercise Rafter through its public APIs:

- **Replicated ledger:** application durability and recovery.
- **Fenced lock service:** linearizable authority and authenticated composition.
- **Sharded counter:** managed scheduling across 64, 1,024, and 4,096 groups.

Each runs against both checkout sources and exact package archives, with durable
process coverage. See [reference consumers](./docs/reference-consumers.md) for
the contracts and [work completion](./docs/work-completion.md) for the proof map.

## Testing

```sh
rustup toolchain install 1.97.1 --profile minimal
cargo +1.97.1 install zcheck --version 0.0.1 --locked
cargo +1.97.1 install zrail --version 0.0.3-rc.8 --locked
zcheck
```

The install toolchain matches CI. Rafter itself is qualified on Rust 1.88.

[zcheck](https://github.com/zsumz/zcheck) runs formatting, lints, documentation,
architecture checks, and workspace tests, with logs and a receipt for each run.
Use `zcheck plan check` to inspect the tasks or `zcheck run full` for deeper lanes.

[zrail](https://github.com/zsumz/zrail) checks the reviewed architecture in
[`zrail.toml`](./zrail.toml): crate layers, sans-IO boundaries, capability and
mutation owners, macro allowances, module docs, sibling tests, and file-size
ratchets. CI checks the same locked contract on every pull request.

Simulation, TLA+, Maelstrom, and the reference applications provide the runtime
evidence. See [development and checks](./docs/development.md) for commands,
platform requirements, and the Rafter-specific guards that remain alongside zrail.

## Benchmarks

The current qualified service comparison measures client requests over TCP,
three-node replication, durable consensus storage, durable application updates,
and responses sent only after application durability.

| Added loopback egress | Rafter pipeline + completion priority | OpenRaft | Ratio | Rafter p99 at 1,000/s | OpenRaft p99 at 1,000/s |
| --- | ---: | ---: | ---: | ---: | ---: |
| 0 ms | **10,263 writes/s** | 2,590 writes/s | **3.96x** | **4.063 ms** | 9.568 ms |
| 2 ms | **5,042 writes/s** | 1,827 writes/s | **2.76x** | **12.583 ms** | 24.117 ms |

Conditions: 3 nodes on one host, 64 clients, 512-byte writes, and medians of
3 repetitions. Added delay affects both client and peer egress. The OpenRaft
arm is the tested asynchronous-flusher integration. The selected Rafter arm had
zero errors, unknown outcomes, or unsent requests and beat the synchronous and
asynchronous OpenRaft controls in all 24 displayed p99 and p99.9 comparisons.
Against same-code FIFO Rafter, completion priority improved saturated throughput
29.3% at 0 ms and 5.9% at 2 ms; its small fixed-load tail tradeoffs remain
visible in the report.

The [immutable report and source cases](https://github.com/zraftz/benchmarks/tree/407c3b7685338792935e489fa2b9f5ef498c19da/reports/qualified-34904893568-wal-34907033422)
contain the methodology, individual runs, accounting, and known limitations.
These are implementation and integration results, not a claim about a faster
Raft algorithm or every workload. The report also retains a completion-aligned
seven-repeat in-memory comparison and the separately named asynchronous
OpenRaft storage control.

Reproduce the in-memory comparison or measure Rafter's durable runtime with:

```sh
scripts/bench-compare.sh
cargo run --release -p rafter-runtime --bin rafter-bench-cluster
```

The comparison harness measures the in-memory implementation path. The
`rafter-bench-cluster` binary measures Rafter's durable runtime path, including
file-backed storage and group commit. The optional pipelined runtime overlaps
eligible leader replication with local persistence while retaining the durable
output boundary.
[`ThreadedPipelinedRaftNode`](./crates/rafter-runtime/README.md) provides the
tested one-credit node/worker composition; application durability and
applied-index recovery remain the embedding's responsibility.

The opt-in shared WAL uses checkpoint-selected generation segments to reclaim
compacted physical history and bound Raft replay work. Its default reclaims on
every compaction; an explicit byte threshold can defer physical generation
replacement while keeping each logical compaction durable. The threshold is
checked only at compaction opportunities, and retained live entries are
additional. That bound does not cover
application-state or snapshot retention, which remain separate policies. A
file-backed runtime can apply an explicit `SnapshotRetention` policy through
`DurableRaftNode::prune_snapshot_files`; the reference durable service keeps
only its manifest-selected snapshot after successful compaction and uses
`cleanup_abandoned_snapshot_temporary_files` after recovery to remove only
recognized interrupted-publication residue.
After durable compaction, the opt-in bounded `LogRetirementWorker` can release
the already-retired in-memory log prefix away from the consensus owner; full or
stopped workers return ownership for immediate inline release.
The runtime's opt-in `application::ApplicationWorker` makes the measured
ordered application pattern reusable: bounded entry and byte credits include
queued, executing, and unconsumed results, and durable completions are verified
against the application's own applied floor so the embedding can answer clients
only after consuming that completion.

## Boundaries

The `rafter` kernel stays deterministic and sans-IO. Storage, networking, and
scheduling live in the surrounding layers you choose. Your application supplies
the state machine and operational policy:

- Application state-machine behavior, durable state, and applied-index recovery.
- Application snapshot contents and validation.
- Certificate provisioning and rotation, peer identity mappings, and membership
  policy, including retiring removed peers.
- Service discovery, deployment, and client-facing APIs.

When you use `rafter-transport-tls`, the transport handles encryption, peer
authentication, and enforcement of the configured peer policy. Your application
supplies the credentials and keeps that policy current.

## Status

Rafter is pre-1.0. APIs and durable formats remain alpha. See the
[changelog](./CHANGELOG.md) for recent work and [release guide](./RELEASE.md)
for published versions and compatibility boundaries.

## License

Licensed under Apache-2.0. See [LICENSE](./LICENSE) and [NOTICE](./NOTICE).
