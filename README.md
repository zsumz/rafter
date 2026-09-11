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

`rafter-runtime` enforces this persist-before-output boundary. You supply the
transport, application state machine, and scheduling policy. See the
[architecture guide](./docs/architecture.md) for the runtime contract and recovery.

## API Layers

Choose the layer that matches how much of the integration you want Rafter to provide.

| Layer | Reach for it when |
| --- | --- |
| `rafter` | You want the deterministic protocol kernel and explicit outputs. |
| `rafter-runtime` | You want a durable persist-before-output node. |
| `rafter-app` | You want an embedded replicated state machine. |
| `rafter-service` | You want async handles and transport traits. |
| `rafter-multiraft` | You want many caller-defined Raft groups in one host. |

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

| Added loopback egress | Rafter pipeline | OpenRaft | Ratio | Rafter p99 at 1,000/s | OpenRaft p99 at 1,000/s |
| --- | ---: | ---: | ---: | ---: | ---: |
| 0 ms | **10,314 writes/s** | 3,744 writes/s | **2.75x** | **2.687 ms** | 18.874 ms |
| 2 ms | **5,532 writes/s** | 2,484 writes/s | **2.23x** | **11.141 ms** | 18.874 ms |

Conditions: 3 nodes on one host, 64 clients, 512-byte writes, and medians of
3 repetitions. Added delay affects both client and peer egress. The OpenRaft
arm is the tested synchronous-storage integration. At no-delay saturation,
Rafter processed 2.75x as many writes but had a higher p99 (23.069 ms versus
19.923 ms) and p99.9 (29.884 ms versus 21.758 ms). Fixed-load and saturation
latency are therefore reported separately.

The [immutable report and source cases](https://github.com/zraftz/benchmarks/tree/main/reports/qualified-34628543562)
contain the methodology, individual runs, accounting, and known limitations.
These are implementation and integration results, not a claim about a faster
Raft algorithm or every workload. A completion-aligned seven-repeat in-memory
comparison and a stronger asynchronous OpenRaft storage control are being
qualified separately.

Reproduce the in-memory comparison or measure Rafter's durable runtime with:

```sh
scripts/bench-compare.sh
cargo run --release -p rafter-runtime --bin rafter-bench-cluster
```

The comparison harness measures the in-memory implementation path. The
`rafter-bench-cluster` binary measures Rafter's durable runtime path, including
file-backed storage and group commit. The optional pipelined runtime overlaps
eligible leader replication with local persistence while retaining the durable
output boundary. [`PersistenceWorker`](./crates/rafter-runtime/README.md) offers
bounded persistence execution; application durability and applied-index recovery
remain the embedding's responsibility.

The opt-in shared WAL uses checkpoint-selected generation segments to reclaim
compacted physical history and bound Raft replay work. That bound does not cover
application-state or snapshot retention, which remain separate policies.

## Boundaries

Rafter is not a database, a transport security layer, or a server framework.
Production embeddings still own:

```txt
application state durability
applied-index recovery
peer identity and authorization
removed-peer fencing
transport encryption
snapshot validation
```

## Status

Rafter is pre-1.0. APIs and durable formats remain alpha. See the
[changelog](./CHANGELOG.md) for recent work and [release guide](./RELEASE.md)
for published versions and compatibility boundaries.

## License

Licensed under Apache-2.0. See [LICENSE](./LICENSE) and [NOTICE](./NOTICE).
