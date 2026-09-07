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

```txt
rafter              pure Raft kernel
rafter-runtime-api  persist-before-output runtime boundary
rafter-storage      hard-state, log, and snapshot stores
rafter-runtime      durable node wrapper
rafter-app          embedded state-machine layer
rafter-service      async handle and transport traits
rafter-multiraft    many-group host
rafter-codec        peer-message wire format
rafter-sim          simulation and model checking
```

Start at the layer that fits your application. Each keeps storage, transport,
scheduling, identity, and recovery policy in your hands. See the
[architecture guide](./docs/architecture.md) for the step loop and the
persist-before-output contract.

## API Layers

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
cargo install zcheck --locked
cargo install zrail --version 0.0.3-rc.8 --locked
zcheck
```

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

Recorded five-run medians: three-node in-memory protocol benchmark, 512-byte
payloads, aarch64 Linux.
Lower latency is better; higher throughput is better.

| Library | Serial props/s | Serial p99 us | Pipelined props/s | Pipelined p99 us |
| --- | ---: | ---: | ---: | ---: |
| `rafter` | 777,013 | 3.9 | 1,988,986 | 142.3 |
| `raft-rs` | 379,723 | 7.4 | 653,298 | 234.3 |
| `openraft` | 111,254 | 19.2 | 540,752 | 172.3 |

Results and per-run measurements are in
[`bench-compare/results/latest.json`](./bench-compare/results/latest.json).
These are hardware-sensitive protocol measurements; commit-latency boundaries
differ across implementations. Reproduce the comparison or measure Rafter's
durable runtime with:

```sh
scripts/bench-compare.sh
cargo run --release -p rafter-runtime --bin rafter-bench-cluster
```

The comparison harness measures the in-memory protocol path. The
`rafter-bench-cluster` binary measures Rafter's durable runtime path, including
file-backed storage and group commit.

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
