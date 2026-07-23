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

Use the lower crates when you want full control. Use the higher crates when you
want application structure without surrendering storage, transport, scheduling,
identity, or recovery policy.

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

let config = NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 10)
    .expect("valid raft config");
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
| Durable node | [`rafter-runtime`](./crates/rafter-runtime/README.md) + [`rafter-storage`](./crates/rafter-storage/README.md) |
| Embedded state machine | [`rafter-app`](./crates/rafter-app/README.md) |
| Async managed handle | [`rafter-service`](./crates/rafter-service/README.md) |
| Many Raft groups | [`rafter-multiraft`](./crates/rafter-multiraft/README.md) |
| Simulation | [`rafter-sim`](./crates/rafter-sim/README.md) |

## Testing

```sh
cargo test --workspace
cargo test -p rafter-sim
cargo run --release -p rafter-sim --bin rafter-model-check-fast
cargo run --locked -p rafter-invariants -- run-all --profile pr
scripts/maelstrom-lin-kv
```

The repository also carries fuzz seeds, TLA+ specs, Maelstrom workloads, and a
simulation harness that can replay and explore bounded failure schedules.
The Raft verification contract lives in
[`docs/raft-invariants.md`](./docs/raft-invariants.md), generated from the
machine-readable catalog at
[`verification/raft-invariants.yaml`](./verification/raft-invariants.yaml).
The model-check profiles, state-count semantics, and reproducible overhead
measurement procedure are documented in
[`docs/model-checking.md`](./docs/model-checking.md).
`run-all` loads one immutable execution plan, runs every required layer, and
aggregates only the evidence produced by that invocation. `check` is the
separate aggregation-only command for existing result bundles. The
production `run` and `run-all` evidence subprocesses require Linux
descriptor-bound executable launch and fail closed on other operating systems.
The macOS CI lane exercises launcher mechanics under test-only fallback; it
does not produce accepted invariant evidence. The
deterministic PR aggregate emits exactly one verdict for each of the 44
reviewed IDs. Branch protection on `main` requires the stable `invariants-pr`
job; missing, malformed, incomplete, or stale evidence makes that job red.
Evidence artifacts are isolated by workflow run attempt. After a partial
GitHub Actions rerun, rerun every invariant evidence job together; a lone
aggregate rerun intentionally reports missing evidence instead of reusing a
prior attempt.
Maelstrom supplies sampled end-to-end evidence in nightly and weekly profiles
and is intentionally excluded from the deterministic PR verdict. Scheduled
`invariants-nightly` and `invariants-weekly` jobs run every required layer,
render the same 44-row report, and remain red on missing evidence or exhausted
coverage budgets.

## Benchmarks

Three-node in-memory protocol benchmark, 512-byte payloads, aarch64 Linux.
Lower latency is better; higher throughput is better.

| Library | Serial props/s | Serial p99 us | Pipelined props/s | Pipelined p99 us |
| --- | ---: | ---: | ---: | ---: |
| `rafter` | 777,013 | 3.9 | 1,988,986 | 142.3 |
| `raft-rs` | 379,723 | 7.4 | 653,298 | 234.3 |
| `openraft` | 111,254 | 19.2 | 540,752 | 172.3 |

Results come from [`bench-compare/results/latest.json`](./bench-compare/results/latest.json).
Run them locally with:

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

Rafter is pre-1.0. The core invariants, durable formats, simulation coverage,
and Maelstrom tests are treated seriously; APIs are still expected to move.

## License

Licensed under Apache-2.0. See [LICENSE](./LICENSE) and [NOTICE](./NOTICE).
