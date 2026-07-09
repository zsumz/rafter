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

Rafter is built as a stack of small crates:

```txt
rafter              deterministic Raft kernel
rafter-runtime-api  persist-before-output runtime trait boundary
rafter-storage      file-backed and in-memory durable stores
rafter-runtime      persist-before-output durable node wrapper
rafter-app          synchronous embedded state-machine layer
rafter-service      async managed handle and transport traits
rafter-multiraft    many-group host for sharded systems
rafter-codec        versioned peer-message wire format
rafter-sim          deterministic simulation and model checking
```

Use the lower crates when you want full control over storage, networking,
authorization, scheduling, and recovery. Use the higher crates when you want
more application-facing structure while keeping those boundaries explicit.

## API Layers

| If you are... | Start with | This layer owns | This layer does not own |
| --- | --- | --- | --- |
| Writing a simulator, custom runtime, or raw protocol integration | `rafter` | deterministic Raft state transitions and explicit outputs | storage, networking, durability fences, authentication, or app-state apply |
| Defining or using a persist-before-output runtime boundary | `rafter-runtime-api` / `rafter-runtime` | the runtime trait contract and a durable node implementation | application state, transport delivery, auth, or recovery policy above Raft storage |
| Building a database or shard-group state machine | `rafter-app` | synchronous proposal, read, membership, apply, poison, and report orchestration | concrete runtime/storage crates, networking, auth, or app snapshot format |
| Wanting a managed async app-facing handle | `rafter-service` | async handles, managed driver shape, watch/membership helpers, and transport contracts | production transport implementation, durable app state, or cluster identity management |
| Hosting many caller-defined groups per process | `rafter-multiraft` | many-group dispatch helpers and typed group routing | group identity allocation, network routing, concrete runtime/storage, or app semantics |

## Example

The core crate is driven by explicit inputs and returns explicit outputs:

```rust
use rafter::{Input, Node, NodeConfig, NodeId, Output};

let config = NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 10)
    .expect("valid raft config");
let mut node = Node::new(config);

for output in node.step(Input::Tick) {
    match output {
        Output::Send { to, message } => {
            // Route this message through your transport.
            let _ = (to, message);
        }
        Output::Apply { index, payload, .. } => {
            // Apply a committed command to your state machine.
            let _ = (index, payload);
        }
        _ => {}
    }
}
```

For durable embeddings, start with `rafter-runtime` and `rafter-storage`.
For application-facing groups, see `rafter-app`, `rafter-service`, and
`rafter-multiraft`.

## Crates

| crate | purpose |
| --- | --- |
| [`rafter`](./crates/rafter/README.md) | deterministic Raft protocol core |
| [`rafter-runtime-api`](./crates/rafter-runtime-api/README.md) | persist-before-output runtime trait boundary |
| [`rafter-storage`](./crates/rafter-storage/README.md) | durable hard-state, log, and snapshot stores |
| [`rafter-runtime`](./crates/rafter-runtime/README.md) | persist-before-output runtime wrapper and group commit |
| [`rafter-codec`](./crates/rafter-codec/README.md) | versioned peer-message codec |
| [`rafter-app`](./crates/rafter-app/README.md) | synchronous embedded replicated-state-machine layer |
| [`rafter-service`](./crates/rafter-service/README.md) | async managed handle and integration traits |
| [`rafter-multiraft`](./crates/rafter-multiraft/README.md) | many-group host for sharded systems |
| [`rafter-sim`](./crates/rafter-sim/README.md) | workspace-only deterministic simulation, replay, and model checking |
| [`rafter-transport-tcp-insecure`](./crates/rafter-transport-tcp-insecure/README.md) | insecure demo-only TCP frame helper for examples and tests |
| [`rafter-maelstrom`](./crates/rafter-maelstrom/README.md) | publish-disabled Maelstrom linearizable KV test node |


## Testing

```sh
cargo test --workspace
cargo test -p rafter-sim
cargo run --release -p rafter-sim --bin rafter-model-check-fast
scripts/maelstrom-lin-kv
```

The repository also carries fuzz seeds, TLA+ specs, Maelstrom workloads, and a
simulation harness that can replay and explore bounded failure schedules.

## Benchmarks

`bench-compare/` compares the in-memory protocol path against raft-rs and
openraft using the same three-node workloads. `rafter-bench-cluster` measures
Rafter's file-backed durable path, including group commit and snapshot
transfer. These are separate claims; compare durable results across
libraries only when the storage and fsync boundaries match.

```sh
scripts/bench-compare.sh
cargo run --release -p rafter-runtime --bin rafter-bench-cluster
```

Benchmark numbers are hardware-sensitive. Treat the checked-in methodology and
raw JSON as evidence, not a permanent scoreboard.

## Production Boundary

Rafter is not a database, a transport security layer, or a complete server.
Production embeddings still own:

```txt
application state durability
applied-index recovery policy
authenticated peer identity
removed-peer fencing
transport encryption and replay protection
application snapshot validation
```

The crates keep those responsibilities visible instead of hiding them behind a
global runtime.

## Status

Rafter is pre-1.0. The core invariants, durable formats, simulation coverage,
and Maelstrom tests are treated seriously, but APIs are still expected to move
while the embedding surface settles.

## License

Copyright 2026 zsumz.

Licensed under Apache-2.0. See [LICENSE](./LICENSE) and [NOTICE](./NOTICE).
