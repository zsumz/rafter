# rafter-sim

Deterministic simulation and model-checking harness for Rafter.

`rafter-sim` drives clusters of `rafter::Node` values through explicit ticks,
message delivery, drops, delays, duplicates, partitions, restarts, snapshots,
and reads. It records applied entries and snapshot installs so tests and model
checks can assert protocol invariants over whole schedules.

Use this crate for scenario tests, replay, bounded exploration, and failure
schedule hardening.

## Bounded Explorers

Bounded model-checking summaries report raw recursive visits as
`explored_states`, distinct canonical states as `unique_states`, and raw action
expansions as `explored_actions`. The DFS pruning key is depth-aware: the same
canonical state is re-expanded if a shorter path reaches it with more depth
remaining, so `Bounds::new(depth)` is exhaustive to that action depth subject to
the configured unique-state and wall-clock caps. Use
`Bounds::with_max_unique_states` for a unique-state cap and
`Bounds::with_max_wall_clock` for a wall-clock cap on larger exploratory runs.

## Soak Seeds

`rafter-model-check-fast` prints every randomized soak seed before and after it
runs a seed. The scheduled `raft-nightly` and `raft-weekly` profiles generate
fresh soak seeds by default:

```sh
cargo run -p rafter-sim --bin rafter-model-check-fast -- --profile raft-nightly
```

Replay a failure by passing the printed seed, or a comma-separated seed list:

```sh
cargo run -p rafter-sim --bin rafter-model-check-fast -- --profile raft-nightly --seed 0x1234
scripts/raft-burn-in nightly --seed 0x1234,0x5678
```

Each soak seed runs against the minimal protocol cluster, a lease-enabled
production cluster, and the membership workload. `raft-soak` and `raft-deep`
keep curated regression seeds by default, including when the nightly workflow
runs them as fixed-seed regression legs. `raft-nightly` and `raft-weekly` use
fresh seed batches unless `--seed` is provided.
