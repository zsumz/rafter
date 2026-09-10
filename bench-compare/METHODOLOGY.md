# bench-compare methodology

`bench-compare` is a standalone Cargo package outside the root workspace. It
exists to keep performance evidence reproducible without changing the frozen
workspace lockfile.

Production crates inherit the root workspace `unsafe_code = "forbid"` policy.
The standalone benchmark package is deliberately outside that workspace and has
two allocation-counting binaries that install custom global allocators, which
requires small benchmark-only `unsafe` blocks. Those allocators are measurement
fixtures only; they are not linked into Rafter's library/runtime/service
crates.

The default mode compares Rafter, raft-rs, and openraft on the same in-memory
three-node protocol workloads:

- `serial`: one 512-byte proposal in flight.
- `pipelined`: up to 64 512-byte proposals in flight.
- `large_payload`: application entries just under Rafter's default append byte
  budget.

These workloads are protocol-only evidence. They do not claim durable fsync
parity across libraries. Use `rafter-bench-cluster` for Rafter's file-backed
durable path and compare durable results across implementations only when the
storage and sync boundaries match.

Rafter-only mode adds workload shape checks that do not have direct
cross-library equivalents in this harness:

- `read_index_load`: read-index barriers registered under write load.
- `read_index_batch`: consecutive read-index barriers submitted through one
  deterministic `step_batch`, sharing one confirmation heartbeat round while
  preserving per-read grants.
- `lease_read`: read barriers served from an explicitly enabled leader lease
  after a current-term commit and quorum acknowledgement establish the lease.
  This workload assumes the documented lease-read tick-skew bound: no node's
  tick driver runs more than twice as fast as another's, and the benchmark keeps
  the pre-vote/check-quorum foundation enabled.
- `leader_failover_queued`: queued proposals replicated to a successor before
  old-leader partition and successor election.
- `tracked_write`: service-level `write_batch` preservation through the runtime
  boundary.
- `append_64x512` and `append_1_large`: AppendEntries encode/decode frame
  evidence.
- `round_robin_batches`: many Raft groups stepped in deterministic group order.

The JSON reports include wall-clock throughput, per-proposal commit latency,
and shape counters. `scripts/bench-compare.sh` runs each benchmark binary five
times by default, stores the raw run set under top-level `runs`, and writes
median values into top-level `results`. Each aggregated workload also carries a
`run_summary` with min/median/max wall time and throughput. Use
`BENCH_COMPARE_RUNS=1` only for a quick smoke run, not for checked-in
performance evidence or public claims.

Benchmark binaries are rotated between run positions so one library does not
always get the same warm-up, throttling, or background-load slot. Aggregated
latency percentiles are medians of each run's reported percentile, not
percentiles recomputed from a merged raw-latency distribution. Deterministic
shape counters must be identical across runs; a varying shape counter fails the
aggregation instead of being hidden by a median.

The checked-in artifacts record the exact compiler under the top-level `rustc`
field. The `Benchmarks` workflow pins that same benchmark-evidence compiler so
CI artifacts are comparable with checked-in artifacts. This is separate from
the workspace `rust-version`, which remains the crate compatibility floor.

Run-order rotation reduces positional bias for any run count. It is perfectly
balanced only when the run count is a multiple of the benchmark binary count:
three for full comparison, four for Rafter-only mode.

The deterministic shape counters are the important regression signal for this
performance work:

- `append_messages_per_proposal`
- `append_entries_per_append_message`
- `leader_broadcast_rounds_per_proposal_batch`
- `log_entry_materializations_per_proposal`
- `commit_evaluations_per_committed_entry`
- `runtime_batches_per_write_batch`
- `runtime_batches_per_group_batch`
- `successor_applies_per_queued_proposal`
- `allocations_per_frame`

Timing-derived companion metrics, such as `encoded_mb_per_s`, are reported and
aggregated by median instead of treated as deterministic shape.

`commit_evaluations_per_committed_entry` is measured as successful leader-side
AppendEntries responses whose acknowledged match index advances past the
leader's current commit index. That is the observable response-side point where
the leader runs the commit tracker under `CommitAdvanceRequiresNewEvidence`.
`log_entry_materializations_per_proposal` counts entries materialized into
outgoing AppendEntries messages per proposal; it is a pressure counter for
log-batch effect materialization, not an allocator profile.

`bench-rafter-profile` is a Rafter-only serial profiling helper. It installs a
counting allocator and reports phase-level allocation and timing deltas for the
same one-proposal-in-flight path, so it is intentionally separate from the
comparison scoreboard. Use it when system profilers such as `perf`,
`cargo-flamegraph`, valgrind, or heaptrack are unavailable.

Run:

```sh
scripts/bench-compare.sh
BENCH_COMPARE_MODE=rafter-only scripts/bench-compare.sh
BENCH_COMPARE_RUNS=1 scripts/bench-compare.sh
cargo run --manifest-path bench-compare/Cargo.toml --release --no-default-features --bin bench-rafter-profile
cargo run --release -p rafter-runtime --bin rafter-bench-cluster
```

Full comparison mode depends on the optional raft-rs/openraft dependencies.
With the current raft-rs prost-codec path, the build needs `protoc`. A system
`protoc` on `PATH` or an explicit `PROTOC=/path/to/protoc` is preferred. When
neither is present, `scripts/bench-compare.sh` will use a matching cached
`protobuf-build` protoc binary if Cargo has already fetched one for the host
platform. If no usable protoc is available, use `BENCH_COMPARE_MODE=rafter-only`
for local Rafter evidence and rerun full mode on a machine with protoc.

Benchmark numbers are hardware-sensitive. Treat checked-in reports as evidence
from one machine, not as a permanent scoreboard.

## Sustained durable measurements

`bench-rafter-durable` extends the root workspace's durable smoke scenario with
fixed proposal counts, selectable batch sizes and payloads, workload isolation,
and an explicit scratch parent. It stays in this standalone package so the
performance harness has its own locked build and qualification lane.

```sh
RUSTUP_TOOLCHAIN=1.96.1 zcheck run bench-durable
```

This opt-in task builds the binary and runs `scripts/bench-durable`. By default,
each of four rounds measures 2,048 batches at sizes 1, 8, 32, and 128, followed
in rotating workload order by a 128 MiB snapshot. Every workload gets its own
process and fresh stores. The runner records raw JSON, stderr, exit status,
process user/system CPU time, host identity, binary hashes, execution order,
and Linux `/proc/stat` snapshots. Results are medians of run metrics, including
each run's percentiles; raw samples remain available. Output directories must
be new, and failed or timed-out trials retain their output and execution record.

For a comparison, build both revisions with the same compiler, lockfile,
features, and harness, preserve their binaries, and run:

```sh
scripts/bench-durable --baseline /path/to/baseline --binary /path/to/candidate \
  --output /path/to/new-evidence-directory
```

The two arms alternate A/B then B/A in successive rounds for every workload.
Use `--directory` to select an existing scratch parent on representative
storage. Both arms must use the same filesystem and CPU allocation. Run this
lane separately from builds and correctness tests to avoid competing for the
benchmark host. CPU steal or storage contention still limits attribution.

The proposal report includes one commit-latency sample per proposal and one
batch-completion sample per submitted batch. These are distinct populations:
many proposals share a durability cycle. `phase_ms` separates leader steps
from synchronous message pumping. Snapshot `preparation_ms` measures source
snapshot creation separately from the existing transfer timer, which includes
receiver staging and final promotion. The original no-argument smoke sizes
remain available through either durable binary.

All three nodes share one filesystem and messages are delivered synchronously.
These closed-loop workloads measure protocol and storage work; they do not
model network latency, arrival queues, or independently provisioned disks.
Trace synchronization calls separately from timed evidence, since syscall
tracing changes execution time. A batch-size sweep is not a claim about a
production batching policy or service-level tail latency.

### Hard-state backend comparison

The durable benchmark defaults to `--hard-state replace`. To compare RFHJ
journaling with replacement in the same binary on fresh stores:

```sh
python3 scripts/bench-durable \
  --binary bench-compare/target/release/bench-rafter-durable \
  --baseline bench-compare/target/release/bench-rafter-durable \
  --hard-state journal --baseline-hard-state replace \
  --runs 3 --batches 1000 --directory /path/to/scratch
```

The Hard-state comparison workflow records paired runs and separate syscall
traces. `bench-hard-state` isolates publication; `scripts/check-hard-state-syscalls`
checks one data sync per write, with zero journal renames or directory syncs
inside the measured region. The journal is opt-in and has no compaction yet.

## CI usage

The `Benchmarks` workflow runs a one-run Rafter-only smoke check on pull
requests and uploads the JSON artifact. It also runs median-of-five Rafter-only
and full comparison jobs on `main`, on schedule, and by manual dispatch. PR
smoke gates on successful benchmark execution. Evidence jobs also gate on
deterministic shape-counter consistency. CI does not gate on absolute
throughput; it preserves the raw run artifacts for review.
