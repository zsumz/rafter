# Release readiness — 2026-10-10

Assessed source: `021f4f9bb2f6960cb75b24f5a627f1872a12d430` (`origin/main`).
Remote branches, pull requests, and workflow results were refreshed on October
10, 2026. The local build changes add a Rust 1.88.0 toolchain pin and setup
instructions; they do not change consensus behavior or declare a release.
CI's shared setup action explicitly selects each job's requested toolchain so
the local pin does not override newer tool-installation, packaging, or fuzz jobs.
The initial build slice's `zrail.lock` update changed only that setup action's
input digest. Subsequent qualification changes are described below. Changes to
these gate inputs require the repository's architecture environment review when
submitted as a pull request; a candidate lock refresh does not grant that review.

Rafter has a strong foundation: a dependency-free deterministic kernel,
persist-before-output runtime, bounded persistence and application workers,
authenticated transport, independent reference consumers, format vectors,
simulation, and TLA+ evidence. The highest-value next work is reproducible
release qualification, executable upgrade contracts, and a smaller integration
burden for durable applications. More throughput work should follow those.

## Build and qualification

The initial local blocker was missing Rust, rather than a compilation error in
Rafter. Rust 1.88.0 now builds the locked workspace and all its targets on Apple
Silicon macOS. `rust-toolchain.toml` selects that compiler, rustfmt, and Clippy
even when a developer's global default is newer. The developer tools retain
their separate Rust 1.97.1 installation requirement.

| Local check | Result |
| --- | --- |
| `cargo build --locked --workspace --all-targets` | Passed on Rust 1.88.0, `aarch64-apple-darwin`. |
| `cargo test --locked --workspace` | Passed: 3,082 tests, zero failures, 58 ignored, including documentation tests. |
| `zcheck run lint --jobs 1` | Passed: 13 tasks, including macOS Clippy, fuzz and Rafter-only benchmark Clippy, strict rustdoc, transport memory, telemetry, and policy checks. The Linux-only Clippy task is replaced by the macOS task. |
| `zrail check` | Passed: 2,026 Rust files analyzed, zero errors, warnings, or unresolved contexts; after the cache initialization described below. |
| `scripts/reference-source-check` | Passed: 480 tests, 56 ignored; formatting, Clippy, and strict documentation also passed. |
| CI compiler selection | Shell checks preserve requested `1.88`, `1.97.1`, and `nightly`; an explicit Rust 1.97.1 environment overrides the local pin. |

Run logs are retained under `target/release-readiness/` in this checkout;
`lint-receipt.json` is the local lint graph's machine-readable receipt.
The Linux-only invariant executor emits dead-code warnings on macOS; the
repository already excludes it from the macOS Clippy lane.

One local qualification issue was reproduced: verifier tests created
`crates/rafter-invariants/target/` without `CACHEDIR.TAG`, and a subsequent zrail
check rejected that excluded directory with `REP-006`. The release branch now
routes those fixture paths through shared, descriptor-confined cache
initialization. It publishes the standard marker atomically, preserves existing
valid markers, and refuses malformed or symlinked markers without overwriting
them. The exclusion guard remains unchanged. All 892 invariant-library tests
passed, and separate fresh-directory checks cover creation, preservation,
truncated/invalid signatures, and symlink refusal. This is separate from
compilation and from the hosted CI failures below.

The build is not equivalent to release qualification. The authoritative Linux
invariant, process, exact-package, and MSRV lanes remain necessary.
This local run does not execute the full Linux invariant aggregate, Maelstrom,
long-duration burn-in, or exact-package process/MSRV suites. Hosted results are
identified separately below; the reference source lane leaves its ignored
process and scale profiles for their dedicated runners.

## Remote branches

`main` and `origin/main` were identical at inspection. The newest main commit
is September 22's qualified-performance documentation update.

| Branch | Finding | Recommended action |
| --- | --- | --- |
| `docs/layered-stack-readme` (`3790e23a`) | The only open PR, [#13](https://github.com/zsumz/rafter/pull/13); six unique commits and four main commits behind its merge base. Adds a standalone TLS counter, guide, and smoke check. | Update onto current main, review the example against the durable lifecycle contracts, and resolve its failing gates before merging. |
| `perf/raft-performance` (`66a7d719`) | [#19](https://github.com/zsumz/rafter/pull/19) and [#20](https://github.com/zsumz/rafter/pull/20) are merged. Its tip tree exactly matches merged main commit `ae951379`; raw ahead counts reflect separate history. | Start new performance work from main; do not merge the historical branch again. |
| `fix/nightly-coverage-contract` / `docs/sync-qualified-performance` | [#21](https://github.com/zsumz/rafter/pull/21) / [#22](https://github.com/zsumz/rafter/pull/22) are merged. | Treat them as historical branches. |
| `codex/snapshot-log-recovery-main` / `refactor/protocol-reading-paths` | [#11](https://github.com/zsumz/rafter/pull/11) / [#12](https://github.com/zsumz/rafter/pull/12) are merged and their tips are ancestors of main. | Keep the recovered snapshot/configuration behavior and walkthroughs already on main. |
| `actions-improvements`, `docs/zrail-readme-main`, `fix/main-review-findings` | Tips are ancestors of main. | No newer work to integrate. |
| `runtime-correctness`, `tla-improvements`, `zrail`, `fix/snapshot-log-recovery` | Older divergent development histories; none has an open PR. Runtime/TLA PRs #6/#5 are merged, and main contains later zrail and snapshot work. | Use them for history or a targeted comparison, not as replacement release bases. |

PR #13 was mergeable but behind. Its lint, test, source/package/MSRV, and
individual invariant jobs passed in the reported run; `invariants-pr`,
`zrail-policy-review`, and `zrail` failed. The architecture environment review
is a maintainer gate, not something a lockfile refresh can satisfy.

## Prioritized code work

### 1. Make candidate qualification reproducible

**Release blocker.** The latest main
[CI run](https://github.com/zsumz/rafter/actions/runs/35743978089) failed its
aggregate: **0/44 green**, with every invariant carrying the harness error
`tla process runtime identity changed for perl`. Thirteen required evidence
results were also reported missing. Individual producer jobs succeeded. This
is an evidence-authentication failure; it is not evidence of 44 protocol
violations.

[`verify_runtime_identities`](../crates/rafter-invariants/src/verification/source/policy.rs)
compares the producer's executable path and SHA-256 with the aggregate job's
local runtime. Ubuntu's moving runner image is not an immutable runtime
contract. The release branch now installs exact Bash, Perl, procps, and time
packages from a [signed Ubuntu snapshot](https://snapshot.ubuntu.com/) in every
Linux evidence producer and aggregate. Maelstrom jobs also normalize Graphviz
and Gnuplot after the upstream setup action. Rust and Java retain their existing
exact toolchain selections. The shared action preserves ordinary unprivileged
execution and the verifier's exact identity validation; it does not claim to pin
the host kernel or every system library. A digest-pinned complete environment
remains a broader reproducibility option. Full hosted qualification is still
required; do not assume fixing Perl makes the complete aggregate green.

The October 4
[nightly failure](https://github.com/zsumz/rafter/actions/runs/37197270717)
was `a_stored_control_plane_failure_refuses_the_requests_queued_behind_it` in
[`process_control_plane.rs`](../reference/fenced-lock/tests/process_control_plane.rs).
It failed because no request was observed behind the breaking query. Separate
socket reader threads can admit all local reads first. Add a deterministic
admission barrier to the test harness so it establishes the ordering it checks;
retain the assertion that later work is refused. A longer sleep or deleting the
non-vacuity assertion would not establish that behavior.

The release branch now supplies bounded, atomic batch admission through the
existing control-plane fault seam. The regression admits the breaking query and
eight later reads together, checks the admission acknowledgement, and rejects
every successful reply from that batch. Ordinary connections reject the harness
command. Indexed replies preserve independent completion and flush accounting.
Validation: 24 concurrent regression runs, all seven control-plane process tests,
and the complete reference source checks passed. Reintroducing the original
per-pass fail-open condition makes the regression fail explicitly on a served
local read, rather than relying on a timeout.

The failed PR-tier logs confirm runner drift: the TLA producer used Ubuntu image
`20260920.314.1`, while its aggregate used `20260907.300.1`. All thirteen missing
evidence rows belong to the rejected TLA layer. Runtime qualification remains
open until the candidate's producer and aggregate receipts authenticate together.

**Acceptance:** all required jobs pass in one attempt at the candidate SHA;
all 44 verdicts have current evidence; the process failure test repeatedly
establishes its fault ordering under load. Rerun every invariant producer and
the aggregate together. The newer
[October 9 nightly](https://github.com/zsumz/rafter/actions/runs/37910932557)
and [October 10 weekly](https://github.com/zsumz/rafter/actions/runs/38033562579)
are green at the same source SHA, but do not repair the failed PR-tier receipt.

### 2. Turn compatibility into an executable contract

**Required before promising stability.** The eleven public crates still declare
`0.0.2-alpha.1`; [RELEASE.md](../RELEASE.md) explicitly leaves API, wire, storage,
and operational contracts alpha. Current format vectors prove byte grammars,
not upgrades between released binaries.

Start with an explicit supported-version matrix. Add archived-package tests
that write with the previous release and recover with the candidate, plus a
three-node mixed-version rolling restart exercising writes, linearizable
reads, membership changes, and snapshot catch-up. Unsupported combinations
must refuse cleanly before mutating durable state. Test downgrade only where
it is promised; otherwise document and enforce the refusal.

Choose the supported storage modes before freezing them. The
[storage contract](../crates/rafter-storage/STORAGE_FORMAT_V1.md) has independent
RFHS, RFHJ, and shared WAL layouts with no implicit migration. A migration tool
needs interruption/restart tests and a recoverable publication boundary.
Retain the current format vectors and exact-archive consumer lanes; add a
public API baseline that detects unintended breaking changes.

The release branch now adds an initial
[archive-based file-store matrix](storage-upgrades.md). It compiles identical
public-API fixtures against checksum-pinned published `0.0.2-alpha.1` archives
and the candidate's exact archives. Log, installed-snapshot, and interrupted
snapshot-transfer cases recover, continue writing, and reopen. A validly
checksummed future hard-state version must be refused without durable mutation.
The deterministic and MSRV package lanes run this matrix and retain evidence;
it does not qualify journal/shared WAL migration, downgrades, or mixed-version
wire behavior.

The [peer-codec matrix](wire-compatibility.md) now adds 22 cases covering every
encodable top-level message, stable/joint membership, snapshot chunks and wide
replication sequence numbers. Both archive-built binaries decode each other's
frames to exact expected values and retain identical canonical bytes. Both
refuse seven malformed inputs and the unsupported whole-snapshot form. This
is codec evidence; a mixed-version cluster test is still required.

**Acceptance:** the advertised upgrade matrix passes from real package
archives, each durable layout has a stated support/migration policy, and public
API diffs are reviewed for every coordinated release.

### 3. Promote the durable service composition into reusable library code

**Highest-value product improvement.** The core mechanisms are already present:
[`ThreadedPipelinedRaftNode`](../crates/rafter-runtime/src/pipelined/threaded.rs),
[`ApplicationWorker`](../crates/rafter-runtime/src/application/worker.rs),
bounded TLS transport, and the
[`pipelined_durable_service` example](../crates/rafter-runtime/examples/pipelined_durable_service.rs).
The owner loop still has to coordinate all of them itself.

Extract a small supported driver/composition that owns bounded admission,
persistence completion, ordered application completion, client outcomes,
snapshot maintenance, and shutdown. Keep application schemas, authentication
policy, and scheduling/runtime selection in the embedding. Do not force the
pipeline into the synchronous
[`PersistedRaftRuntime`](../crates/rafter-runtime-api/src/contract.rs)
trait without representing its pending state and durability fences.

Use PR #13's counter as an independent adopter, after updating it. Test actual
TLS processes with restart, full queues, stalled fsync, leadership changes,
canceled callers, snapshot install, and shutdown during outstanding work.
An acknowledged write must survive recovery; an ambiguous write must retain
its identity and report an unknown outcome. Application idempotency must remain
explicit—the existing request ID is correlation, not deduplication.

**Acceptance:** an external consumer composes the supported fast durable path
without copying an owner loop from the benchmark/example, with the same
behavior in source and exact-package tests.

### 4. Decide the application snapshot boundary before freezing the API

**Scope decision, not a claim that network snapshots are unbounded.** Wire and
storage transfer are already chunked and have multi-gigabyte coverage. The
application-facing [`ApplicationSnapshot`](../crates/rafter-app/src/state_machine.rs)
still contains `payload: Vec<u8>` for locally built snapshots and an optional
descriptor for stored installs. A general public streaming application API is
explicitly deferred in [work completion](work-completion.md).

If large state machines are in the first stable scope, provide bounded
reader/writer or descriptor-based creation/install with cancellation, checksums,
applied-floor validation, and crash-safe publication. Exercise a state larger
than the configured memory budget, including interruption and restart. If that
scope is deferred, keep an additive extension path and state the supported
application snapshot limits before freezing the public shape.

### 5. Qualify operational bounds and performance together

**Before a production-stability claim.** Expose the existing progress and credit
counters through a cohesive observation surface: durable/apply lag, available
queue credits, admission refusals, WAL/replay bytes, snapshot bytes, and
persistence/application latency. Keep metrics out of consensus decisions and
avoid requiring a telemetry framework in the kernel.

Extend the existing qualified benchmarks to separate hosts and disks, with
slow/minority followers, lossy links, disk pressure, compaction, recovery, and
TLS enabled. Publish throughput alongside p99/p99.9 latency, retained memory,
write amplification, and restart/catch-up time. Preserve the present clear
separation between in-memory and durable results. Require bounded overload
behavior and recovery before pursuing a larger throughput headline.

Keep the Rust 1.88/MSRV and exact-package gates. Add a current-stable compiler
lane and an explicit OS/filesystem support matrix for the shipped libraries;
the existing macOS launcher test does not qualify all durable storage behavior.
Add dependency/advisory and license checks for the published graph, especially
its TLS dependency chain, without conflating repository-only verifier tools
with shipped dependencies.

## Suggested sequence

1. Repair reproducible CI evidence and the process fixture; update and qualify
   PR #13. This is the next concrete change set.
2. Establish the API/format/upgrade contract and implement the durable service
   composition against independent consumers. Decide snapshot API scope here.
3. Run the full Linux candidate qualification and realistic operational soak
   against one exact archive set. Produce a coordinated prerelease candidate
   only after its evidence and compatibility matrix are complete.

This assessment is a build-and-readiness review, not an exhaustive independent
proof of the Raft implementation. No branch was merged, version changed,
package published, or hosted check bypassed during this work.
