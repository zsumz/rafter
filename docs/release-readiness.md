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
The candidate now passes strict whole-workspace Clippy on macOS, including the
private invariant verifier. Its former compile-time platform gate removed the
sole caller of the shared replay pipeline and produced dead-code errors. A
constant platform check now keeps that pipeline type-checked while refusing
non-Linux replay before compilation or fixture effects. The macOS CI launcher
job and local `clippy-macos` task both lint the entire workspace. A platform
regression requires refusal without effects on macOS and exactly one backend
attempt on Linux; this does not enable or qualify Linux replay on macOS.
Local validation passes 36 replay tests, 53 architecture/CI/source guards, and
the exact 64-case process inventory (60 executed, four existing cases reserved
for Linux-authoritative timing evidence). Deliberately bypassing platform
admission fails the new regression. The candidate lock records only inventory,
macro input, and qualification input changes for this slice, with no new grants;
maintainer architecture approval remains required.

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

CI at `0ad8dc2f` exposed another setup failure: four invariant jobs received
HTTP 503 responses while retrieving the pinned snapshot packages, before their
producers could start. The shared setup now retries metadata and package
retrieval separately, retaining its private archive cache, with four attempts
per phase, 10/20/40-second backoffs, and an eight-minute overall deadline.
Installation runs once with downloads disabled and verifies every installed
version. Signed sources, exact package pins, and executable authentication remain
enforced. Eleven shell execution tests cover recovery, persistent failures,
cancellation, and version refusals; four deliberate bypasses fail those tests.
The tests use fake package tools. Real package verification and the resulting
producer/aggregate identities still require hosted qualification.

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
is codec evidence. The [mixed-version TLS scenario](rolling-upgrades.md) now
builds the full public consumer from both archive sets and checks rolling
replacements, bidirectional snapshot installation, joint membership changes,
cached command replay, retirement recovery, and a nonempty independently checked
history. Its forced snapshot phases cold-restart the senders and temporarily
remove quorum availability. Historical application binaries, crash-during-upgrade
qualification, and broader supported-version/downgrade policies remain open.

**Acceptance:** the advertised upgrade matrix passes from real package
archives, each durable layout has a stated support/migration policy, and public
API diffs are reviewed for every coordinated release.

### 3. Promote the durable service composition into reusable library code

**Highest-value product improvement.** The core mechanisms are already present:
[`ThreadedPipelinedRaftNode`](../crates/rafter-runtime/src/pipelined/threaded.rs),
[`ApplicationWorker`](../crates/rafter-runtime/src/application/worker.rs),
bounded TLS transport, and the
[`pipelined_durable_service` example](../crates/rafter-runtime/examples/pipelined_durable_service.rs).
The new `service::DurableService` coordinates the two workers and hands peer
sends to the caller's existing authenticated transport.

The application worker now accepts validated contiguous ranges through
`ApplicationEntry::first_log_index`. The durable reference service uses this
to cover protocol-only prefixes with the next application command, avoiding
gap-sized placeholder allocations and batch-limit failures. Range support is
an application-dispatch building block; unified bounded admission, completion,
snapshot, and shutdown orchestration remains to be extracted.
Budgeted prefix dispatch now keeps unsubmitted commands with the owner and
reuses completion credits while draining recovery. A 4,100-command recovery
regression exceeds the default 4,096 credits and verifies durable restart;
the previous reference loop panicked on `Full`. The library dispatch operation
is nonblocking; the reference scheduler waits for a completion under pressure.

The first supported owner now derives recovery from the application floor,
retains ordered output work through backpressure, checks read/application
completion fences, hands off the idle store for descriptor installation, and
drains owned work before shutdown. Channel-gated tests hold fsync and application
work to verify no early success. The independent ledger adopts the public API
over real Raft files and its transactional journal, including a backlog larger
than credits, durable tracked completion, snapshot installation, and reopen.

Local creation/compaction now also runs through the owner at a consumed
application floor, with a stable chunk source and returned retired log prefix.
A 2 MiB + 17-byte file-backed test observes nine reads of at most 256 KiB while
the application budget is 64 bytes. Metadata refusals leave service usable;
source/publication/compaction failures fence it, including a failure after the
new snapshot was published. The ledger uses this path and the existing bounded
retirement worker before snapshot restore and reopen.

Completion wake integration now covers both workers, including terminal
storage/application panics and application restarts after snapshot installation
or compaction. `poll_next` distinguishes internal progress from pending work;
embeddings drain progress before waiting on a retained, coalescing notification.
The real-file ledger consumer uses this contract without timer polling, and
gated tests cover completion between polling and waiting, coalesced wakes,
and stale notifications from replaced workers.

The application worker now lends an immutable verified view through
`try_read(required_application_floor, callback)`. Updates and validation hold
the same private lock; readers never wait for application I/O and receive the
observed durable floor with an owned result. Refusals preserve the callback,
and callback panic closes admission, poisons the view, and wakes termination
without letting queued writes reuse it. There is no query mailbox or model
copy. The real-file ledger consumer reads through the public API and compares
the result with reopened state. This primitive still needs service lifecycle
and managed client integration: it does not itself establish a Raft read grant.

The existing managed `TransportRaftDriver` now also bounds completed results
retained for clients. Its original per-kind unresolved limit allowed an
unbounded sequence of resolved-but-unpolled futures. A shared retained-waiter
limit defaults to 2048 and covers pending and completed reads/writes until
their futures are consumed or dropped. Refusal spends no identity or protocol
step. Abandonment still preserves unknown outcomes for late polls. This is an
entry-count bound; application query/result bytes still need application limits.

Remaining composition work includes integrating managed client/read ownership and
TLS process adoption of the owner. Ordinary consensus inputs retain synchronous
I/O. Its memory claim covers one existing output batch, one converted command,
and worker credits; it does not bound the kernel log or an entire recovery
output batch. Keep application schemas, authentication policy, and scheduling
selection in the embedding. Do not force pending pipeline states into the
synchronous `PersistedRaftRuntime` trait.

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

**Decision: support large application snapshots additively.** Wire and storage
transfer are already chunked and have multi-gigabyte coverage. The
application-facing [`ApplicationSnapshot`](../crates/rafter-app/src/state_machine.rs)
still contains `payload: Vec<u8>` for locally built snapshots and an optional
descriptor for stored installs; that form remains available.

The first public streaming piece is
[`SnapshotPayloadReader`](../crates/rafter-runtime/src/snapshot_io.rs). It reads an
existing immutable chunk source with explicit total/chunk budgets, cooperative
cancellation, exact returned-length/capacity checks, CRC32 verification, and a
non-regressing applied floor. Only complete verified consumption produces a
descriptor-bound receipt. A source panic fences a caught-and-reused reader.
The application must decode into temporary state, validate its schema, and
atomically publish state plus floor after verification. This helper does not
own a database, destination allocation, or filesystem publication policy.

The companion `SnapshotPayloadWriter` computes length and CRC32 while an encoder
writes into private staging, retaining no payload buffer. It requires an exact
application floor, enforces total and per-sink-call budgets, handles partial
writes and retryable interruptions, and refuses completion after cancellation,
terminal sink errors, invalid counts, zero progress, or a caught sink panic.
Completion flushes the sink and returns its descriptor; fsync, immutable source
construction, and publication remain the embedding's responsibility. Encoder
errors must propagate before completion, since byte accounting cannot validate
an application format.

An independent archive consumer creates 2 MiB + 17 bytes through the writer
with a 113-byte encoder buffer, explicitly syncs its staging file, publishes via
the existing file-store chunk-source API, and reopens the descriptor. It reads
the payload through 4 KiB source chunks and a 113-byte application buffer.
Cancellation and checksum failure preserve the old installed file/floor; a
fresh attempt publishes and reopens the complete new state/floor. This is normal
reopen evidence, not a power-loss sweep. Source/reader failure tests also cover
early completion, wrong lengths, oversized allocations, empty payloads, and
regressing or excessive descriptors before I/O.

The ledger now installs its actual version-1 schema through that bounded reader.
Promoted descriptor kind, version, index, and configured byte-limit checks run
before opening the snapshot store. One shared grammar decodes inline bytes,
streamed chunks, and journal images into temporary model state, checking counts
before record allocation. A model-owned restorer centralizes bounds, uniqueness,
and supply validation without intermediate snapshot-record vectors. Publication
requires complete schema consumption and the reader's final CRC32 receipt.
The stream retains one encoded chunk of at most 16 KiB; the old live ledger and
new temporary ledger remain proportional to the configured model bounds.

A generated 2 MiB + 46-byte schema restores 131,072 accounts in 129 bounded
source calls. A separate real-file consumer installs a multi-chunk ledger,
reopens its journal, and replays an exact cached request without another deposit.
Negative checks cover descriptor admission before source/store effects, malformed
records, configured count limits before decoding, checksum failure, and faulty
source chunks. Schema version and valid encoded bytes remain unchanged.
The streaming-decoder slice passes 496 source tests and 501 archive/MSRV tests
(57 existing ignored profiles in each lane), strict consumer lint and docs,
zrail, and all seven exact-package phases including storage, wire, and mixed TLS
compatibility on Rust 1.88.0. Four deliberate bypasses of checksum completion,
account/session admission, and source-opening admission fail their regressions.

The ledger now also exposes `write_snapshot_to` on both adapters. It borrows
stable model state and emits the unchanged v1 schema through the existing
payload writer with one 16 KiB buffer, avoiding a model clone and a complete
encoded image. The inline builder and journal encoder share its output grammar.
Schema/floor mismatch and prior cancellation refuse before sink effects; both
durable builders refuse a store requiring recovery. Failed drains, flushes,
cancellation during an accepted write, and sink unwinding return no descriptor.
Successful completion flushes staging; fsync and publication remain explicit
caller obligations.

An independent 131,072-account fixture encodes 2 MiB + 46 bytes into a counting
sink with bounded calls and a known CRC32. Literal vectors protect every command
mutation, cached result, and business-rejection tag. Real-file staging promotes
through the existing snapshot store, installs, and reopens the same ledger. The
durable-owner consumer now uses this file-backed encoder during compaction.
All eleven new encoder/refusal checks pass. Removing the buffer drain, floor
check, or either durable-builder health check fails its regression. The complete
source lane passes 507 tests and the exact-package/MSRV lane passes 512 tests
(57 existing ignored profiles each). Strict consumer Clippy, docs, formatting,
size checks, and zrail pass, along with all seven package phases and the storage,
wire, and mixed-version TLS compatibility matrices on Rust 1.88.0.

Journal replacement now measures the borrowed model in one pass and streams its
unchanged v1 image in a second pass, with a 16 KiB staging buffer and shared
fixed-size frame records. Length-field admission precedes staging. Publication
preserves file sync, rename, directory sync, and adoption only after success.
Fault offsets span the whole journal across write calls; exhaustive small-image
cuts and multi-buffer cuts preserve the exact staged prefix and old-or-new
recovery. A 131,072-account journal has an independently known length/CRC32 and
bounded sink calls without a retained encoded image.

This work also found and fixes a public store admission defect: an incoming
ledger could carry different resource bounds, be acknowledged, and then fail
recovery under the journal's bounds. Both commit and replacement now refuse that
configuration before writes or write-plan consumption, including empty models
whose bounds would otherwise change future application semantics.
Nine new format, refusal, and fault-sweep tests pass; removing either model
configuration gate, cumulative fault accounting, or final buffer draining fails
its regression. Final source qualification passes 516 tests and all seven
exact-package/MSRV phases pass with 521 tests (57 ignored profiles each), plus
storage, wire, and mixed-version TLS compatibility on Rust 1.88.0. Strict
consumer lint/docs, formatting, size checks, and zrail pass without a lock change.

Journal append now shares the measured frame and 16 KiB publication buffer,
removing its full image, frame, and unsealed-copy allocations. The first-byte
mask sits beneath checksumming and buffering; checksums retain the sealed value,
while the file receives the unsealed mark first. Full frame sync still precedes
the seal and its sync. New regressions cover a 2 MiB frame's independent CRC32,
partial/interrupted/zero writes, exact multi-buffer fault prefixes, both sync
boundaries, and subsequent append offsets after recovery. A complete unsealed
frame remains an explicit refusal without shortening the journal.
Five new tests and the existing crash suite pass; deliberate early sealing,
sealed-first emission, missing byte accounting, and drop-time retry mutations
fail their regressions. Source qualification passes 521 tests and all seven
archive/MSRV phases pass with 526 tests (57 ignored profiles each), including
storage, wire, and mixed-version TLS compatibility on Rust 1.88.0. Strict consumer
lint/docs, formatting, size checks, and zrail pass without a lock change.

Journal recovery now verifies frames with fixed records and 16 KiB reads, then
decodes each verified image through a 16 KiB buffer. The second read rechecks the
image checksum before adoption. This preserves checksum-before-schema error
precedence and the exact refusal/repair rules without retaining the whole
journal, an encoded image, or a copied unsealed tail. File offsets use `u64`.
The old slice parser remains an oracle for the production reader across the
existing exhaustive corruption cases. Tests also cover I/O failures, partial
and interrupted reads, changed bytes, a 5 MiB zero tail, and a real 6 MiB journal
whose damaged middle frame is refused before an explicit repair and later append.
All eight new cases pass, and six deliberate checksum, truncation, changed-image,
and I/O-error bypasses fail their regressions. Full source qualification passes
529 tests; all seven archive/MSRV phases pass with 534 tests (57 ignored profiles
each), including storage, wire, and mixed-version TLS compatibility on Rust
1.88.0. All nine reviewed ledger process cases pass separately, including kill,
restart, repair readiness, and cached retries. Strict consumer lint/docs,
formatting, size checks, and zrail pass without a lock change.

Still required before freezing this API: broader interrupted/crash-safe
publication qualification and operational measurements of the second recovery
read. The inline snapshot builder still materializes a buffer.
Current/restored models, adoption, and compaction remain proportional to
configured state bounds. Deterministic file-fault tests do not establish physical
power-loss behavior or qualify these remaining paths.

One prerequisite is now fixed in the ledger consumer's existing schema decoder:
untrusted account/session counts previously reached `Vec::with_capacity` before
the decoder checked whether the payload could contain those records. Checked
minimum-byte admission now runs before record decoding, including space for
the field following the account records. Regressions cover impossible and
maximum counts, every truncated prefix of a valid frame, unchanged v1 bytes,
and refused promoted files preserving the journal and applied floor after
reopen. Removing either admission check fails the modest-count regression.
This bounds allocation amplification from malformed counts; application encoding
and publication crash qualification remain open.
The earlier count-admission slice passed 487 source-consumer tests and 492 archive/MSRV tests, with
57 existing ignored profiles in each lane. All seven exact-package phases,
including storage, wire, and mixed-version TLS compatibility, pass on Rust
1.88.0. Strict consumer lint/documentation checks and zrail also pass; this
reference-only slice requires no architecture-lock changes.

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
