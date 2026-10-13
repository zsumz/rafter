# Release readiness — 2026-10-12

Status snapshot: October 13, 2026 at 01:19 UTC (October 12 locally). A fresh read
of all 21 remote heads still places `main` at
`021f4f9bb2f6960cb75b24f5a627f1872a12d430`, matching the initial branch review.
The recent commits are on the release-work branches. Rust 1.88.0 builds the
library; the initial local blocker was the missing toolchain. The repository now pins that
toolchain, while CI explicitly selects each job's separately required compiler.

Rafter already has authenticated TLS transport. The durable owner, reference
services and standalone counter reuse it. The remaining release work is to
qualify the final integration, establish deployment bounds and select the
supported operational contract. A green component test is not a stable-release
claim.

## Current code checkpoint

The frozen integration is
[`b2847c07`](https://github.com/zsumz/rafter/commit/b2847c07a8582b93fe2df31adec2adab179ff7c3)
on `codex/stable-release-slices`, in draft
[PR #23](https://github.com/zsumz/rafter/pull/23). Its PR and Nightly TLA+ producers
are still running, so its invariant aggregates remain outstanding. Maintainer
architecture approval is also outstanding. Local `zrail.lock` refreshes do not
grant that approval.

Six later commits through
[`d439da22`](https://github.com/zsumz/rafter/commit/d439da22d240c4ccb5b7060ee903d1dcde7537f1)
on `codex/stable-release-counter-directories` add ledger publication crash tests,
their dependency-lock receipt, and ordered directory provisioning for all three
durable reference consumers. Their combined archive qualification is running.
They have not been integrated into the frozen candidate. Both checkpoints leave
the published library sources, root workspace manifest/lock and toolchain
unchanged from the library checkpoint at
[`78382a8d`](https://github.com/zsumz/rafter/commit/78382a8d6a8b155e9af5a212ff8c14b664aa7983).

The default-off `rafter-service/durable` feature supplies bounded managed writes,
local/quorum reads, leadership transfer, shutdown, snapshot maintenance/handoff,
and opt-in membership proposal admission. It exposes retained credits, refusals
and persistence-worker diagnostics. Membership receipts identify durable append;
the embedding still tracks exact configuration commits, executes membership
plans, and persists peer authorization and retirement policy. Streaming snapshot
adapters are additive and bounded; applications still own schema validation and
atomic publication of their state and applied floor. See the
[capability boundaries](work-completion.md#additive-capabilities-and-remaining-work)
and [release checklist](../RELEASE.md).

| Qualification | Verified checkpoint and scope |
| --- | --- |
| Integrated library build | `78382a8d`: all-target build, 3,349 tests passed, 59 ignored, strict all-target Clippy and rustdoc on Rust 1.88/Apple Silicon macOS. Ignored tests have separate reviewed selections. |
| Previous complete invariant profiles | [PR at `bc6d1677`](https://github.com/zsumz/rafter/actions/runs/38296623957) and [Nightly at `bc6d1677`](https://github.com/zsumz/rafter/actions/runs/38297161459): both invariant aggregates independently checked, with 44 invariants, 125 required clauses, 77 fixtures and 79 bindings. All 16 applicable Nightly jobs passed. The old PR run was subsequently cancelled when `b2847c07` was pushed; its unapproved architecture-review job was cancelled and the dependent gate failed. It was never a fully approved CI run. |
| Frozen integration | [CI at `b2847c07`](https://github.com/zsumz/rafter/actions/runs/38326474147) and [Nightly at `b2847c07`](https://github.com/zsumz/rafter/actions/runs/38326565330): completed runnable jobs passed; TLA+ producers and final invariant aggregates remain pending. The CI merge tree matches the branch tree. |
| Native test jobs | The `b2847c07` CI passes 3,255 Linux workspace tests (53 ignored), a separate 440-test durable selection, and 321 macOS storage tests (one ignored). Counts overlap. Independent log and native-receipt checks bind Ubuntu 24.04.5/ext4 and macOS 15.7.9/APFS to the recorded merge tree. See the [platform observations](platform-qualification.md#observed-candidate-test-storage). |
| Public source API | The `b2847c07` CI passes all 22 default/all-feature comparisons across 11 crates against `0.0.2-alpha.1`. Readback verifies commands, reports and all 2,837 source files. This is source API evidence; wire and storage compatibility have separate executable checks. |
| Exact archives, MSRV and upgrades | [Archives at `b2847c07`](https://github.com/zsumz/rafter/actions/runs/38326581942): seven phases, 56 reviewed process cases, 11 archives, Rust 1.88 MSRV, storage upgrades, bidirectional wire exchange and rolling TLS upgrades pass. All 2,837 source files match Git. The dependency projection covers 51 packages, including 40 registry packages. Retained artifacts contain archive hashes and receipts, not the archive bytes. |
| Linux I/O and snapshot faults | [I/O run at `b2847c07`](https://github.com/zsumz/rafter/actions/runs/38326471338): four I/O scenarios, two TLS deadline regressions and 36 snapshot process-loss cases pass. Readback checks 138 pre-recovery snapshot inputs, 24 node-crash inputs, 90 resource observations and the complete source inventory. Process loss retains the kernel/page cache. |
| Owner lifetime and compaction | The `b2847c07` Nightly owner job passes 96 cold-recovery cycles (12,288 writes and reads) and 256 compactions with the same live owners (32,768 writes and reads). Cold recovery preserves the final balance and the next write reaches 32,769. Exact cycle inventories, all source bytes and the resolved dependency lock were checked. |
| Simulator | The `b2847c07` Nightly bundle independently verifies 77 detector executions, 79 bindings, 14 exhausted bounded models, 18 source-seeded soaks of 1,024 steps each, and 180 liveness report bindings. This does not replace the pending full invariant aggregate. |
| Ledger publication and directory sync | [Linux at `4079be1d`](https://github.com/zsumz/rafter/actions/runs/38330349594): all four jobs pass. The ledger job executes six directory-helper tests and 13 actual `SIGKILL` publication cases, retaining 15 inputs before recovery. Two cases require explicit repair; strict open is not weakened. All 2,844 source files and the resolved reference lock were verified. |
| Other directory callers | `d82aa927`: fenced-lock passes 219 ordinary tests and 20 process cases. `d439da22`: counter passes 124 ordinary tests and 27 process cases. Both pass strict lint/docs and source/architecture checks. [Combined archive qualification](https://github.com/zsumz/rafter/actions/runs/38331550435) is still running at the snapshot time above. |

The previous PR TLA+ primary model at `bc6d1677` exhausted its queue after
36,058,645 distinct states, along with its two required bounded obligations.
The same checkpoint's Nightly exhausted four mandatory bounded models but its
separate larger continuation stopped at the 195-minute budget with 55,325,720
states queued. Both profiles passed 11 named negative cases and 34 mutation
controls. Retained raw receipts preserve TLC's fingerprint collision estimates.
These are bounded results; the larger Nightly model has not been exhaustively
checked, and previous results do not qualify a later Git identity by inheritance.

## Operational evidence and limits

The retained 30-minute local TLS workload at `1dac862b` used previously qualified,
unchanged compiled inputs and recorded 180,152 operations. It acknowledged 26,531
writes, completed 6,632 reads and retained 117 uncertain writes. Independent
history replay selected one uncertain write, and cold recovery preserved 26,532
increments; the next write reached 26,533. An actual leader `SIGKILL` demonstrated
majority progress before the victim restarted. The run preserves raw outcomes,
latency distributions and sparse process-resource observations. It is one local
host, not sustained multi-host qualification or a whole-host memory bound.

The `b2847c07` Linux run adds a source-bound 60-second concurrent TLS history with
7,826 operations and an independently replayed 2,013-step witness. The separate
[durable performance baseline](../bench-compare/METHODOLOGY.md#local-release-readiness-baseline)
at `0eb06666` retains 40 isolated timing samples comparing checkpoint replacement
with the optional journal on local APFS. Neither result establishes peak service
throughput, a latency ceiling or superiority over other libraries. The teaching
HTTP ingress has no whole-server connection/thread bound, and that application
does not schedule periodic compaction; owner admission budgets alone do not
close those deployment limits.

Native receipts record OS, compiler, filesystem and mount options. The completed
Linux jobs use Ubuntu 24.04/x86-64 with ext4 root mounts reporting `nobarrier` and
`data=writeback`; private loop/ext4 fault volumes have separate receipts. Passing
process-loss tests on these mounts supplies no physical-power-loss guarantee.
Local observations use APFS. The [platform matrix](platform-qualification.md)
keeps executed coverage separate from support commitments.

The directory fixes close a demonstrated missing synchronization obligation.
The reference helper creates one directory, synchronizes its parent, then creates
the next child. Retry first synchronizes the nearest existing ancestor's parent,
covering residue from its own failed attempt. Its negative control rejects plain
`create_dir_all`. Externally supplied ancestors must already be durable. The
counter's staged Raft activation already synchronized its new directory and
parent at explicit crash boundaries; those boundaries are preserved. The
[storage protocol](../crates/rafter-storage/DURABILITY_PROTOCOL.md#directory-provisioning)
still defines the caller's provisioning obligations. This is not physical
power-loss qualification of arbitrary application databases.

## Remaining release priorities

1. Finish the existing `b2847c07` PR/Nightly producers and authenticated aggregates,
   and the `d439da22` archive run. Verify their retained evidence before integrating
   the six reference follow-ups, then qualify the final candidate. Keep results
   attached to their actual source rather than adding overlapping test counts.
2. Obtain maintainer architecture approval and reconcile the still-open,
   overlapping [PR #13](https://github.com/zsumz/rafter/pull/13). A local gate-input
   refresh, draft PR or passed code check does not satisfy that approval.
3. Set the supported integration contract before promising stability: the durable
   feature is opt-in, membership execution/committed-change tracking and general
   peer policy remain embedding responsibilities, and applications own atomic
   snapshot publication. The [completion map](work-completion.md) records these
   boundaries; the implementation work above should not be relisted as absent.
4. Qualify sustained multi-host TLS load, deployment-wide memory/latency bounds
   and the intended storage stack's power-loss behavior. Host access and the target
   environment remain unspecified. Preserve uncertain outcomes and recovery
   witnesses, and keep optional diagnostics out of timing comparisons.
5. Select OS/filesystem support commitments and dependency-license policy.
   Resolve remaining benchmark comparator dependency findings separately from
   the audited public graph. Refresh the final archive/advisory evidence before
   any coordinated release candidate; do not infer support from compilation.

The sections below retain the initial assessment and implementation history.
Their counts, pending states and proposed sequences belong to their named
checkpoints; the current status and remaining priorities are above. No change has
been merged into `main`, tagged or published as a release during this work.

## Initial build and qualification

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
| `docs/layered-stack-readme` (`3790e23a`) | Pre-existing open PR [#13](https://github.com/zsumz/rafter/pull/13); six unique commits and four main commits behind its merge base. Adds a standalone TLS counter, guide, and smoke check. | The counter, guide, and checker are now ported and qualified on this release branch. The fixed-membership counter now uses the durable owner; reconcile the overlapping PR before merging. |
| `perf/raft-performance` (`66a7d719`) | [#19](https://github.com/zsumz/rafter/pull/19) and [#20](https://github.com/zsumz/rafter/pull/20) are merged. Its tip tree exactly matches merged main commit `ae951379`; raw ahead counts reflect separate history. | Start new performance work from main; do not merge the historical branch again. |
| `fix/nightly-coverage-contract` / `docs/sync-qualified-performance` | [#21](https://github.com/zsumz/rafter/pull/21) / [#22](https://github.com/zsumz/rafter/pull/22) are merged. | Treat them as historical branches. |
| `codex/snapshot-log-recovery-main` / `refactor/protocol-reading-paths` | [#11](https://github.com/zsumz/rafter/pull/11) / [#12](https://github.com/zsumz/rafter/pull/12) are merged and their tips are ancestors of main. | Keep the recovered snapshot/configuration behavior and walkthroughs already on main. |
| `actions-improvements`, `docs/zrail-readme-main`, `fix/main-review-findings` | Tips are ancestors of main. | No newer work to integrate. |
| `runtime-correctness`, `tla-improvements`, `zrail`, `fix/snapshot-log-recovery` | Older divergent development histories; none has an open PR. Runtime/TLA PRs #6/#5 are merged, and main contains later zrail and snapshot work. | Use them for history or a targeted comparison, not as replacement release bases. |

PR #13 was mergeable but behind. Its lint, test, source/package/MSRV, and
individual invariant jobs passed in the reported run; `invariants-pr`,
`zrail-policy-review`, and `zrail` failed. The architecture environment review
is a maintainer gate, not something a lockfile refresh can satisfy.

The release branch now carries PR #13's counter, guide, and extraction checker
without importing its stale root README, CI, or architecture lock. The standalone
project now pins `7151197c051502173b387934000a2d1829047e17`, retaining the existing
live retained-credit and budget-refusal APIs and adding the qualified storage
directory and TLS handshake fixes. Typed managed-admission errors and the patched
rustls minimum remain.
This revision update changes only the nine Rafter Git identities; the preceding
security slice already updated the standalone rustls lock to 0.23.45. Both guide
modes still exercise the same application and on-disk schema.
The checker has separate checkout and pinned modes: the first validates every
resolved Rafter path against this checkout, and the second requires the exact
Git source and canonical lock. Both extract the checked guide into a temporary
project before building. Fifteen controls reject stale/mixed/missing/private
graphs and preserve cleanup ownership when a smoke-test shutdown request or wait
fails. Both cleanup regressions fail the original harness; deliberately omitting
all checkout overrides is rejected before compilation.

Both modes pass Rust 1.88 formatting, strict Clippy, ten application/policy tests,
and the real three-process TLS smoke sequence: acknowledged writes, redirected
reads, leader loss and catch-up, missing/corrupt state refusal, complete cluster
crash recovery, quorum loss with an uncertain write, application-save failure
and committed replay after repair, and clean shutdown. Python
3.13 initially refused the generated server certificates for missing authority
key identifiers. The fixture now emits CA/leaf subject identifiers and leaf
authority identifiers; the peer probe explicitly enables strict X.509 checks
before verifying client-certificate refusal. CI and zcheck run both modes and
the controls; the repository checker requires Python 3.11 or newer.

The counter now explicitly enables `rafter-service/durable`, retains the existing
TLS sender and inbound validator, and gives HTTP a managed handle and observable
command sender. The main
thread uniquely owns the driver and uses bounded event turns. Eight writes and
eight reads have independent admission budgets; writes enter as encoded bytes,
linearizable reads use the real quorum/application fence, and local status reads
are explicitly diagnostic. Shutdown continues driving until storage is released
before HTTP and transport workers are joined.

Fixed-size application entries preserve protocol prefixes and result correlation;
accepted work retains its credit until completion is consumed. The existing
checksummed checkpoint format is retained. Adoption refuses contradictory,
removed-member, enlarged/erased retirement, duplicate-member, and future-position
records instead of reconstructing a weaker policy. Empty checkpoints require
pristine Raft state. The example refuses any configuration outside its original
three voters, including intermediate crossings and snapshot boundaries; this
restricted policy does not implement general managed membership.

Real-process checks reject seven valid-checksum unsafe checkpoint variants.
A failed application save returns an uncertain client error, exits, and replays
the committed increment after repair/restart. A temporary-copy probe upgrades
all three processes from the synchronous counter at `e2280e3a`, runs mixed
processes, then cold-reopens the whole cluster with its state, checkpoints, and
TLS sessions intact. Another deterministic probe requests shutdown between a
Ready observation and tick admission: handling `Unavailable(Draining)` exits
cleanly, while the unhandled version exits with that lifecycle error. A second
lifecycle probe showed that an HTTP worker panic could leave the owner running
without its client endpoint. A worker-scope drop guard now requests shutdown
on both return and unwind: the prior process stays alive after the controlled
panic, while the guarded process drains the owner and reports failure. The
original PR remains unmerged. Final qualification also caught a leader-status
fixture race after cold reopen: status can precede a usable quorum read. The
leader helper now waits for a successful read barrier, with a regression that
fails the prior predicate; no uncertain writes are retried.

The separate POSIX pressure lane now blocks the leader's application publication
on a FIFO while real TLS peers keep committing/applying in the same leader term.
Eight timed-out clients retain all eight write slots; a ninth write refuses within
two seconds. HTTP derives uncertainty from the public `WriteError::fate()` contract,
so this proven pre-append refusal reports false while deadlines remain uncertain.
A quorum read stays behind the blocked application floor. `/metrics` stays live
without borrowing the application. Shutdown is observed in `Draining` before the
fixture unblocks the writer into a forced rename failure; the prior counter file
is byte-for-byte unchanged, the process releases the backend and exits failed,
and restart replays all eight committed writes before fresh admission succeeds.
The fixture explicitly forces rename failure because macOS can accept FIFO fsync.
CI and default zcheck lint require `counter-pressure`; checkout and pinned-Git
modes can both run it. This is a controlled blocked-publication and failure test,
not a physical slow-disk/power-loss qualification or a bound on HTTP connection
queues. Snapshot transfer and sustained multi-host/disk soak remain open.


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

The completed [October 11 aggregate at `5b1d4e3f`](https://github.com/zsumz/rafter/actions/runs/38103501529/job/114399595770)
narrows that blocker: it authenticates the tests, simulator, and Maelstrom
evidence and reports 36/44 green invariants. All thirteen remaining evidence
issues belong to TLA: two `invariant_violation` rows and eleven
`coverage_not_reached` rows from the same counterexample below. The aggregate
report contains no non-TLA issues. This establishes the pinned runtime path for
that checkpoint; it does not qualify the corrected model or a later candidate.

**Qualification blocker and local correction (October 11):** the retained
[nightly run at `5b1d4e3f`](https://github.com/zsumz/rafter/actions/runs/38103501529)
reports `CommittedEntriesHaveQuorum` violations for CM-02.a and CM-02.b; the
other eleven TLA rows are incomplete because checking stopped at that
counterexample. The primary TLC process exited 12, without a timeout. Its
21-state trace has leader `n1` enter joint membership, append a final configuration
removing itself at index 2, append a command at index 3, and advance directly to
`Commit(n1,3)`. The captured `Raft.tla` matches the model before this correction.
Two-state replays reproduce the failure with a command after removal and with
commands on both sides. The per-entry certificate incorrectly required every
newly committed entry to be the final configuration, even though `Commit`
advances the whole prefix atomically. The corrected certificate binds every
entry to that advancement, its current-term final configuration, and its actual
prior joint membership. It retains the new-membership quorum requirement and
rejects reuse of the removed leader's authority in a later advancement.

Both corresponding three-node Rust regressions pass without changing production
code: actual follower responses commit the batch only after both new voters
acknowledge it; the leader steps down and rejects subsequent proposals. All 34
TLA regression/mutation controls pass, including nonvacuous removal-only,
suffix, and prefix-plus-suffix cases. Independent mutations restoring the old
certificate or removing the new boundary, prior-membership, or term checks fail
as expected. See [the certificate explanation](model-checking.md#self-removal-commit-certificates).
The failed nightly remains retained evidence. A fresh source-bound producer and
aggregate run must qualify the corrected model before a release claim; these
focused checks do not replace the full tier.

The corrected checkpoint's [October 11 nightly burn-in](https://github.com/zsumz/rafter/actions/runs/38117336352/job/114404749801)
found a separate `RD-06 client history linearizability` failure at seed
`0x5f20b7ca1233fddd`, step 230: read 2 (`start=8`, `end=11`) returned
`model-proposal-1`. The saved artifact is `burn-in-logs`, ID `11694408322`.
A local standalone replay using the exact nightly workload bounds and standard
three-node configuration reproduced that same failure. Captured client state
identified a checker defect: the value came from an accepted invocation that
had not returned, and RD-06 discarded that invocation. The checker now retains
pending and accepted writes as optional operations, alongside unknown outcomes;
rejected writes remain excluded and completed writes remain mandatory.

The exact 1,024-step seed now completes safety and liveness checks. A separate
three-node partition/election case demonstrates the same behavior without the
long restart trace. These and a direct incomplete-invocation case fail under the
old rule; the corrected checker passes all 15 linearizability tests. Three
independent mutations that make completed writes optional, admit rejected writes,
or remove real-time precedence fail the new negative cases. All 393 simulator
library/binary tests pass. The invariant registry explicitly covers incomplete
invocations, and its source-bound detector fingerprint is refreshed without
changing the 77 fixtures, 79 evidence bindings, or two targets. No kernel change
was needed. Fresh hosted qualification remains required; the earlier failed
receipt is retained, and its TLA producer is separate evidence.

The follow-up [nightly at `961d621e`](https://github.com/zsumz/rafter/actions/runs/38121179596)
now passes in full: every producer, authenticated aggregate evidence readback,
and the final 44-of-44 invariant gate. Its downloaded report contains 44 distinct
green rows with all required clauses and evidence present. This qualifies the
model/checker corrections and the repaired qualification pipeline at that exact
checkpoint. Later library changes still need a fresh complete candidate run;
this result does not qualify the current branch tip by inheritance.

A later [PR test job at `275a876f`](https://github.com/zsumz/rafter/actions/runs/38147347821/job/114493483985)
exposed an observer-fixture ordering gap: the late-completion hook waited for
direct-child exit, but not release of its inherited lifetime lease. A concurrent
fork can retain a close-on-exec writer after that child exits. The test-only hook
now waits for both facts within its existing lifecycle deadline. A held-lease
regression fails the old hook and passes after release with the corrected hook.
Production deadline/cleanup behavior and the original timeout, retained-output,
and no-signal assertions are unchanged; the exact process inventory now has 65
scenarios.

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
Retain the current format vectors, exact-archive consumer lanes and published
source API comparison gate to detect unintended breaking changes.

An initial public-API comparison now covers all eleven crates against the
explicit published `0.0.2-alpha.1` baseline, separately with default and all
features. It uses pinned cargo-semver-checks 0.51.0 on Rust 1.97.1; library
build/test qualification remains on Rust 1.88. The check found an unintended
service API change: inserting new admission/transfer variants shifted existing
numeric error-kind values. A downstream Rust 1.88 consumer reproduced
`WriteErrorKind::UnknownOutcome as usize` changing from 3 to 4. Appending the new
variants preserves all 35 published write/read/transfer categories; a regression
also preserves their ordering. Both full API comparisons now pass with no
findings. This is source API evidence, not a Rust memory-layout or wire-encoding
promise.

`scripts/check-public-api`, CI `public-api` and `zcheck run public-api` now run
both comparisons against the public inventory shared with `RELEASE.md`. The
runner snapshots current working files (including uncommitted additions and
deletions), retains that source and its digests, and builds in isolation. It
rejects lint overrides, warnings, missing/duplicate crate comparisons, narrowed
lint counts and changed source inputs; failed runs retain diagnostics without a
success receipt. The pinned tool's policy runs 229 lints and leaves 31 at its
built-in allow level. Updating that policy or baseline requires review. The
checker resolves its own documentation dependencies, so this gate does not
replace the locked MSRV or exact-archive lanes. CI checks the Linux host target;
other targets and feature combinations remain follow-up work. Branch protection
must require the new job separately in repository settings.

The checker is not a complete compatibility proof. A deliberate removal of
the handwritten `Default` implementation for `RunningCrc32` passed its API
comparison, while an ordinary Rust 1.88 downstream `Default::default()` consumer
failed to compile. Its derived-trait lint does not cover that handwritten case.
The external `rafter-crc32/tests/published_api.rs` consumer now supplements that
specific gap with generic default construction, `std::mem::take` reset and the
independent IEEE check vector. The same consumer passes against the published
alpha; removing the trait fails compilation and using the wrong default state
fails its behavior check. Other handwritten trait contracts still need review.
The actual enum
regression above is detected by the automated gate, with no success receipt
written on failure.

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
copy. `DurableService::try_read_application` now applies lifecycle admission
before lending that view, preserving callbacks on refusal and fencing caught
query panics. The real-file ledger consumer queries after actual read grants
before and after reopening, including protocol-only barriers, then checks the
same state after compaction and snapshot installation. Managed client ownership
remains to be integrated; the read operation itself does not establish a Raft
read grant.

The independent ledger now also composes three durable owners with the existing
TLS transport over real sockets and files. Its finite transport queues and one
application credit are checked while a paused transaction holds both write
completion and a real quorum read behind application durability. Cold reopening
all replicas preserves acknowledged balances and application deduplication after
a different leader is elected. The same test runs in source and exact-package
lanes. A second case forces a two-chunk snapshot from compacted files, restarts
the follower at a durably staged 64 KiB boundary, and checks the exact recovered
transfer identity and offset before accepting more traffic. Application handoff
refuses reads and inputs until installation completes. The restored follower
later becomes leader after another full reopen and preserves request
deduplication. The fixture provisions identical nonempty genesis to exceed one
chunk; replicated deposits provide the state change being recovered. Wire limits
include metadata beyond the fixed 64 KiB payload chunk. This is same-process,
fixed-membership evidence with orderly restarts; process fault isolation, managed
clients, and full-queue liveness remain unqualified. The fixture polls the bounded
TLS inbox because it has no inbound wake callback.

The owner/TLS qualification now passes 531 source-consumer tests and 536 exact-package/MSRV tests on
Rust 1.88.0, with 57 existing profiles ignored in each lane. All seven package
phases pass, including the published storage, wire, and rolling-TLS matrices.
Both TLS cases pass 32 repetitions across four concurrent processes. Reversing
the owner's read durability fence fails the read regression; omitting partial
transfer rehydration fails the snapshot regression at its exact recovered-offset
assertion.
Strict consumer lint, documentation, formatting, and size checks pass. Architecture
analysis has no findings or policy changes for this reference-only slice.

The existing managed `TransportRaftDriver` now also bounds completed results
retained for clients. Its original per-kind unresolved limit allowed an
unbounded sequence of resolved-but-unpolled futures. A shared retained-waiter
limit defaults to 2048 and covers pending and completed reads/writes until
their futures are consumed or dropped. Refusal spends no identity or protocol
step. Abandonment still preserves unknown outcomes for late polls. This is an
entry-count bound; application query/result bytes still need application limits.

Managed-client review also reproduced a metrics retention bug: canceling a
`MetricsWatch::changed` future left its executor task retained until another
publication, and polling with a new task accumulated obsolete wakeups. A pending
call now owns one registration and replaces its previous waker. Completion or
cancellation removes the registration without consuming a later update. Cleanup
and notification run outside both watch locks; reentrant task destruction and
self-cancellation are covered. An independent ledger consumer checks the same
ownership contract through public APIs in source and archive modes. This closes
that existing leak; the asynchronous owner's client handoff remains separate
integration work.

The metrics slice passes 325 service tests, 532 source-consumer tests, and 537
archive/MSRV tests on Rust 1.88.0, with strict service/consumer lint and docs.
Its eight new unit cases include reentrant destruction, self-cancellation, and
publication during waker replacement; three deliberate ownership/locking faults
fail their regressions. Its first exact-package run failed the rolling TLS
matrix after the other phases passed. A diagnostic reproduction captured the
cause on run 30: local proposal tracking was dropped during final replica
restarts, and the client correctly received an unknown outcome. The test had
incorrectly required that every restart-time write immediately settle.

The rolling client now retries identical idempotent commands with a shared
eight-unknown-outcome allowance. Every attempt stays in the history, all original
read checks remain, and one real client reply is deliberately lost on every run.
The scenario explicitly admits up to 32 history operations; the default remains
24 and the 200,000-configuration bound is unchanged. Removing an uncertain
attempt makes a cached-retry history fail validation. Connection failures are
also returned as unknown, preserving the application's responsibility to retry.
The receipt refuses missing loss/retry evidence or a history that omits attempts.

This slice passes 535 source-consumer tests and 540 archive/MSRV tests, with 57
existing profiles ignored in each lane, plus all five authenticated-process
cases. Three new checker controls preserve the default limit and reject both
impossible histories and an omitted uncertain attempt; all 18 archive-runner
tests pass. Disabling retry or hiding the lost request in an isolated consumer
fails the rolling scenario. Strict consumer lint/docs, formatting, size checks,
and zrail pass without another policy or lock change. The restored scenario
passes 32 repetitions across four concurrent processes; every transcript passes
the loss/retry/history receipt checks. The final Rust 1.88.0 archive run passes
all seven phases, including all three compatibility matrices. This does not
resolve the TLA counterexample recorded above.

At that checkpoint, remaining composition work included the managed command
interface, membership operations, and TLS process adoption of the owner; the
later slices below implement those surfaces and state their limits. Ordinary consensus inputs retain synchronous
I/O. Its memory claim covers one existing output batch, one converted command,
and worker credits; it does not bound the kernel log or an entire recovery
output batch. Keep application schemas, authentication policy, and scheduling
selection in the embedding. Do not force pending pipeline states into the
synchronous `PersistedRaftRuntime` trait.

The first managed client slice is now the default-off `rafter-service/durable`
feature. `DurableClientOwner` uniquely owns `DurableService`; cloned write
senders cannot extend backend lifetime. Commands arrive already encoded, so a
busy application worker does not serialize admission. Credits cover queued,
in-flight, completed-uncollected, and canceled-but-owned writes; encoded `Vec`
capacity supplies the byte charge. Only durable application outcomes produce
success. Shutdown closes admission, releases the backend, then resolves remaining
waiters as proven refusal or unknown outcome. Caught outcome/waker panics fence
the owner; terminal publication attempts every waiter even when one waker panics.

Nine real-worker tests cover these boundaries, and an independent ledger test
uses three file-backed owners over existing mutual TLS. It withholds follower
turns before quorum, pauses application durability after quorum, cancels queued
work while the application is busy, and cold-reopens the acknowledged state with
a different leader. Default dependencies stay unchanged; two explicit optional
runtime/storage edges are recorded in the candidate architecture lock and remain
subject to maintainer review. CI and zcheck explicitly test and lint the feature;
strict rustdoc includes its public surface.

The read-capable constructor now adds bounded encoded local and linearizable
queries. A grant captures its original term and translated application floor
before another consensus input; the query preserves any higher caller floor and
runs only under a verified immutable application borrow. Read cancellation keeps
credits while the kernel owns the quorum round, and releases them after a grant,
cancellation, or backend disposal. Raw local read IDs cannot collide with managed
ones. Local queries refuse committed replica removal; lease reads are unsupported.
Both queues close after caught query/waker panics, and terminal publication spans
all queues before invoking any waiter. Application result sizes remain outside
the encoded byte bound.

Fourteen new real-worker tests cover these boundaries. Four independent mutations
that lower caller floors, require a phantom application no-op, release canceled
quorum credit early, or replace a grant's term fail their exact regression cases.
The independent TLS ledger consumer verifies the write/application fence and
read-your-writes after a cold reopen under another leader. Its new read test also
withholds follower turns to prove that a read requires live quorum progress.
The earlier write test now waits for the actual application completion event;
the durable floor can legitimately become visible before that event is published.
Snapshot resumption now fences both the runtime and the managed client owner
before calling application code. An error or caught panic closes cloned read
and write endpoints immediately, without needing another owner poll. Existing
waiters remain owned until backend release. Three managed regressions reproduce
the earlier admission/panic defects; a fourth verifies successful installation
still serves the restored state. A direct runtime regression also rejects a
caught floor-callback panic, later input, and attempted resumption. All four
managed cases pass with the fix; against the prior implementation, the three
failure cases fail while successful installation still passes. The full runtime
suite passes 236 tests, retaining its two existing process-profile ignores.

Consensus input and polling now keep the client fence armed across backend
storage and command-adapter calls as well as outcome publication. Caught panics
close both endpoints immediately; they cannot leave newly accepted work waiting
for a later owner poll. Three real-worker fault cases cover election persistence,
proposal commit persistence, and committed-command decoding. Each fails against
the prior implementation. A fourth case verifies a temporary busy refusal keeps
healthy read and write admission usable. Release-order wakers check that both
storage and application ownership end before unresolved writes become terminal,
with the original request identity preserved.

The durable owner now exposes `RaftGroupMetrics<()>` with its verified application
durable floor and separate pending proposal, submitted barrier, pending query,
and uncollected proof counts. Local reads and completed errors are retained
credits rather than reserved Raft reads. Canceled quorum rounds stay counted
until their terminal output is consumed and owner cleanup releases them. Metrics
return `None` during node or application ownership handoff and after backend
release; no cursor or floor is invented. Future collection and cancellation
notify after releasing future ownership and any now-unowned credits, supporting
accurate later watch updates.
Nine focused cases exercise actual workers, real quorum traffic, failed/stopped
incarnations, and notification ordering. Snapshot-handoff and independent TLS
consumer cases also check these observations. Six deliberately incorrect floor,
proof-count, cancellation, and wake-order variants fail their exact regressions.

Metrics publication and closure now notify every registered subscriber before
resuming a custom waker panic. Previously, one panic discarded the rest of the
wakeups after removing their registrations, which could strand executor tasks.
Both publication and closure regressions fail the prior implementation; the
independent ledger consumer exercises terminal notification through public APIs.
The tests also preserve the first of multiple panics and verify reentrant
observation, visible state, registration cleanup, and subsequent publication.

The optional `DurableRaftDriver` now owns the client backend and an existing
transport sender while `DurableCommandSender` implements the managed handle
contract. Group validation precedes admission, and read proofs retain their
actual grant/floor under the chosen group identity. Metrics watches preserve
the last genuine snapshot during ownership handoffs. A single retained transfer
slot covers queued, submitted, and uncollected work; acknowledgement follows
immediate output routing, and backend release preserves uncertainty through
`Unconfirmed`. Duplicate and canceled shutdown requests cannot finish before
backend release. Disposal catches each callback panic separately so all clients
and watchers are notified before the first panic resumes.

Eighteen public group tests exercise real workers and quorum traffic, including
snapshot installation during shutdown, attachment refusal preserving resources,
cancellation and credits, transport loss followed by a durable write, and
backend/transport/metrics/terminal callback failures. Seven deliberately weakened
variants fail their regressions: wrong-group admission, early transfer success,
an enlarged control slot, early duplicate shutdown, terminal notification before
backend release, lost transfer uncertainty, and a lowered proof floor.
A late snapshot return after shutdown previously reported `NotReady(Stopped)`
but changed the wrapper to `Failed`. The new regression reproduces that mismatch
on the prior code. The guard now preserves `Stopped`, closed clients and metrics,
idempotent polling, and duplicate shutdown behavior; live-owner snapshot errors
still fence the incarnation.
The independent ledger consumer adopts the full handle over TLS through writes,
reads, leadership transfer, shutdown, and cold reopen. Inbound authentication, membership policy,
snapshot installation, and link lifecycle remain explicit embedding duties.
Managed membership and adoption by the full TLS process consumer remain open.
Local compaction now forwards through both managed owners without releasing
queued requests or uncollected results. It retains the runtime's actual failure
classification, honors handle shutdown before invoking the builder, and updates
metrics only after maintenance. Eight real-worker cases cover retained client
ownership, ordinary refusals, builder/floor failures, caught builder/source-drop
panics, and metrics notification failure after publication. Five deliberately
weakened failure/admission fences fail their exact regressions. The independent
TLS consumer compacts all three file-backed replicas through the grouped driver,
retires old entries with the bounded worker, transfers leadership, and reopens
the same application state. This maintenance remains synchronous; source staging,
chunks, and retired-entry disposal still require explicit budgets.
The managed TLS fixture now also routes elections through the grouped driver and
handles actual application snapshot handoff. A lagging replica recovers through
multiple authenticated chunks after the majority compacts and all links reopen,
so pre-compaction transport queues cannot satisfy catch-up. Its managed local
read remains pending while the application is handed out and metrics retain the
previous durable floor. Streamed atomic installation and resumption release the
query at the new floor. After another full reopen, that replica becomes leader,
serves a quorum read, and replays a cached write without changing the balance.
Skipping application installation makes the new case fail at its required read
floor. This extends independent source/archive consumer coverage; it does not
add a transport or change production code. Same-process TLS checks do not claim
OS-process fault isolation or physical power-loss coverage.

The feature lane requires exactly 393 tests in both CI and zcheck.

Managed write/read budget exhaustion now reports public `Backpressure` variants
and kinds instead of `Transport` with inaccessible private causes. The durable
owner counts requests and retained vector capacity; the existing transport
driver counts unresolved and retained waiters. Refusals allocate no operation ID,
append no command, and start no read barrier or query. Writes prove `NotAppended`.
Completed results and canceled in-flight operations keep their existing credit
lifetimes. A buffer exceeding the total byte budget needs a smaller retained
capacity or a larger configured budget, not repeated retries. These additive
non-exhaustive variants change the classification callers should handle for
budget exhaustion; actual transport errors retain their existing typed causes.
The external TLS ledger scenario exercises count and byte refusal through public
senders, cancellation/re-admission, then its usual durable writes, reads,
compaction, transfer, shutdown, and reopen sequence.

Two additional managed-write regressions partition a leader after two local
appends, elect the connected majority, and commit a different command at one
of the same log indices. They heal through ordinary replication and through
actual snapshot installation from the receiver's store. Both original callers
retain their local/application identities and receive `UnknownOutcome`; neither
can receive the replacement command's success. Uncollected terminal results
retain admission credits, collection releases them, and the former leader can
query the replacement state and admit a fresh request. Real persistence and
application workers drive genuine generated peer messages. Both cases reject a
mutation that omits managed proposal-drop handling. This adds qualification of
existing behavior, with no library change; that checkpoint passed the exact
395-test durable-service inventory. Strict durable-service Clippy and source/CI guards
pass, as do 128 replacement cases across 64 repetitions with four concurrent
runners. Architecture analysis passes with no new grants or debt; its candidate
inventory and qualification-input refresh still requires maintainer review.
The in-memory partition fixture does not qualify physical disk stalls or
snapshot publication under power loss.

A complementary case blocks the old leader's application after a command has
committed, lets the majority elect a new leader and compact later state, and
delivers the real snapshot while that worker remains blocked. Snapshot handoff
waits for the old application's completion: its client receives the original
index, term, and result before installation replaces the store. A subsequent
query sees the installed value. Deliberately substituting the new term fails
the receipt check; bypassing the busy-application handoff fence fails at the
lower layer's `ApplicationShutdown(Busy)` refusal. This also qualifies existing
behavior without changing production code. All 396 durable-service tests,
strict Clippy, and 13 relevant source/CI guards pass. The three replacement
cases pass 64 repetitions across four concurrent runners (192 executions).
Architecture analysis has no diagnostics, new grants, or debt; its candidate
inventory/input refresh retains the maintainer-review requirement.

The library checkpoint `9893ac18` passes 327 default service tests and 393 with
`durable`, plus the real counter pressure sequence, default-workspace and
feature Clippy, strict docs, and 45 source/API/CI guards. Its architecture change
tightens one file-size ratchet without grants or debt. The subsequent managed
snapshot fixture changes no library code and passes all six TLS owner cases,
540 complete source-consumer tests, and 545 archive/MSRV consumer tests on Rust
1.88.0. Both consumer lanes retain 57 explicitly ignored profiles. Strict consumer
lint/docs/formatting/size checks and zrail pass; this fixture slice needs no
architecture lock change. The PR's maintainer review remains open.
Earlier managed-client qualification also ran the 23 write/read tests for 64
repetitions across four concurrent runners (1,472 executions), with 105
source/API/CI guards and no new architecture grants or adoption debt. The current
archive lane passes all seven package phases, including published storage, peer
wire, and rolling TLS processes. The rolling runner resolves the unchanged process consumer and
its required workspace members for both versions; candidate-only ledger APIs
stay in the complete consumer lane rather than being requested from older
packages. Nineteen runner controls and strict lint/docs, source/dependency/size,
workflow/profile, and architecture checks pass. Hosted qualification of this
new candidate remains required.

The PR #13 counter now adopts the durable owner in real TLS processes, including
leader loss, crash recovery, uncertain writes, application-publication failure,
and graceful shutdown. A blocked-publication lane also fills the write budget
with canceled callers and verifies the read fence and shutdown during outstanding
work. Next qualify physical stalled fsync, sustained mixed client loads, and
snapshot installation/transfer. General managed
membership remains open; the counter deliberately supports only its fixed voter
set. An acknowledged write must survive recovery; an ambiguous write must retain
its identity and report an unknown outcome. Application idempotency must remain
explicit—the existing request ID is correlation, not deduplication.

The existing synchronous membership API now honors `RemoveNode` for learners.
It previously sent every removal to the voter-only kernel command and refused
an existing learner with `VoterNotFound`. Learner removal from a stable
configuration now uses the existing safe learner-only change path, preserving
the voters and other learners. Voter removal keeps its joint-consensus path;
unknown-node and last-voter refusals are unchanged. This repairs that existing
API; it does not add a managed membership executor to the durable owner.

The promotion planner also includes the final `LeaveJoint` obligation. Executing
its former two-step plan left both replicas in a committed joint configuration.
The caller must observe that joint commit before submitting the final step, then
observe the stable commit before completing the flow. A two-node protocol test
executes the returned obligations, requires stable membership on both replicas,
and successfully commits a subsequent learner addition.

The transport driver's `change_membership` now returns a typed
`ManagedDriverError::MembershipRejected` for protocol refusals, preserving the
reason and leader hint. Previously these requests returned `Ok(())` because
the group carried the refusal inside an otherwise successful step report. The
driver still routes the full report, reconciles membership, drains poisoned
waiters, and publishes metrics before returning. Regressions cover invalid and
last-voter removals, follower redirection, an uncommitted prior configuration,
successful retry, and other effects emitted alongside a refusal. Admission
still requires a later committed-membership observation to prove completion.

`MembershipController::remove_node_from` expands removal from a known stable
membership into explicit joint/finalization/fencing steps for voters, while
learners use the stable removal path. Other members are preserved; joint
starting configurations, unknown targets and last-voter removal are refused.
Real protocol/runtime tests execute both paths through the commit required for
fencing. The caller must serialize changes, recheck the starting configuration
and observe the required commits. The older `remove_node` plan remains a
high-level removal obligation: its executor must finish any voter finalization
inside `RemoveNode` before fencing. Mapping that obligation to just one kernel
input leaves a joint configuration; the new helper makes this work explicit.

`MembershipChange::into_raft_input` now exposes the same translation used by
`RaftGroup` to manually driven durable consumers. It preserves learner removal,
promotion barriers and explicit joint finalization without a second command
mapping. The caller supplies fresh effective membership, retries translation
when busy admission makes that observation stale, and retains responsibility
for metadata and transport policy. Real durable-driver tests cover learner
removal, voter removal with premature finalization refused, both promotion
forms, protocol refusals, and a durable write plus quorum read after removal.
This is a shared translation boundary; it does not yet add a queued membership
executor or a committed-change receipt to the durable owner.

The opt-in `DurableRaftDriver::attach_with_membership` now admits bounded
membership proposals through `DurableCommandSender::propose_membership`. One
retained control credit is shared with leadership transfer, including canceled
submitted work and uncollected results. Target and promotion-barrier vectors are
normalized and charged by capacity; opaque metadata is discarded. This bounds
retained queue payload, not transient dispatch allocations or the kernel log.
Translation uses effective membership at dispatch and preserves the original
request for busy retry. A receipt identifies the durably appended index and term
after immediate effects drain; it does not establish commit. Exact refusals and
uncertain release outcomes remain distinct. Default attachment keeps manual
membership inputs; managed attachment guards them to preserve correlation.
Sixteen real-worker regressions cover admission versus commit, prior-output
isolation, typed refusals, payload/credit bounds, cancellation, shutdown,
callback failures, and progress under client/control backlog. Full plan
execution, committed-change tracking, peer authorization and retirement
remain the embedding's responsibility.

The independent ledger consumer now exercises opt-in membership admission over
the existing TLS transport, file-backed Raft stores and application journal.
Voter-to-learner demotion and promotion each require matching index/term
configuration commits for both joint and stable phases. Cold reopens under
different leaders preserve membership, ledger writes and quorum reads. Recovery
observations cover configurations above the application's durable applied
floor; they do not imply a complete retirement history. A separate case obtains
an uncommitted proposal receipt while withholding follower owner turns, reopens
the WAL to verify the exact durable entry, and then observes another leader
replace it without a matching configuration commit. These are clean cold-reopen
cases with the same three authorized identities, not physical power-loss or
peer-retirement qualification.

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

`DurableCommandSender::retained` now exposes the existing write/query count and
encoded-capacity charges together with the shared membership/transfer credit
and its normalized payload charge. Observing these counters neither polls the
backend nor invokes callbacks, and uncollected results stay visible after
backend release. Disabled query support is distinct from an empty query budget.
The counters can change independently under concurrent admission/release and
do not reserve capacity or establish a whole-host memory ceiling. Real-worker
tests cover queued cancellation, paused application work and terminal retained
results; the independent TLS consumer also inspects its filled and drained
client budgets through this public endpoint.

`backpressure_refusals` adds cumulative observations of actual count and payload
budget refusals, shared by raw and grouped write/query senders. The control
counter includes both membership and transfer, including membership payload
normalization refusals. Other errors are excluded. The counters saturate,
survive backend release and reset with fresh owners after recovery; reading
them drives no work. Concurrent caller tests check exact quiescent totals,
unchanged admission state, error classification and pre-attachment history.
The independent TLS consumer verifies count and byte-budget refusal totals.

The durable service now retains the existing optional persistence-worker
diagnostics that completion consumption previously discarded. Borrowed access
through the service, client owner and grouped driver exposes the last consumed
sample without extra polling, callbacks or event reordering. Counters cover
the persistence thread's lifetime and exclude recovery and synchronous work on
the owner thread; pending or failed work can leave them stale. Collection stays
opt-in and should be disabled for timing runs. Runtime tests cover disabled
collection, passive observation during blocked persistence, shutdown and
fresh recovery. The file/TLS consumer checks actual write-stage counters and
preservation through local application snapshot maintenance.

The counter pressure lane above establishes one bounded overload/recovery case
with real processes and TLS. It does not establish latency distributions, memory
ceilings for the whole host, or sustained throughput.

A three-voter durable-driver regression now keeps independent four-request
write and read windows replenished across 128 writes and 128 local/quorum reads.
One-proposal batches preserve backlog in both queues. It checks progress in both
streams, retained count/byte limits, caller freshness floors, quorum views
against the acknowledged write history, and final follower durability. Forcing
either fixed scheduling priority fails with the corresponding starvation
verdict. This is a bounded regression with real workers and in-memory stores
and links; physical disk/TLS soak and latency qualification remain separate.

The independent ledger consumer now repeats its mixed workload through three
load/compaction/cold-recovery cycles, rotating through all three leaders while
preserving one ledger. Each cycle completes 128 deposits alongside 128 local/quorum
reads through real file stores, application journals and mutual TLS. Two-request
windows stay replenished; exact retained counts, bounded encoded capacities and
cumulative refusal counts are checked while both streams remain offered. Every
cycle checks receipt identity, freshness and quorum views against its acknowledged
history, converges all replicas, compacts their snapshots and reopens under the
next leader. Retrying the last acknowledged deposit must preserve the balance
without applying it twice; the recovered owner starts with empty client budgets
and no refusal history, then serves the next full workload. The complete case
covers 384 deposits and 384 mixed reads plus three recovery retries and quorum
reads. This is a bounded, single-host correctness case, not a sustained performance
or physical-fault claim; diagnostic collection remains enabled in this fixture.

Extending the file/TLS workload to 96 cycles at `4d20785d` exposed a ready-query
fairness defect after 61 completed cycles. Two of four diagnostic repetitions
also failed after eight and 35 cycles; the other two completed all 96. In a
failure, 64 writes completed while a local read and a quorum-confirmed read
remained pending, despite no pending leader read barriers or transport refusals.
The owner treated both an absent read and a busy application view as permission
to dispatch another write, allowing new writes to keep that view occupied.

The fix retains the ready read's admission turn until already-owned application
work releases the view. Three real-worker regressions cover repeated polling,
cancellation releasing the turn, and unconfirmed quorum reads allowing writes.
The original implementation fails the blocked-view regression; removing only
priority retention fails its repeated-poll check. All 440 service tests, strict
lint/docs, 47 guards and nine canonical TLS cases pass. The candidate also passes
all nine TLS cases with the temporary 96-cycle extension: 12,288 load writes, 12,288
mixed reads, 96 exact retries and 96 quorum reads after recovery. The source
snapshot is preserved, and all 831 public production Rust files match the applied
checkout. These are single-host correctness results with diagnostics enabled,
not throughput, whole-host memory or physical-fault guarantees.

The same backlog review reproduced a control-plane liveness defect: a full
local-read stream could indefinitely postpone an accepted or rejected leadership
transfer reply. The driver now drains the transfer's already-admitted effects
before taking fresh client work, then grants clients one bounded turn before
another control request can start. Discarding a queued cancellation also leaves
that client turn available. Four regressions cover progress in both directions,
completion of queued reads and release of their credits. A successful transfer
still requires its immediate sends to be routed; it does not promise that the
target has already been elected. Existing cancellation, failure and backend
release contracts remain covered by the grouped-driver suite.

Recovery of the shared WAL now bounds its fixed 36-byte authority manifest
read to 37 bytes, enough to refuse an oversized file. Previously the whole file
was read and allocated before its length was checked. Reader-budget, interrupted
I/O, truncation, checksum and trailing-byte cases preserve strict rejection;
a real oversized-manifest case leaves the selected WAL files intact and refuses
an obsolete fallback.

The fixed RFHS hard-state file also avoids whole-file allocation during open.
A streaming checksum reader retains its 47-byte body prefix and a 4 KiB read
buffer, preserving the original checksum-before-schema error precedence and
trailing-byte counts. The existing slice decoder is the oracle across truncated,
corrupt and re-signed inputs, including short/interrupted reads. An oversized
real file is refused without mutation. The full checksum still requires a
linear scan; other metadata readers and whole-host memory limits remain separate
qualification work.

The RFLC compaction marker uses the same bounded checksum reader with a 13-byte
body prefix. Its strict decoder remains the oracle for truncated, corrupt,
re-signed and oversized inputs, including the maximum log-index boundary.
An oversized real marker refuses both strict open and uncommitted-tail repair
without changing either file. Restoring a valid marker explicitly permits the
eligible tail repair while preserving the retained suffix. This removes another
whole-file allocation, while retaining a full linear checksum scan.

The RFSM current-snapshot selector also uses the bounded checksum reader. It
retains at most the format's 65,550-byte body, including the maximum `u16`
filename, and preserves the full trailing-byte count beyond that prefix.
Short/interrupted reads and malformed filenames match the existing decoder.
An oversized real selector refuses open without choosing an older snapshot or
cleaning up incomplete pending-transfer files; explicit restoration recovers
the selected snapshot and resumes normal cleanup.

Pending-transfer RFPT manifests retain at most the existing 4 MiB nested-metadata
cap plus their 53-byte header during outer checksum verification. The decoder
preserves nested checksum/payload errors, exact trailing-byte counts and their
precedence over transfer-progress/identity errors. Short/interrupted reads and
I/O failures retain the original diagnostics. An oversized real manifest leaves
the selected snapshot and resumable transfer body unchanged; explicit restoration
recovers both. These metadata readers still perform linear checksum scans.

The shared WAL batch decoder now grows its entry vector only after each nested
entry decodes successfully. A checksummed 16,413-byte malformed body previously
reserved 491,520 bytes from its claimed count before rejecting the first entry;
an isolated allocation probe now observes a largest allocation of 16,413 bytes
for the same refusal. Recovery regressions preserve diagnostics, outer-checksum
precedence and complete records in both legacy and manifest-selected generations,
and reopen a valid 4,096-entry batch. The existing 64 MiB body bound and legitimate
decoded-entry memory still apply; this is not a whole-process memory ceiling.
The probe is now a recurring `check-wal-recovery-memory` binary in the existing
standalone benchmark package. CI `lint` and local `zcheck` lint execute four
modest-count cases, with and without a valid entry prefix. They bound both the
largest Rust allocation request and cumulative requested bytes during refusal,
while requiring the nested-entry error and unchanged WAL contents. This measures
allocation requests rather than live heap or RSS; unrelated benchmark comparator
features stay disabled.

The selected WAL checkpoint reader also grows encoded-entry buffers from bytes
actually delivered by its bounded reader. A 123-byte truncated checkpoint
previously requested a 64 MiB allocation from its forged entry length before
returning `UnexpectedEof`. The same probe now makes no allocation larger than
the existing 256 KiB read buffer. Geometric growth is capped at the complete
entry size; short reads and interruptions remain supported, unrelated I/O errors
propagate, and the running checksum advances only after a complete entry.
Five reader regressions cover these boundaries and buffer reuse. The existing
allocation gate additionally publishes and reopens a real checkpoint, tests six
truncation cases with 8/64 MiB claims, preserves the selected checkpoint, manifest
and segment bytes, and reopens the original state after restoration. Its bounds
follow actual supplied bytes plus fixed recovery buffers. Valid entry decoding,
the 64 MiB size limit and on-disk formats are unchanged.

Extend the existing qualified benchmarks to separate hosts and disks, with
slow/minority followers, lossy links, disk pressure, compaction, recovery, and
TLS enabled. Publish throughput alongside p99/p99.9 latency, retained memory,
write amplification, and restart/catch-up time. Preserve the present clear
separation between in-memory and durable results. Require bounded overload
behavior and recovery before pursuing a larger throughput headline.

The candidate now adds a separate `current-stable` CI build lane and
`zcheck run current-stable`, also included in the opt-in `full` graph. It builds
every workspace target and the optional durable-service targets with locked
dependencies and records the resolved compiler identity. CI retains both Rust
and Cargo identities. The current stable channel resolves to Rust 1.99.0 on
October 11, 2026. Rust 1.88 remains the strict test/lint/documentation and
exact-package baseline; this additive compiler lane does not change the MSRV.

The [platform qualification matrix](platform-qualification.md) records native
target coverage and filesystem evidence; stable support commitments remain open.
The existing macOS 15 CI job now runs the complete locked storage
suite alongside strict lint and launcher checks. Storage failures propagate
through the required `invariants-pr` aggregate, and the normal diagnostics
artifact retains the storage log. Linux runs the same storage tests through
the workspace suite. This adds recurring native recovery/format/ownership
coverage; it does not establish physical power-loss behavior or a wider
filesystem support policy.
An initial RustSec audit of all eleven publishable crates, including the durable
feature, found the workspace's `rustls 0.23.42` affected by
[RUSTSEC-2026-0285](https://rustsec.org/advisories/RUSTSEC-2026-0285.html).
The candidate raises both rustls manifest floors to `0.23.45` and updates the
workspace, benchmark, and standalone-counter locks; the reference and fuzz
locks already selected that patched version. The required WebPKI update is
also retained in the workspace and benchmark locks. A forced downgrade to
`0.23.44` is refused by the new manifest floor.

The isolated publishable graph contains 52 packages, including 40 third-party
packages whose exact identities match the workspace lock. With cargo-audit
0.22.2 and advisory database `7eebec69c352c7191b1f13eb95dd510eeca5d1de`, the
patched graph reports zero advisories and warnings with no ignored advisories
or platform filters. The normal/build dependency closure of the actual archive
consumer also passes the same audit: all eleven public roots and 40 third-party
packages, preserving the consumer lock's exact versions and checksums. Both
audits cover the optional durable service feature. The TLS crate passes all
159 tests and strict Clippy. All seven archive/MSRV phases pass on Rust 1.88.0,
including 545 tests, storage and wire compatibility, and rolling TLS upgrades;
57 existing process/profile cases remain assigned to their dedicated lanes.
Architecture analysis passes, with the rustls requirement replacement recorded
as one granted and one revoked dependency edge requiring maintainer review.
The advisory check is now an executable per-PR `dependency-audit` job and
`zcheck run dependency-audit`, included in `full`. It discovers the public set
from Cargo and checks the release inventory, enables every feature in an isolated
consumer, and proves the resolved registry versions/checksums match the workspace
lock before auditing. It retains the lock, graph, tool versions, exact manifest
hashes and freshly fetched database identity. Missing roots/features, unexpected
paths, changed versions/checksums, narrowed advisory settings, vulnerabilities,
informational warnings and yanked packages fail. An explicit isolated policy
prevents inherited developer exclusions. License policy still requires review.
Eleven adversarial controls pass, and the real pre-fix rustls lock is rejected
with RUSTSEC-2026-0285. The existing 28 CI contracts and all ten immutable action
pin checks pass. The audit gate's three source files are content-addressed by
the architecture contract without adding dependency grants or adoption debt.

A separate complete-workspace audit found quick-xml 0.39.4 affected by
[RUSTSEC-2026-0194](https://rustsec.org/advisories/RUSTSEC-2026-0194.html) and
[RUSTSEC-2026-0195](https://rustsec.org/advisories/RUSTSEC-2026-0195.html).
It is only a dev dependency of the unpublished invariant verifier, used by one
generated-JUnit parseability test through a plain `Reader`; that use does not
iterate attributes or resolve namespaces. The requirement and lock now select
patched 0.41.0 (dependency MSRV 1.79), without changing public dependencies.
The full 272-package workspace lock, including 256 registry packages, now passes
the same strict audit with no exclusions. All three report tests preserve the
golden formats, escaping and complete invariant coverage. The separate benchmark,
fuzz and reference workspaces are outside this root-lock assessment.
CI and the local `dependency-audit` task now also audit this complete root lock
through `--include-workspace`. They retain separate public and workspace locks,
raw reports, package counts and database identities. A clean public graph cannot
mask a workspace advisory, checker failure or a changed audit input. Fourteen
controls exercise those boundaries; neither graph uses advisory exclusions.

Exact-archive dependency auditing is now executable in both per-PR package
jobs. Offline package qualification retains the final lock/metadata pair and
projects all public archives' normal/build dependencies, requiring every public
feature. The separate online audit verifies that projection and its source
hashes before and after inspection, with the same pinned checker and explicit
policy as the source audit. It preserves the tested versions and checksums
without resolving dependencies again. The 51-package archive graph, including
40 registry packages, passes; 25 projection/audit controls cover substituted
inputs, missing features/archives, private paths, changed evidence, checker
failures and refusal to overwrite prior receipts. The seven offline MSRV and
compatibility phases pass, with 545 tests and 57 existing ignored profiles.

The reference workspace's sole advisory warning was the unmaintained
`rustls-pemfile` dev dependency ([RUSTSEC-2025-0134](https://rustsec.org/advisories/RUSTSEC-2025-0134.html)).
Its frozen TLS client's certificate/key fixture loading now uses the maintained
`PemObject` API already re-exported by rustls, removing the wrapper without a new
dependency or changes to its wire encoder. The reference lock now audits cleanly
with 55 packages and 40 registry dependencies. The standalone fuzz and counter
locks also audit cleanly. Benchmark comparator dependencies remain a separate
assessment: their lock reports protobuf and rkyv advisories, unmaintained fxhash,
and previously yanked validit; these packages are outside the shipped library
graph. The validit lock is now 0.2.6, whose twelve Rust source files match 0.2.5;
the release also declares the correct Rust 1.88 minimum. OpenRaft remains pinned
to 0.9.24. Strict comparator Clippy and all three release workloads pass on Rust
1.88 (2,000 serial, 20,000 pipelined and 256 large-payload proposals). This is
execution qualification, with no new published performance comparison.
The yanked warning is cleared without exclusions. Protobuf and fxhash are in
the active raft-rs comparator tree. The later benchmark-only update to
[`rust_decimal` 1.43.0](https://github.com/paupino/rust-decimal/releases/tag/1.43.0)
removes its unused optional rkyv 0.7 dependency and the obsolete dependency chain,
reducing that lock from 211 to 195 packages. The fresh before/after audit against
RustSec database `25a288f68f2d5ebe658b243a334110c7217a2569` confirms that
RUSTSEC-2026-0235 is removed. It still refuses the complete benchmark graph for
protobuf's RUSTSEC-2024-0437 and fxhash's RUSTSEC-2025-0057, with no exclusions.
The affected OpenRaft comparator passes strict Clippy, a release build, and all
three workloads on Rust 1.88 (22,256 proposals). Comparator versions, Rafter
library source, and the root workspace lock are unchanged. This closes one
benchmark dependency finding; it does not supply a clean benchmark audit or a
new performance comparison.
