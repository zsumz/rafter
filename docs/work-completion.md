# Work Completion

This document is the non-release evidence map for the completed Rafter
foundation. It names stable commands and workflow jobs, not one transient green
run. `scripts/work-completion-check` verifies every script and job named in the
machine-readable manifest at the end of the document.

This is engineering-completion evidence. It does not create an RC, a `1.0.0`
release, a version change, a tag, a publication, or a mixed-version guarantee.

## Completed acceptance systems

| System | Completed capability | Stable proof |
| --- | --- | --- |
| Durable storage | Native format, recovery, ownership, and failure-boundary tests on Linux and macOS | `cargo test --locked -p rafter-storage`; CI `test` and `invariants-launcher-macos` |
| Durable storage | Allocation-counted refusal of malformed shared-WAL entry counts, preserving error and file contents | `zcheck run wal-recovery-memory`; CI `lint` |
| Durable storage | Verified process termination at 36 streamed/publication/promotion/cleanup cases, preserved recovery inputs and subsequent publication | `zcheck run snapshot-process`; CI `test`; I/O qualification `snapshot-publication` |
| Replicated ledger | Deterministic application/recovery acceptance and an independent bounded linearizability checker | `scripts/reference-source-check`; CI `reference-source` |
| Replicated ledger | Durable per-replica process composition, including fail-closed journal repair escalation | `scripts/reference-process-check`; CI/main and Nightly `reference-process` |
| Replicated ledger | Exact archive deterministic and process execution | `scripts/reference-package-check`; `scripts/reference-package-process-check`; CI `reference-package` and `reference-package-process` |
| Fast durable service | Exact-archive execution of the three-voter WAL, persistence-pipeline, bounded-transport, durable-application, snapshot, and recovery composition | `scripts/reference-package-check`; CI `reference-package` and `reference-package-msrv` |
| Durable service owner | Fixed 96-cycle cold-recovery and 256-cycle continuous-owner file/TLS workloads; bounded credits, fairness, compaction, quorum reads, and deduplicated retry | `scripts/reference-owner-soak`; Nightly `reference-owner-soak` |
| Standalone TLS counter | Extracted teaching application with checked dependency provenance, bounded durable owner, blocked-publication backpressure, and real process failures/recovery in checkout and pinned-Git modes | `scripts/check-counter-guide`; CI `lint` |
| Fenced lock | Deterministic lock histories, an independent linearizability checker, and an independent guarded-resource checker | `scripts/reference-source-check`; CI `reference-source` |
| Fenced lock | Insecure integration process composition and the authenticated bounded production-composition fixture | `scripts/reference-process-check`; CI/main and Nightly `reference-process` |
| Fenced lock | Exact archive deterministic and process execution | `scripts/reference-package-check`; `scripts/reference-package-process-check`; CI `reference-package` and `reference-package-process` |
| Sharded counter | Public managed-scheduler adoption with independent scheduler/oracle/fairness auditing | `scripts/reference-source-check`; CI `counter-reference-fast` |
| Sharded counter | Deterministic 64, 1,024, and 4,096-group profiles with retained seeds and histories | `scripts/counter-profile counter-fast`; Nightly `counter-reference-nightly`; Weekly `counter-reference-weekly` |
| Sharded counter | Durable authenticated process composition with multiplexed TLS host connections, fail-closed transport sessions, replayable removal, pass-aware fairness evidence, and exact-archive process execution | `scripts/reference-process-check`; `scripts/reference-package-process-check`; CI/main `reference-process` and `reference-package-process` |

The ledger repair acceptance test is intentionally `#[ignore]` because it
starts and kills operating-system processes. It is selected by
`verification/reference-process-test-inventory.txt` and executed with
`--ignored` by `scripts/reference-process-check`. In CI, the ordinary `test`
job runs the replicated-KV process test; the separate `reference-process` job
runs the ledger, lock, production-lock, and counter process inventories. Looking
only at the ordinary process step therefore does not establish ledger coverage.

The macOS job also runs the complete locked storage suite, including integration
and documentation tests, and retains `macos-storage.log` with its diagnostics.
Its failure propagates through the existing required `invariants-pr` aggregate.
The native Linux test and macOS storage jobs also retain a storage-environment
receipt before execution, including the filesystem and mount options for the
workspace, Rust-selected temporary directory, and runner temporary directory.
The recorder queries the active Rust toolchain for its temporary path, rather
than inferring it from another language. These observations identify the tested
environment at capture time; supported filesystem and physical power-loss
qualification remain separate release work.

The sharded-counter process inventory also fails closed on missing application
records for active, Removed, and Tombstoned slots; sweeps every directed
retirement-intent crash point; restarts a terminal tombstone; and requires at
least one fully certified immutable scheduler pass. The shared process client
does not replay opaque requests after a send- or receive-stage failure. Its
transport inventory additionally refuses missing or corrupt durable connection
sessions and sends a stale-incarnation probe over the normal authenticated
multiplexed peer connection.

## Public-surface and architecture completion

- `scripts/rustdoc-check` builds the eleven publishable crates together under
  `-D warnings -D missing-docs`. This compiler backstop covers public variants,
  fields, trait methods, associated types, and re-exports. It then builds the
  full workspace under `-D warnings`.
- `public_api_docs_guard` remains the source-policy gate for intentional enum
  exhaustiveness, untracked public-doc TODO/FIXME markers, risky library
  macros, and the reviewed invariant allowlist.
- `scripts/check-public-api` compares every public crate against published
  `0.0.2-alpha.1` with default and all features. It retains the current working
  source snapshot and requires complete, unsuppressed patch-level comparisons
  from the pinned checker. CI `public-api` covers the Linux host target;
  other targets and feature combinations remain separate qualification.
- `managed_policy_boundary` rejects scheduler dependencies on consumer
  schemas, lifecycle-retention policy, authentication/certificate policy,
  unpublished simulation hooks, and test-only observation surfaces.
- Exact-package lanes compile every public crate used by the consumers from the
  archives they receive. They also execute the packaged fast durable-service
  reference. `rafter` now declares no features at all, so that shape is also
  what every workspace command builds; the lanes' rejection of hidden test
  features remains as a standing boundary against reintroduction. The per-PR
  package and MSRV jobs also audit their retained public dependency graphs,
  separately from the source/workspace audit and offline package qualification.

## Verification lanes

| Tier | Stable jobs and commands | Claim |
| --- | --- | --- |
| Every pull request | CI `lint`, `current-stable`, `public-api`, `dependency-audit`, `test`, `reference-source`, `reference-package`, `reference-package-msrv`, `counter-reference-fast`, and `invariants-pr` | Workspace quality; current-stable workspace and optional durable builds; published source API comparisons; public dependency advisories across all features and a separate complete-workspace audit; source and exact-package deterministic consumers; process and owner-soak inventory membership; published-shape Rust 1.88; fast scheduler profile; deterministic 44-invariant aggregate |
| Push to main | CI `reference-process` and `reference-package-process` | Every reviewed durable process suite in source and exact-package shapes |
| Nightly | Nightly `reference-process`, `reference-owner-soak`, `burn-in`, `invariants-nightly`, `invariants-maelstrom`, `multi-gigabyte-test`, and `counter-reference-nightly` | Process rerun, repeated owner load/recovery, burn-in, randomized/replayable invariant evidence, real pinned Maelstrom, bounded multi-gigabyte snapshot streaming, and the 1,024-group scheduler profile |
| Weekly | Weekly `invariants-weekly`, `invariants-maelstrom`, and `counter-reference-weekly` | Deep tests/model checking, storage and snapshot histories, three-trial pinned Maelstrom, and the retained 4,096-group scheduler profile |
| Manual exact evidence | `scripts/reference-package-process-check` | One archive set, recorded SHA-256 hashes, every exact-package process suite, and published-shape Rust 1.88 smoke |

The owner-soak job records every tracked source hash and requires a clean checkout
before execution. Its always-run verification requires the same Git head, the
same tracked bytes and a clean checkout afterward, including when the soak fails.
Both inventories and status records accompany the log. The fast, nightly, and
weekly counter-profile jobs enforce the same clean-source endpoint checks and
retain compiler identity, workload output, and the complete profile log. Their
64-, 1,024-, and 4,096-group histories use the real adapter in a deterministic
in-memory fixture; they do not measure file-backed or multi-host TLS service
performance. The generated `reference/Cargo.lock` remains a separate retained
input for these jobs and the owner soak, not a tracked source file. These
endpoint checks detect retained changes; they do not prove that a file was never
transiently modified and restored during execution.

The Maelstrom workflows use the repository-pinned verifier setup and preserve
receipts/replay artifacts. They are sampled system evidence and do not replace
the deterministic invariant verdict.

## Additive capabilities and remaining work

- C1 streaming snapshots were outside the completed initial scope. The additive
  `SnapshotPayloadReader` verifies bounded, cancellable reads from the existing
  descriptor/chunk path before application publication. Its companion
  `SnapshotPayloadWriter` computes a descriptor while encoding into private
  staging without retaining the payload. Both live in the runtime layer.
  The ledger integrates both paths with its application schema and recovery
  tests. Other applications supply their own schema and publication policy;
  physical power-loss qualification remains open. See
  [release readiness](release-readiness.md#4-decide-the-application-snapshot-boundary-before-freezing-the-api).
- C2 replication pipelining is now an opt-in additive runtime interface. Its
  bounded persistence worker removes the benchmark's private executor, and a
  complete public-API reference composition now exercises WAL storage, bounded
  transport, durable application, snapshots, and recovery from an exact
  package archive. The default-off `rafter-service/durable` feature now adds a
  bounded client owner and a driver attached to the existing TLS transport.
  Automatic membership-plan execution, persistent retirement policy, and
  deployment management remain embedding responsibilities.
- `rafter-sim` no longer depends on a hidden core-crate feature. The kernel
  self-check it needed is the documented public `Node::validate_derived_state`,
  and `internal-test-hooks` is deleted from `rafter`. The crate stays
  unpublished by decision rather than by blocker; adding a simulation and
  model-checking harness to a publish list is separate work nobody has taken
  on.
- `lock-production-node` is a bounded acceptance fixture proving that the
  public crates compose with authenticated transport, durable identity/replay
  state, recovery, readiness, and resource limits. It is not a generic server,
  transport product, certificate platform, or deployment controller.

## Exact-run evidence

A completion run records the checked-out SHA, toolchains, commands, counts,
seeds, archive SHA-256 values, and artifact directories in an external
run-specific evidence directory. The directory is deliberately not named in
this stable manifest: a new run must not make an old workflow ID look current.
The handoff for a completed run reports that directory and exact SHA together.

## Machine-readable proof manifest

The following block is consumed by `scripts/work-completion-check`. Script
paths are repository-relative. Workflow job names are YAML job identifiers,
not display names.

```text completion-manifest
script scripts/rustdoc-check
script scripts/check-public-api
script scripts/check-counter-guide
script scripts/record-storage-environment
script scripts/audit-published-dependencies
script scripts/reference-source-check
script scripts/reference-package-check
script scripts/reference-process-check
script scripts/reference-owner-soak
script scripts/reference-package-process-check
script scripts/counter-profile
script scripts/maelstrom-lin-kv
script scripts/raft-burn-in
workflow-job .github/workflows/ci.yml lint
workflow-job .github/workflows/ci.yml current-stable
workflow-job .github/workflows/ci.yml public-api
workflow-job .github/workflows/ci.yml dependency-audit
workflow-job .github/workflows/ci.yml test
workflow-job .github/workflows/ci.yml invariants-maelstrom
workflow-job .github/workflows/ci.yml invariants-launcher-macos
workflow-job .github/workflows/ci.yml invariants-pr
workflow-job .github/workflows/ci.yml reference-source
workflow-job .github/workflows/ci.yml reference-package
workflow-job .github/workflows/ci.yml reference-package-msrv
workflow-job .github/workflows/ci.yml reference-package-process
workflow-job .github/workflows/ci.yml reference-process
workflow-job .github/workflows/ci.yml counter-reference-fast
workflow-job .github/workflows/io-stall.yml snapshot-publication
workflow-job .github/workflows/nightly.yml burn-in
workflow-job .github/workflows/nightly.yml invariants-maelstrom
workflow-job .github/workflows/nightly.yml invariants-nightly
workflow-job .github/workflows/nightly.yml reference-process
workflow-job .github/workflows/nightly.yml reference-owner-soak
workflow-job .github/workflows/nightly.yml multi-gigabyte-test
workflow-job .github/workflows/nightly.yml counter-reference-nightly
workflow-job .github/workflows/weekly.yml invariants-maelstrom
workflow-job .github/workflows/weekly.yml invariants-weekly
workflow-job .github/workflows/weekly.yml counter-reference-weekly
```
