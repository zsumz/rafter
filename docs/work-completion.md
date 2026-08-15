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
| Replicated ledger | Deterministic application/recovery acceptance and an independent bounded linearizability checker | `scripts/reference-source-check`; CI `reference-source` |
| Replicated ledger | Durable per-replica process composition, including fail-closed journal repair escalation | `scripts/reference-process-check`; CI/main and Nightly `reference-process` |
| Replicated ledger | Exact archive deterministic and process execution | `scripts/reference-package-check`; `scripts/reference-package-process-check`; CI `reference-package` and `reference-package-process` |
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

The sharded-counter process inventory also fails closed on missing application
records for active, Removed, and Tombstoned slots; sweeps every directed
retirement-intent crash point; restarts a terminal tombstone; and requires at
least one fully certified immutable scheduler pass. The shared process client
does not replay opaque requests after a send- or receive-stage failure. Its
transport inventory additionally refuses missing or corrupt durable connection
sessions and sends a stale-incarnation probe over the normal authenticated
multiplexed peer connection.

## Public-surface and architecture completion

- `scripts/rustdoc-check` builds the ten publishable crates together under
  `-D warnings -D missing-docs`. This compiler backstop covers public variants,
  fields, trait methods, associated types, and re-exports. It then builds the
  full workspace under `-D warnings`.
- `public_api_docs_guard` remains the source-policy gate for intentional enum
  exhaustiveness, untracked public-doc TODO/FIXME markers, risky library
  macros, and the reviewed invariant allowlist.
- `managed_policy_boundary` rejects scheduler dependencies on consumer
  schemas, lifecycle-retention policy, authentication/certificate policy,
  unpublished simulation hooks, and test-only observation surfaces.
- Exact-package lanes compile every public example and target against the
  archives a consumer receives. `rafter` now declares no features at all, so
  that shape is also what every workspace command builds; the lanes' rejection
  of hidden test features remains as a standing boundary against reintroduction.

## Verification lanes

| Tier | Stable jobs and commands | Claim |
| --- | --- | --- |
| Every pull request | CI `lint`, `test`, `reference-source`, `reference-package`, `reference-package-msrv`, `counter-reference-fast`, and `invariants-pr` | Workspace quality; source and exact-package deterministic consumers; process-inventory membership; published-shape Rust 1.88; fast scheduler profile; deterministic 44-invariant aggregate |
| Push to main | CI `reference-process` and `reference-package-process` | Every reviewed durable process suite in source and exact-package shapes |
| Nightly | Nightly `reference-process`, `burn-in`, `invariants-nightly`, `invariants-maelstrom`, `multi-gigabyte-test`, and `counter-reference-nightly` | Process rerun, burn-in, randomized/replayable invariant evidence, real pinned Maelstrom, bounded multi-gigabyte snapshot streaming, and the 1,024-group scheduler profile |
| Weekly | Weekly `invariants-weekly`, `invariants-maelstrom`, and `counter-reference-weekly` | Deep tests/model checking, storage and snapshot histories, three-trial pinned Maelstrom, and the retained 4,096-group scheduler profile |
| Manual exact evidence | `scripts/reference-package-process-check` | One archive set, recorded SHA-256 hashes, every exact-package process suite, and published-shape Rust 1.88 smoke |

The Maelstrom workflows use the repository-pinned verifier setup and preserve
receipts/replay artifacts. They are sampled system evidence and do not replace
the deterministic invariant verdict.

## Deferred additive work

- C1 streaming snapshots are outside this completed initial scope. A future
  public streaming interface remains additive; the existing bounded
  descriptor/chunk path and its current contracts are unchanged.
- C2 replication pipelining is outside this completed initial scope. A future
  pipelined replication interface remains additive; no compatibility promise
  is inferred here.
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
script scripts/reference-source-check
script scripts/reference-package-check
script scripts/reference-process-check
script scripts/reference-package-process-check
script scripts/counter-profile
script scripts/maelstrom-lin-kv
script scripts/raft-burn-in
workflow-job .github/workflows/ci.yml lint
workflow-job .github/workflows/ci.yml test
workflow-job .github/workflows/ci.yml invariants-maelstrom
workflow-job .github/workflows/ci.yml invariants-pr
workflow-job .github/workflows/ci.yml reference-source
workflow-job .github/workflows/ci.yml reference-package
workflow-job .github/workflows/ci.yml reference-package-msrv
workflow-job .github/workflows/ci.yml reference-package-process
workflow-job .github/workflows/ci.yml reference-process
workflow-job .github/workflows/ci.yml counter-reference-fast
workflow-job .github/workflows/nightly.yml burn-in
workflow-job .github/workflows/nightly.yml invariants-maelstrom
workflow-job .github/workflows/nightly.yml invariants-nightly
workflow-job .github/workflows/nightly.yml reference-process
workflow-job .github/workflows/nightly.yml multi-gigabyte-test
workflow-job .github/workflows/nightly.yml counter-reference-nightly
workflow-job .github/workflows/weekly.yml invariants-maelstrom
workflow-job .github/workflows/weekly.yml invariants-weekly
workflow-job .github/workflows/weekly.yml counter-reference-weekly
```
