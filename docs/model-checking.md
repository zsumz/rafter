# Model Checking

`rafter-model-check-fast` explores bounded, deterministic Raft schedules. The
profiles differ in bounds and scheduling breadth; all exhaustive checks must
end with `frontier_exhausted`. A state, time, or memory budget ending the run
is incomplete coverage, not a pass.

## State Counts

Each exhaustive check reports two distinct cardinalities:

- **Protocol states** hash the simulated protocol and scheduler state while
  excluding retained verifier history. Scheduler counters remain included, so
  this is the model state used by the explorer, not a count of abstract Raft
  paper states.
- **Verifier states** hash the complete exploration state, including retained
  evidence needed to detect temporal violations. This is the deduplication and
  unique-state-budget key.

Profile totals add each check's independently explored cardinality. They are
not a globally deduplicated union. The scheduled `raft-nightly` and
`raft-weekly` gates enforce unchanged lower bounds on both totals: 100 million
and 250 million states respectively.

## Cost Evidence

Run a source-bound comparison with:

```sh
MODEL_CHECK_BASE_REF=main \
MODEL_CHECK_PROFILES=fast \
MODEL_CHECK_RUNS=6 \
scripts/model-check-profile-compare
```

The harness builds measured commits in release mode with `--locked`, then
alternates base/current execution order across six paired runs, balancing each
revision in each process-order position. Multiple profiles also run in
alternating order. It consumes structured `RAFTER_EVENT` records, requires
independent protocol and verifier counts, requires every exhaustive check to
pass with an exhausted frontier, and rejects shape drift between repeated
samples of one revision. Human-readable summary lines and legacy compatibility
counts are never accepted as calibration data. Profiles used for cost
comparison must contain at least one exhaustive check; the soak-only profile
remains liveness evidence rather than state-space cost evidence.

Comparisons that do not cross a reviewed profile-contract change remain strict
like-for-like runs: profile headers, check IDs, and configured depths must be
identical. A mismatch without a matching source-controlled migration is a
harness error and therefore red.

### Pinned Contract Migrations

`verification/model-check-contract-migrations.json` is the only migration
input. It pins the migration commit and its sole parent, the exact changed-path
set, canonical old/new contract digests for every affected profile, and every
configured-depth increase. The planner verifies those identities against Git
and requires the requested baseline to be an ancestor of the current commit.
There is no runtime flag that permits arbitrary bound drift.

When a comparison crosses a pinned migration, the harness emits three evidence
segments:

1. requested baseline to the pivot parent, under the old contract;
2. pivot to current `HEAD`, under the new contract; and
3. pivot parent to pivot, as a two-run contract and coverage delta.

The unchanged 2.25x wall and 1.75x peak-RSS ceilings apply independently to
both non-empty like-for-like segments. The migration delta is not a performance
comparison. It must reproduce both pinned contract digests, preserve profile
semantics and the exact check set, and match only the reviewed monotone depth
increases. Every increased bound must reach a deeper frontier with nondecreasing
protocol-state, verifier-state, and explored-action counts. A segment whose
endpoints are the same commit is explicitly marked not required. Missing,
malformed, failed, or source-mismatched segment evidence makes the aggregate red.

Schema-v3 `compare.json` preserves source trees, lockfile and binary digests,
toolchain and host metadata, every raw sample, additive state totals, wall time,
peak RSS, the validated migration identity when applicable, complete segment
reports, and per-check coverage deltas. `compare.md` summarizes the aggregate.
CI uploads the report, raw events, timing logs, build logs, and a SHA-256
manifest even when validation or report construction fails. Main pushes use the
pre-push commit as baseline; scheduled runs use `HEAD^`; manual runs require an
explicit baseline input.

The first comparison against a revision that predates structured events uses
the source-recorded evidence-format baseline `9770d1a` and records both the
requested and effective baseline. It never parses legacy human output as
equivalent evidence. Every like-for-like segment requires an unchanged
protocol-state shape plus paired median current/base ceilings of 2.25x wall
time and 1.75x peak RSS. Verifier-state growth is reported separately and is
expected when sound history is added; protocol-state drift or a cost ceiling
breach fails the job after the JSON and Markdown reports have been written.

The default requires a clean checkout so a commit names the measured source.
`MODEL_CHECK_ALLOW_DIRTY=1` exists only for directional local experiments; such
a run records `clean: false` and is not release or threshold evidence.

## Producer Provenance Threat Model

Invariant producers run on a trusted CI host. Before `run` or `run-all` executes
evidence checks, the CLI publishes its bytes as a regular, non-symlink,
read-only executable at
`target/rafter-invariants/producer-images/<sha256>/rafter-invariants` and
re-executes that image. Schema-v13 receipts bind the exact path, digest, and
preserved executable artifact. This prevents nested Cargo builds, stale target
paths, partial publication, symlinked artifact paths, and later deletion of the
bootstrap executable from changing which producer image the aggregate accepts.

Schema-v13 source receipts also carry a `git-head-worktree-raw-v1`
materialization. The producer enumerates the immutable `HEAD` tree with Git
replacement objects disabled, rejects tracked symlinks, and reads each tracked
regular file as raw bytes. It checks every Git blob ID and the exact owner
executable bit, then SHA-256 binds the ordered mode, path, and content inventory.
This catches index flags such as
`assume-unchanged` and `skip-worktree` that can make porcelain status appear
clean after bytes or modes change. Ignored paths are permitted only in reviewed
generated-output roots (including the invariant harness's own nested Cargo
target), and ignored symlinks fail closed. Rust input validation starts from
each exact resolved workspace and path-package Cargo target root, treats those
roots as Rust regardless of filename extension, and follows only actual tracked
module, `include!`, and literal `#[path]` edges transitively with the same rule;
unreferenced source files do not create inputs. Raw include and path identifiers
are normalized, and direct, qualified, transitively included, and multi-hop use
aliases are resolved to a fixed point before validation. Macro-generated,
dynamically selected, or target-conditional compiler inputs fail closed.
Workspace and path-package build scripts are prohibited. Registry
build scripts are admitted only from
the full locked metadata graph when their crate archive has a Cargo.lock
checksum; the lockfile binds their source archive and the preserved producer
executable digest binds their compiled effects. Gitlinks, noncanonical paths,
filesystem aliases, and platform materializations that cannot preserve the
reviewed raw-byte and mode semantics fail closed; symlinks and submodules are
not part of the contract.

The registry checksum is a source-identity proof, not a hermetic-build proof.
A registry build script can observe host files, clocks, kernel behavior, or
other runner state that Cargo.lock does not describe. Effects that reach the
producer executable are nevertheless frozen by the independently preserved
executable digest, and aggregation executes that exact artifact rather than
rebuilding it. Effects that depend on external state without being reflected in
the executable, malicious build scripts, and compromised build runners remain
outside this portable contract. Proving the stronger source-to-binary claim
would require a hermetic build sandbox and external build attestation; the
invariant report does not claim that property.

This is deterministic repository provenance, not hostile-host attestation. It
does not defend against a malicious producer binary, compromised kernel or CI
runner, SHA-256 compromise, or a hostile same-UID process that can replace files
between verification and `exec`. Those threats require an external build
attestation system or OS-specific sealed execution and are outside the portable
Linux/macOS invariant gate contract.

Source capture also fails closed on Cargo configuration that can alter the
compiled dependency graph without identifying the replacement source. In
particular, every top-level `[patch]` configuration is forbidden, including a
patch whose path currently points inside the checkout. The receipt binds the
configuration bytes and path string, but it does not recursively bind an
arbitrary replacement source tree. Reviewed overrides therefore belong in the
tracked workspace manifests and lockfile, not in ancestor or Cargo-home
configuration.

Receipt `duration_ms` and `peak_rss_kib` fields are execution metrics derived
only from the hashed child-process logs attached to that receipt. They measure
the compiled tests, simulator/model checker, TLC, or Maelstrom process groups;
they do not claim to measure parent-producer planning, source capture, artifact
hashing, or report rendering. Model-check performance comparisons use the
simulator process group's wall time and peak RSS together with separately
reported protocol-state and verifier-state counts.

Simulator detector fixtures have two independent execution checks. Source
preflight resolves local calls by exact crate-module identity across the tracked
Cargo target graph and recursively inspects every plausible reachable helper.
Untracked, symlinked, out-of-tree, or item-macro-generated source outside the
bound test context fails closed. The analyzer binds `test`, the host target,
and disabled package features to the exact host-targeted
`--no-default-features` detector compile contract; custom and profile-sensitive
`cfg` predicates without an execution binding remain red. At runtime, the parent
producer creates a fresh challenge but withholds it until the trusted detector
wrapper has recorded at least one real rejecting invocation. The final verifier
requires the ordinary witness inventory and the challenge-bound proof inventory
to match exactly. An early return cannot qualify without that post-invocation
challenge. The proof socket uses a separate random pathname, rejects symlinked
managed-directory components, and prunes stale managed sockets; the challenge
itself never appears in the pathname. The proof channel is covered by the same
trusted-host boundary described above; hostile same-UID processes are not part
of this repository-local provenance contract.
