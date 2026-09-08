# Development and checks

## Local qualification

Install [zcheck](https://github.com/zsumz/zcheck) and the same
[zrail release](https://github.com/zsumz/zrail/releases/tag/v0.0.3-rc.8) as CI:

```sh
rustup toolchain install 1.97.1 --profile minimal
cargo +1.97.1 install zcheck --version 0.0.1 --locked
cargo +1.97.1 install zrail --version 0.0.3-rc.8 --locked
zcheck
```

Rafter remains qualified on Rust 1.88. These developer tools have a newer
Rust source requirement; the explicit 1.97.1 install toolchain matches CI.
The qualification scripts also require Python 3.11 or newer.

The default graph in [`zcheck.toml`](../zcheck.toml) runs formatting, Clippy,
rustdoc, proof-reference checks, the architecture contract, and workspace tests.
`zcheck plan check` shows the graph. Each run records logs and a receipt.
`zcheck run full` adds the deeper local lanes; inspect the plan for platform
requirements. Invariant evidence execution requires Linux, Python 3.11+, Java
21, and the pinned TLC JAR at `tools/cache/tla2tools.jar`. Install the asset from
[the pinned TLC release](https://github.com/zactionsz/tla-tools/releases/tag/tla2tools-2026.08.11.125311)
and verify it against `tools/tla/SHA256SUMS` before running
`scripts/invariants-pr-preflight`. CI installs the reviewed asset through its
pinned `zactionsz/tla-tools` action.

The full invariant task allows eight hours: its required layers have sequential
budgets of 40 minutes for tests, 40 for simulation, and 338 for TLA, plus an hour
for compilation, aggregation, and finalization. The preflight checks this sum
against the manifest so a profile change cannot silently outgrow its parent.
The default `zcheck` graph remains the shorter development check. On macOS, the local
Clippy task excludes the Linux-specific invariant executor; Linux CI checks it.

The underlying commands remain directly runnable:

```sh
cargo test --workspace
cargo test -p rafter-sim
cargo run --release -p rafter-sim --bin rafter-model-check-fast
cargo run --locked -p rafter-invariants -- run-all --profile pr
scripts/maelstrom-lin-kv
scripts/reference-source-check
scripts/reference-package-check
scripts/reference-package-process-check
zrail check
```

The reference consumers occupy an independent workspace, excluded by the root
`Cargo.toml`. The three reference commands cover checkout-patched source, exact
package archives, and exact-package process/MSRV evidence. CI runs deterministic
and MSRV lanes on pull requests and the full process lanes on main.

## Architecture ownership

[`zrail.toml`](../zrail.toml) declares architecture intent; `zrail.lock` binds it
to reviewed repository state. `zrail-baseline.toml` holds per-file adoption debt:
ceilings only shrink unless a change is explicitly reviewed. Run `zrail check`
after edits and inspect `zrail diff --base HEAD` before updating the lock.
Every proposed grant must receive an explicit approval or denial with a reason.
`scripts/zrail-reconcile` only prunes stale generated allowances. New spellings,
item sites, unknown origins, and opaque-input changes require a manual identity
review; the helper leaves the contract untouched if any such finding exists.
Its successful exit means the generated block is stable; only `zrail check`
provides the architecture verdict.

PR CI fetches `pull_request.base.sha` and runs `zrail diff --deny-grants`
against that authority after checking the proposed contract. Grants, adoption
debt increases, and unknown changes block the required `zrail` check pending
an `architecture-policy-review` environment approval. The retained artifact
binds the full diff to the base, proposal, and run attempt. The designated
maintainer must explicitly approve or deny every change with a reason before
approving that deployment; a refreshed lock or an approval file in the same
patch cannot supply that decision. A new PR push cancels the previous run.
Repository settings require this environment review and the documented PR
checks while retaining signed commits and linear history.

zrail resolves dependency macro exports offline from Cargo's registry archives.
On a fresh checkout, run `cargo fetch --locked` before invoking `zrail check`
directly. The zcheck graph and CI perform this fetch before architecture analysis.

Rafter retains guards where its requirements exceed the published zrail engine:
leading `//!` module comments, strict facade shapes and named module layouts,
narrower scenario-size limits,
excluded fuzz sources, wire-tag expressions, public API documentation and enum
contracts, exact dependency-kind policy, unresolved mutation patterns, and the
invariant tooling's domain and evidence rules. Package, MSRV, release, and
runtime correctness checks also remain independent qualification gates.

## Invariant evidence

The [Raft verification contract](./raft-invariants.md) is generated from
[`verification/raft-invariants.yaml`](../verification/raft-invariants.yaml).
[Model checking](./model-checking.md) documents profiles, state-count semantics,
and reproducible overhead measurements. The [completion map](./work-completion.md)
links each claim to its executable proof.

`run-all` loads one immutable plan, runs every required layer, and aggregates
only evidence from that invocation. `check` aggregates existing result bundles.
Production `run` and `run-all` subprocesses require Linux descriptor-bound
executable launch and fail closed on other operating systems. The macOS CI lane
exercises launcher mechanics under a test-only fallback; it does not produce
accepted invariant evidence.

The deterministic PR aggregate emits exactly one verdict for each of the 44
reviewed IDs. The stable `invariants-pr` job fails on missing, malformed,
incomplete, or stale evidence and is the check to require in branch protection.
Evidence artifacts are isolated by workflow run attempt. After a partial
GitHub Actions rerun, rerun every invariant evidence job together; an aggregate
rerun alone intentionally reports missing evidence instead of reusing a prior
attempt.

Maelstrom supplies sampled end-to-end evidence in nightly and weekly profiles
and is excluded from the deterministic PR verdict. Scheduled
`invariants-nightly` and `invariants-weekly` jobs run every required layer,
render the same 44-row report, and fail on missing evidence or exhausted budgets.
