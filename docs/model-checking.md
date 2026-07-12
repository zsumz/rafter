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

The harness builds both commits in release mode with `--locked`, then alternates
base/current execution order across six paired runs, balancing each revision in
each process-order position. Multiple profiles also run in alternating order.
It consumes structured `RAFTER_EVENT` records, requires independent protocol and verifier counts,
requires every exhaustive check to pass with an exhausted frontier, and rejects
shape drift between repeated samples of one revision. Human-readable summary
lines and legacy compatibility counts are never accepted as calibration data.
Profiles used for cost comparison must contain at least one exhaustive check;
the soak-only profile remains liveness evidence rather than state-space cost evidence.

`compare.json` preserves source trees, lockfile and binary digests, toolchain and
host metadata, every raw sample, additive state totals, wall time, and peak RSS.
`compare.md` reports min/median/max time and memory. CI uploads the report, raw
events, timing logs, build logs, and a SHA-256 manifest even when report
construction fails. Main pushes use the pre-push commit as baseline; scheduled
runs use `HEAD^`; manual runs require an explicit baseline input.

The default requires a clean checkout so a commit names the measured source.
`MODEL_CHECK_ALLOW_DIRTY=1` exists only for directional local experiments; such
a run records `clean: false` and is not release or threshold evidence.
