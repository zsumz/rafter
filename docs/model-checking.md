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
