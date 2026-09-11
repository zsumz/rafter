# Protocol reading work

Baseline: `2a58d6e3` on merged `main`. Branch: `refactor/protocol-reading-paths`.

The goal is to make a rule, its production transition, and the execution that
requires it easy to find together. Changes preserve protocol behavior and keep
the existing deterministic core and invariant catalog.

1. Capture representative elections, writes, append conflicts, joint quorums,
   and read barriers before changing production code. Compare ordered effects
   and decision-relevant state after every input and actual multi-input batch.
2. Name the core cursor `dispatched_index`, retain `applied_index()` as a
   compatibility alias, and distinguish quorum replication from commitment.
   Application execution and its durable floor retain their own vocabulary.
3. Present acknowledgement mutation as statements, name append rejection
   reasons, and place commit authorization beside its quorum calculation.
   Keep reconciliation at its current semantic boundaries.
4. Verify walkthrough observations by executing those production scenarios.
   Link each worked rule to its source and existing counterexample evidence.
5. Improve the existing failure-to-replay route and provide a reader exercise
   with an answer rubric. Report only reader measurements actually performed.

Qualification uses focused local regression and architecture checks, with
full required qualification left to CI. The existing kernel benchmarks provide
the performance comparison; production quorum selection stays unchanged.
Architecture authority changes require a reviewed semantic diff. This document
does not approve new grants or claim CI or a reader study has completed.

## Implementation and evidence

The five implementation changes are ready for review. The commitment path now
uses `advance_commit_index_into` and its neighboring `quorum_replicated_index`
calculation, then `emit_committed_outputs`. Voting keeps term adoption before
eligibility, and append handling retains the frame-confirmed commit boundary.
No persistence sequence or production quorum-selection algorithm changed.

The [walkthroughs](../crates/rafter/PROTOCOL_WALKTHROUGHS.md) cover five scenarios
through both stepping APIs: 90 input/batch boundaries and 100 complete state
observations. The explicit observation schema retains ordered outputs and all
node state fields; the core compatibility alias is checked at each boundary.
The expected bytes come from unmodified merged main. During fixture review, a
read acknowledgement was corrected to echo a round actually emitted in both
APIs, then rerun on an isolated checkout of the same main revision before its
expected bytes were updated. The earlier capture was retained in local evidence.

Current baseline SHA-256:
`5d7a9db9aba9c90d33ff2e9da57581f3262e25420093fb71f1ba39d62fe4c2b9`.
The fixture records explicit protocol keys, rather than hashing `Node` or
normalizing away output or state differences. New internal fields require an
explicit capture decision because the ownership groups are destructured
exhaustively.

Local verification passed 962 tests across the affected crate suites and
follow-up runs, including 307 core and 349 simulator library tests. An obsolete
architecture test module name and a test's Markdown-anchor conversion were
corrected during qualification; their affected targets then passed. The final
walkthrough comparison, affected-target Clippy with warnings denied, core and
runtime rustdoc with missing-doc warnings denied, generated invariant docs, and
zcheck formatting also passed.

The existing follower-append and acknowledgement-window benchmarks used the
same 30-sample, 1-second warm-up, 3-second measurement settings before and after.
Neither reported a statistically detected change. The intervals were broad
(append change approximately -25% to +110%; acknowledgement approximately -50%
to +69%), so these samples do not establish a tight performance bound.

New and enlarged Rust files stay within 300 lines; six existing files above
that target retain their prior size. The election module now fits within 300,
so its old size exception is removed. The protected lock refresh was reviewed
and approved; it adds no grants or debt increases. Full CI qualification and the
[reader exercise](protocol-reader-exercise.md)
have not run; no cross-implementation superiority claim is made.

## Walkthrough review follow-up

The review of `74d8e605` identified a write success acknowledging index 4 with
the no-op's sequence 1. Ordinary successes now derive their term, sequence, and
confirmed end from an actual emitted append. The final write echoes sequence 3
with single calls and sequence 2 with batching. A real follower receives the
emitted write frames in order and must produce those exact responses; a negative
control rejects the old sequence-1/index-4 pairing. Injected guard probes are
labeled, and pre-vote and binding-vote helpers have separate names.

The corrected scripts were recaptured on an isolated checkout of unchanged
production `2a58d6e3`, with only the unit-test adapter mounted. The candidate is
compared against those bytes, not against a capture from itself. The correction
retains 90 input/batch boundaries and 100 complete state observations. The prior
capture remains available in commit `74d8e605` and local evidence. Read replies
also select emitted frames; append-conflict commands use readable text payloads.

The document now presents before/input/changes/ordered-outputs/why sections for
the primary story and shows only observed batch differences afterward. It uses
explicit prose for exercised messages and fails on an unsupported variant,
instead of falling back to Rust debug output. Full state and ordered-output
comparison remain separate from this selective human view.

Follow-up local qualification passed all 468 core-crate tests, including the
correlation controls, full observation comparison, generated-document check,
architecture guards, and doctests. Full required CI remains pending a PR; the
follow-up changes only walkthrough fixtures, rendering, evidence, and documentation.
Its lock refresh was reviewed and approved: the inventory and two transitive
source fingerprints change, with no grants, revocations, or debt changes.
