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

Baseline SHA-256:
`c47f7dccdc1fc6c14a3bafce41ddcda8a8395c433da3b6d1494a9260d16c3906`.
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
