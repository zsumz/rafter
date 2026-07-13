# Rafter core style

The `rafter` crate is protocol code. Its style should make authority, ordering,
and state ownership obvious before it tries to be concise.

## Principles

1. **Name the Raft concept.** Prefer `committed_membership`,
   `first_sendable_index`, and `follower_id` over context-dependent names such
   as `current`, `next`, or `peer`.
2. **Keep transitions top to bottom.** A transition should read as: reject stale
   authority, accept newer authority, validate the request, mutate state, then
   emit ordered effects.
3. **Preserve explanatory symmetry.** Similar Raft paths may remain visibly
   parallel when their differences are load-bearing, especially pre-vote versus
   vote and stable versus joint quorum handling.
4. **Abstract decisions, not mechanics.** Small domain enums and pure decision
   helpers are welcome. Frameworks, callbacks, and trait-object dispatch are not
   substitutes for readable protocol flow.
5. **Keep output ordering visible.** `Vec<Output>` order is part of the kernel
   contract. Helpers may build or append outputs, but must not obscure their
   ordering.
6. **Comment the proof obligation.** Comments explain why a branch is required,
   which stale or reordered execution it excludes, or why an apparently simpler
   implementation would be unsafe.

## Modules

- `node/mod.rs` is a state map. `types/mod.rs`, `types/snapshot/mod.rs`,
  `node/event/mod.rs`, `message/mod.rs`, `node/config/mod.rs`,
  `node/replication/mod.rs`,
  `node/replication/snapshot/mod.rs`, `node/commit/mod.rs`,
  `node/membership/mod.rs`, `node/bootstrap/mod.rs`,
  `node/state.rs`, and `node/state/membership/mod.rs` are vocabulary or domain
  facades. None of them owns transition implementation.
- Each production module begins with a short `//!` contract explaining what it
  owns and what it deliberately does not own.
- A file should represent one protocol concept or one direction of a protocol
  exchange. Split files when a second independent vocabulary appears.
- Public callers continue to use the flat `rafter::{...}` facade; internal file
  layout is not part of the public API.

## Functions

- Prefer early returns for authority and validation gates.
- Separate protocol phases with one blank line.
- Avoid positional booleans in nontrivial calls. Use a small enum or a named
  helper when `true` and `false` encode policy. Replication uses
  `ReplicationDemand`, never a bare contact boolean. Snapshot replies use
  `SnapshotReply`, never a positional success flag.
- Long functions should be decomposed at semantic boundaries such as
  `classify`, `validate`, `apply`, and `respond`. Snapshot reception uses a
  named disposition before it stages, installs, or replies.
- A helper should make the caller read more like the protocol, not merely move
  lines elsewhere.
- `step_batch` uses one small ordering-preserving accumulator. Changing batch
  kind flushes the previous kind before any later effects are emitted.

## State

Use these words consistently:

- **persistent**: canonical protocol state that survives restart;
- **volatile**: process-local protocol state;
- **leader**: state meaningful only while leading;
- **derived**: state recomputable from canonical state;
- **local-only**: correlation metadata outside Raft semantics.

State mutations should have an obvious owning module. `ElectionState` owns the
local timeout and campaign grants; `LeaderState` owns authority that resets on
step-down; `DerivedState` owns only recomputable acceleration structures.

Derived indexes expose protocol queries, not their storage representation.
`ConfigurationIndex` is updated only by canonical log mutation and validates
itself against the retained log.

Configuration distinguishes **requested** from **effective** behavior. Builder
methods record caller intent; accessors apply timeout and feature-dependency
rules without erasing the original request.

## Formatting and spacing

`rustfmt` owns mechanical layout. Do not hand-align fields, match arms, or
assignments.

- Use one blank line between distinct protocol phases.
- Use blank lines inside a struct only to reveal ownership groups.
- Keep tightly related construction together; do not add decorative vertical
  whitespace inside one expression.
- Break method chains at semantic verbs rather than placing every `.` on a new
  line.
- Split long diagnostic strings at grammatical boundaries.
- Use `formatter` for `fmt::Formatter` parameters throughout the crate.

## Tests

Tests are executable protocol stories:

1. arrange the cluster or node state;
2. perform one named protocol event or fault schedule;
3. assert the safety or liveness property in domain language.

Scenario helpers should say what protocol fact they establish. Negative tests
must exercise the detector or transition being claimed, not only the final
error-reporting path.

Production modules never embed test bodies. Small unit tests live in named
sibling files such as `tracker_test.rs`; protocol scenarios live under
`node/tests/`. This keeps the production reading path uninterrupted while
retaining access to private implementation vocabulary through the parent
module.

The test tree mirrors source domains once a concept has a stable home. Batching
belongs in `tests/dispatch`, configuration policy in `tests/config`, elections
in `tests/election`, durable recovery in `tests/bootstrap`, membership stories
in `tests/membership`, replication modes in `tests/replication`, read authority
in `tests/read`, transfer handoff in `tests/transfer`, and snapshot transfer
scenarios in `tests/snapshot`. Shared scenario setup lives in a local `support`
module rather than a repository-wide fixture grab bag.

A protocol scenario file targets 400 lines. When independent stories push it
past that boundary, replace it with a declarative facade and child modules named
for the behavior under test. Do not split one coherent story merely to satisfy a
number.

## Architecture guards

- Production modules begin with `//!` contracts.
- One shared facade manifest drives declarative and size-budget checks.
- Production and mature test facades remain declarative; test scenarios live in
  focused child modules.
- Every test module begins with a `//!` scenario contract, and production modules
  contain no inline test-module bodies.
- Facades use tighter line budgets.
- Load-bearing state mutation is restricted to reviewed owning modules.
- Guard exceptions require a narrow reason and a tracking label; broad style
  allowlists are not an acceptable substitute for structure.
