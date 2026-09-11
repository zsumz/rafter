# Rafter protocol-core architecture

`rafter` is a deterministic, sans-I/O Raft state machine. It accepts typed
`Input` values, mutates protocol state, and returns ordered `Output` values. It
never opens files or sockets, spawns tasks, reads wall-clock time, or decides
when durable effects have reached storage.

## Start with a behavior

The [production-core walkthroughs](PROTOCOL_WALKTHROUGHS.md) run real inputs
and verify their observations in the normal test suite. Start with the rule
that raised your question; the domain map below is the reference index.

| Question | Read together |
| --- | --- |
| Why did a rejected vote change the term? | `handle_request_vote` in [election.rs](src/node/election.rs), then the authority reset in [lifecycle.rs](src/node/lifecycle.rs). Term adoption precedes eligibility. |
| Why did this follower accept an append? | [receive.rs](src/node/replication/receive.rs): authority, matching prefix, named splice rejection, mutation, confirmed commit floor, and response. |
| Why did this acknowledgement commit a write? | [response.rs](src/node/replication/response.rs) separates contact from matching progress; [advance.rs](src/node/commit/advance.rs) places quorum replication beside current-term authorization; [emit.rs](src/node/commit/emit.rs) emits the effects. |
| Which voters determine this quorum? | [Membership views](src/node/membership/view.rs) select the effective configuration; [advance.rs](src/node/commit/advance.rs) requires each constituent majority. |
| What permits this read? | [read_index.rs](src/node/read_index.rs) qualifies acknowledgement sequences; the embedding waits for application execution. |

The [rule and counterexample guide](../../docs/protocol-rules.md) links these
conditions to existing invariant detectors. The [reader exercise](../../docs/protocol-reader-exercise.md)
provides questions and a correctness rubric without claiming a completed study.

## Cursors and the embedding boundary

| Position | What it establishes |
| --- | --- |
| `commit_index` | Committed log prefix known to this core. |
| `dispatched_index` | Committed prefix processed for output dispatch, including no-ops, configurations, and the effective recovery floor. |
| Application-applied position | Commands actually executed by the external state machine. It need not name a no-op or configuration index. |
| Durable application-applied position | Execution and state that survive an application restart; supplied by the embedding's durability contract. |

`Node::applied_index()` and `DurableRaftNode::applied_index()` remain compatibility
aliases for `dispatched_index()`, without deprecation warnings. Existing public
error names and their `applied_index` fields also remain available and document
that they refer to dispatch. Constructors named `*_applied_through` keep that
name: the caller is declaring durable application progress, from which the core
establishes its dispatch floor. Application APIs that report actual execution
retain `applied` vocabulary.

Returning `Output::Apply` prepares a command; it does not execute it. The raw
core performs no persistence. `rafter-runtime` persists dependent state before
releasing outputs; `rafter-app::RaftGroup::step` then executes application
commands, so its `report.applied` describes completed work and must not be
applied again. A read at a no-op waits for preceding application commands,
not an application callback at the exact no-op index.

## Domain maps

### Public type vocabulary

- `types/mod.rs` is the flat public type facade.
- `types/id.rs` owns protocol identities, terms, indexes, and local correlation ids.
- `types/replication.rs` owns read-only leader progress vocabulary.
- `types/configuration.rs`, `types/membership.rs`, and `types/payload.rs` own
  their corresponding public domains.
- `types/snapshot/mod.rs` is the snapshot vocabulary facade.
- `types/snapshot/metadata.rs` owns the compacted boundary and payload descriptor.
- `types/snapshot/transfer.rs` owns transfer identity, directives, staging, and
  restart progress.
- `types/snapshot/status.rs` owns read-only transfer observability and counters.
- `types/snapshot/source.rs` owns runtime-provided payload access.
- `types/snapshot/error.rs` and `types/snapshot/identity.rs` own validated error and identity
  vocabulary.

### Configuration and dispatch

- `config/mod.rs` is the static-configuration vocabulary facade.
- `config/state.rs` validates static membership and constructs a complete config.
- `config/features.rs` stores caller intent separately from effective behavior.
- `config/options.rs` owns builders and effective accessors.
- `dispatch.rs` is the single public transition boundary.
- `PendingBatch` coalesces only adjacent proposals or reads and flushes before
  every authority, membership, message, tick, or transfer boundary.

### Replication

- `replication/receive.rs` validates and splices follower append frames.
- `replication/response.rs` advances leader progress from append acknowledgements.
- `replication/send.rs` fills probe, replication, and snapshot send modes.
- `replication/authority.rs` records lease and check-quorum evidence.
- `replication/progress.rs` keeps progress aligned with effective membership.
- `replication/proposal.rs` admits and batches application proposals.
- `replication/snapshot/` owns snapshot transfer in both directions.

### Snapshots and bootstrap

- `replication/snapshot/send.rs` materializes bounded outbound chunk directives.
- `replication/snapshot/receive/` separates whole-snapshot reception, chunk
  disposition, staging, and final installation.
- `replication/snapshot/reply.rs` names accepted and rejected follower replies.
- `replication/snapshot/response.rs` advances leader progress from follower
  acknowledgements.
- `replication/snapshot/validate.rs` owns identity, authorization, shape, and
  rejection accounting.
- `replication/snapshot/transfer.rs` validates durable partial-transfer recovery.
- `bootstrap/state.rs` defines the durable image accepted at restart.
- `bootstrap/validate/` validates vote, snapshot, log geometry, and committed
  configuration identity before hydration.
- `bootstrap/error.rs` is the closed restart-validation error vocabulary.

### Internal state

- `state/core.rs` separates restart-persistent state from process-local state.
- `state/election.rs` owns the local timeout and collected vote/pre-vote grants.
- `state/leader.rs` resets all leader-only authority and progress together.
- `state/progress.rs` owns one replica's send mode and in-flight window.
- `state/membership/` maps protocol node IDs to compact quorum/progress slots.
- `state/proposal.rs` tracks volatile local proposal correlation.
- `state/snapshot.rs` tracks inbound snapshot byte progress without owning bytes.
- `state/derived.rs` owns recomputable indexes and their synchronization checks.

### Membership and commitment

- `membership/view.rs` derives static, effective, committed, and snapshot views.
- `membership/change.rs` constructs safe stable and joint transitions.
- `membership/validate.rs` owns transition preconditions and promotion barriers.
- `commit/advance.rs` derives quorum replication and authorizes current-term commitment together.
- `commit/emit.rs` dispatches the newly committed prefix in log order.

## State ownership

- `PersistentState` owns term, vote, committed configuration, snapshot, and
  log. Bootstrap, election, log, and follower replication mutate it.
- `VolatileState` owns role, commit/dispatch cursors, local proposal correlation,
  incoming snapshot progress, leader hints, and diagnostics.
- `ElectionState` owns the local timeout and collected vote/pre-vote grants.
  Election, lifecycle, and accepted leader traffic mutate it.
- `LeaderState` owns observed replication progress, heartbeat rounds,
  check-quorum, leases, reads, and leadership transfer. It resets as one
  authority unit. Follower acknowledgements are protocol evidence and cannot
  be recreated from the leader's log.
- `ProgressSet::reconcile_membership` preserves that evidence while rebuilding
  membership slots, initializes new replicas as probes, and updates local
  progress. `reconcile_follower_progress_mut` names the maintenance it performs
  before lookup. Reconciliation remains at send, response, and commit boundaries
  because membership can change within a step or batch.
- `DerivedState` owns indexes exactly recomputable from canonical state.
- `ConfigurationIndex` locates configuration entries in the retained log. Log
  mutation updates it; membership code reads it only through domain queries.

A change that writes one of these fields should live in, or be delegated through,
the corresponding owning module.

## Transition and output ordering

```text
Input
  |
  v
Node::step / Node::step_batch
  |
  +-- mutate deterministic protocol state
  |
  `-- return ordered Output values
          |
          v
   embedding persists dependent state
          |
          v
   embedding releases sends, applies, or read grants
```

The order of `Output` values is load-bearing. For example, a staged snapshot
chunk must reach durable storage before the acknowledgement emitted later in the
same step is released. The protocol core expresses this order; `rafter-runtime`
and production embeddings enforce the persistence boundary.

## Membership vocabulary

- **static membership**: the startup configuration supplied through `NodeConfig`;
- **effective membership**: the configuration governing authority now, including
  an uncommitted configuration entry;
- **committed membership**: the latest configuration at or below the commit
  index;
- **snapshot membership**: committed membership carried at the compacted
  snapshot boundary;
- **target membership**: a requested future stable configuration.

Using the precise term matters because elections, replication progress, commit
quorums, and recovery consult different views at different moments.

## Test architecture

The test tree follows the same conceptual map as production code without making
production modules carry test bodies:

- `message/*_test.rs`, `node/**/**_test.rs`, and `types/*_test.rs` hold narrow
  unit checks for private vocabulary;
- `node/tests/election/` separates campaign, voting, heartbeat, and timing;
- `node/tests/bootstrap/` separates hydration, validation, application recovery,
  and snapshot-boundary behavior;
- `node/tests/replication/` separates follower, leader, and pipelined send modes;
- `node/tests/snapshot/` separates whole installation, chunk reception, and
  bounded streaming;
- `node/tests/transfer/` separates request validation, catch-up handoff, and
  `TimeoutNow` authority changes.

Each test module begins with a one-sentence scenario contract. Facades declare
only child modules and shared imports; scenario setup belongs in the nearest
`support.rs`.

## Verification map

The repository-level invariant catalog in `verification/raft-invariants.yaml`
maps stable invariant IDs to executable evidence. Internal module documentation
should mention the invariant IDs a transition directly preserves when that
connection is useful to a reader.

## Architecture ratchets

Repository tests keep the presentation contract executable:

- every production module begins with a concise `//!` ownership contract;
- facade modules may declare vocabulary and re-exports, but no implementation
  functions or `impl` blocks;
- load-bearing term, vote, role, commit, dispatch, election, and configuration-index
  mutations stay in their documented owning modules;
- one shared facade manifest drives both declarative-structure and size guards;
- facade files use tighter size budgets than implementation or test files;
- focused tests mirror mature source domains including `bootstrap`, `config`,
  `dispatch`, `election`, `membership`, `replication`, `read`, `snapshot`, and
  `transfer`; their facade modules remain declarative.
- production modules contain no embedded test bodies; narrow unit checks live in
  sibling `*_test.rs` modules and protocol stories live under `node/tests/`.
- protocol scenario modules use a 400-line presentation target; independent
  stories move behind a declarative test facade before they become scroll-heavy.
- new and substantially revised Rust modules stay within 300 lines. Existing
  size debt remains a reviewed ratchet. If a cohesive protocol argument needs
  an exception, review its reading cost and the exact policy delta first.
- retired flat test modules are forbidden from silently returning beside the
  mirrored tree.
