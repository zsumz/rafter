# Rafter protocol-core architecture

`rafter` is a deterministic, sans-I/O Raft state machine. It accepts typed
`Input` values, mutates protocol state, and returns ordered `Output` values. It
never opens files or sockets, spawns tasks, reads wall-clock time, or decides
when durable effects have reached storage.

## Reading path

A reader can understand the kernel in this order:

1. [`src/node/event/`](src/node/event): roles, inputs, ordered outputs,
   and rejection vocabulary.
2. [`src/message/`](src/message): peer messages, log entries, and shared append
   payloads.
3. [`src/types/`](src/types): identities, membership, configuration, replication
   observability, and snapshot vocabulary behind the flat public facade.
4. [`src/node/mod.rs`](src/node/mod.rs): the `Node` ownership groups and module map.
5. [`src/node/config/`](src/node/config): requested configuration and the
   effective feature policy derived from safety dependencies.
6. [`src/node/state/`](src/node/state): durable, volatile, election, leader, and
   derived state representations.
7. [`src/node/dispatch.rs`](src/node/dispatch.rs): the single public transition
   boundary and message dispatch.
8. [`src/node/lifecycle.rs`](src/node/lifecycle.rs): follower and leader state
   resets.
9. [`src/node/election.rs`](src/node/election.rs): pre-vote, voting, and leader
   election.
10. [`src/node/replication/`](src/node/replication): follower append reception,
   leader acknowledgement handling, outbound replication, authority evidence,
   and snapshot transfer.
11. [`src/node/commit/`](src/node/commit): quorum-derived commit candidates and
   ordered application of the committed prefix.
12. [`src/node/membership/`](src/node/membership): static, effective, committed,
   and snapshot membership views plus safe stable/joint transitions.
13. [`src/node/read_index.rs`](src/node/read_index.rs): linearizable read
    barriers and leases.
14. [`src/node/log.rs`](src/node/log.rs) and
    [`src/node/replication/snapshot/`](src/node/replication/snapshot): retained
    logs, compaction, and snapshot transfer in both directions.
15. [`src/node/bootstrap/`](src/node/bootstrap): durable-state vocabulary,
    ordered validation, and restart hydration.

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
- `commit/tracker.rs` derives the quorum-backed commit candidate.
- `commit/apply.rs` enforces the current-term rule and emits committed effects.

## State ownership

- `PersistentState` owns term, vote, committed configuration, snapshot, and
  log. Bootstrap, election, log, and follower replication mutate it.
- `VolatileState` owns role, commit/apply cursors, local proposal correlation,
  incoming snapshot progress, leader hints, and diagnostics.
- `ElectionState` owns the local timeout and collected vote/pre-vote grants.
  Election, lifecycle, and accepted leader traffic mutate it.
- `LeaderState` owns replication progress, heartbeat rounds, check-quorum,
  leases, reads, and leadership transfer. It resets as one authority unit.
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
- load-bearing term, vote, role, commit, apply, election, and configuration-index
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
- retired flat test modules are forbidden from silently returning beside the
  mirrored tree.
