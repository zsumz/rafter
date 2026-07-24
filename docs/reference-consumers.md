# Reference Consumers

Status: active API-discovery and acceptance program for Rafter 1.0.

Sequencing: the reference consumers are built before the 1.0 public surface is
frozen. They reveal which mechanisms belong in Rafter and prove that each
public crate composes cleanly. Release polish and crate-by-crate API review run
alongside this program; a premature version deadline must not force an
unproven public abstraction.

Rafter will use a small portfolio of independent reference consumers to prove
that its public layers compose into real systems without making the library
specific to any one product. These are acceptance systems, not showcase demos:
each one exists to pressure a different contract and to expose missing or
over-specialized APIs before 1.0.

Rafter already has key/value-shaped coverage through
[`rafter-maelstrom`](../crates/rafter-maelstrom/README.md) and the durable
[`replicated_kv`](../crates/rafter-runtime/examples/replicated_kv.rs) example.
The reference portfolio deliberately does not add another ordinary key/value
store.

## Portfolio

| Reference system | Primary Rafter surface | What it must prove |
| --- | --- | --- |
| Replicated ledger | `rafter-app`, runtime, storage | Atomic application transactions, applied-index durability, bounded request deduplication, unknown outcomes, snapshots, and linearizable queries |
| Fenced lock service | `rafter-service`, reads, transport boundary | Linearizable authority, cancellation on leadership loss, replicated logical expiration, persistent fencing tokens, retries, and stale-owner exclusion |
| Sharded counter service | `rafter-multiraft`, managed scheduler, lifecycle | Many-group routing, quantitative fairness, quotas, isolation, lifecycle, restart, backpressure, and snapshot pressure |

The three systems are intentionally not a shared product:

- The ledger is the application-durability proof.
- The lock service is the authority and linearizable-read proof.
- The sharded counter is the many-group scheduler and isolation proof.

A large pinned downstream application may run as an additional product canary,
but it is not the authority on whether Rafter can release.

## Goals

The reference consumers must:

1. use only documented public Rafter APIs;
2. exercise the same persistence and recovery contracts expected of external
   users;
3. define application-level safety properties independently of Raft's own
   invariants;
4. run in deterministic, process, and packaged-consumer modes;
5. turn discovered generic gaps into small, policy-free Rafter APIs; and
6. become mandatory evidence for the 1.0 release.

They must not:

- move ledger, locking, sharding, authentication, or scheduling policy into the
  Raft kernel;
- depend on unpublished internal test hooks in package-consumer mode;
- share transition code with their independent reference models;
- treat an in-memory demo as application-durability evidence;
- treat the insecure TCP example as production-composition evidence; or
- grow a general reference framework before two consumers demonstrate the same
  need.

## Repository and Dependency Boundary

The intended end-state is a separate, unpublished Cargo workspace:

```text
reference/
  Cargo.toml
  ledger/
  fenced-lock/
  sharded-counter/
  harness/             # added only after demonstrated reuse
```

The root workspace will exclude `reference/`. This prevents the consumers from
accidentally inheriting root workspace dependencies, features, or metadata.
Their canonical manifests will depend on versioned Rafter crates.

Two dependency modes will use those same consumer sources:

### Fast source mode

Local development will pass an explicit Cargo configuration that patches the
versioned Rafter dependencies to the checkout. Path overrides belong in the
development command, not in the canonical consumer manifests.

### Package-consumer mode

The acceptance job will:

1. package every public Rafter crate;
2. inspect and unpack the exact package archives into a temporary directory;
3. patch the consumer workspace to those unpacked archives, never to source
   directories in the checkout;
4. generate a fresh lockfile;
5. build and run the reference tests;
6. reject path dependencies that resolve back into the checkout;
7. reject internal test hooks and unpublished private crates; and
8. verify required package contents, including README and format documents.

This mode tests the artifact users receive, not merely the source tree that
produced it.

Until `rafter-sim` has an intentional public surface without hidden internal
hooks, package-mode consumers will use a small consumer-owned deterministic
cluster driver over public APIs. Repository-internal verification may
additionally run the richer `rafter-sim` checks. The consumer driver is itself
a useful test: an external user must be able to orchestrate and observe Rafter
without privileged access.

## Shared Construction Rules

Every reference system will contain six distinct pieces:

1. **Command and result schema.** Bounded, versioned application messages with
   explicit rejection results.
2. **Pure implementation model.** The deterministic state machine used by the
   Rafter-backed service.
3. **Independent reference model.** A second executable specification used as
   an oracle.
4. **Rafter adapter.** Public-API integration for proposals, reads, recovery,
   snapshots, and proposal outcomes.
5. **History recorder and checker.** A black-box checker over client
   invocations and terminal outcomes.
6. **Durable process composition.** Real processes, application storage, Raft
   storage, transport, readiness, and crash points.

The implementation and reference model may share command and result data
schemas. They may not share transition functions, validation helpers,
deduplication logic, state mutation code, or snapshot reconstruction code.

Histories must retain:

- invocation and completion order;
- successful and rejected operations;
- operations with unknown outcomes;
- retries under the same request identity;
- overlapping operations;
- leader changes, disconnections, and process restarts; and
- the state needed to replay a failing seed exactly.

Application invariants and linearizability are separate checks. A preserved
aggregate, such as total ledger balance, does not prove that the observed
operations admit a legal real-time ordering.

## Replicated Ledger

### Purpose

The ledger proves the boundary between committed Raft entries and a
transactional application state machine. It is the first consumer to build
because it is small, deterministic, and intolerant of duplicate or partially
durable effects.

### Initial operations

```text
OpenAccount
Deposit
Transfer
CloseAccount
GetAccount
GetLedgerSummary
```

Every mutating operation carries a bounded request identity:

```text
client id
session epoch
sequence
request fingerprint
```

The first slice will define the exact session protocol, including session
creation, epoch replacement, sequence gaps, stale sequences, and conflicting
reuse. Its baseline policy is one outstanding mutation per client and one
cached completed result per active session:

- the next sequence executes once;
- an exact retry of the highest completed sequence returns its cached result;
- reuse of that identity with different command bytes is rejected;
- older sequences are rejected as stale;
- gaps are rejected; and
- an older session epoch cannot displace a newer one.

This keeps deduplication bounded per active client. Session retirement and any
bound on active clients must be explicit before durable process admission.

### Safety properties

The independent checker must establish:

- a transfer preserves total balance;
- total balance equals initial supply plus successful external deposits, minus
  any explicitly modeled external withdrawals;
- no account becomes negative unless overdrafts are later made explicit;
- an account closes only with a zero balance;
- every accepted request identity changes state at most once;
- an exact retry returns the original result;
- conflicting request bytes under one identity are rejected;
- reads and mutations are linearizable; and
- snapshot, restart, and replay reconstruct identical balances, account
  states, sessions, fingerprints, cached results, and applied progress.

### Durability transaction

The durable implementation must use a transactional application backend. One
transaction atomically commits:

```text
account mutations
session and deduplication mutation
command result
applied Raft index
```

The backend is an implementation choice; this atomicity is not. Crash points
must cover every boundary before, during, and after that transaction, including
the interval after application persistence but before a client reply.

The application snapshot must contain the session and deduplication state.
Compaction must never make an acknowledged command executable again.

## Fenced Lock Service

### Purpose

The lock service proves linearizable authority across leader changes and client
retries. It also demonstrates that application locks and Rafter's optional
leader-lease read optimization are different concepts.

### Initial operations

```text
Acquire
Renew
Release
ExpireThrough(logical_time)
GetLock
```

`ExpireThrough` advances replicated logical time monotonically. It does not
promise expiration after a real-world duration. Only the service's authorized
expiration driver may submit it; authorization remains outside the replicated
state machine.

Initially, every query uses an ordinary linearizable barrier. The application
does not claim to exercise app-layer lease reads while that path is unsupported.

### Fencing contract

Every successful acquisition receives a monotonically increasing fencing
token. The per-resource token high-water mark survives:

- release;
- logical expiration;
- deletion and recreation;
- snapshot and compaction; and
- restart.

The test system includes an independent guarded resource. It records the
highest accepted fencing token and rejects operations carrying an older token.
The required safety property is not merely that tokens increase; it is that a
stale former owner cannot modify the guarded resource after a later owner is
established.

The history checker must cover acquisition and renewal retries, leadership
loss, read cancellation, unknown outcomes, isolated former leaders, snapshot
recovery, and stale-owner attempts against the guarded resource.

## Sharded Counter Service

### Purpose

The sharded counter is an acceptance workload for the managed multi-Raft
scheduler intended for the 1.0 stack. Repeated calls to the current manual
`tick_all` host are useful examples, but they are not evidence of production
scheduling, bounded fairness, or isolation.

### Workload

The deterministic workload will model thousands of independent groups. The
process suite will use a smaller production-shaped group count.

It must include:

- hot and cold groups;
- a group with deliberately slow storage;
- a snapshot-heavy group;
- a poisoned group;
- group creation, draining, removal, reopening, and tombstoning;
- messages arriving after removal;
- global and per-group queue limits;
- per-group work quotas;
- heartbeat and election traffic competing with bulk replication; and
- global backpressure and recovery.

### Scheduler contract

Fairness must be quantitative. Before the scheduler is accepted, its
configuration will declare an executable bound equivalent to:

> Absent global resource exhaustion, every continuously ready group receives a
> scheduling opportunity within one complete pass over the ready set.

The final formula may account for worker count and quotas, but it must remain a
deterministic assertion rather than a latency impression from benchmarks.

Additional properties include:

- work and failure in one group do not corrupt another;
- a poisoned group cannot stop unrelated groups;
- removed groups cannot be resurrected by late traffic;
- lifecycle transitions are idempotent or reject conflicts explicitly;
- queue limits fail closed without silently discarding accepted work; and
- control traffic continues to receive its documented opportunity under bulk
  load.

## Process Composition Levels

Process tests have two explicit levels.

### Integration composition

Early slices may use the reference file store and insecure TCP transport. These
tests prove process boundaries, routing, restart, and application recovery, but
are labeled integration evidence only.

### Production composition

The 1.0 acceptance composition requires:

- persisted replica identity;
- a transactional application backend;
- the selected production Raft storage implementation;
- bounded frames and queues;
- authenticated transport;
- readiness gating after complete recovery; and
- structured metrics and failure diagnostics.

No test using intentionally insecure demo plumbing closes this release
criterion.

## Verification Lanes

| Lane | Required work |
| --- | --- |
| Every PR | Package build, pure implementation and reference-model tests, codec vectors, short deterministic simulations, and small history checks |
| Main/nightly | Durable process tests, restart, partitions, duplication, snapshots, and application crash points |
| Weekly | Long randomized histories, storage faults, snapshot pressure, and hot/cold multi-group scheduling |
| Release | Exact package archives, full process suite, mixed-version tests, long scheduler and recovery canaries, and the pinned downstream product canary |

Randomized jobs must always print and retain their seed and minimized failing
history. A sampled green run supplements deterministic proofs; it does not
replace them.

## API Promotion Rule

A reference consumer is allowed to reveal missing Rafter APIs. It is not
allowed to dictate them.

Before adding an API to a public Rafter crate, classify the need:

1. **Raft or durable-lifecycle mechanism:** eligible for Rafter.
2. **Application or deployment policy:** remains in the consumer.
3. **Observation needed only by a test:** prefer public neutral events or
   black-box history; do not expose internal mutation hooks.

A promoted API must:

- use no ledger, lock, counter, broker, or other product vocabulary;
- define resource bounds and typed failure behavior;
- preserve sans-IO ownership where applicable;
- be usable by at least one other plausible consumer, or follow directly from
  a documented Raft correctness contract; and
- receive its own focused tests outside the reference application.

Repeated plumbing is extracted only after the second consumer demonstrates the
same shape. Shared harness code may own process orchestration, fault injection,
history recording, and package setup. It may not own domain transitions,
validation, deduplication, or oracle logic.

## Delivery Plan

Work proceeds in vertical, reviewable slices. Each slice ends with its focused
tests and keeps the repository green.

### Foundation

1. Add the separate reference workspace with the ledger as its only consumer.
2. Exclude that workspace from the root and add the fast source-mode dependency
   override.
3. Add a boundary check that rejects internal hooks and accidental root
   workspace inheritance.

Do not create an empty framework crate, shared harness, generic history schema,
or package runner before the ledger supplies a real use for it.

### Ledger

1. Write the command, session, replay, and snapshot contract.
2. Build the pure implementation model and its focused invariant tests.
3. Build the structurally independent model, history generator, and small
   checker.
4. Add the public `rafter-app` adapter and deterministic three-node driver.
5. Add the package-archive consumer runner and checkout-path rejection now that
   it has a real consumer to build.
6. Add linearizable reads, retries, unknown outcomes, snapshots, and restart.
7. Add the transactional backend and application crash points.
8. Add durable process-per-node and package-consumer coverage.

No shared application framework is extracted during the first three ledger
slices.

### Fenced lock

1. Write the logical-time, session, token, and guarded-resource contract.
2. Build independent implementation and reference models.
3. Add the `rafter-service` adapter and linearizable query path.
4. Add leadership-loss, cancellation, retry, and stale-owner histories.
5. Add durable snapshots, restart, processes, and package-mode tests.
6. Extract only the orchestration code now proven common with the ledger.

### Sharded counter and scheduler

1. Write the managed scheduler lifecycle, queue, quota, and fairness contract.
2. Build the deterministic ready-set scheduler and counter oracle.
3. Add hot/cold, slow, poisoned, and snapshot-heavy groups.
4. Add removal, tombstone, reopening, and late-message behavior.
5. Add bounded process composition and package-mode tests.
6. Add long nightly, weekly, and release scheduling profiles.

### Release integration

1. Add all three consumers to the documented verification lanes.
2. Add mixed-version package-consumer coverage.
3. Pin the downstream product canary revision and Rafter dependency.
4. Make the release lane mandatory for a 1.0 tag.

## First Implementation Milestone

The first milestone is complete when the ledger has:

- a reviewed command and session contract;
- bounded command and result types;
- a pure deterministic implementation model;
- a structurally independent reference model;
- deterministic invariant tests for balances, closure, replay, conflict, stale
  sequence, and sequence gaps;
- a history interface that represents completion, rejection, and unknown
  outcomes; and
- no transport, disk backend, shared framework, or new Rafter public API.

That stopping point gives the Rafter adapter an application contract to meet
instead of allowing integration convenience to define the application.

## Portfolio Completion

The portfolio is complete only when:

- all three systems pass their independent safety and history checks;
- source and exact-package modes both run;
- application and Raft crash windows are covered;
- snapshots preserve every safety-relevant piece of application metadata;
- resource limits and failure behavior are explicit;
- the scheduler meets a deterministic fairness bound;
- production-composition tests use the blessed 1.0 stack; and
- no reference-system policy has leaked into Rafter's generic public surface.
