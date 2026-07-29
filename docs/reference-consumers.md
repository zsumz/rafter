# Reference Consumers

Status: the initial three-consumer engineering program is complete. The ledger
has deterministic acceptance, an independent linearizability checker, source
and exact-package modes, durable integration-process composition, and
exact-package process execution. The fenced lock adds independent
linearizability and guarded-resource checkers, durable integration-process
composition, a bounded authenticated production-composition fixture, and
exact-package process execution. The sharded counter uses the public managed
scheduler beside an independent model/oracle/fairness audit, deterministic
64/1,024/4,096-group profiles, durable process composition, source and exact
package/process modes, and nightly/weekly profiles. The consumers share only a
neutral bounded history-search engine. Promoted APIs are recorded in
[`docs/api-promotions.md`](./api-promotions.md).

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
  harness/             # neutral mechanisms proven by at least two consumers
```

The root workspace excludes `reference/`. This prevents the consumers from
accidentally inheriting root workspace dependencies, features, or metadata.
Their canonical manifests depend on versioned Rafter crates. The two history
checkers also reach the unpublished harness through one workspace-local path;
package mode copies that complete workspace and never resolves the harness from
the checkout.

Two dependency modes use those same consumer sources:

### Fast source mode

Local development passes an explicit Cargo configuration that patches the
versioned Rafter dependencies to the checkout. Path overrides belong in the
development command, not in the canonical consumer manifests.

[`scripts/reference-source-check`](../scripts/reference-source-check) is that
command. It runs the workspace's file-size gate, then patches every publishable
Rafter crate — the whole set, not only the ones a consumer reaches today — and
runs the reference workspace's format check,
`clippy --all-targets -D warnings`, the tests, and `cargo doc --no-deps` under
`RUSTDOCFLAGS=-D warnings`. A partially patched graph would link checkout code
against published code from the same workspace, and a list derived from what
the consumers currently reach goes stale the moment one of them takes a new
dependency, so the override list is the publishable set. The rustdoc build is
there because these consumers are read as exemplars: a doc comment that cannot
build is a defect in the artifact they exist to be.

The file-size gate is first because it needs no toolchain and no build. It is
the root workspace's `crates/rafter/tests/file_size_guard.rs` in the one place
that guard cannot reach: that guard is a `#[test]` in the `rafter` crate and
enumerates `git ls-files -- crates fuzz`, and running it here would make this
lane build the root workspace, which is exactly what this workspace's exclusion
from it exists to prevent. So the shape is shared by copy and only the
inventory and the numbers differ — the hard limits are the root workspace's
own, and the script carries the argument for the targets that are not.

`reference/Cargo.lock` is deliberately untracked. Source mode resolves the
patched crates to checkout paths, and package-consumer mode resolves the same
manifests against unpacked archives and generates its own lockfile. Neither
resolution is the other's, so committing one would pin a lockfile that the
other mode has to rewrite on every run. The reference workspace depends only on
its dependency-free harness, Rafter crates, and their published dependencies,
so nothing else needs pinning here; the root workspace lockfile remains the
pinned build.

### Package-consumer mode

The acceptance job:

1. packages every public Rafter crate;
2. inspects and unpacks the exact package archives into a temporary directory;
3. patches the consumer workspace to those unpacked archives, never to source
   directories in the checkout;
4. generates a fresh lockfile;
5. builds and runs the reference tests;
6. rejects path dependencies except the exact copied-workspace harness edge;
7. rejects checkout paths, internal test hooks, and unpublished private product
   crates; and
8. verifies required package contents, including README and format documents.

`scripts/reference-package-check` runs that deterministic job and reports every
boundary it finds violated. `scripts/reference-package-process-check` uses the
same materialization and rejection phases; it does not carry a second publish
list or construct its own patch table.

Step 7 is the only check anywhere that builds `rafter` in its published feature
shape. `rafter-sim` depends on `rafter` with the hidden `internal-test-hooks`
feature, and resolver 2 unifies features across every package one invocation
selects, so every `--workspace` command over the root workspace compiles
`rafter` with that hook on. Only this lane resolves a graph in which it is off.

The lanes package with Cargo's per-archive verification disabled, then perform
the stronger portfolio checks themselves. Once a Rafter version exists on
crates.io, Cargo can verify a dependent archive against that older published
sibling instead of the sibling archive produced by the current checkout. The
shared runner therefore unpacks every newly produced archive, patches the copied
consumer workspace to those exact directories, and builds and tests that graph.

There are four distinct evidence boundaries:

- **Source deterministic** runs ordinary tests, clippy, and rustdoc against
  checkout-patched crates.
- **Source process** runs the reviewed ignored process inventories against those
  checkout-patched crates.
- **Exact-package deterministic/process** builds ordinary targets or executes
  all 32 reviewed process tests against one set of unpacked `.crate` archives.
- **Published-shape MSRV** creates archives with the current packaging Cargo,
  then regenerates the copied consumer lockfile, builds every target, runs the
  deterministic suites, and runs a ledger process smoke with Rust/Cargo 1.88.

The exact-package process and MSRV phases can be run separately in CI, or
together with the default `scripts/reference-package-process-check`; the
combined command uses the same archive set for both. None of these lanes proves
mixed-version compatibility.

The `reference-package-process` and `reference-package-msrv` jobs pass an
explicit artifact directory. Each upload contains archive SHA-256 values, the
generated patch table and lockfile, Cargo metadata, the boundary verdict,
toolchain versions, inventory counts and copies, and per-suite logs. A failed
runner retains its temporary workdir and preserves the original exit status;
the artifact directory contains every diagnostic produced before the failure.

This mode tests the artifact users receive, not merely the source tree that
produced it.

## Shared Reference Harness Boundary

`reference/harness` exists because the ledger and fenced-lock checkers proved
the same bounded-search shape independently. It is unpublished, has no
dependencies, and owns only:

- the shared `OperationId` and an already-parsed operation interval;
- explicit operation and configuration limits;
- real-time predecessor construction and minimal-candidate selection;
- bounded depth-first search, two-way branch traversal, and failed-state
  memoization; and
- searched/discharged/configuration counts plus the deepest failed frontier.

Each consumer still owns its event vocabulary and parser, malformed-history
errors, sequential oracle, transition and query logic, state key, typed
mismatch reasons, and replay rendering. The guarded-resource checker remains
entirely in the fenced-lock consumer. The harness has no generic event schema
and no process orchestration; the latter is a separate extraction only after
its duplicated mechanisms are inventoried.

Architecture tests scan every harness production source path and body for
product vocabulary and reject any dependency from the harness back into a
consumer. Package mode permits the one copied-workspace harness edge while
continuing to reject every checkout path.

Until `rafter-sim` has an intentional public surface without hidden internal
hooks, package-mode consumers use a small consumer-owned deterministic cluster
driver over public APIs. Repository-internal verification may additionally run
the richer `rafter-sim` checks. The consumer driver is itself a useful test: an
external user must be able to orchestrate and observe Rafter without privileged
access. What "consumer-owned" covers has narrowed as the program ran: the lock
now drives its replicas through the promoted `rafter-service` transport driver
and owns only the deterministic network and the cluster orchestration around
it, which is the correct direction — a consumer should own what a deployment
owns.

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

The lock records full command dispositions and typed linearizable-query
results in both deterministic and process compositions. Its bounded black-box
checker searches those client-visible histories against the structurally
independent oracle, including retries, leadership loss, read cancellation,
unknown and provably absent outcomes, isolated former leaders, and restart.
Local/stale reads are not mislabeled as linearizable evidence.

Guarded-resource writes have a separate recording wrapper and checker. It
proves per-resource token monotonicity, equal-token retry acceptance, exact
stale-token refusal, and resource-name separation without merging the
downstream guard into the lock's sequential specification. Substantive
deterministic and process scenarios run both checks and reject an empty proof.

## Sharded Counter Service

### Purpose

The sharded counter is the acceptance workload for the public managed
multi-Raft scheduler. Its deterministic adapter drives real three-replica
`RaftGroup`s through the managed host and audits public admission, pass,
dispatch, and metrics records independently. The manual `tick_all` host remains
a useful lower-level API, but none of the scheduler claims below rely on it.

### Workload

The independent deterministic workload models 3,000 groups. The replayable
real-Rafter profiles drive 64, 1,024, and 4,096 groups, and the durable process
suite uses 16 groups (48 Raft replicas). That smaller process count keeps
`SIGKILL`, socket, storage, and lifecycle scenarios bounded while retaining
separate hot, cold, slow, poisoned, snapshot, and bulk cohorts.

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

The unpublished fenced-lock `lock-production-node` fixture closes this
composition criterion with one bounded implementation:

- durable per-group monotonic identity allocation and per-replica identity
  records, with committed removal permanently spending an ID;
- the transactional fenced-lock application backend and
  `FileRaftNodeStores` correctness-oriented Raft storage;
- Rustls mutually authenticated peer channels whose leaf certificate maps to
  exactly one node and must agree with both envelope identities;
- a durable 64-frame per-peer replay window and monotonic connection sessions;
- a 2,163,089-byte receive-frame ceiling, 256-frame outbound per-peer queues,
  128-frame inbound per-peer queues, a 512-frame global inbound queue, 16 peer
  connections, 16 client connections, and 64 pending client requests;
- readiness after identity, TLS, replay metadata, Raft, application,
  checkpoint, committed application floor, and workers are ready; and
- JSON Lines lifecycle/transport diagnostics plus an `OBSERVE` object covering
  role, term, leader, indexes, membership phase, queue depth/overflow,
  authentication/refusal/replay counts, checkpoint epoch, and readiness.

`tests/process_production.rs` exercises authenticated service, unknown and
mismatched credentials, replay and removed-peer refusal, checkpoint
loss/corruption, readiness under incomplete recovery, connection overflow,
monotonic replacement, and both independent lock/guarded-resource checkers.
`scripts/reference-process-check` selects it through a reviewed five-test
inventory. The fixture proves that the public Rafter crates can be composed this
way; it is not a generic server, certificate platform, deployment controller,
or high-throughput WAL claim. The separate insecure `lock-node` remains
integration evidence only.

The allocation clause is the deployment's half of a contract Rafter states and
cannot enforce for it. A `NodeId` is single-use within its group: a committed
removal retires it, and a replacement replica joins under a fresh one, because
the retirement floor a driver publishes to its link layer covers every identity
at or below the greatest the group has ever committed that the peer set does not
name. Rafter enforces this within one driver's lifetime and says
so in `rafter::NodeId`'s own documentation; enforcing it across restarts needs
a durable record of what has been allocated, and how long to keep one is a
retention decision — classification 2, the same ground on which the counter's
group tombstones stayed in the consumer.

**Monotonic per-group allocation is required, not merely recommended.** Every ID
a group admits must be greater than every ID it has ever committed. That is what
lets a driver derive which identities a removal has spent from one number rather
than from a set that grows with every removal for the life of the group — the
unbounded structure the kernel declines to keep, and no more affordable one layer
up. The cost of the derivation is that gaps below the mark are unallocatable:
"fresh" means greater than anything ever committed, not merely unused, so a group
that has committed node 5 can never admit node 3. A deployment that allocates
non-monotonically has its fresh IDs refused as spent, which is fail-closed and
deliberate. A per-group counter costs one number and avoids all of it.

The rule reaches the local replica too. A committed removal of the replica a
driver is running spends *that* identity, so the driver stops serving clients
with a typed state, keeps running the protocol until its supervisor releases the
group, and refuses to be re-adopted under the removed ID. The supervisor's move
is release, then adopt a fresh one.

**Restarting a replica is not removing it.** A replica that is killed and
reopened from its own durable state under the same ID was never removed, so
nothing retires and nothing changes for it. That is the lock's process suite
working as intended, not an exception to the rule above.

**A production composition persists the driver's peer control plane.** The
derivation above reads a high-water mark and the live committed set it is judged
against, and a restarted process can rebuild neither: retirement is the
*difference* between two committed configurations, a new process observes only
the latest, and compaction erases the rest. A process that dropped them would
publish a floor that covers nothing and would let an identity a committed removal
spent be allocated again — the same window the monotonic allocator is the last
backstop for. So the composition reads
`TransportRaftDriver::control_plane_checkpoint`, makes it durable under whatever
crash discipline it already uses for small metadata, and hands it back at
`TransportRaftDriver::with_control_plane_checkpoint`. Persist on
`control_plane_checkpoint_epoch`, which moves on exactly the facts that must not
be lost; a crash between a change and its persistence loses that change and no
more.

The fenced lock is where this is wired, and it is wired in the consumer that does
not need it. Its contract says its cluster performs no membership changes, so its
checkpoint names one committed set and one mark and never records a removal —
which is the point: a persistence path that exists only in the consumer that
exercises it is a path nobody has run. What a *removal* costs across a restart is
proven where a driver can be destroyed and rebuilt at all,
`crates/rafter-service/tests/transport_service_state.rs`.

**A consumer that drives `rafter-app` directly has no control plane to persist.**
The ledger is deliberately built on `rafter`, `rafter-app`, `rafter-runtime`, and
`rafter-storage` and not on `rafter-service`, which is what makes it independent
acceptance evidence for the app layer. Peer authorization policy and identity
retirement are the managed driver's, so none of this section's driver-level obligations
reach it; its process composition owns the equivalent decisions itself.

## Verification Lanes

| Lane | Required work | Executed by |
| --- | --- | --- |
| Every PR | Source and exact-package deterministic tests, process-inventory membership, published-shape Rust 1.88 build/test plus process smoke, the counter-fast profile, and the deterministic invariant aggregate | `reference-source`, `reference-package`, `reference-package-msrv`, `counter-reference-fast`, and `invariants-pr` in `ci.yml` |
| Main | All reviewed process tests in source mode and against exact archives | `reference-process` and `reference-package-process` in `ci.yml` |
| Nightly | All reviewed source process tests, burn-in, real pinned Maelstrom, randomized/replayable invariant evidence, bounded multi-gigabyte snapshot streaming, and the 1,024-group counter profile | `reference-process`, `burn-in`, `invariants-maelstrom`, `invariants-nightly`, `multi-gigabyte-test`, and `counter-reference-nightly` in `nightly.yml` |
| Weekly | Deep tests/model checking, storage and snapshot histories, three-trial pinned Maelstrom, and the 4,096-group randomized counter profile with slow groups, snapshot/bulk pressure, poison, and lifecycle churn | `invariants-weekly`, `invariants-maelstrom`, and `counter-reference-weekly` in `weekly.yml` |
| Release | Exact package archives, full same-version process suite, published-shape MSRV, mixed-version tests, long scheduler and recovery canaries, and the pinned downstream product canary | Partly. `RELEASE.md`'s pre-publish block runs the source, exact-package, process, and MSRV lanes by hand; there is no release workflow, no mixed-version coverage, and no pinned downstream canary |

The third column is the whole point of the table. Until the two lanes were
wired, the "required work" column described work that nothing performed:
`reference/` is excluded from the root workspace, so no `cargo` command in
`ci.yml` compiled a consumer, and neither lane script had a caller anywhere in
`.github/`. A row here is a claim about a job that exists, or it says which
jobs do not exist yet.

The release row remains intentionally incomplete. Mixed-version package
coverage, a pinned downstream canary, and a tag-gating release workflow are
separate release-integration slices and are not inferred from the same-version
exact-package lanes.

Both process dependency modes read
`verification/reference-process-suites.txt` and run each `#[ignore]`d selection
through `scripts/cargo-test-exact` against one reviewed inventory per suite.
The shared registry prevents source and package modes from drifting, while each
expected count is derived from its inventory so count and names cannot
disagree. Neither check is decoration: `--ignored` alone reports success over
zero tests when attributes drift, and a count alone accepts a right number of
different tests. The inventories reject both.

The membership check and the execution sit on different tiers on purpose.
`reference-process` runs the suite, and it is skipped on pull requests --
which is why it is deliberately not a required status check, since a required
check that never reports blocks every merge. But a gate that only fails after
merge is a gate that reports too late, and this one demonstrated it: the
reviewed count was left at seven when an eighth test landed, and the lane sat
red on main because no pull request could reach it. So `reference-source`, which
runs on every pull request, now also runs `scripts/reference-process-check
--list-only`: it compiles nothing extra -- that job already builds
`tests/process_cluster.rs` -- lists the selection, and compares it to the
inventory without running it. Adding, removing, or renaming a process test now
fails on the pull request that does it. What still only happens after merge is
the suite actually passing, which is the part that genuinely costs half an hour.

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
same shape. Shared harness code may own neutral search and process mechanisms,
fault injection, history recording, and package setup once each has two real
uses. It may not own event schemas, parsing policy, domain transitions,
validation, deduplication, or oracle logic.

Every API promoted under this rule is recorded in
[`docs/api-promotions.md`](./api-promotions.md).

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

All three are complete.

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

All eight are complete. The later fenced-lock work extracted their common
bounded-search mechanics, but no application framework or ledger behavior.

### Fenced lock

1. Write the logical-time, session, token, and guarded-resource contract.
2. Build independent implementation and reference models.
3. Add the `rafter-service` adapter and linearizable query path.
4. Add leadership-loss, cancellation, retry, and stale-owner histories.
5. Add durable snapshots, restart, processes, and package-mode tests.
6. Extract only mechanisms now proven common with the ledger.

Slices 1 through 5 are complete, including one operating-system process per
replica and black-box lock and guarded-resource checks over the real-process
history. Slice 6 has begun with the shared bounded-search substrate. Process
orchestration remains consumer-owned until its duplicated responsibilities are
inventoried and extracted separately.

### Sharded counter and scheduler

1. Write the managed scheduler lifecycle, queue, quota, and fairness contract.
2. Build the deterministic ready-set scheduler and counter oracle.
3. Add hot/cold, slow, poisoned, and snapshot-heavy groups.
4. Add removal, tombstone, reopening, and late-message behavior.
5. Add bounded process composition and package-mode tests.
6. Add long nightly and weekly scheduling profiles.

All six slices are complete. `ManagedScheduler`,
`ReferenceScheduler`, and the real Rafter-backed managed counter remain three
structurally independent shapes. The real matrix covers all workload classes,
queue pressure and lossless retries, full lifecycle/reopen/tombstone behavior,
slow and poisoned isolation, actual application and descriptor-based snapshot
install paths, late-incarnation fencing, and a 1,024-group fairness profile.
The canonical manifest names only versioned public Rafter crates, so both
source and exact-package lanes exercise the scheduler through an external
consumer shape. The process fixture adds three durable hosts, bounded sockets,
transactional application records, `SIGKILL`/clean restart, real compaction,
exact retry, late-client/peer fencing, and lifecycle removal/reopen/tombstone.
The 64/1,024/4,096-group profiles retain replay inputs and quantitative
fairness, conservation, failure, and lifecycle artifacts.

This completes the counter's initial integration-composition scope. Its peer
link is deliberately unauthenticated and makes no production-transport claim.
Authenticated counter transport, production configuration and secrets,
operational metrics export, and deployment evidence are additive future work,
not hidden requirements of this completed scope. The portfolio's bounded
production-composability evidence is the fenced-lock fixture described above.

### Release integration

1. Add all three consumers to the documented verification lanes.
2. Add mixed-version package-consumer coverage.
3. Pin the downstream product canary revision and Rafter dependency.
4. Make the release lane mandatory for a 1.0 tag.

Slice 1 is done for the every-PR, main/nightly, and weekly rows of the lane
table above, but not the release row. Slices 2 through 4 are not started: there
is no mixed-version coverage, no pinned downstream canary, and no release
workflow for a tag to be gated on.

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
