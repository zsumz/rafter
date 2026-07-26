# Sharded Counter Service Contract

Status: third reference-consumer contract for Rafter 1.0 API discovery. This is
its foundation milestone: the contract, the bounded schema, a deterministic
scheduler, a structurally independent oracle, the history vocabulary, and their
tests.

This crate runs the program's sequencing backwards from its siblings, on
purpose. The ledger and the fenced lock were written against Rafter surfaces
that already existed, and each reported what it found missing. The managed
many-group scheduler this document describes does not exist yet. Repeated calls
to the current manual `tick_all` host are a useful example and are not evidence
of production scheduling, bounded fairness, or isolation, so there is nothing
here to report on — there is something to specify. This contract is the design
input for that scheduler, and the crate has **no dependencies at all**, so that
every bound and every edge below had to be arguable before any API existed to
argue it against.

The sharded counter is deliberately small. It exists to prove:

- a quantitative fairness bound that a test decides rather than a benchmark
  suggests;
- group lifecycle transitions that are idempotent or explicitly conflicting;
- that a removed group is refused by late traffic rather than resurrected by it;
- that queue limits fail closed without silently discarding accepted work;
- per-group work quotas that bound throughput share without touching
  opportunity share;
- that work and failure in one group reach no other; and
- agreement between an implementation and a structurally independent oracle.

The counter itself is the smallest replicated state that can still be got
wrong. Nothing in this contract is about counting.

## Resource Model

`SchedulerConfig` fixes five bounds when a scheduler is created:

```text
max_groups              addressable group slots
workers                 dispatches that may be in flight at once
max_clients_per_group    client session slots inside one group
max_group_queue         items one group may hold
max_global_queue        items the whole host may hold
```

Every bound is nonzero, and `max_global_queue` is at least `max_group_queue`. A
global bound below a group's would make the group bound unreachable and hide
which limit a workload is actually hitting.

A group slot's state is allocated when the slot is first created, so a
configuration that permits a million groups costs nothing until the groups
exist. Arming a pass costs one traversal of the *ready* set rather than of the
configured range, so a host with thousands of idle slots pays nothing for the
handful that have work. Both are properties the deterministic workload leans
on: it drives thousands of groups, and a scheduler that scanned its whole
address space every tick could not.

## Group Identity

A group ID names an administrative slot. A slot that has been removed and
created again is the same ID under a strictly greater `GroupIncarnation`.

Traffic carries both. Administration carries only the ID: the operator is the
party that changed the incarnation, so there is no stale administrative request
to detect, and a lifecycle request that had to name a generation would be
asking the operator to guess at its own last action.

Incarnations start at one and never wrap. A slot whose generation space is
exhausted can never reopen, which fails closed rather than letting a late
message name a generation that has already retired.

## Commands

Work is submitted to one group incarnation and is one of three shapes:

```text
Counter { request, command, cost }      client work, scheduled as Command
System  { class, cost }                 scheduler traffic: Control, Snapshot, Bulk
Faulty  { class, cost }                 work whose application poisons its group
```

Counter commands are:

```text
Add(delta)      nonzero signed adjustment
Read            the value at service time
```

Sessions are opened out of band:

```text
OpenSession(group, incarnation, client_id, session_epoch)
```

Reads are scheduled work like any mutation. They take a queue slot, consume
quota, and carry a request identity, because a read that skipped the queue would
be a second admission path with bounds of its own to get wrong.

A zero delta is unrepresentable. A mutation that cannot change state still
consumes a request sequence and a queue slot, and admitting one would make the
queue bound depend on traffic that means nothing.

`Faulty` is a work *shape* rather than a scheduler command, because a group is
poisoned by what its own work did and never by an operator asking for it. The
isolation property is only interesting when the poison arrives through the
ordinary queue.

### Schema version

This is version 1 of the counter schema. The schema is closed and in-memory:
this milestone has no transport, so it has no frame, and a version number
without a frame to carry it would be decoration.

What the version means is a rule the adapter inherits rather than invents. When
a frame exists it must carry this version, and a build that does not recognize
a version must refuse the frame rather than reinterpret it — the discipline the
ledger's and the lock's codecs already keep. Adding a variant to any type below
is a change to this document first.

## Session Protocol

Sessions are scoped to a group. A client addressing two groups holds two
independent sessions, which is what lets one client have one mutation
outstanding *per group* rather than one across the whole service. A sharded
service whose clients could only have one request in flight host-wide would be
a sharded service in name only.

Scoping has a price, and it is stated here rather than discovered later:
removing a group discards its session table. That is exactly why a late retry
addressed to a removed slot must be *refused* rather than executed — there is no
cache left to recognize it as a retry, so executing it would apply an
acknowledged command a second time. The tombstone and incarnation rules below
are what make the refusal decidable.

Session epochs and request sequences are nonzero. `OpenSession` behaves as
follows:

- an unused client slot accepts its first epoch;
- the current epoch is idempotent and preserves its cached completion;
- a greater epoch replaces the session and clears its sequence and cache;
- a lower epoch is rejected as stale;
- a client ID outside the group's configured slot range is rejected.

The session table needs no capacity refusal of its own. The addressable client
range *is* its bound: a client outside the range is refused before it can take a
slot, so at most `max_clients_per_group` distinct slots can ever exist. A second
refusal for a state that cannot be reached would be a promise about behavior no
test could observe.

Each active session stores at most one outstanding request and at most one
completed one:

```text
current session epoch
outstanding sequence, exact command, and queue slot
highest completed sequence, exact command, and cached result
```

### Deduplication happens at admission

The siblings deduplicate when a command is applied. This one deduplicates when
a command is *admitted*, and the difference is deliberate.

Admission is the point where a request identity is bound to a queue slot.
Letting two copies of one identity occupy two slots would make the per-group
queue bound depend on how often clients retry, which is the one thing the bound
must not depend on. So the admission gate answers a retry from the session
before any slot is taken.

For a `Counter` submission, in this order:

1. The group must exist, not be tombstoned, and be the incarnation named.
2. The group's lifecycle state must admit `Command` work.
3. The group must not be poisoned.
4. The client slot must be in range and its session open at the exact epoch.
5. The supplied fingerprint must equal the fingerprint of the supplied command.
6. Sequence admission, against the completed record and then the outstanding
   one.
7. The per-group queue bound, then the global one.

Envelope self-consistency is decided before sequence admission, because a
request whose fingerprint does not describe its own command is malformed
wherever its sequence falls.

Sequence admission, with `highest` the highest completed sequence:

- a sequence below `highest` is stale;
- `highest` repeated with the same command returns the cached result;
- `highest` repeated with another command is a conflicting retry;
- the outstanding sequence repeated with the same command returns the slot it
  already holds, and takes no second one;
- the outstanding sequence repeated with another command is a conflicting
  retry;
- `highest + 1` is the only new sequence that may be admitted; anything above it
  is a gap, including the sequence after one that is still outstanding — the
  expected sequence does not move until the outstanding request completes; and
- a session whose sequence space is exhausted must open a greater epoch.

**The queue bounds are consulted last, and that ordering is load bearing.** An
acknowledged request has to stay confirmable while the queue is full, or a
client would be told to retry a command that already took effect. The completed
cache answers before any bound is reached.

Session, sequence, fingerprint, and conflicting-retry rejections consume
nothing: no sequence, no queue slot, no work identifier.

### Request fingerprints

The fingerprint is a deterministic 64-bit digest of the command's canonical
encoding. It binds a request identity to the command the client believes it
sent, which is what an adapter needs to route a retry after an unknown outcome.

It is **not** the admission key. Retry and conflict decisions compare the exact
bounded command, so a digest collision can never admit a conflicting retry as an
exact one. The fingerprint is checked for self-consistency and cached; it never
substitutes for exact comparison.

### Session retirement

This slice retires nothing within a live incarnation. Client slots are fixed by
configuration, a greater epoch replaces a slot's session in place, and a
group's session table is emptied only when the group is removed. Retirement and
eviction policy for a long-lived group must become explicit before durable
process admission.

### Sessions are not replicated here

`OpenSession` is an admission-gate action rather than queued work. Queueing it
would mean a client needed a queue slot to open the session that a queue-full
rejection tells it to retry under, which is a circle with no exit.

When the Rafter adapter exists, session establishment becomes a replicated
command like its siblings'. It is immediate here because this milestone models
the scheduler, not the log.

## Group Lifecycle

Six states, one administrative axis:

```text
Creating     the slot exists and its durable state is being established
Recovering   the slot is replaying and catching up
Serving      the slot is fully serviceable
Draining     the slot admits nothing and is retiring what it accepted
Removed      the slot is gone; its ID may be created again
Tombstoned   the slot is gone; its ID may never be created again
```

`Removed` and `Tombstoned` are different answers to different questions. A
removed ID may be reopened as a greater incarnation, and late traffic naming the
old one is refused as stale. A tombstoned ID is terminal for the identity, and
the refusal outranks every incarnation question. Without both, "reopening" and
"tombstoning" would be the same operation and the program's workload asks for
each separately.

### What each state admits

| State | Serviceable | Admits `Control` / `Snapshot` / `Bulk` | Admits `Command` |
| --- | --- | --- | --- |
| `Creating` | no | no | no |
| `Recovering` | yes | yes | no |
| `Serving` | yes | yes | yes |
| `Draining` | yes | no | no |
| `Removed` | no | no | no |
| `Tombstoned` | no | no | no |

A recovering group is schedulable for the traffic that recovers it and refuses
client commands. Parking commands behind a recovery of unknown length would turn
one slow group into a queue-limit outage for the whole host; refusing them lets
the client retry the same request identity when the group is serving.

A draining group is serviceable and admits nothing. That is the entire content
of draining: it is how accepted work leaves.

### Transition table

Every cell is idempotent, an applied edge, or an explicit refusal naming both
states. There is no cell that silently does nothing.

| From \ Request | `Create(q)` | `Recover` | `Serve` | `Drain` | `Remove` | `Tombstone` |
| --- | --- | --- | --- | --- | --- | --- |
| *absent* | **Created(1)** | `GroupUnknown` | `GroupUnknown` | `GroupUnknown` | `GroupUnknown` | `GroupUnknown` |
| `Creating` | idempotent, or `QuotaConflict` | → `Recovering` | `Conflict` | → `Draining` | `Conflict` | `Conflict` |
| `Recovering` | `Conflict` | idempotent | → `Serving` | → `Draining` | `Conflict` | `Conflict` |
| `Serving` | `Conflict` | `Conflict` | idempotent | → `Draining` | `Conflict` | `Conflict` |
| `Draining` | `Conflict` | `Conflict` | `Conflict` | idempotent † | → `Removed`, or `QueueNotDrained` | `Conflict` |
| `Removed` | **Created(n+1)** | `Conflict` | `Conflict` | `Conflict` | idempotent | → `Tombstoned` |
| `Tombstoned` | `GroupTombstoned` | `GroupTombstoned` | `GroupTombstoned` | `GroupTombstoned` | `GroupTombstoned` | idempotent |

† A repeated drain retires a poisoned group's queue; see below.

Three cells deserve their reasons.

**`Create` on a creating slot with a different quota is refused, not absorbed.**
A quota belongs to an incarnation. Accepting the repeat would discard the number
the caller asked for while reporting that nothing changed.

**`Remove` requires `Draining`.** Removing a serving group would discard work it
had accepted. The only route out is the drain, and the drain accounts for every
item.

**`Remove` on a draining slot with a queue is refused with the count still
owed.** A healthy group's accepted work leaves by being serviced; a poisoned
group's leaves through the failure records its drain emitted. Neither vanishes,
and neither can be outrun.

### Accepted work is never silently discarded

This is the enforcement point the doc asks for, stated as one rule:

> Every admitted item reaches exactly one terminal disposition — serviced, or
> failed with a record naming it. There is no third outcome and no silent one.

A healthy group drains by servicing. A poisoned group can service nothing, so
draining it retires its queue and reports each item individually as
`GroupPoisoned`. That is a failure, not a disappearance: the count is
observable, the identifiers are named, and the conservation law
`admitted = serviced + failed + queued` holds over any history.

Because a group can be poisoned by work it services *while* draining, the
retirement is attached to the drain **request** rather than to the transition.
Draining an already-draining poisoned group is how an operator clears a backlog
that `Remove` keeps refusing.

## Work Classes

Four classes, in descending service priority:

```text
Control     heartbeat and election traffic
Command     client counter commands
Snapshot    snapshot build and transfer pressure
Bulk        bulk log replication catch-up
```

- **Control first.** Losing an election because a heartbeat queued behind a
  snapshot chunk is the failure this ordering exists to prevent.
- **Command before Snapshot and Bulk.** Client-visible progress outranks
  catching a lagging peer up.
- **Snapshot before Bulk.** A snapshot exists to *replace* bulk catch-up;
  deferring it makes the backlog it would have retired larger.

Snapshot pressure is modeled as queued items with a class and a cost, and
deliberately as nothing else. A snapshot-heavy group is a group whose queue
carries expensive `Snapshot` items. This crate does not build snapshots; it
specifies what their pressure costs the scheduler.

### Priority fills a quota; it never reorders a pass

This is the rule that reconciles control-traffic priority with fairness, and it
is the one a managed scheduler is most likely to get wrong:

> Work-class ordering decides which of a group's **own** items fill its quota.
> It never decides which groups are in a plan, and it never moves a group
> earlier in one.

So control traffic's opportunity bound is the same one-pass bound as everything
else. Priority guarantees that control work is serviced *first within the turn*,
not that it takes another group's turn. A scheduler in which an urgent class
could rearm or reorder a plan would have traded a fairness bound for a latency
heuristic.

Within a class, items are serviced in arrival order.

### What bulk traffic is not promised

Sustained higher-class load starves bulk work, and that is intended. This
contract promises bulk progress only in the absence of saturating control,
command, and snapshot traffic. Stating the non-promise is the point: a
scheduler that quietly reserved capacity for bulk would be making a policy
decision this document has not argued for.

## The Fairness Bound

`docs/reference-consumers.md` requires a bound equivalent to:

> Absent global resource exhaustion, every continuously ready group receives a
> scheduling opportunity within one complete pass over the ready set.

The rest of this section makes that decidable.

### Definitions

**Ready.** A group is *ready* at an instant when all of these hold:

1. it has been created and its lifecycle state is serviceable — `Recovering`,
   `Serving`, or `Draining`;
2. it is not poisoned;
3. it is not stalled by an external readiness report;
4. it is not currently occupying a worker; and
5. its queue holds at least one item.

Condition 4 is not an exclusion from service. A group occupying a worker is not
starved; it is being served, and offering it a second concurrent turn would let
one group hold two workers while another holds none.

**Plan and pass.** A *plan* is an ordered snapshot of the ready set, taken when
the scheduler *arms* it. A *pass* is one traversal of one plan. Each pass has a
monotone index, and a pass *completes* when every group in its plan has been
*offered* its turn.

**Offer and opportunity.** Offering a group its turn is the opportunity the
bound quantifies over. An offer either *dispatches* the group — a worker takes
it and services up to its quota — or *skips* it. A skip is still an
opportunity: the bound is about being offered a turn, and a group that cannot
use the turn it was offered has not been starved of anything.

**Global resource exhaustion.** The condition that every worker is occupied. A
pass that reaches it *suspends*: it keeps its remaining plan and resumes at the
same position on a later tick. It is never abandoned, never rearmed, never
restarted at the head.

### The bound

Two statements, both safety properties over recorded decisions:

> **F1 — Plan completeness.** Every group in an armed plan is offered exactly
> once before that plan retires, and no plan is armed while another is open.
>
> **F2 — Plan totality.** A plan names exactly the ready set at the instant it
> was armed: every ready group and no other.

F1 and F2 together give the required bound directly. A group that is ready when
a plan is armed is in that plan (F2) and is offered within that pass (F1). A
group that becomes ready part way through a pass is not in the plan in progress
and is in the next one, so it waits at most one complete pass. That is the
doc's sentence, and this is its proof.

### The executable assertion

Define, for each group `g`, its **opportunity gap**: the longest run of
consecutive complete passes in which `g` was ready at arm time and absent from
the plan. The bound is:

```text
widest_gap == 0
```

That single number is what a test asserts, and the audit that computes it
reports which group produced the worst run, which pass the run began at, and how
long it lasted. A fairness failure a report could not point at would be a
benchmark impression rather than a proof.

The audit reports `widest_gap` on success too, so a green run says which number
it proved rather than only that it did not fail.

### Worker count and quotas

The doc allows the final formula to account for worker count and quotas. It
accounts for them by showing that neither belongs in it.

**Worker count `W` does not appear in the bound.** `W` decides how many turns
can be handed out per tick, and therefore how many *ticks* a pass takes. It does
not decide which groups a plan contains. A host with one worker and three
thousand ready groups completes the same pass as a host with sixty-four; it
takes longer. The bound is invariant under `W`, and that invariance is why the
bound survives resource exhaustion rather than being excused by it.

**Quotas bound throughput share, not opportunity share.** A group's quota `Q_g`
decides how much one turn does:

```text
serviced(g, turn) = min(Q_g, pending(g))
```

except when the turn stops early because the group poisoned itself. Raising one
group's quota lets it do more per pass; it never lets it take another group's
turn. A hot group with a thousand queued items and quota 2 receives 2 items of
service per pass, exactly like a cold group with 2 items — and that is the whole
mechanism by which a hot group cannot crowd out a cold one.

A quota of zero is unrepresentable. It would put a group in the ready set that
no opportunity could ever drain, which is starvation wearing a configuration's
clothes.

### Why this is a safety property

The bound is checked over recorded decisions, with no reference to time,
throughput, or tick counts. That is deliberate. A liveness statement about a
scheduler needs assumptions about arrival rates and service times, and a test of
one is a measurement; converting the requirement into "no plan ever omits a
ready group, and no plan is ever abandoned" makes it a property a fold over a
history decides exactly, on every run, at any scale.

### The ready set is exact, and what follows

The scheduler maintains ready-set membership exactly and in constant time per
change. A consequence worth naming: a plan entry's readiness can only be revoked
by something the scheduler did not do itself. Its queue emptying, its lifecycle
moving, its own work poisoning it — each requires the dispatch that the offer
in question would have been. The only external revocation is a readiness report,
so `Stalled` is the only reason a turn can be skipped, and its being the only
one is a property rather than an omission.

### Pass order

A plan is offered in group order, rotated to start after the group that led the
previous completed plan, so a short supply of workers does not always favor the
same head.

Order is a courtesy, not a guarantee. It changes who is served earliest within a
pass; it never changes who is in the plan. That is precisely why the bound does
not mention it.

## Queue and Quota Bounds

Both queue bounds fail closed at admission, and are checked in a fixed order:
the per-group bound first, then the global one. A group over its own limit
learns which limit it hit; a group under its own limit learns the host is full.
Reporting the global bound first would tell a misbehaving group that the host
was busy.

Nothing already accepted is touched by either refusal. The bounds decide what
may enter, never what may stay.

A per-group bound is what keeps one group from consuming the host's whole queue,
and the global bound is what keeps the sum of well-behaved groups from doing the
same. Neither substitutes for the other, which is why the configuration requires
both and refuses a global bound below a per-group one.

## Poison and Isolation

A group is poisoned when servicing its own work fails irrecoverably. Poison is a
**health** fact and is orthogonal to the lifecycle, which is an
**administrative** one. Folding poison into the lifecycle would make "may this
group be removed?" depend on two unrelated axes at once; keeping them apart is
what lets a poisoned group leave by the ordinary drain-and-remove path.

A poisoned group:

- stops being ready, so it is never in another plan;
- stops the turn it was taking, at the item that broke it, short of its quota —
  the items behind the failure stay queued for the drain that will report them;
- admits nothing new, reported as `GroupPoisoned`; and
- keeps its accepted work until a drain retires it explicitly.

The required property is not that a poisoned group stops. It is:

> A group that its own work destroyed takes nothing else down with it. Unrelated
> groups keep taking their turns, keep making progress, and keep their state.

That is asserted directly rather than inferred: a poisoned group runs alongside
healthy ones and the healthy ones are checked for progress, not merely for
survival. The fairness audit is the second half of the same claim — a poisoned
group is owed no turn, so it cannot absorb one either.

## Readiness Reports

External availability is the only readiness input the scheduler does not derive
for itself. It models backpressure the host learns from outside — a storage
device that stopped accepting writes, a peer link that closed — and it is
sticky: a stalled group stays out of the ready set until it is reported
available again, however much work it accumulates.

Reports are applied before a plan is armed, so a stall observed at a tick keeps
its group out of a plan armed at that tick, and strands it only in a plan armed
earlier.

A stalled group is a different thing from a slow one. A slow group is modeled by
work that costs more ticks of worker occupancy: it is dispatchable and takes
longer. Conflating the two would make "this group is expensive" and "this group
is unavailable" the same signal, and only one of them should keep a group out of
a plan.

## Result Taxonomy

Every submission produces exactly one stable admission answer:

```text
Queued(work)            a queue slot was taken
AlreadyQueued(work)     an exact retry joined the slot it already holds
Replayed(result)        an exact retry of the highest completed request
Rejected(rejection)     refused before any slot was taken
```

Admission rejections, none of which consumes a sequence:

```text
GroupOutOfRange
GroupUnknown
GroupTombstoned
StaleIncarnation(current)
FutureIncarnation(current)
GroupNotAcceptingWork(state, class)
GroupPoisoned
GroupQueueFull(limit)
GlobalQueueFull(limit)
ClientOutOfRange
SessionNotOpen
StaleSession(current)
FutureSession(current)
StaleSequence(highest)
SequenceGap(expected)
ConflictingRetry
SequenceExhausted
FingerprintMismatch(expected)
```

Counter results, produced when the work is serviced:

```text
Added(value)
Value(value)
Rejected(CounterOverflow(current))
```

Overflow fails closed rather than saturating. A saturated counter that silently
stopped counting would satisfy every aggregate check in this crate while losing
the adds that reached it. The refusal consumes and caches its sequence like any
other outcome, so a retry replays it rather than trying again against a state
that may have changed.

Lifecycle outcomes:

```text
Created(incarnation)
Applied(from, to, incarnation)
Idempotent(state, incarnation)
Rejected(rejection)
```

A first creation and a reopening are one outcome because they differ in exactly
one observable way, and that way is the incarnation.

## Invariants

The implementation and oracle must establish:

1. Every group in an armed plan is offered exactly once before that plan
   retires.
2. No plan is armed while another still owes a group its turn.
3. A plan names exactly the ready set at the instant it was armed.
4. Therefore `widest_gap` is zero: no continuously ready group ever goes a
   complete pass without a turn.
5. Worker exhaustion suspends a pass and never restarts it.
6. One turn services `min(quota, pending)` items, or fewer only when the group
   poisoned itself part way through.
7. A turn services its own classes in priority order, and within a class in
   arrival order.
8. Class priority never changes plan membership or plan order.
9. Every admitted item is serviced or reported as failed; nothing is dropped.
10. `admitted = serviced + failed + queued` over any history.
11. Both queue bounds refuse admission without touching accepted work.
12. An acknowledged request is replayable from the session cache while the
    queue is full.
13. Every accepted request identity changes one counter at most once.
14. An exact retry returns the original result, whether the original is queued
    or completed.
15. Conflicting request reuse never changes state.
16. An older session cannot act after a newer epoch opens, and a newer epoch
    never cancels work the older one had accepted.
17. Every lifecycle request is idempotent, an applied edge, or an explicit
    conflict naming both states.
18. Removal never outruns a queue.
19. Late traffic naming a removed or reopened slot is refused and resurrects
    nothing.
20. A tombstoned identity refuses everything, forever, ahead of every
    incarnation question.
21. A poisoned group stops itself and no other group.
22. A group's counter is the sum of the deltas serviced for that group, and no
    other's.

Aggregate invariants do not imply linearizability. The later adapter will record
invocation, completion, rejection, unknown outcome, provable refusal, and
real-time ordering for an independent history checker.

## Independent Oracle Rule

The implementation and reference oracle share command, result, and inspection
types only. Two pieces of the shared schema deserve naming, because both are
encoding rules rather than transitions: the fingerprint digest, and the rule
that work identifiers are issued consecutively from one on admission. The second
makes identifiers a checksum of agreement — if the two disagree about whether a
submission was admitted, their identifiers diverge and the divergence is loud.

They do not share:

- transition functions;
- validation helpers;
- session or sequence decision code;
- lifecycle decision code;
- queue mutation helpers;
- readiness bookkeeping;
- scheduling decisions; or
- fairness accounting.

No code is shared with any other reference consumer either. No common harness
exists, and none will be extracted before a second consumer proves the same
shape is needed.

### How they differ

`ManagedScheduler` keeps live books and no history: a dense slot table indexed by
group ID, four class queues per group, a ready set maintained incrementally in
constant time per change, a rotating cursor, and worker occupancy. It emits a
tick report once and forgets it, so a scheduler that has run for a billion ticks
is the same size as one that has just started.

`ReferenceScheduler` keeps a history and no books. It schedules nothing. It
answers every question by folding its log: the counters, the lifecycles, the
queue contents, the ready set at each arming, and the per-group opportunity
gaps are all consequences of the recorded events rather than state kept beside
them. Its groups live in an ordered map, its queue is one flat list in arrival
order whose priority head is found by scanning, and it holds no ready set at all
— at each arming it recomputes readiness from first principles and compares.

A bookkeeping mistake in either has nothing to hide behind in the other. A
scheduler that dropped a group from its ready set produces a plan the oracle's
recomputed ready set does not match, and the fold names the group.

### The oracle folds requests and decisions, never conclusions

The history has three families: what callers asked for, what the scheduler
decided, and what callers observed. The oracle folds exactly the first two.

A conclusion it copied would be a conclusion it could not contradict. So the
oracle is told that a group was offered a turn and that a particular item was
serviced, and it works out for itself whether the group should have been in the
plan, whether that item was the one owed the slot, and what servicing it does to
the counter. Corrupting every observation in a history changes nothing the
oracle derives, and that is asserted rather than assumed.

## History Vocabulary

A recorded history contains, in real-time order:

```text
Invoked(operation_id, operation)                       a caller's request
Completed(operation_id, outcome)                       a terminal observation
Unknown(operation_id)                                  an unresolved observation
NotAdmitted(operation_id)                              a provable refusal

AvailabilityReported(tick, group, availability)        an external input
WorkerReleased(tick, group)                            a scheduler decision
PassArmed(pass, tick, plan)                            a scheduler decision
GroupOffered(pass, group, outcome)                     a scheduler decision
WorkServiced(pass, group, work)                        a scheduler decision
PassCompleted(pass, tick)                              a scheduler decision
```

Deterministic rejections are ordinary `Completed` observations. Every invocation
carries its full operation, so retries under one request identity are
recoverable from the history alone.

A queue slot is not a terminal outcome. A submission that is queued completes
when its work is serviced or retired, and the history records that completion
against the original invocation.

### Mutation outcomes

The three lost-outcome forms differ only in what the caller can prove:

- `Completed` carries the observed answer, admission and counter rejections
  included.
- `NotAdmitted` means the submission provably never took a queue slot. No copy
  of it can be serviced later, so it changed no counter, consumed no sequence,
  and left its request identity free for a fresh attempt. A checker must treat
  it as never having happened.
- `Unknown` means the caller cannot tell. The submission may have taken a slot,
  so the caller must retry the *same* request identity and let the session
  decide.

`NotAdmitted` is the counterpart of the siblings' `NotCommitted`, and it is
reserved rather than earned in this milestone: there is no transport yet, so
nothing here can prove a refusal it did not observe directly. What earns it is
defined where the refusal becomes observable — in the adapter — and until then
the vocabulary slot exists so that a future transport has somewhere honest to
put the distinction. Recording a provable refusal as `Unknown` would let an
implementation that serviced it be explained away.

## What This Milestone Does Not Close

This is the foundation slice. It contains the contract, bounded command and
result types, a deterministic ready-set scheduler with a per-group counter
machine, a structurally independent oracle, the history vocabulary, invariant
tests including a negative control for the fairness audit, and seeded
differential workloads over thousands of groups.

It intentionally contains no Rafter dependency, no adapter, no transport, no
disk backend, no shared reference framework, and no new Rafter public API. The
stopping point is the same one the ledger and the lock used: it gives the
adapter an application contract to meet instead of letting integration
convenience define the application.

Named consequences, so they are not discovered later:

- **The scheduler is not integrated.** `rafter-multiraft` has no managed
  scheduler today, and this contract is the input to designing one rather than a
  report on using one. The next slices add hot/cold, slow, poisoned, and
  snapshot-heavy groups against a real host, then removal, tombstone, reopening,
  and late-message behavior, then bounded process composition.
- **Sessions are not replicated.** Session establishment is immediate here, and
  becomes a replicated command when there is a log to put it in.
- **Ticks and costs are not durations.** A tick is the scheduler's unit of
  attention and a cost is worker occupancy. Nothing in this document may be read
  as a wall-clock or timeout guarantee, and the fairness bound in particular
  makes no latency claim of any kind.
- **Work is applied at dispatch.** A worker's occupancy models what the work
  cost, not a window during which the work is half-done. The unit of application
  is one item, so there is no partially applied state to represent.
- **A pass boundary is one per tick.** A tick arms at most one plan and retires
  at most one. This is a modeling choice that keeps the pass-to-tick
  relationship crisp; it costs a tick of capacity at each pass boundary and
  changes nothing the bound asserts.
