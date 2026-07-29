# Sharded Counter Service Contract

Status: third reference consumer for Rafter 1.0 API discovery. The independent
contract, model, and oracle now sit beside the first real adoption of Rafter's
managed many-group scheduler: three-node counter groups, replicated sessions,
bounded codecs and snapshots, and deterministic peer routing.

This crate ran the program's sequencing backwards from its siblings, on
purpose. The ledger and fenced lock began against existing Rafter surfaces;
this counter first specified the missing managed layer with a dependency-free
model and structurally independent oracle. The promoted API is now exercised
through versioned public Rafter dependencies. Source mode supplies checkout
patches and package mode supplies exact unpacked archives; the canonical
manifest contains neither checkout paths nor unpublished hooks.

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

### What the bounds cost

`max_groups` costs nothing on its own. No slot exists until one is created, so
a configuration that permits a million groups and holds none is the same size
as one that permits four.

**The slot table is dense, and that is the cost this document owes.** Slots are
indexed by group ID, so the table spans one past the highest ID ever created —
not the number of live groups, and not the configured bound. Creating a single
group at ID 999_999 spans a million slots to hold it, and the slots below stay
empty for as long as the scheduler runs. `slot_span` reports the number, and the
distance between it and the count of live groups *is* the cost. A host that
addresses groups sparsely at the top of a wide range pays for the range below;
this crate's workloads allocate dense IDs from zero, which is the shape the
contract expects, and a sparse addressing scheme is a change to this document
first.

The density is deliberate twice over. It buys constant-time slot lookup, and it
keeps the implementation structurally unlike the oracle, whose groups live in an
ordered map. A differential between two halves that agreed on the structure most
likely to hide a bookkeeping mistake would be worth much less than one between
two halves that do not — which is why the honest fix here was to state the cost
rather than to adopt the oracle's shape and lose the disagreement.

Nothing in the tick loop pays it. Arming a pass costs one traversal of the
*ready* set rather than of the configured range, and ready-set membership is
maintained exactly and in constant time per change, so a host with thousands of
idle slots pays nothing for the handful that have work. That is the property the
deterministic workload leans on: it drives thousands of groups, and a scheduler
that scanned its whole address space every tick could not. The two methods that
do traverse the table — the state view and the aggregate summary — are
checkpoint tools, not something the scheduler runs.

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

This is version 1 of the counter schema. The independent model vocabulary stays
closed and in-memory. The real adapter gives its replicated command an explicit
bounded frame carrying this version; an unrecognized version, oversized frame,
malformed field, or trailing byte is refused rather than reinterpreted. Adding a
variant to either vocabulary is a change to this document first.

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

Session epochs and request sequences are nonzero. `OpenSession` is an admission
gate and answers the same refusals work submission does, in this order:

1. the group must exist, be in range, not be tombstoned, and be the incarnation
   named — `GroupOutOfRange`, `GroupUnknown`, `GroupTombstoned`,
   `StaleIncarnation`, `FutureIncarnation`;
2. the group's lifecycle state must establish sessions — `Recovering` and
   `Serving` do, and every other state is refused with
   `GroupNotAcceptingSessions(state)`;
3. the group must not be poisoned — `GroupPoisoned`;
4. the client ID must be within the group's configured slot range —
   `ClientOutOfRange`.

**Rule 2 is not rule 2 of a submission, and it does not borrow its refusal.** A
`Recovering` slot establishes sessions and refuses `Command` work. Admitting the
session early is the point: a client that has its session in place can submit
the instant the slot serves, instead of discovering during the first command
that it also has a session to negotiate. Reporting that refusal as
`GroupNotAcceptingWork(state, Command)` would have named a class the session
gate never consulted, and would have said `Command` was refused by a state that
admits sessions precisely so that `Command` work can follow.

Past the gates:

- an unused client slot accepts its first epoch;
- the current epoch is idempotent and preserves its cached completion;
- a greater epoch replaces the session and clears its sequence and cache;
- a lower epoch is rejected as stale.

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
  expected sequence does not move until the outstanding request completes.

#### The sequence ceiling

There is no exhaustion refusal, and the reason is worth stating rather than
leaving as an absence.

A session's highest completed sequence starts at one and advances by exactly
one, because `highest + 1` is the only new sequence admitted. Reaching the
numeric ceiling therefore takes `u64::MAX` completed requests. And *at* the
ceiling, every request a client can construct is already answered: one below it
is `StaleSequence(highest)`, and one equal to it replays if the command matches
and is a `ConflictingRetry` if it does not. Nothing exceeds the maximum, so no
request can ask for the successor that does not exist.

The vocabulary carried a `SequenceExhausted` refusal for that successor. No
input produced it — it sat behind a comparison nothing satisfies — and this
document has already argued, about the absent session-table capacity refusal,
that a refusal for a state that cannot be reached is a promise about behavior no
test could observe. The same argument retires this one. What remains is the type
guard: the sequence successor is `None` at the ceiling and fails closed rather
than wrapping onto a cached completion, and that is asserted directly.

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

### Model sessions and real adapter replication

`OpenSession` is an admission-gate action rather than queued work. Queueing it
would mean a client needed a queue slot to open the session that a queue-full
rejection tells it to retry under, which is a circle with no exit.

The independent model therefore establishes a session immediately. The real
adapter admits the same policy action as control work and replicates
`OpenSession` through the group's Raft log before counter commands can use it.
This difference is deliberate: the model remains a scheduler oracle rather
than sharing the adapter's log transitions, while both produce the same
client-visible session state.

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
4. it is not occupying a worker whose cost is unpaid; and
5. its queue holds at least one item.

Condition 4 is not an exclusion from service. A group occupying a worker is not
starved; it is being served, and offering it a second concurrent turn would let
one group hold two workers while another holds none. **That sentence is load
bearing for the whole bound, and it holds only because an occupancy cannot be
opened by a turn that served nobody** — see rule one of "Occupancy is derived,
not reported".

Conditions 1, 2, and 5 follow from what callers asked for, and are read off the
history directly. Condition 4 follows from what the scheduler *decided*, and
that is exactly why it may not be reported: a condition the audited party
defines for itself is not a condition. It is derived instead, from the work each
turn recorded.

**Condition 3 is the only one the history reports rather than implies**, and
that is what makes it the delicate one. It models an input from outside the
scheduler, so it cannot be derived; but a scheduler that could move it freely
would define any group out of the fairness question. It is *constrained* rather
than derived — see "A stall excuses only while it is unbroken" — and the
constraint is that a stall cleared and re-raised without a plan naming the group
in between excuses nothing, at any point in the pass cycle **but one**: while
the open pass has more entries pending than the pool has free workers. Inside
that one it excuses everything, for as long as the host keeps it up.

Constrained is deliberately weaker than bounded, and the difference is the
fourth row of "What falls outside that scope", which is marked **No** under
"Bounded?". Three of that rule's four qualifications deny a group one window
each and cost the host something to re-establish; the fourth costs nothing,
holds from the arming tick of any plan wider than the pool, and is the known
limit of the rule rather than a qualification on it.

A sentence here that reads wider than that row is a claim this document cannot
keep, and the previous revision of this paragraph made both of the available
mistakes: it called condition 3 bounded, and it claimed the constraint held at
every point in the pass cycle without exception. The escape's own row already
said No to the first and *was* an exception to the second, and two audits that
hunted for exactly this shape read past it — a universal in prose has nothing
that fails when it stops being true. So it has one now:
`gen6_contract_stall_boundary::every_statement_of_the_stall_boundary_names_its_exception`
requires each place this document states the boundary to name the escape in the
same breath, and refuses the retired wordings by name.

### Occupancy is derived, not reported

A dispatch opens a worker occupancy. The occupancy is fully determined by the
work the turn did, so an observer computes it rather than being told it:

```text
cost(turn) = sum of ServiceCost over the items the turn serviced
due(turn)  = tick of the dispatch + cost(turn)
```

Both inputs are recorded — the serviced items, as `WorkServiced` events, and the
tick the turn was taken at — so the deadline is a fact about the history rather
than a claim inside it.

**"The items the turn serviced" means the `WorkServiced` events it recorded, and
nothing else.** The head of the group's queue is what the turn is *offered
against*; it is not what the turn cost, because a turn may fail to move it. A
generation of this document said "serviced" while the audit priced the turn from
the queue, and the difference is the whole of the following, which the audit
accepted:

```text
audit: passes_completed=25 opportunities=25 widest_gap=0;
group queued=1 serviced host-wide=0
```

Twenty-five turns, each charged for one item, none of which moved.

**A turn ends at the first recorded event that is not one of its own services.**
That is what makes it priceable: another dispatch, a release, a plan retiring, or
the end of the record all settle it. The rule is not an extra constraint but
"work is applied at dispatch" made checkable — a turn is one indivisible act, so
its services follow its dispatch with nothing between them. A turn is settled
before the event that ended it is judged, so a release is always weighed against
an occupancy whose price is already known.

Six rules follow, and every one of them has a scheduler in
`tests/redteam_controls.rs` written to break it and a control that differs from
that scheduler by one decision:

- **A turn services exactly the items it was offered against.** Ending a turn
  with any of them still queued is `DispatchLeftWorkUnserviced`; servicing past
  them is `DispatchServicedBeyondItsWork`; servicing the wrong one is
  `ServiceOrderViolation`. Without the first of those, a dispatch buys a full
  occupancy with nothing.
- **A dispatch is priced by its work.** A turn that reports any cost other than
  the sum of the `ServiceCost`s of the items it serviced is refused, and a turn
  that reports a count other than the number of services it recorded is refused.
  Otherwise a scheduler names its own occupancy, and every rule below is
  measured against a number it chose.
- **An occupancy ends at `due`, and ends only there.** A release recorded after
  `due` held a worker past its cost; one recorded before `due` returned a
  worker that was still busy, which lets the host run more dispatches at once
  than it has workers. Both are refused. This says nothing about a release
  having to be *recorded*: the deadline is computed, so a history with no
  `WorkerReleased` events at all is timed exactly like one full of them, and
  `redteam_occupancy::an_occupancy_ends_at_its_deadline_with_no_release_ever_recorded`
  is that history.
- **A release pairs with a dispatch or it is refused.** A release naming a
  group that holds no worker is a fault, not a no-op. Absorbing it would let a
  scheduler clear an occupancy it never opened.
- **An occupancy past `due` stops excluding its group from the ready set.**
  A group inside an occupancy is owed no turn — it is being served. A group
  inside an occupancy that has outlived its cost is being served by nobody, so
  it is ready, is owed every plan armed from that instant, and accrues gap for
  each one that omits it.
- **Turns are concurrent, and each keeps its own owed set and its own
  deadline.** A pool of `W` workers may hold `W` turns at once, within one pass
  or across passes, and the pool is counted rather than reported. A single
  dispatch slot for a pool of workers is how the second dispatch in a pass came
  to discard the first one's work with nothing complaining.

The fifth rule is what makes the bound hold against a scheduler that controls
only what it legitimately controls. Before it, `servicing` was a bit the
scheduler set on dispatch and cleared on release, and neither was checked: one
omitted release put a group permanently outside the ready set, permanently owed
nothing, and permanently invisible to a `widest_gap` of zero. The starved group
did not appear in the report because, by the report's own definition, it was
never starved.

The first rule is what makes the *fifth* mean anything. "A group inside an
occupancy is owed no turn — it is being served" is load bearing for the whole
bound, and it is true only because an occupancy can no longer be opened by a
turn that served nobody.

### What the scheduler is left free to decide

This document previously read: "The scheduler retains exactly two freedoms over
readiness, and both are legitimate: which items it services in a turn — bounded
by quota and class order — and when it arms the next plan." That sentence did
not enumerate. It named a freedom that is not one, and omitted a freedom that
was.

There is **one** freedom, and each entry below is backed by the check that holds
it rather than by this paragraph:

| Decision | Free? | What holds it |
| --- | --- | --- |
| When to arm the next plan | **yes** | Bounded only by `PassBoundaryReused`: at most one plan armed and one retired per tick. Liveness is deliberately not claimed; see "What the audit does not claim". |
| Which items a turn services | no | Fully determined by quota, class order, and arrival order. Every deviation is named: `DispatchLeftWorkUnserviced`, `DispatchServicedBeyondItsWork`, `ServiceOrderViolation`, `ServiceCountMismatch`, `QuotaExceeded`. |
| How long the resulting occupancy lasts | no | `DispatchCostMismatch`, `WorkerHeldPastCost`, `WorkerReleasedEarly`, `SpuriousWorkerRelease`, `WorkerCountExceeded`. |
| Which groups a plan names | no | `PlanIncludedUnreadyGroup`, `PlanRepeatedGroup`, `OpportunityGap`, `PassArmedWhileOpen`, `PassCompletedWithUnofferedGroup`. |
| Whether a group is stalled | **partly** | An external input. Breaking a stall to dodge a plan is refused at every instant but one, and that one — an open pass with more entries pending than the pool has free workers — is unbounded: see "A stall excuses only while it is unbroken", and the fourth row of "What falls outside that scope". |

"Which items it services in a turn" was a freedom only in its degenerate form —
the scheduler could decline to service them — and that form is now
`DispatchLeftWorkUnserviced`.

### A stall excuses only while it is unbroken

Readiness condition 3 is the one condition the history *reports* rather than
implies, and it was a third freedom over readiness for exactly as long as it was
sampled only at arm instants. What replaced that sampling takes the freedom back
at every instant but one — while the open pass has more entries pending than the
pool has free workers — and inside that one the freedom remains, entire and
unbounded. A group reported `Stalled` immediately before every arming and
`Available` immediately after every retirement is never sampled as ready, so it
is owed no plan and accrues no gap:

```text
audit: passes_completed=20 widest_gap=0; starved group stalled=false queued=1
```

Twenty completed passes, a group the audit's own final view calls available and
holding a backlog, and a perfect score.

The previous generation's rule closed that window and left its dual open. It
read "a group returned to `Available` at an instant when a plan could have been
armed to name it — **no pass is open**, and nothing but the stall was keeping it
out", and the justification offered for the pass qualification was that "a stall
raised while a pass was already open still excuses, because no plan could have
been armed to name the group". That sentence is about the instant a stall is
*raised*; the code applied it to the instant availability is *restored*. Those
are different instants, an open pass apart, and the scheduler authors both the
availability reports and the pass boundaries — so it picked the window:

```text
passes_completed = 20   opportunities = 20   serviced = 20 host-wide, 0 for the starved group
widest_gap       = 0    starved group: queued 1, stalled true, servicing false
```

A group available for eight of every ten ticks, holding a backlog throughout,
receiving nothing, and a clean audit. The service floor does not catch it
either: twenty items moved.

#### The rule, and exactly what it covers

> **Scope.** For every recorded `Available` report naming a group that is, *at
> that instant*, stalled, dispatchable but for its availability, holding no
> worker, and not waiting behind an open pass with more entries pending than
> the pool has free workers: the group is *owed the next plan armed*. The debt
> is discharged by a plan naming the group, and by nothing else — not by a
> `Stalled` report, and not by a second `Available` report saying what the
> first already said.

A group owed a plan it does not appear in accrues gap. It is still not *ready*,
so a plan that named it would break plan totality; the only way to settle the
debt is to stop breaking the stall.

Where the report falls in the *pass cycle* is not by itself part of that, and it
was. What replaced it is narrower — an open pass with more entries still pending
than the pool has free workers to offer them with. No plan may be armed while
one is open and no plan may retire owing a turn, so such a pass does stand
between the scheduler and the next arming. Narrower is not absent: the
replacement is still a condition on the open pass, so a sentence saying the pass
cycle does not enter at all reads wider than the rule.

It is **not** the required bound's own "absent global resource exhaustion"
precondition, and earlier revisions of this section called it that. What the
comparison prices is the width of the plan against the size of the pool, and
both are things the host brings to the tick. The fourth row below is where that
is stated at its real cost.

#### What falls outside that scope

Four qualifications remain. The first three deny a group at most **one** window
each, because a scheduler cannot re-establish any of them without either giving
the group up or serving it; the fourth is unbounded and is the known limit of
the rule rather than a claim about it. Each has a test.

| Outside the scope | Why it is kept | Bounded? | Test |
| --- | --- | --- | --- |
| Restored while the group holds a worker | The worker was what kept it out, and its price is work the group did | Once: to be occupied again it must be dispatched, which takes a plan naming it | `redteam_occupancy::restoring_availability_inside_an_occupancy_denies_one_window_and_no_more` |
| Restored while the group holds no work, is poisoned, or is not serviceable | The stall was not what kept it out | Once: to empty the queue again it must be serviced | `redteam_occupancy::restoring_availability_with_an_empty_queue_denies_one_window_and_no_more` |
| Reported available when it was not stalled | Not a transition; ordinary readiness already owes it every plan | Once: the window before a group's first stall | `redteam_occupancy::availability_reported_for_a_group_that_was_never_stalled_opens_no_debt` |
| Restored while the open pass has more entries pending than the pool has free workers | Named below; it is a limit, not a justification | **No** — it holds from the arming tick, at no cost | `redteam_occupancy::a_plan_wider_than_the_pool_is_excused_from_its_arming_instant` |

The fourth row is the honest statement of what this rule does not reach, and it
is stated without a consoling clause. A host that keeps more plan entries
pending than it has free workers, and lets a group's availability appear only
there, is owed nothing however long that runs.

It is not the cost it was previously described as. `pending.len() > free` is
satisfied at the instant a plan is armed — `pending` is the whole plan and no
worker is yet held — so **any** plan naming more groups than the pool has
workers qualifies from its arming tick, with the pool idle and nothing
dispatched. A host whose shard count exceeds its pool is in this state
continuously and cannot leave it, because a plan that omitted a ready group
would be an `OpportunityGap`. So it is not "a host that is simply busy"; it is
the ordinary shape of a large host, busy or idle:

```text
arming-only escape: passes_armed=12 passes_completed=12 serviced=36
widest_gap=0; starved group holds 1 items and received nothing across 12 passes
```

It also turns on a number the history never records. One event stream, folded
under two configs differing only in `workers`, draws two verdicts —
`redteam_occupancy::the_excuse_turns_on_a_pool_size_no_decision_records` pins
that.

**The obvious tightening is refuted, in the other direction.** Requiring genuine
saturation — not one worker free — reads as saturated almost never: an occupancy
is due at the tick its cost is paid and is not counted held at that tick, and a
report is folded before that tick's releases and dispatches, so a host running
unit-cost work on every worker it has reads zero workers held at exactly the
instants the question is asked. Applied to
`differential::independent_models_agree_across_thousands_of_groups` — four
thousand groups on thirty-two workers, plans over half the host, passes spanning
about a hundred ticks — it convicted the honest model of starving a group over
an availability window eighteen ticks wide, during which no plan could be armed
at all. Both candidates fail, in opposite directions, and this document has no
third. That is why the row is stated rather than closed.

The strictness of the comparison is what keeps the row from swallowing the rule
above it, at both ends. A pass with an entry pending and a worker free to offer
it with is not a pass the host *cannot* finish; it is one it chose not to
(`redteam_occupancy::a_pass_held_open_with_a_worker_to_spare_excuses_nothing`).
Neither is a pass with nothing pending at all, however booked out the pool is
(`redteam_occupancy::a_pass_held_open_past_its_last_entry_excuses_nothing`).

What this rule does *not* touch at all is a stall that holds. A group reported
`Stalled` once and never reported available again is owed nothing however long
it holds work, and the audit says so: that is the external input doing its job,
and
`redteam_occupancy::a_stall_held_unbroken_is_the_one_legitimate_way_a_group_receives_nothing`
is the assertion.

**A tick arms at most one plan.** This is the modeling choice named at the end
of this document, and it is also what stops the derivation from being evaded by
standing still. Deadlines are measured in ticks; a scheduler that could arm an
unbounded number of plans at one tick could deny a group all of them while no
occupancy it held ever came due. Arming twice within a tick, retiring twice
within a tick, and recording a tick earlier than one already recorded are each
refused.

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
consecutive *armed plans* in which `g` was ready at arm time and absent from the
plan. Readiness is sampled at each arming, which is where a plan either owes a
group a turn or does not; counting armed plans rather than retired passes is the
stricter of the two, because a plan that omitted a ready group and then never
completed is still counted. The bound is:

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
takes longer.

**`W` does appear in one of the bound's qualifications, and the audit's verdict
is not invariant under it.** An earlier revision of this paragraph said the
bound was invariant under `W` and that this was "why the bound survives resource
exhaustion rather than being excused by it". The first half is true of the
formula and false of the audit: the fourth row of "What falls outside that
scope" compares plan width against `W`, so one recorded event stream — the same
plan, the same turns, the same releases — draws a violation under a pool of
three and an acceptance under a pool of two.
`redteam_occupancy::the_excuse_turns_on_a_pool_size_no_decision_records` is that
pair. The second half was the conclusion drawn from the first, and it does not
follow: that row is where the bound is *excused* by exhaustion, not where it
survives it.

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

### What the audit does not claim

Being a safety property has a price, and it is paid here rather than discovered
by a reader who assumed otherwise.

**A green audit says no recorded decision broke a rule. It does not say
decisions were made.** A scheduler that arms no plan at all satisfies F1 and F2
vacuously: there is no plan omitting a ready group, because there is no plan.
The audit reports `Ok` with `widest_gap == 0` over a host of permanently ready
groups that received nothing, and that is the correct answer to the question it
asks.

So the report carries `passes_armed`, `passes_completed`, `opportunities`, and
`serviced` beside the gap, and **a fairness assertion is incomplete without a
floor on `serviced`**. That is a rule this crate's tests keep, not a nicety: two
models that both did nothing agree perfectly, and an audit of the emptiness
certifies it. One test pins the vacuous case directly, so the shape is on the
record rather than implied.

**The floor is on service, and not on plans, because arming is free.** A floor
on `passes_completed` was the previous recommendation and it does not hold. An
empty plan names exactly the ready set when nothing is ready, so a host that has
stopped can arm and retire plans forever and clear any threshold on them.
`ServiceCost` is bounded only by `u32`, so a single admitted item can book the
only worker to tick 4_294_967_296 while the pass counter climbs over a host
doing nothing:

```text
audit: passes_completed=40 serviced=1 widest_gap=0; group holds 6 items
and is servicing=true until tick 4294967296
```

Nothing in that history lies, the one turn services exactly what it was offered,
and the audit is right to accept it. `serviced` is the only number in the report
an empty plan cannot inflate.

**`ServiceCost` is deliberately not bounded by configuration.** That was the
alternative, and the argument against it is that it does not close anything: any
ceiling only rescales the wedge, since a host with one worker and a cost just
under the ceiling is wedged for just under the ceiling. It would also make "a
group whose storage is slow" unrepresentable past a number this document has no
basis to pick, and this document has already said that a tick is not a duration
— so a numeric ceiling on cost would be a duration claim wearing a bound's
clothes. The honest fix was to stop letting the pass counter stand in for work.

**A green audit says no rule the audit names was broken. It does not say the
rule set is complete.** Every rule below has a deliberate violator and a control
— see "Invariant 24 is the one that keeps the rest honest" — and that
demonstrates the rules fire, not that they cover. Three starvations have now
been found that broke no rule this document had written, all of them in the one
readiness condition the history reports rather than implies. One further
starvation is *known* and left open: see the fourth row of "What falls outside
that scope", under "A stall excuses only while it is unbroken".

**This document offers no bound on that fourth row, and no number in the report
is one.** An earlier revision of this paragraph sent a reader wanting one to the
`serviced` floor. That was wrong twice over: a host inside that row is servicing
at the full rate of its pool, so the floor rises with the evasion rather than
against it, and the row does not even require the host to be busy — the
qualification holds from the arming tick of any plan wider than the pool, with
every worker idle.

**Liveness is deliberately not claimed, in either of its two forms.** That
plans continue to be armed, and that an armed plan eventually retires, are both
statements about progress over time. A pass legitimately spans many ticks —
worker exhaustion suspends it, and the suspension is the exception the bound is
built to survive — so "armed but not yet retired" is an ordinary state and not
an auditable fault, at any threshold. Naming a threshold would mean naming a
tick budget, and a tick is not a duration; a scheduler could satisfy any such
budget by arming narrower plans. The difference `passes_armed -
passes_completed` is nevertheless reported, so a caller that wants to say
something about progress has the number to say it with, and says it under its
own assumptions rather than this document's.

### The ready set is exact, and what follows

The scheduler maintains ready-set membership exactly and in constant time per
change. A consequence worth naming: a plan entry's readiness cannot be revoked
between its plan being armed and its turn arriving, except by an external
readiness report. Two of the three ways it could be — its queue emptying, its
own work poisoning it — each require the dispatch that the offer in question
would have been, so neither can happen first. The third does not: an operator
may move a planned group's lifecycle mid-pass, with no dispatch involved. It
still revokes nothing, because the only edge that would is removal, and removal
is refused while the group holds a queue it has not drained — which is the same
queue that made it ready. So `Stalled` is the only reason a turn can be skipped,
and its being the only one is a property rather than an omission.

The enumeration is backed rather than argued. `SkipReason` has one variant, so a
skip for any other reason is unrepresentable; a skip reported for a group nothing
stalled is `SkippedAvailableGroup`, and
`redteam_controls::Cheat::SkipAGroupThatIsAvailable` is the scheduler that takes
it. A dispatch of a group an external report stalled after the plan was armed is
`DispatchedUnreadyGroup`, and `Cheat::DispatchAGroupThatIsNotReady` is its
counterpart.

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

**Stickiness is what the audit charges for.** Being the only readiness input the
scheduler does not derive makes this signal the one place a scheduler could
define a group out of the fairness question, and it did — twice, from opposite
sides of the same pass. A stall flicked on before every arming and off after
every retirement kept a group out of twenty consecutive plans at a `widest_gap`
of zero; so did a stall flicked on for each arming and off for the whole
interior of the pass that followed. So the excuse a stall buys is charged
against how long it is held rather than against whether it happens to be raised
at the instant readiness is sampled — with one exception, and the exception *is*
a fact about where in the pass the stall is broken: an open pass with more
entries pending than the pool has free workers excuses the whole of it, from the
arming tick, unbounded. "A stall excuses only while it is unbroken" states the
rule and enumerates what falls outside it, the fourth row being that exception;
this section is where the signal it constrains is defined.

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
GroupNotAcceptingSessions(state)
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
FingerprintMismatch(expected)
```

Two refusals are deliberately absent, and both for one reason: a refusal for a
state that cannot be reached is a promise about behavior no test could observe.
There is no session-table capacity refusal, because the addressable client range
is the table's bound. There is no sequence-exhaustion refusal, because no
request can name a sequence above the ceiling. Every rejection listed above is
produced by some input, and that is the standard this vocabulary is held to.

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
4. Therefore `widest_gap` is zero: no group that was ready when a plan was armed
   is ever left out of it.
5. Worker exhaustion suspends a pass and never restarts it.
6. One turn services `min(quota, pending)` items, or fewer only when the group
   poisoned itself part way through.
6a. A turn's worker occupancy is the sum of the `ServiceCost`s of the items it
    **serviced** — the `WorkServiced` events it recorded, not the queue it was
    offered against — and it ends at the tick that dispatched it plus that sum,
    neither earlier, nor later, nor never.
6b. No more groups occupy workers at once than the configuration has workers,
    and no group occupies two. A turn still being recorded holds one.
6c. A group whose occupancy has outlived its cost is ready again, whether or
    not its release was ever recorded.
6d. A turn services exactly the items it was offered against: no fewer, no
    more, and none in another order. A turn that ends owing work bought its
    occupancy with nothing.
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
23. An external stall excuses a plan only while it is unbroken. A stall cleared
    at an instant when the host could have reached an arming that named the
    group — anywhere in the pass cycle, and not merely between passes — and
    re-raised before that plan was armed, excuses nothing. One exception
    remains, and it is not a precondition of the bound but an unbounded limit
    on this invariant: an open pass with more entries pending than the pool has
    free workers, which holds from the arming tick of any plan wider than the
    pool. "A stall excuses only while it is unbroken" states the scope and
    enumerates what falls outside it.
24. Every rule this audit enforces has a scheduler in the test suite written to
    break exactly it, and a control that differs from that scheduler by one
    decision and is accepted. A *rule* is a place the fold decides one, not a
    variant it answers with: the two differ, five times over. What this
    invariant does not claim is that the rule set is complete — see "Invariant
    24 is the one that keeps the rest honest".

Aggregate invariants do not imply linearizability. The later adapter will record
invocation, completion, rejection, unknown outcome, provable refusal, and
real-time ordering for an independent history checker.

### Invariant 24 is the one that keeps the rest honest

Three generations of this crate shipped a rule, a paragraph explaining it, and a
workload that could not break it. The reason is structural rather than an
oversight in the workloads:

> `ManagedScheduler` always services what it dispatches, always releases what it
> takes, and always plans what is ready. **No history the `Recorder` can produce
> distinguishes a rule the audit checks from one it ignores.**

Scale does not fix that. "Occupancy is derived, not reported" was tested against
thousands of groups while the audit derived occupancy from work that was never
done, because the derivation was never asked a question whose answer it could
get wrong. The positive control was the vacuity.

So `tests/redteam_controls.rs` holds a deliberately-violating scheduler for
every rule the audit decides, each asserted as a pair: the cheating history must
trip the named check and produce the named fault, and the same history with that
one decision taken correctly must be accepted and must have moved real work.

#### A rule is a site, and exhaustive matching closes variants

The previous generation of that file matched `SchedulingViolation` exhaustively
and concluded that no rule could exist without a control. **That conclusion is
one scope wider than the mechanism reaches**, which is the failure mode this
document has now hit four times. The fold decides **29 rules at 29 places** and
reports them through **24 variants**: five checks report a variant another check
already reports. A rule added by reusing a variant needed no new match arm, and
three had arrived that way and were exercised by nothing at all — the two
retire-side pass-ordering checks, the retire-side tick-reuse check, and the
release-side held-past-cost check. The compiler had been asked a question about
variants and answered it correctly; the claim written above it was about rules.

`FaultSite` is the registry that closes the gap: one variant per place the fold
decides a rule, carried on `Replay::fault` beside the violation, and matched
exhaustively by `control_for`. Four mechanisms hold it, and each is stated with
what it does and does not close:

| Mechanism | Closes | Verified by |
| --- | --- | --- |
| `control_for` matches `FaultSite` with no catch-all | a rule with no control | compile error `E0004` |
| `FaultSite::ALL` walks an exhaustive `next` chain, const-checked against declaration order | a site the chain does not reach, including the laziest possible arm | compile error `E0080` |
| `FaultSite::EndOfSites`, whose discriminant is the site count | a site appended and otherwise forgotten | compile error, via the length of `ALL` |
| `oracle::tests::every_fault_site_is_raised_by_exactly_one_check` scans the fold's own source | two checks passing one site | test failure |

The last row is a test rather than a compile error, and that is stated rather
than glossed: Rust exposes no variant-reflection surface that can enumerate
these sites, so the test scans the fold's own source. What *no* mechanism here
closes is a site declared after the end marker — which is a statement that the
marker is not the end, rather than an omission. `Cheat::ALL` is closed the same
way and for the same reason: it was `[Self; 23]`, a literal a twenty-fourth
variant could be left out of without anything complaining, under a test
asserting the family covered what it claimed to.

Two sites declare themselves unreachable instead of claiming a control, and each
has to survive this document's own standard for an unreachable answer, stated
twice in "Result Taxonomy": a promise about behavior no test could observe is
not made.

- `WorkUnaccountedFor` is arithmetic the fold performs over four counters it
  moves itself and in step, so no recorded history can separate them; invariant
  10 is asserted over the model's summary instead, where it is observable.
- `OfferedAnUnknownGroup` is *shadowed* rather than unreachable: an offer
  reaches it only for a group the open plan named, and a plan naming a group
  that was never created is refused at the arming. That argument is worth
  exactly what the arm-side check is worth, so it is asserted rather than
  asserted about, in
  `redteam_controls::an_offer_to_a_group_that_never_existed_is_caught_when_the_plan_named_it`.

#### What invariant 24 does not claim

It says every rule the audit *names* fires on a history that breaks it, and that
the near-miss beside it is accepted. **It says nothing about whether the rule
set is complete**, and the remainder is not small. The starvation this generation
closed broke no rule the audit named:

```text
passes_completed = 20   opportunities = 20   serviced = 20 host-wide, 0 for the starved group
widest_gap       = 0    starved group: queued 1, stalled true, servicing false
```

Twenty passes, twenty items moved, a group available for eight ticks in ten
holding a backlog throughout, every control in `redteam_controls.rs` green, and
the audit correct by its own rules. A total mapping from rules to controls says
nothing about a rule nobody wrote. The `serviced` floor is the only thing in the
report that even declines to certify such a run, and here it did not: work
moved, for somebody else.

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

No code from this consumer is shared with another reference consumer. The
neutral harness now used by the ledger and fenced-lock checkers was extracted
only after those two consumers proved the same search shape; this crate has no
dependency on it or on anything else.

### How they differ

`ManagedScheduler` keeps live books and no history: a dense slot table indexed by
group ID, four class queues per group, a ready set maintained incrementally in
constant time per change, a rotating cursor, and worker occupancy. It emits a
tick report once and forgets it, so a scheduler that has run for a billion ticks
is the same size as one that has just started.

`ReferenceScheduler` keeps a history and no books. It schedules nothing. It
answers every question by folding its log: the counters, the lifecycles, the
queue contents, the ready set at each arming, the worker occupancies and their
deadlines, and the per-group opportunity gaps are all consequences of the
recorded events rather than state kept beside them. Its groups live in an
ordered map, its queue is one flat list in arrival order whose priority head is
found by scanning, and it holds no ready set at all — at each arming it
recomputes readiness from first principles and compares.

The occupancy table is where the two shapes differ most sharply. The model
holds a fixed array of worker slots and a per-group flag saying that one of
them is taken; the oracle holds none of either, and instead keeps a deadline per
group that it computed from the *services* the turn recorded — not from the
dispatch that claimed them, and not from the queue the turn was offered against.
Neither can borrow the other's answer, which is the point: the model's flag is
what the oracle is checking.

The oracle also keeps one turn open at a time while its services arrive, and
prices it when the history moves on. The model has no counterpart to that at
all: it applies a turn's work inside one call and never has a half-recorded
turn to represent. That asymmetry is deliberate too — the oracle has to be able
to represent a turn that reported work it did not do, because that is the shape
it exists to catch.

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
GroupOffered(pass, tick, group, outcome)               a scheduler decision
WorkServiced(pass, group, work)                        a scheduler decision
PassCompleted(pass, tick)                              a scheduler decision
```

Every scheduler decision that happens at an instant records that instant, and
`GroupOffered` is one of them because a dispatch opens a worker occupancy that
comes due at its own tick plus what its services were worth. An offer that did
not say when it happened would leave that occupancy with no deadline anyone
could compute, and an occupancy nobody can time out is a group nobody can prove
was starved.

`WorkServiced` carries no tick, because it happens within the turn that does,
and that is now a rule rather than an economy. **A turn's services follow its
dispatch with nothing between them**, so the `WorkServiced` events between one
`GroupOffered` and the next recorded decision are exactly the work that turn
did — which is what makes the turn priceable at all. It is the same fact as
"work is applied at dispatch", read from the history's side.

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
reserved rather than earned in this milestone: there is no client process
transport yet, so nothing here can lose a refusal and later prove where it
stopped. What earns it is defined where that refusal becomes observable, and
until then the vocabulary slot exists so a future transport has somewhere
honest to put the distinction. Recording a provable refusal as `Unknown` would
let an implementation that serviced it be explained away.

## What This Milestone Does Not Close

The foundation contains the contract, bounded command and result types, a
deterministic ready-set model with a per-group counter machine, a structurally
independent oracle, the history vocabulary, invariant tests, a
deliberately-violating scheduler for every one of the 29 rules the audit
decides, and seeded differential workloads over thousands of groups.

The differential workloads cover the dimensions separately and together, and the
numbers they run at are restated here rather than described:

```text
exhaustive short histories   14 symbols to depth 4, 41_370 distinct histories
thousands of groups          3_000 groups, 900 ticks, 241_356 events,
                             9 passes, widest plan 2_999, gap 0        (2 seeds)
churn and combination        512 groups, 600 ticks, ~81_500 events,
                             14-15 passes, widest plan ~437, gap 0,
                             ~7_775 serviced, ~76 failed, 174-183 reopened,
                             257 removed, 33-39 tombstoned, 10 poisoned
seeded random workloads      121 and 219 retired passes on a roomy and a
                             deliberately cramped host
```

Those numbers are unchanged by this generation's two fixes, and saying so is the
point: the model was not touched, and the corrected stall rule accepts every
history the model produces at every one of those scales. A rule that had needed
the workloads to move to fit it would have been a rule fitted to the workloads.

**What they cannot cover is stated here rather than assumed away.** Every one of
those workloads is driven by `ManagedScheduler`, which always services what it
dispatches, always releases what it takes, and always plans what is ready. No
history it can produce distinguishes a rule the audit checks from one it
ignores, so the differential workloads prove agreement and prove nothing about
the audit's teeth. **Scale is the wrong axis for that question**: 241_356 events
over 3_000 groups is the same single history shape repeated, and the two
starvations this generation found are each expressible in twenty passes. That is
what the red-team schedulers are for, and it is why invariant 24 exists — and
invariant 24 in turn proves that each rule fires, not that the rules cover. Both
limits are named where each claim is made.

The first adapter slice now adds versioned public Rafter dependencies and
drives a managed local host containing three independent groups. Every group
has three real `RaftGroup` replicas on a bounded deterministic network. Session
establishment and request deduplication are replicated; counter commands use a
bounded versioned codec; state snapshots round-trip the value, applied index,
and bounded session cache. Queue overflow returns the untouched proposal, pass
plans prove one opportunity per ready group, and a consumer apply fault poisons
only its group while later groups keep committing.

Named consequences, so they are not discovered later:

- **The complete acceptance matrix is still separate work.** The first real
  slice proves three groups, bounded admission, sessions, reads, fairness, and
  poison isolation. Hot/cold and slow groups, snapshot pressure, lifecycle
  churn, removal/tombstone/reopening, late traffic, and scale profiles remain
  the next deterministic acceptance slice.
- **The deterministic network is not production transport.** It is bounded,
  routes every peer envelope, and uses real Raft groups, but has no sockets,
  authentication, disk backend, or wall-clock behavior. Process composition
  and production-composition evidence remain later milestones.
- **Ticks and costs are not durations.** A tick is the scheduler's unit of
  attention and a cost is worker occupancy. Nothing in this document may be read
  as a wall-clock or timeout guarantee, and the fairness bound in particular
  makes no latency claim of any kind. A turn's occupancy is nonetheless an
  exact quantity: it accumulates in 64 bits because a turn services at most
  `WorkQuota` items of at most `ServiceCost` each, both 32-bit, so the widest
  turn any configuration admits fits and cannot saturate. A saturating
  accumulator under-charged the worker and reported the shortfall as an
  ordinary cost, which is the one arithmetic failure this crate's own principle
  forbids everywhere else.
- **Work is applied at dispatch.** A worker's occupancy models what the work
  cost, not a window during which the work is half-done. The unit of application
  is one item, so there is no partially applied state to represent. This is also
  what gives a turn a settlement point in the history: a turn's `WorkServiced`
  events follow its dispatch with nothing between them, so the first event that
  is not one of them ends the turn and prices it. A vocabulary that let other
  decisions fall inside a turn would be a vocabulary in which a turn's cost is
  not decidable.
- **A cost has no configured ceiling.** `ServiceCost` is bounded only by `u32`,
  and `SchedulerConfig` does not bound it further. The consequence is that one
  admitted item can hold a worker for longer than any run will observe, and the
  audit will accept the history because nothing in it is false. What that costs
  is stated where it bites — the vacuity floor is on `serviced` rather than on
  `passes_completed`, because a wedged host goes on arming plans. Bounding the
  cost instead would only rescale the wedge and would make a slow group
  unrepresentable past a number this document cannot justify.
- **A pass boundary is one per tick.** A tick arms at most one plan and retires
  at most one. This began as a modeling choice that keeps the pass-to-tick
  relationship crisp; it costs a tick of capacity at each pass boundary and
  changes nothing the bound asserts. It is now also enforced, because every
  occupancy deadline is measured in ticks and a scheduler free to arm plans at
  a standstill clock could deny a group all of them without one falling due.
