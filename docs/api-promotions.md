# API Promotions

Status: the durable log of public Rafter APIs promoted from reference-consumer
evidence, under the API Promotion Rule in
[`docs/reference-consumers.md`](./reference-consumers.md).

A reference consumer is allowed to reveal a missing API. It is not allowed to
dictate one. Every entry below therefore records what the consumer could not
express, what the workaround costs today, why the need is a Raft or
durable-lifecycle mechanism rather than application policy, and the exact
public shape that answers it. The consumer's reported signature is treated as
a statement of need; where reading the code produced a better shape, this
document designs the better shape and says why.

Three reading conventions hold throughout. Origin and blast-radius citations
are as of discovery: a promotion that succeeds deletes the workaround it
cites, so a dead path or out-of-range line in an Origin section is the
promotion working, not an error to repoint. Design blocks are immutable as
approved: when adoption evidence revises a design, the revision lands as its
own dated subsection and the original stays, with forward pointers connecting
the two — the latest revision plus the After-state is current truth. And an
After-state describes what its promotion left behind at adoption time; later
slices keep moving the same files.

The first five entries all come from the replicated ledger's `rafter-app`
adapter and its consumer-owned deterministic driver. They are not five
unrelated additions: two of them are one change to step reporting, two are one
restart surface, and the fifth completes the restart story. The sixth comes
from the fenced lock service and is the only entry so far that corrects a
behavior rather than exposing a missing one.

The next five come from the fenced lock's `rafter-service` adapter, and they
are one cluster rather than five findings that happened to arrive together. The
service layer publishes a managed composition — a handle, a driver, a transport
boundary, and a state-machine contract — that has never been composed, and four
of the five entries are consequences of that. The service errors render every
typed error they are handed; the transport traits have no driver; a driver that
gives up or lets go has no vocabulary for either; and the driver boundary is
not nameable from the crate root. The fifth is the state-machine contract
itself, which requires a snapshot implementation from every application and
gives none of them a way to decline.

The last three are kernel corrections rather than promotions, and they arrived
the other way round: a sixth-generation adversarial hunt attacked
`crates/rafter` — the crate this programme has changed least and therefore
scrutinized least — and two of the three consumer symptoms it produced were
already reproducible from the reference stores. They are recorded here because
the entries above are where this workspace writes down *why* a public shape is
what it is, and each of these three changes a public kernel shape. None of them
adds an API a consumer asked for; each removes a promise the kernel was making
and could not keep.

The last entry is the same kind of arrival one crate higher. A cold audit of
`crates/rafter-multiraft` — the only public crate no adversarial generation had
read — found that the many-group host drops committed apply results and starves
every group behind a failing one. Its consumer has not been written yet, so the
entry is unusual in this document: it is designed against a *stated* acceptance
contract rather than against a workaround someone had to write. That is the
weaker kind of evidence, and the entry says which of its shapes the contract
forces and which are the author's judgement.

The couplings are recorded in [Coupled designs](#coupled-designs), and the
implementation sequence in [Adoption order](#adoption-order).

Every entry is a pre-1.0 change. Breaking changes are permitted here;
gratuitous ones are not. Each entry states which parts break and why the break
is justified.

## Full-Fidelity Query Reads

### Origin

The ledger's driver never calls `RaftGroup::read`. It assembles the barrier
itself, records the report, waits for `ReadEvent::Granted`, and then reaches
into `RaftGroup::state_machine()` to serve the query under a `ReadBarrier`
rebuilt from the proof:
[`reference/ledger/tests/support/cluster.rs:428-476`](../reference/ledger/tests/support/cluster.rs).
The workaround costs the driver a `read_proofs` map, a `read_failures` map, a
`read_under_proof` helper, and a hand-written translation from `ReadEvent`
values back into a client-facing outcome — all of it code the helper exists to
remove.

Rafter's own managed driver pays the other half of the same bill. It does call
the helper, at
[`crates/rafter-service/src/driver/read.rs:27`](../crates/rafter-service/src/driver/read.rs),
and `handle_read_outcome` routes peer messages in exactly one arm:

```rust
ReadOutcome::Pending { peer_messages, .. } => {
    self.network.extend(peer_messages);
    Ok(None)
}
```

`Ready`, `Rejected`, `Canceled`, and both freshness arms extend nothing,
because the outcome value has nothing to extend from. Every other driver path
in that crate routes a report through `route_report`
([`crates/rafter-service/src/driver/state.rs:85-87`](../crates/rafter-service/src/driver/state.rs));
the read path is the only one that cannot, and the driver has no way to fix
that from its own side.

### Classification

Raft mechanism. The app layer's own contract makes the argument: kernel output
ordering is load-bearing, and `RaftGroup::apply_raft_outputs` already documents
that callers "must not reorder, drop, or replay raw outputs unless they also
own the resulting protocol and application semantics"
([`crates/rafter-app/src/group/output.rs:216-220`](../crates/rafter-app/src/group/output.rs)).
A sans-IO caller owns delivery. An API that steps the runtime and then denies
the caller the step's outputs is asking the caller to honor a contract it has
withheld the evidence for.

The finding's headline example is stronger than the code supports, and the
correction matters. A `ProposalEvent::Applied` cannot currently be co-emitted
with a read's step: `Input::ReadIndex` never advances the commit index
([`crates/rafter/src/node/read_index.rs:23-103`](../crates/rafter/src/node/read_index.rs)),
which the kernel does only when assuming leadership, handling a replication or
snapshot response, appending a proposal, or validating a membership change.
Nor can a single read both broadcast and be rejected: `read_index_batch`
returns before broadcasting when no slot is available, and `RaftGroup` submits
one read at a time, so the partial-overflow path that mixes the two is
unreachable from this API. Peer `Send` outputs therefore land in
`ReadOutcome::Pending`, which does carry them.

What is reachable today is narrower and still disqualifying:

- **Snapshot chunk directives.** The read-index broadcast reaches
  `replicate_snapshot_to_follower` for any follower behind the snapshot
  boundary
  ([`crates/rafter/src/node/replication/send.rs:96-153`](../crates/rafter/src/node/replication/send.rs)),
  emitting `RaftOutput::SendSnapshotChunk`. The app layer records that as
  `SnapshotEvent::SendChunk`
  ([`crates/rafter-app/src/group/output.rs:403-409`](../crates/rafter-app/src/group/output.rs)),
  and `ReadOutcome::Pending` carries `peer_messages` only. `DurableRaftNode`
  resolves those directives into `Send` before the app sees them, so the loss
  lands on embedders with their own `PersistedRaftRuntime` — of which this
  workspace already has seven.
- **Another barrier's granted proof.** Every step ends in
  `complete_ready_reads`
  ([`crates/rafter-app/src/group/read.rs:237-295`](../crates/rafter-app/src/group/read.rs)),
  which resolves **every** pending barrier whose read index is now satisfied.
  For a barrier started with `begin_read_barrier`, resolution removes it from
  `pending_reads` and emits `ReadEvent::Granted` carrying the only copy of its
  proof. When the state machine's applied index moved through the documented
  `RaftGroup::state_machine_mut` maintenance hook, that resolution can happen
  inside a `read` call for a different barrier, and discarding the report
  destroys the proof permanently — the barrier is gone from the table, so no
  later step re-emits it.
- **The metrics snapshot**, unconditionally, on every helper read.

The contract argument does not depend on that enumeration, which is the point.
The set of outputs a `ReadIndex` step can produce is a property of today's
kernel and of `DurableRaftNode`'s directive resolution. `RaftGroup` is generic
over `PersistedRaftRuntime`; the app layer must not encode which outputs it may
silently drop based on what one runtime happens to emit. Every other stepping
operation on `RaftGroup` returns its report, and this one is prevented from
doing so by its signature.

Second plausible consumer: `rafter-service`, which is structurally unable to
route a report on its read path while routing one on every other path.

### Design

`RaftGroup` already has a naming convention for this exact split, established
twice: the base name returns the full-fidelity report, and an `_outcome`
suffix marks the lossy convenience form. `begin_read_barrier` /
`begin_read_barrier_outcome`
([`crates/rafter-app/src/group/read.rs:73-101`](../crates/rafter-app/src/group/read.rs))
and `begin_proposal` / `begin_proposal_outcome`
([`crates/rafter-app/src/group/proposal.rs:31-69`](../crates/rafter-app/src/group/proposal.rs))
both follow it. The query read is the only member of the family that has the
lossy form under the base name and no full-fidelity form at all. The smallest
coherent family completes the pattern rather than adding a third convention.

A new report struct joins the two that already exist beside it in
`crates/rafter-app/src/group/types.rs`:

```rust
/// Full-fidelity result of a state-machine read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadReport<G, Q, R> {
    pub outcome: ReadOutcome<G, Q>,
    pub report: GroupStepReport<G, R>,
}
```

It carries three type parameters where `ProposalBeginReport` and
`ReadBarrierBeginReport` carry two, because a query read is the only operation
whose outcome type (`A::QueryResult`) differs from the report's result type
(`A::CommandResult`).

```rust
pub(super) type ReadReportResult<G, A, R> = GroupResult<
    A,
    R,
    ReadReport<
        G,
        <A as ReplicatedStateMachine>::QueryResult,
        <A as ReplicatedStateMachine>::CommandResult,
    >,
>;
```

```rust
/// Attempts a synchronous state-machine read using the requested consistency
/// mode, and returns the outcome plus the full step report generated while
/// serving it.
///
/// Local reads do not contact Raft, may be stale, and do not carry or consume
/// `ReadId`s. A local read can return [`ReadOutcome::LocalFreshnessUnavailable`]
/// when `min_applied_index` is above the local applied index; that outcome does
/// not reserve read state. Linearizable reads use the same read-index barrier
/// and pending-read table as [`RaftGroup::begin_read_barrier`]; callers that
/// receive [`ReadOutcome::Pending`] should route the report's peer messages,
/// continue driving normal group steps, then retry with the same [`ReadId`],
/// freshness requirement, and context to consume the completed proof. Callers
/// that receive [`ReadOutcome::LinearizableFreshnessUnavailable`] should also
/// keep driving and retry with the same local read parameters, or call
/// [`RaftGroup::cancel_read`] before abandoning the read. Once a linearizable
/// read-index operation is submitted, that `ReadId` is consumed even if the
/// caller cancels or drops local helper state. Rafter does not compare opaque
/// query values. Lease reads are rejected until lease support is explicitly
/// configured in this layer.
///
/// The returned report is the complete record of the step this call ran: peer
/// messages the caller must route, committed applies, proposal events, read
/// events belonging to other barriers, snapshot events, membership events,
/// leadership-transfer events, and the metrics snapshot. Those effects are
/// produced whether this read completes, stalls, is rejected, or is canceled;
/// the outcome value alone never carries them. Reads that consume an already
/// completed proof, and every [`ReadRequest::Local`] read, do not step the
/// runtime and return an empty report for this group.
///
/// A terminal read event clears local waiter state, so a caller that keeps
/// retrying after observing [`ReadEvent::Rejected`] or [`ReadEvent::Canceled`]
/// in the report receives [`GroupError::NonMonotonicReadId`] rather than a
/// second statement of the rejection. Read the report's read events before
/// retrying.
///
/// # Errors
///
/// Returns a group error when the group is poisoned, the request targets a
/// different group, the runtime rejects the underlying read-index request, the
/// state machine cannot report its applied index, or the state-machine read
/// fails.
pub fn read(&mut self, request: ReadRequest<G, A::Query>) -> ReadReportResult<G, A, R>;
```

```rust
/// Attempts a synchronous state-machine read and returns only its immediate
/// outcome.
///
/// This outcome-only helper intentionally discards the co-emitted step report.
/// It is lossless for [`ReadRequest::Local`], which never steps the runtime,
/// and for a retry that consumes an already completed proof. For a
/// [`ReadRequest::Linearizable`] read that starts a barrier it discards peer
/// messages, applies, proposal events, other barriers' read events, snapshot
/// events, membership events, leadership-transfer events, and metrics emitted
/// while the barrier started. A discarded [`ReadEvent::Granted`] destroys the
/// only copy of that barrier's proof, and a discarded snapshot chunk directive
/// is a lost protocol effect the caller was responsible for delivering. Use
/// [`RaftGroup::read`] unless this group holds no other waiters and the caller
/// routes no peer traffic.
///
/// # Errors
///
/// As [`RaftGroup::read`].
pub fn read_outcome(&mut self, request: ReadRequest<G, A::Query>)
    -> ReadOutcomeResult<G, A, R>;
```

### Semantics and edge cases

- **Relationship to the `begin_*` family.** After this change every group
  operation that steps the runtime has the same two forms, and the lossy form
  is always the one a caller has to ask for by name. `begin_proposal_batch`
  remains report-only with no `_outcome` sibling; nothing here changes that,
  and no consumer has asked for one.
- **Rework versus sibling.** `read` is reworked rather than joined by a
  `read_with_report`. Leaving the lossy form under the obvious name preserves
  exactly the defect the consumer found, and `rafter-service` demonstrates that
  the obvious name is what a careful caller reaches for. Deprecating a footgun
  is allowed pre-1.0.
- **Empty reports are meaningful.** `ReadReport.report` is a
  `GroupStepReport` for this group with every stream empty when no step ran. The
  field is not optional, so a caller routes it unconditionally rather than
  branching on whether this particular read touched the runtime.
- **Metrics.** The read path builds its report through the same
  `StepReportOptions::default()` path as `RaftGroup::step`, so `report.metrics`
  is populated. A future read variant taking explicit options is not part of
  this promotion.
- **Rejected and canceled reads.** These clear local state
  ([`crates/rafter-app/src/group/read.rs:198-235`](../crates/rafter-app/src/group/read.rs))
  and the consumed `ReadId` is not reusable. The report is what tells a caller
  which of its barriers ended, including barriers it did not name in this call.
- **Poison.** `read` still rejects a poisoned group before doing anything, so
  no report is produced on that path. Poison drains waiters into
  `poisoned_waiters` exactly as before.
- **Interaction with the leader-hint change.** The report's
  `ProposalEvent::Rejected` gains a `leader_hint` field in the same release;
  see [Coupled designs](#coupled-designs).

### Blast radius

Breaking: the return type of `RaftGroup::read` changes.

| File | Change |
| --- | --- |
| [`crates/rafter-app/src/group/types.rs`](../crates/rafter-app/src/group/types.rs) | Add `ReadReport`, add `ReadReportResult` alias |
| [`crates/rafter-app/src/group/mod.rs:46-50`](../crates/rafter-app/src/group/mod.rs) | Re-export `ReadReport` |
| [`crates/rafter-app/src/group/read.rs:159-197`](../crates/rafter-app/src/group/read.rs) | Rework `read`, add `read_outcome`; `read_linearizable` and `read_local` return the report alongside the outcome |
| [`crates/rafter-service/src/driver/read.rs:27,62-136`](../crates/rafter-service/src/driver/read.rs) | Take `.report`, route it through `route_report`, keep the outcome dispatch |
| [`crates/rafter-app/examples/replicated_kv_manual.rs:91,107,217-239`](../crates/rafter-app/examples/replicated_kv_manual.rs) | Route the report instead of only `Pending` peer messages |
| [`crates/rafter-app/tests/group_read.rs`](../crates/rafter-app/tests/group_read.rs) | 7 call sites |
| [`crates/rafter-app/tests/group_read_lifecycle.rs`](../crates/rafter-app/tests/group_read_lifecycle.rs) | 18 call sites, several asserting exact `ReadOutcome` equality |
| [`crates/rafter-service/tests/adoption.rs:147`](../crates/rafter-service/tests/adoption.rs) | 1 call site |

`rafter-multiraft` and `rafter-maelstrom` have no read path over `RaftGroup`
and do not change. The break is justified: the one production consumer of the
old signature cannot route a read step's effects, and no additive change lets
it start, because the effects never reach it.

### Focused-test plan

In `crates/rafter-app/tests/group_read.rs`:

- `read_report_carries_snapshot_chunk_directives_emitted_by_the_barrier_step`
  — over the scripted runtime in `tests/support`, return a
  `SendSnapshotChunk` output alongside the read-index outputs; assert it reaches
  the caller as `SnapshotEvent::SendChunk` in the report. This is the effect
  `ReadOutcome::Pending` structurally cannot carry.
- `read_report_carries_another_barriers_granted_event` — start barrier A with
  `begin_read_barrier`, leave it freshness-stalled, advance the state machine's
  applied index through `state_machine_mut`, then `read` for B; assert
  `ReadEvent::Granted` for A is in B's report.
- `read_outcome_discards_co_emitted_read_events` — the same fixture through
  `read_outcome`; assert A's proof is unrecoverable afterwards. The footgun is
  documented behavior and must be pinned.
- `read_report_is_empty_for_local_reads` and
  `read_report_is_empty_when_consuming_a_completed_proof`.
- Negative: `read_rejects_a_wrong_group_request_without_a_report`,
  `read_rejects_a_poisoned_group_without_a_report`, and
  `read_retry_after_a_terminal_read_event_is_non_monotonic`.

In `crates/rafter-service/tests/in_memory_read.rs`:

- `managed_read_routes_every_effect_the_barrier_step_emitted` — over
  `ScriptedReadRuntime`, script a read-index step that also emits a snapshot
  chunk directive; assert the managed driver observes it. Today the read path
  has nowhere to receive it.

### Rejected alternatives

- **Additive `read_with_report`.** Introduces a third naming convention beside
  `begin_*`/`begin_*_outcome`, and leaves the lossy form as the default answer
  to "how do I read".
- **Add `peer_messages` to every `ReadOutcome` variant.** Duplicates part of
  the report into a six-variant enum, still loses applies and every non-read
  event, and gives a caller two places to look for one step's effects.
- **Remove `read` entirely.** That is the consumer's current workaround
  promoted to a contract. The helper's value is proof caching, `ReadId`
  discipline, and freshness retry; deleting it makes every consumer rewrite
  all three.

### After-state

The ledger driver's `read` becomes a bounded retry loop over
`group.read(request)` that records each report through the same
`record_report` path as every other step. `read_proofs`, `read_under_proof`,
and the manual `ReadBarrier` reconstruction disappear. The driver keeps
watching read events, because a terminal event is still how an asynchronously
observed rejection arrives.

Corrected in adoption: it does not watch them only in the report the read call
returns. A barrier is canceled by whatever step observes the leadership loss —
usually a tick or a delivery — so the driver still keeps one small
`ReadId`-keyed map of terminal read outcomes, filled by `record_report` and
drained by the read loop. What that map no longer holds is proofs. The
distinction is the whole adoption: the driver stopped caching the group's
proof in order to serve the query itself, and kept exactly the one-slot buffer
any driver needs between the step that observes a terminal event and the
caller waiting on it. The loop must check that buffer *before* it retries: a
terminal event clears the group's waiter, so the next `read` with the same
`ReadId` returns `GroupError::NonMonotonicReadId` rather than restating the
rejection.

The `rafter-service` consumer takes the same shape and gains a stronger
statement of it. Its `drive_read` routes the report through the same
`record_report` that already handles proposal and tick reports, deletes the
`ReadOutcome::Pending` arm that made a read the one step whose peer messages
came from somewhere else, and stops translating `ReadOutcome::Rejected` and
`ReadOutcome::Canceled` a second time — the report's read events already
resolved the waiter. An assertion in that arm pins the property: a terminal
read outcome must reach the driver as a read event in the report of the step
that produced it.

## Owned Pending Snapshot Transfer

### Origin

The ledger's storage support cannot implement `RaftSnapshotStore` over a shared
handle without a cache. `SharedSnapshotStore`
([`reference/ledger/tests/support/storage.rs:107-190`](../reference/ledger/tests/support/storage.rs))
wraps `Rc<RefCell<InMemoryRaftSnapshotStore>>` and carries a duplicated
`pending_mirror: Option<PendingSnapshotTransfer>` field refreshed after every
one of the five mutating methods plus `reopen()`. Its doc comment names the
cause. The mirror is not merely verbose: it is a second copy of durable state
that can disagree with the medium whenever another handle to the same medium
mutates it.

The blocked shape is
[`crates/rafter-storage/src/raft_snapshot_store/contract.rs:105`](../crates/rafter-storage/src/raft_snapshot_store/contract.rs):

```rust
fn current_pending_snapshot_transfer(&self) -> Option<&PendingSnapshotTransfer>;
```

An `Rc<RefCell<_>>` or `Arc<Mutex<_>>` store cannot return a reference that
outlives its guard. A store that reads staging from the medium on demand has
nothing to borrow from.

### Classification

Durable-lifecycle mechanism. This does not add capability; it removes a
representation choice that excludes implementations the storage contract
otherwise invites. The contract already anticipates handles that reopen and
fence themselves ("The reference handle then rejects later mutations until
reopen", contract.rs:16-20), which is a lifecycle a borrow-returning read
cannot express.

The consistency argument settles it without appeal to any consumer. Every other
read on every storage trait is owned:

| Trait | Read | Return |
| --- | --- | --- |
| `RaftSnapshotStore` | `current_snapshot` | `Option<RaftSnapshot>` |
| `RaftSnapshotStore` | `current_pending_snapshot_transfer` | `Option<&PendingSnapshotTransfer>` |
| `RaftHardStateStore` | `current` | `RaftHardState` |
| `RaftLogSegment` | `replay_entries` | `Vec<PersistedRaftLogEntry>` |
| `RaftLogSegment` | `next_index`, `compacted_through` | `LogIndex` |

`current_snapshot` already returns an owned value with exactly the same
allocation cost: `RaftSnapshot` and `PendingSnapshotTransfer` both carry one
`RaftSnapshotMetadata` and otherwise only scalars
([`crates/rafter/src/types/snapshot/metadata.rs:192-196`](../crates/rafter/src/types/snapshot/metadata.rs)).
A store that must clone the metadata for one read has no ground to refuse the
other. The kernel's own accessor is already owned as well:
`RaftNode::pending_snapshot_transfer(&self) -> Option<PendingSnapshotTransfer>`
([`crates/rafter/src/node/replication/snapshot/transfer.rs:16-21`](../crates/rafter/src/node/replication/snapshot/transfer.rs)),
and `resume_pending_snapshot_transfer` consumes one by value. This method is
the last borrow-returning read in the storage surface.

Second plausible consumer: any multi-tenant host, any store behind a
connection pool or lock, and any store that keeps staging on the medium rather
than in memory.

### Design

```rust
/// Returns the logical pending inbound snapshot transfer, if one is
/// resumable.
///
/// The value is owned so the trait can be implemented over shared handles,
/// interior mutability, or staging read from the medium on demand: a store
/// holding its staging area behind a guard has no borrow to hand out, and a
/// store that reads staging lazily has nothing to borrow from. Stores that
/// keep the transfer in a field clone it. The clone allocates only when a
/// transfer is staged; a store with an empty staging area returns `None`
/// without allocating, which is the steady state on the per-step probe.
///
/// The returned value must reflect durable staging rather than a cache that
/// can disagree with the medium, and `None` must mean "nothing is staged". A
/// store that reports `None` to hide a failed read defeats both repairs that
/// depend on this value: an interrupted promotion is left unpromoted and
/// fails every later reopen, and an abandoned staging area survives into the
/// next incarnation as a transfer the protocol has already moved past.
fn current_pending_snapshot_transfer(&self) -> Option<PendingSnapshotTransfer>;
```

### Semantics and edge cases

- **Allocation.** `PendingSnapshotTransfer` is `Clone`, not `Copy`
  ([`crates/rafter/src/types/snapshot/transfer.rs:64-72`](../crates/rafter/src/types/snapshot/transfer.rs)).
  It embeds `RaftSnapshotMetadata`, which owns two `String` identities and an
  optional committed configuration containing a `MembershipConfig`. A clone is
  therefore 2 allocations at the floor, 4 in the common stable-membership case,
  and 6 under joint consensus. The value itself is roughly 264 bytes; the
  allocations, not the copy, are the cost.
- **The per-step probe does not pay it.**
  `clear_abandoned_snapshot_staging_for_step`
  ([`crates/rafter-runtime/src/lib.rs:615-627`](../crates/rafter-runtime/src/lib.rs))
  runs once per step, but it queries the store only when the kernel has no
  pending transfer, and in that state the store almost always has none either —
  returning `None`, which allocates nothing. The allocation happens exactly in
  the case that immediately clears the staging area. No new probe method and no
  runtime-side cache is needed.
- **The per-chunk paths must read their own field.** `stage_snapshot_chunk` and
  `promote_staged_snapshot` in both concrete stores currently call their own
  trait method to feed validation
  ([`crates/rafter-storage/src/raft_snapshot_store/file.rs:91,126`](../crates/rafter-storage/src/raft_snapshot_store/file.rs),
  [`crates/rafter-storage/src/raft_snapshot_store/in_memory.rs:108,132`](../crates/rafter-storage/src/raft_snapshot_store/in_memory.rs)).
  Those must read `self.pending` directly, keeping the inbound staging path
  allocation-free per chunk. The internal validation helpers keep taking
  `Option<&PendingSnapshotTransfer>` and do not change.
- **Object safety.** Unaffected either way; nothing in the tree uses
  `dyn RaftSnapshotStore`.
- **Does `current_snapshot` symmetry settle it?** Yes. The two reads clone the
  same metadata, so a store already paying for one cannot object to the other,
  and after this change no storage-trait read returns a borrow — a rule an
  implementor can hold in their head.

### Blast radius

Breaking for implementors of `RaftSnapshotStore` outside the workspace; every
in-tree change is mechanical.

| File | Change |
| --- | --- |
| [`crates/rafter-storage/src/raft_snapshot_store/contract.rs:105`](../crates/rafter-storage/src/raft_snapshot_store/contract.rs) | Signature and doc |
| [`crates/rafter-storage/src/raft_snapshot_store/file.rs:165-167`](../crates/rafter-storage/src/raft_snapshot_store/file.rs) | Clone out of `self.pending`; `:91,126` read the field |
| [`crates/rafter-storage/src/raft_snapshot_store/in_memory.rs:160-162`](../crates/rafter-storage/src/raft_snapshot_store/in_memory.rs) | Same; `:108,132` read the field |
| [`crates/rafter-runtime/src/construction.rs:159,239`](../crates/rafter-runtime/src/construction.rs) | Drop `.cloned()` |
| [`crates/rafter-runtime/src/tests/snapshot/chunk_transfer.rs:177`](../crates/rafter-runtime/src/tests/snapshot/chunk_transfer.rs), [`.../failing_stores.rs:111,164`](../crates/rafter-runtime/src/tests/snapshot/failing_stores.rs) | Three fault-injection implementors |
| `crates/rafter-storage/src/raft_snapshot_store/pending_transfer_test.rs`, `pending_transfer_cleanup_test.rs` | 14 assertions lose a `&` |
| `crates/rafter-storage/src/raft_snapshot_store/pending_transfer/recovery_test.rs:88,110` | Drop `.cloned()` |
| Every other in-tree assertion site — `validation_test.rs`, `health_test.rs`, `inventory_test.rs`, `storage_failpoint_test/snapshot_test.rs`, `tests/open_recovery_report.rs`, `tests/store_conformance/support.rs`, `crates/rafter-runtime/src/tests/snapshot/**` | Compare owned values; most need no edit at all |
| [`reference/ledger/tests/support/storage.rs`](../reference/ledger/tests/support/storage.rs) | Mirror deleted |

`reference/` is excluded from the root workspace and `bench-compare/` is not a
member, so `cargo check --workspace` does not cover them; both must be built
separately for this step.

### Focused-test plan

In `crates/rafter-storage/tests/store_conformance/`:

- `shared_handle_store_satisfies_the_snapshot_store_contract` — a test-support
  store wrapping `Arc<Mutex<InMemoryRaftSnapshotStore>>` runs the existing
  conformance suite. This does not compile today, which is the whole finding;
  it is the promotion's primary evidence.
- `pending_transfer_read_through_a_second_handle_observes_staging_from_the_first`
  — stage through one handle, read through another, assert agreement. Negative
  case for a cached mirror.

In `crates/rafter-storage/src/raft_snapshot_store/pending_transfer_test.rs`:

- Keep the existing staging, continuation, and clearing assertions; they now
  compare owned values against the owned fixtures they always used.
- Negative: `an_empty_staging_area_reports_none` and
  `a_promoted_transfer_reports_none`, so no implementation can satisfy the
  contract by returning a stale copy.

In `crates/rafter-runtime/src/tests/snapshot/chunk_transfer/pending.rs`:

- `resume_and_clear_drive_correctly_over_a_guarded_store` — the runtime's open
  and per-step staging repairs must work when the store is only readable
  through a guard.

### Rejected alternatives

- **`Cow<'_, PendingSnapshotTransfer>`.** Requires `Clone` regardless, keeps a
  lifetime in the signature that a guard-based store still cannot satisfy, and
  buys nothing: every caller in the tree either reads scalar fields off a
  temporary or wants ownership.
- **Add `has_pending_snapshot_transfer(&self) -> bool` to keep the probe
  cheap.** Adds public surface to avoid a cost that does not exist, and creates
  a second source of truth an implementor can let disagree with the first.
- **Return `Option<PendingSnapshotTransferView<'_>>` with copied scalars.** A
  new type whose only purpose is to avoid cloning two `String`s that the kernel
  already clones on the neighbouring accessor.

### After-state

`SharedSnapshotStore` loses `pending_mirror`, `refresh_pending_mirror`, and six
refresh calls; the method becomes a one-line delegation. Any store the durable
slices later introduce — pooled, locked, or lazily loading — is implementable
without a cache.

Corrected in adoption: `reopen()` does not become `self.clone()`, it is
deleted. Reopening by handle only existed because the driver had to hold a
second handle for the next incarnation, and decomposition ends that. The store
itself stays a guarded handle on purpose: it is the consumer-side evidence that
a store whose staging lives behind a `RefCell` — or a pool, a lock, or a lazy
read from the medium — satisfies the contract with no cache at all. It is now
the ledger's only non-plain store, and that asymmetry is the finding: hard
state and the log never needed a handle, because neither of their reads ever
returned a borrow.

## Group and Runtime Decomposition

### Origin

An in-process restart cannot reclaim anything from the incarnation it replaces.
`RaftGroup` takes the runtime and state machine by value
([`crates/rafter-app/src/group/types.rs:248,259`](../crates/rafter-app/src/group/types.rs))
and hands back only shared references. `DurableRaftNode` consumes its three
stores and exposes `snapshot_store(&self) -> &S`
([`crates/rafter-runtime/src/lib.rs:338`](../crates/rafter-runtime/src/lib.rs))
and nothing for the other two.

The ledger's driver says so in a comment at
[`reference/ledger/tests/support/cluster.rs:377-382`](../reference/ledger/tests/support/cluster.rs):

```rust
// `RaftGroup` has no decomposition path, so the surviving application
// state has to be taken through the state-machine accessor before the
// old incarnation is dropped.
let app = self.nodes[index].group.state_machine().clone();
```

That clone requires `A: Clone`. The ledger's state machine can afford it; a
transactional application backend holding a connection or a file handle cannot,
and the ledger's own durable slices are scheduled to replace exactly that state
machine. Storage costs more: because the runtime never returns the stores, the
driver holds every store in parallel behind consumer-written shared handles
([`reference/ledger/tests/support/storage.rs`](../reference/ledger/tests/support/storage.rs)),
which is the sole reason `SharedHardStateStore` and `SharedLogSegment` exist.

The documented restart recipe assumes a fresh process reopening file stores by
path. A single-process supervisor — the deterministic driver every
package-mode consumer must own, per the program's own constraint — has no such
path.

### Classification

Durable-lifecycle mechanism, and additive. The lifecycle contract already
states that restart from durable storage is the only exit from a poisoned
runtime
([`crates/rafter-runtime/src/lib.rs:214-216`](../crates/rafter-runtime/src/lib.rs)),
and that a poisoned group is "permanently poisoned until the caller replaces it
through an explicit recovery path"
([`crates/rafter-app/src/group/types.rs:9-12`](../crates/rafter-app/src/group/types.rs)).
Neither layer supplies the replacement path it requires. Decomposition is that
path.

Second plausible consumer: the sharded counter service, whose contract requires
group creation, draining, removal, reopening, and tombstoning within one host;
every one of those transitions has to release a group's runtime and storage
without ending the process.

### Design

Two methods at two layers, because the layering forbids one. `rafter-app`
speaks to `R: PersistedRaftRuntime`, and `rafter-runtime-api` depends only on
the kernel — it has no notion of a store. The group level can therefore return
the runtime and never the storage; the runtime level returns the storage and
never the state machine. The chain composes:

```rust
let parts = group.into_parts();
let storage = parts.runtime.into_storage();
```

In `crates/rafter-app/src/group/types.rs`:

```rust
/// The reusable pieces of a decomposed [`RaftGroup`].
#[derive(Debug)]
pub struct RaftGroupParts<G, A, R> {
    pub group_id: G,
    pub node_id: NodeId,
    pub runtime: R,
    pub state_machine: A,
    pub local_proposal_id_watermark: Option<LocalProposalId>,
    pub read_id_watermark: Option<ReadId>,
    pub fatal_state: GroupFatalState,
    pub poisoned_waiters: PoisonedWaiters,
}
```

A ninth field, `poison_cause: Option<ErrorCause>`, joins this list later and
ships between `fatal_state` and `poisoned_waiters` rather than appended after
them; see [Typed Service Failure Surface](#typed-service-failure-surface).

```rust
/// Consumes the group and returns the parts a caller can reuse.
///
/// This is the in-process teardown path. An embedder replacing a group — after
/// poison, on a supervised restart, or when a host closes one group of many —
/// reclaims the state machine and the runtime instead of dropping them. It is
/// the group-level half of decomposition; `DurableRaftNode::into_storage` in
/// `rafter-runtime` is the runtime-level half that reaches the durable stores.
///
/// Decomposition never steps the runtime, never applies, and never emits
/// outputs, so no protocol effect can be lost by calling it. What ends is local
/// waiter tracking: every pending proposal and every reserved read disappears
/// with the group. A proposal already appended may still commit and apply under
/// a later incarnation, so a caller that has acknowledged nothing must treat
/// each dropped waiter exactly as
/// [`crate::proposal::ProposalEvent::UnknownOutcome`] — the write may or may not
/// have taken effect.
///
/// Decomposition is allowed on a poisoned group, because poison is the state a
/// caller most needs to leave. `fatal_state` and `poisoned_waiters` travel with
/// the parts, so a caller that decomposes without inspecting the group first can
/// still resolve its clients.
///
/// The returned watermarks are load-bearing when `runtime` is carried into a new
/// group. A live runtime still tracks local proposal IDs for entries it has not
/// yet committed, and a new group starts with no watermark of its own, so it must
/// be given IDs strictly above both returned watermarks. Reusing an ID completes
/// the new group's waiter with the older proposal's result, at the older
/// proposal's index — silently, because both the runtime and the new group are
/// behaving exactly as documented. A runtime rebuilt from durable storage carries
/// no local proposal tracking, and a group over it may restart its IDs at zero.
///
/// The applied floor is not returned: the state machine reports it through
/// [`crate::state_machine::ReplicatedStateMachine::applied_index`], and a group
/// never advances its own floor past what the state machine reported.
///
/// Nothing is closed and nothing is flushed. The runtime and its stores stay
/// live until the caller drops them, so a caller reopening the same durable
/// medium must drop the returned runtime first when the store requires
/// exclusive access.
#[must_use]
pub fn into_parts(self) -> RaftGroupParts<G, A, R>;
```

In `crates/rafter-runtime/src/lib.rs`:

```rust
/// The durable stores of a decomposed [`DurableRaftNode`].
#[derive(Debug)]
pub struct DurableRaftNodeStorage<H, L, S> {
    pub hard_state_store: H,
    pub log_segment: L,
    pub snapshot_store: S,
}
```

```rust
/// Consumes the runtime and returns its durable stores.
///
/// This is how an in-process restart reclaims storage from the incarnation it
/// replaces. The kernel's volatile state is discarded; the returned stores are
/// the durable truth a new [`DurableRaftNode`] recovers from. Decomposing a
/// poisoned runtime is the sanctioned recovery — poison means in-memory state
/// ran ahead of the medium, and the medium is exactly what comes back.
///
/// The stores are returned as the runtime held them. A store that fenced itself
/// after a failed mutation is still fenced; reopening the medium is that store's
/// own operation, not a side effect of this call. Nothing is flushed, because
/// every durable obligation the runtime accepted was already satisfied before it
/// released the outputs that depended on it.
#[must_use]
pub fn into_storage(self) -> DurableRaftNodeStorage<H, L, S>;
```

Both methods sit on the unconstrained impl blocks
([`crates/rafter-app/src/group/types.rs:292`](../crates/rafter-app/src/group/types.rs),
[`crates/rafter-runtime/src/lib.rs:97`](../crates/rafter-runtime/src/lib.rs)),
so decomposition works for a group or node whose stores no longer satisfy the
bounds required to step it.

### Semantics and edge cases

- **Poisoned group or node: allowed, not refused, and not a typed error.**
  Refusing would remove the only exit from poison. Returning a `Result` would
  force every caller to handle a failure it cannot act on — there is no second
  way to recover the state machine. The poison travels in the parts instead, so
  it cannot be silently discarded.
- **Drop order.** Neither `RaftGroup` nor `DurableRaftNode` implements `Drop`,
  so decomposition has no ordering effect of its own. The obligation is the
  caller's: a file-backed store that holds a lock releases it when the returned
  handle drops, and reopening the medium before that drop is the store's
  business to reject.
- **Nothing can be lost.** No step runs, no output is generated, and no report
  is produced. The only state that ends is local correlation: `pending_proposals`,
  `pending_reads`, `pending_query_reads`, `completed_query_reads`. A completed
  read proof that was never consumed is discarded with them.
- **Watermark hazard, made visible.** Carrying the same runtime into a new group
  and restarting IDs at zero completes the new waiter with an older proposal's
  result. The mechanism is reproducible and is now a permanent negative test: the
  kernel's local-proposal tracking is volatile state that survives decomposition,
  the app layer correlates applies by local proposal ID alone, and a rebuilt group
  has no watermark of its own to reject the reused ID. Nothing rejects it, so the
  watermarks are in the returned parts precisely so a caller can avoid this
  without reading the source.
- **Group level versus node level.** Two methods, not one, because
  `rafter-app` is generic over a storage-free runtime trait. A consumer that
  needs only application state calls one; a consumer that needs storage calls
  both.
- **No `from_parts`.** Reconstruction already has two constructors, and the
  only thing they cannot restore is the watermarks — which matter only for the
  same-runtime rebuild no consumer has demonstrated. Per the promotion rule,
  repeated plumbing is extracted after the second consumer shows the same
  shape, not before.
- **`rafter-service` still has no teardown.** Its driver takes groups by value
  ([`crates/rafter-service/src/driver/adoption.rs:29-32`](../crates/rafter-service/src/driver/adoption.rs))
  with no way back out. `into_parts` does not change that; a driver-level
  release is a separate need with no consumer behind it yet.

### Blast radius

Additive at both layers. No existing call site changes and nothing breaks.

| File | Change |
| --- | --- |
| [`crates/rafter-app/src/group/types.rs`](../crates/rafter-app/src/group/types.rs) | Add `RaftGroupParts` and `RaftGroup::into_parts` |
| [`crates/rafter-app/src/group/mod.rs:46-50`](../crates/rafter-app/src/group/mod.rs) | Re-export `RaftGroupParts` |
| [`crates/rafter-runtime/src/lib.rs`](../crates/rafter-runtime/src/lib.rs) | Add `DurableRaftNodeStorage` and `DurableRaftNode::into_storage` |

### Focused-test plan

In `crates/rafter-app/tests/group_lifecycle.rs`:

- `into_parts_returns_the_state_machine_and_runtime_it_was_built_with`.
- `into_parts_reports_poison_and_the_waiters_it_drained` — poison a group
  with waiters outstanding, decompose without calling
  `drain_poisoned_waiters` first, assert both arrive in the parts.
- `into_parts_reports_id_watermarks_after_use`.
- Negative: `a_group_rebuilt_over_the_same_runtime_completes_a_reused_id_with_the_older_result`
  — over a real kernel runtime, append a proposal without committing it,
  decompose, build a new group over the returned runtime, restart IDs at zero,
  and assert the new waiter is completed by the retired incarnation's entry:
  right ID, older index, older result, and no event at all for the proposal the
  caller actually made. This makes the documented hazard executable.

  The hazard cannot be written as a rejection. A group built by
  `RaftGroup::new` or `RaftGroup::with_applied_index` starts with no ID
  watermark, so it does not reject an ID at the retired group's watermark — that
  absence *is* the hazard, and it is why the watermarks have to leave in the
  parts rather than being recoverable from the runtime.

In `crates/rafter-runtime/src/tests/recovery.rs`:

- `into_storage_returns_stores_that_recover_to_the_same_state` — append,
  decompose, recover from the returned stores, assert identical hard state, log
  tail, and snapshot boundary.
- `into_storage_after_a_fatal_persistence_error_returns_the_durable_stores` —
  using the existing fault-injection stores in
  [`crates/rafter-runtime/src/tests/snapshot/failing_stores.rs`](../crates/rafter-runtime/src/tests/snapshot/failing_stores.rs);
  assert the recovered node matches the durable medium, not the in-memory state
  that ran ahead.
- Negative: `into_storage_does_not_flush_or_close` — a recording store observes
  no writes during decomposition.

### Rejected alternatives

- **Tuple returns, as originally proposed.** `(G, NodeId, R, A)` puts two
  opaque generic parameters next to each other; a caller that swaps them gets a
  type error only when the types differ. Named fields also let the watermarks
  and poison state ride along, which a tuple would have made unbearable.
- **`snapshot_store_mut` and two sibling accessors instead of decomposition.**
  Mutable access to a store the runtime is still driving invites exactly the
  interleaving the persist-before-output contract forbids.
- **`Result<RaftGroupParts, _>` refusing on poison.** Removes the only exit
  from poison to protect against a caller who has already decided to discard the
  incarnation.

### After-state

`Cluster::restart` stops cloning the state machine, which removes the `A: Clone`
requirement that a transactional backend will not satisfy. `NodeStorage`,
`SharedHardStateStore`, and `SharedLogSegment` collapse: the driver takes the
stores back from the runtime it is retiring and hands them to the one it is
opening, instead of holding a parallel handle to every medium for the life of
the cluster. Combined with the owned pending transfer, the consumer's storage
support file has almost nothing left to own.

Confirmed and extended in adoption. `NodeStorage` collapses into
`DurableRaftNodeStorage` itself — the promoted type is the bundle both
consumers construct once and thereafter only receive — and both consumers drop
their `storage` field entirely. The fenced-lock's storage support file is
deleted outright, which also fixes a real defect the parallel-handle shape was
hiding: its `open_group` built a fresh `InMemoryRaftSnapshotStore` on every
restart, so each incarnation silently replaced the previous one's snapshot
medium. `into_storage` returns all three stores, so it cannot.

Two frictions the design did not anticipate:

- `into_parts(self)` needs a movable slot. The ledger's replicas live in a
  `Vec` it can `remove` and `insert`; the fenced-lock's live inside an
  `Arc<Mutex<NodeState>>` shared with every cloned handle, which nothing can
  move out of, so its group field became `Option<LockGroup>` with two
  `expect`ing accessors. That is the shape any single-process supervisor with a
  lock-guarded group will need — including the sharded counter host this
  promotion names as its second consumer — and the doc-comment does not
  mention it.
- The watermarks are inert for a restart that rebuilds the runtime. Both
  consumers ignore `local_proposal_id_watermark` and `read_id_watermark`
  because they drop the returned runtime and recover a new one from the
  returned storage, which is exactly the case the doc-comment's last paragraph
  describes. The hazard is real but belongs to the same-runtime rebuild, which
  neither consumer performs.

## Committed Application Index

### Origin

Readiness gating after complete recovery is an explicit 1.0
production-composition requirement. `RaftGroupMetrics` reports `commit_index`
and `applied_index`
([`crates/rafter-app/src/metrics.rs:16-17`](../crates/rafter-app/src/metrics.rs)),
but elections and membership changes commit `Noop` and `Configuration` entries
the state machine never sees
([`crates/rafter/src/node/commit/apply.rs:66-90`](../crates/rafter/src/node/commit/apply.rs)),
so `applied_index == commit_index` is not reachable in general and cannot be the
gate.

The ledger's driver reconstructs the predicate by walking the durable log
through the public runtime accessor and filtering entry kinds
([`reference/ledger/tests/support/cluster.rs:255-283`](../reference/ledger/tests/support/cluster.rs)).
Its comment states the gap directly: "Progress therefore has to be measured
against committed application entries, which the app layer does not report on
its own." Every convergence check in the driver — `settle()` — depends on that
reconstruction, and the reconstruction depends on `DurableRaftNode`'s concrete
`log_entries_from`, which the generic `PersistedRaftRuntime` trait does not
have. A consumer generic over its runtime cannot write it at all.

### Classification

Raft mechanism, following directly from a documented correctness contract.
`ReplicatedStateMachine::apply_batch` promises that a state machine can recover
with all effects through the highest returned applied index
([`crates/rafter-app/src/state_machine.rs:48-59`](../crates/rafter-app/src/state_machine.rs)),
and `RaftGroup::with_applied_index` exists so a restart can declare that floor
([`crates/rafter-app/src/group/types.rs:252-258`](../crates/rafter-app/src/group/types.rs)).
Between those two lies a question neither answers: has this replica applied
everything it knows to be committed? That predicate is entirely about Raft log
structure — which committed entries carry application payloads — and contains
no application policy whatsoever.

Second plausible consumer: `rafter-service` readiness, which today can only
publish `commit_index` and `applied_index` and let an operator guess; and the
sharded counter host, which must decide per group whether a reopened group may
serve.

### Design

The value is derived from the runtime, never from what a group has observed.
That distinction is the whole design. A group-observed floor — the highest
`RaftOutput::Apply` a group has seen — reports "caught up" for a group whose
runtime still holds undrained recovery outputs, which is precisely the case a
readiness gate exists to catch. A runtime-derived floor reports the truth and
fails closed.

In `crates/rafter-runtime-api/src/lib.rs`, a new required method on
`PersistedRaftRuntime`:

```rust
/// Returns the index the local state machine must reach to have consumed
/// every committed application command.
///
/// This is the highest index at or below
/// [`PersistedRaftRuntime::commit_index`] whose log entry carries an
/// application payload, or [`PersistedRaftRuntime::snapshot_index`] when the
/// snapshot boundary is higher — a snapshot subsumes every application entry
/// it covers. It is `LogIndex::ZERO` when the node has committed no
/// application entry and holds no snapshot.
///
/// Elections and membership changes commit entries the state machine never
/// sees, so this is not `commit_index`, and a fully caught-up state machine
/// may trail the committed index forever.
///
/// The value never decreases within one node incarnation: committed entries
/// are never truncated, and the snapshot boundary only advances.
///
/// The value is local. It says nothing about what the cluster has committed:
/// a stale follower and an isolated former leader each report their own view,
/// and both can report a fully applied state machine while missing entries a
/// current leader has committed. It is a recovery and readiness signal, not a
/// freshness proof — a linearizable read still requires a read-index barrier.
///
/// Implementations must report the true value for their own log rather than
/// an optimistic bound. A runtime that reports zero makes a readiness gate
/// pass before recovery has replayed anything.
fn committed_application_index(&self) -> LogIndex;
```

In `crates/rafter-app/src/group/output.rs`, beside `metrics`:

```rust
/// Returns the index this group's state machine must reach to have applied
/// every committed application command.
///
/// Compare it with the state machine's own applied index to gate readiness
/// after recovery:
///
/// ```text
/// state_machine.applied_index()? >= group.committed_application_index()
/// ```
///
/// Use `>=`, never equality. A state machine that installed a snapshot whose
/// boundary sits above the last committed application entry legitimately
/// reports a higher applied index, as does one seeded through
/// [`RaftGroup::with_applied_index`].
///
/// The predicate is false while a restarted node still holds recovery outputs
/// the caller has not applied, which is exactly when a readiness gate must hold
/// a replica back. It is not a linearizability signal: it proves only that this
/// replica has applied everything *it* knows to be committed. Group poison does
/// not change it — a poisoned group reports the same runtime value and will
/// never apply again, so a readiness gate must check
/// [`RaftGroup::fatal_state`] as well.
///
/// A caller that compacts at a boundary above the index its state machine
/// reports applied raises this value past what that state machine will ever
/// reach. Compact at the applied index, as
/// [`crate::state_machine::ReplicatedStateMachine::build_snapshot`] already
/// requires.
#[must_use]
pub fn committed_application_index(&self) -> LogIndex;
```

`DurableRaftNode` implements it with a backward scan over the committed
retained suffix, using public kernel accessors only — `commit_index`,
`snapshot_index`, and `log_entries_slice_from`
([`crates/rafter/src/node/log.rs:57`](../crates/rafter/src/node/log.rs)) —
terminating at the first application entry it finds and falling back to the
snapshot boundary. No kernel change and no new derived state is required.

### Semantics and edge cases

- **Method, not a metrics field.** `RaftGroupMetrics` is materialized on every
  step by default
  ([`crates/rafter-app/src/group/output.rs:300-302`](../crates/rafter-app/src/group/output.rs)),
  and `StepReportOptions` exists specifically so a hot path can opt out of an
  observation walk
  ([`crates/rafter-app/src/group/types.rs:116-121`](../crates/rafter-app/src/group/types.rs)).
  Putting a log scan behind the default step would contradict that decision. The
  value is also not an observation: it is a precondition a caller evaluates at
  readiness boundaries, not a number to publish every step.
- **Index, not a count.** `pending_application_applies: usize` answers "how far
  behind" rather than "am I caught up". It moves whenever either side moves, so
  it cannot be logged as a target, persisted, or compared with the applied index
  an application already stores. The index can.
- **Exact definition.** Highest committed application entry index, floored at
  the snapshot boundary. Not a count of pending applies, not the commit index,
  not the kernel's dispatch floor.
- **Monotonicity.** Non-decreasing within one incarnation. Committed entries are
  never truncated, so the set of committed application indexes only grows; the
  snapshot boundary only advances. Compaction can raise the value in one jump
  when a snapshot boundary sits above the last committed application entry, which
  is still non-decreasing.
- **Snapshot interaction.** A Raft-driven install forces the state machine's
  applied index to the snapshot boundary
  ([`crates/rafter-app/src/group/snapshot.rs:19-32`](../crates/rafter-app/src/group/snapshot.rs)),
  so the predicate holds immediately after install. A locally chosen compaction
  boundary is the caller's responsibility, as documented above.
- **Why it cannot be read as a linearizability signal.** It is derived from the
  local commit index, which is knowledge, not truth. A partitioned former leader
  satisfies the predicate while the cluster has moved on. The doc says so, and
  the name says "committed application index", not "up to date".
- **Fakes and scripted runtimes.** A runtime that does not model a log must
  still answer honestly for whatever it does model. The contract is stated in
  terms the fakes can meet.

### Blast radius

Breaking: `PersistedRaftRuntime` gains a required method.

| File | Change |
| --- | --- |
| [`crates/rafter-runtime-api/src/lib.rs`](../crates/rafter-runtime-api/src/lib.rs) | New trait method |
| [`crates/rafter-runtime/src/lib.rs`](../crates/rafter-runtime/src/lib.rs) | Inherent method plus the trait impl at `:717-780` |
| [`crates/rafter-app/src/group/output.rs`](../crates/rafter-app/src/group/output.rs) | `RaftGroup::committed_application_index` |
| [`crates/rafter-app/tests/support/mod.rs:184,310`](../crates/rafter-app/tests/support/mod.rs) | `KernelRuntime`, `ScriptedRuntime` |
| [`crates/rafter-service/tests/support/mod.rs:239,348`](../crates/rafter-service/tests/support/mod.rs) | `ScriptedReadRuntime`, `ScriptedWriteRuntime` |
| [`crates/rafter-service/tests/in_memory_write.rs:216`](../crates/rafter-service/tests/in_memory_write.rs) | `BatchRecordingRuntime` |
| `bench-compare/src/bin/bench-rafter-multiraft.rs:230`, `bench-compare/src/bin/bench-rafter-service.rs:211` | `RecordingRuntime` |

`bench-compare` is not a workspace member, so this break does not surface under
`cargo check --workspace`; it must be built for this step.

The break is justified because no default is both safe and useful. A default of
`LogIndex::ZERO` fails open on a readiness gate — the one failure mode the gate
exists to prevent. A default of `commit_index` fails closed but never opens on
any cluster that has ever held an election, which makes the gate useless. An
implementor must answer for its own log.

### Focused-test plan

In `crates/rafter-runtime/src/tests/recovery.rs` and `replay_recovery.rs`:

- `committed_application_index_ignores_noop_and_configuration_entries` — elect a
  leader so the tail is a `Noop`, assert the value is below `commit_index`.
- `committed_application_index_uses_the_snapshot_boundary_after_compaction`.
- `committed_application_index_is_zero_on_a_node_with_no_application_entries`.
- Negative: `committed_application_index_does_not_decrease_across_conflict_repair`
  — reuse the fixtures in
  [`crates/rafter-runtime/src/tests/conflict_repair.rs`](../crates/rafter-runtime/src/tests/conflict_repair.rs)
  to truncate an uncommitted application suffix and assert the value is
  unchanged.

In `crates/rafter-app/tests/group_apply.rs`:

- `readiness_predicate_is_false_until_recovery_outputs_are_applied` — recover a
  runtime with an applied floor, build the group, assert the state machine's
  applied index is below `committed_application_index()` before
  `apply_raft_outputs`, and at or above it afterwards. This is the case a
  group-observed floor gets wrong, and the reason the value is runtime-derived.
- `committed_application_index_is_reported_by_a_poisoned_group`.

### Rejected alternatives

- **A `RaftGroupMetrics` field.** Puts a log scan on the default step path and
  misclassifies a precondition as an observation.
- **`pending_application_applies: usize`.** Cannot be persisted, compared, or
  used as a target; see above.
- **A group-observed floor tracked from `RaftOutput::Apply`.** O(1), and wrong
  in the one case that matters: it reports ready for a group whose runtime still
  holds unapplied recovery outputs.
- **Exposing `log_entries_from` on `PersistedRaftRuntime`.** Hands every app
  layer the raw log to answer one structural question, leaks payload bytes into
  a trait that deliberately has none, and makes every consumer re-derive the
  same filter.
- **A `has_applied_all_committed() -> Result<bool>` predicate.** Hides the two
  numbers an operator needs in a diagnostic, and forces the group to fold the
  state machine's fallible applied-index read into a readiness answer.

### After-state

`LedgerCluster::committed_application_floor` disappears, and `settle()` compares
two public numbers. `committed_commands` keeps its log walk, because it needs
the decoded payloads rather than the floor — an honest remainder, not a
workaround.

Corrected in adoption: `committed_application_entries` does not disappear, and
the sentence above contradicted itself in saying so. It *is* the log walk
`committed_commands` keeps. What disappears is the `.last()` fold over it that
reconstructed the floor, in both consumers.

## Leader Hint on Proposal Rejection

### Origin

`ProposalBegin::Rejected` carries `leader_hint`
([`crates/rafter-app/src/proposal.rs:67-72`](../crates/rafter-app/src/proposal.rs)),
but `ProposalEvent::Rejected` does not
([`crates/rafter-app/src/proposal.rs:119-122`](../crates/rafter-app/src/proposal.rs)).
A caller that submits and then waits therefore loses the redirect on any
rejection observed asynchronously.

The ledger's driver records the loss where it happens
([`reference/ledger/tests/support/cluster.rs:511-524`](../reference/ledger/tests/support/cluster.rs)):

```rust
// A rejection observed as a later lifecycle event carries no leader
// hint of its own; only the immediate begin outcome does.
```

and hard-codes `leader_hint: None` into the client-visible outcome, which is
the redirect a real client needs and will not get.

`rafter-service` pays more. It carries a dedicated `rejection_leader_hint`
helper
([`crates/rafter-service/src/driver/write.rs:246-272`](../crates/rafter-service/src/driver/write.rs))
that scans the report for a `NotLeader` rejection, then tries
`report.metrics.leader_hint`, then falls back to re-reading
`group.leader_hint()`. Its production write path steps with
`StepReportOptions::without_metrics()`
([`crates/rafter-service/src/driver/write.rs:72-76`](../crates/rafter-service/src/driver/write.rs)),
so `report.metrics` is always `None` there and the hint always comes from the
later re-read — a different moment than the rejection — and one hint is applied
to every rejected proposal in the batch.

### Classification

Raft mechanism. The hint is protocol knowledge the local node already holds at
the moment it records the rejection: the emit site is inside `record_raft_output`
([`crates/rafter-app/src/group/output.rs:338-348`](../crates/rafter-app/src/group/output.rs)),
which reads `self.raft.leader_hint()` a few lines away for
`LeadershipTransferEvent::Rejected` (`:364`), as does `record_rejected_read` for
`ReadEvent::Rejected`
([`crates/rafter-app/src/group/read.rs:210`](../crates/rafter-app/src/group/read.rs)).
Three of the four rejection events in the app layer carry the hint; the fourth
withholds it and forces every consumer to reconstruct it later and less
accurately.

Second plausible consumer: `rafter-service`, whose reconstruction is in
production today and silently degrades when metrics are disabled.

### Design

```rust
pub enum ProposalEvent<R> {
    // ...
    /// The local node refused this proposal before replication.
    ///
    /// `leader_hint` is the leader this node believed in when the rejection was
    /// recorded. It is a redirect hint, never authority: it may be `None`, it
    /// may already be stale, and it may name this node when the rejection was
    /// not about leadership. It is recorded at the same point as the hints on
    /// [`crate::read::ReadEvent::Rejected`] and
    /// [`crate::group::LeadershipTransferEvent::Rejected`], so a caller that
    /// observes this rejection asynchronously sees the same value the immediate
    /// [`ProposalBegin::Rejected`] carries for the same rejection.
    Rejected {
        local_proposal_id: LocalProposalId,
        reason: ProposalRejection,
        leader_hint: Option<NodeId>,
    },
    // ...
}
```

`proposal_begin_from_report`
([`crates/rafter-app/src/group/proposal.rs:173-186`](../crates/rafter-app/src/group/proposal.rs))
takes the hint from the event instead of re-reading `self.raft.leader_hint()`
after the whole report is built, so the immediate and asynchronous views of one
rejection can never disagree.

### Semantics and edge cases

- **Variant field, not a shared struct.** Three variants in this crate already
  carry a bare `leader_hint: Option<NodeId>` beside a rejection reason. A
  `Rejection { reason, leader_hint }` wrapper would restate a settled pattern and
  break four sites for symmetry alone.
- **Timing.** The hint is read when the output is recorded, inside the same step
  that produced the rejection — earlier and more precisely than any
  reconstruction from a post-step accessor.
- **Meaning of `None`.** No leader is known. It is not "not applicable"; a
  caller must handle it as "retry discovery", exactly as for the other three
  events.
- **Exhaustiveness.** `ProposalEvent` is `#[non_exhaustive]` at the enum level
  ([`crates/rafter-app/src/proposal.rs:105-107`](../crates/rafter-app/src/proposal.rs)),
  which forces a wildcard arm but leaves each variant's field list exhaustive.
  Every pattern that destructures `Rejected` without `..` therefore breaks, and
  so does every expression that constructs it.
- **The variant stays constructible.** Marking the variant `#[non_exhaustive]`
  would make field additions non-breaking, but it would also stop every
  downstream test from constructing an expected event for an equality
  assertion — a style used in this repository at
  [`crates/rafter-app/tests/group_proposal_lifecycle.rs:999`](../crates/rafter-app/tests/group_proposal_lifecycle.rs)
  and
  [`crates/rafter-service/src/driver/write.rs:618`](../crates/rafter-service/src/driver/write.rs),
  and available to consumers for exactly the same purpose. The enum-level
  attribute already covers new variants, which is the more common evolution.

### Blast radius

Breaking: an added field on a struct variant.

| File | Change |
| --- | --- |
| [`crates/rafter-app/src/proposal.rs:119-122`](../crates/rafter-app/src/proposal.rs) | Field and doc |
| [`crates/rafter-app/src/group/output.rs:338-348`](../crates/rafter-app/src/group/output.rs) | Populate from `self.raft.leader_hint()` |
| [`crates/rafter-app/src/group/proposal.rs:173-186`](../crates/rafter-app/src/group/proposal.rs) | Take the hint from the event |
| [`crates/rafter-app/tests/group_proposal_lifecycle.rs:999`](../crates/rafter-app/tests/group_proposal_lifecycle.rs) | Constructs the variant |
| [`crates/rafter-service/src/driver/write.rs:246-272,413,618`](../crates/rafter-service/src/driver/write.rs) | Delete `rejection_leader_hint`, read the field |
| [`reference/ledger/tests/support/cluster.rs:511-524`](../reference/ledger/tests/support/cluster.rs) | Exhaustive pattern; workaround removed |

Patterns already using `..` — `crates/rafter-app/src/group/types.rs:231`,
`crates/rafter-service/src/driver/write.rs:253,315,345` — are unaffected.
`rafter-sim` maps its own local proposal events and does not match this variant.
The break is justified: the field is one word, the reconstruction it replaces is
in production and imprecise, and there is no additive way to attach data to an
existing variant.

### Focused-test plan

In `crates/rafter-app/tests/group_proposal_lifecycle.rs`:

- `rejected_proposal_event_carries_the_leader_hint` — a follower that knows its
  leader rejects a proposal with `ProposalRejection::NotLeader`; assert the hint.
- `rejected_proposal_event_carries_no_hint_when_no_leader_is_known` — the same
  on a node with no leader hint.
- `rejected_proposal_event_hint_matches_the_immediate_begin_outcome` — assert
  `ProposalBegin::Rejected.leader_hint` equals the event's hint for the same
  rejection in the same step. This is the invariant the shared source of truth
  buys.
- Negative: `a_non_leadership_rejection_still_reports_the_current_hint` — the
  hint is the node's belief, not a claim about the rejection reason.

In `crates/rafter-service/tests/in_memory_write.rs`:

- `write_rejection_reports_a_leader_hint_when_metrics_are_disabled` — the
  regression the current reconstruction misses on the production path.

### Rejected alternatives

- **Leave it to `RaftGroupMetrics`.** The metrics snapshot is optional per step
  and absent on the one production path that needs the hint.
- **A shared `Rejection` struct across all four events.** Larger break, no new
  information, and it would have to be threaded through `ProposalBegin` and
  `ReadProofOutcome` for consistency.
- **Re-read `leader_hint()` in the caller.** That is the workaround; it reads a
  different moment and cannot distinguish per-proposal rejections in a batch.

### After-state

The ledger driver deletes its comment and its hard-coded `None`, and reports the
same redirect for an asynchronously observed rejection as for an immediate one.
`rafter-service` deletes `rejection_leader_hint` and stops applying one hint to
a whole batch.

## Read-Barrier Application Floor

### Origin

A linearizable read cannot be answered after a leader election until an
unrelated application entry commits. This is not a delay measured in rounds; on
a read-only tail it never ends.

Every term's leader appends a `Noop` as its first entry
([`crates/rafter/src/node/lifecycle.rs:42`](../crates/rafter/src/node/lifecycle.rs)),
and the kernel refuses read barriers until that entry commits
([`crates/rafter/src/node/read_index.rs:50-52`](../crates/rafter/src/node/read_index.rs)).
The moment it commits, barriers are granted at
`read_index = self.volatile.commit_index`
([`:71`](../crates/rafter/src/node/read_index.rs)) — which is the `Noop`. The
state machine never sees that entry: `apply_committed_into` advances the
kernel's applied index over every committed entry but emits `Output::Apply`
only for `LogEntryKind::Application`
([`crates/rafter/src/node/commit/apply.rs:66-90`](../crates/rafter/src/node/commit/apply.rs)).
`complete_ready_reads` then compares the barrier against the state machine's own
cursor
([`crates/rafter-app/src/group/read.rs:293-302`](../crates/rafter-app/src/group/read.rs)):

```rust
let required_applied_index = max(read_index, min_applied_index.unwrap_or(read_index));
if local_applied_index >= required_applied_index {
```

`local_applied_index` comes from `A::applied_index()`, which advances only
through `apply_batch` and `install_snapshot`
([`crates/rafter-app/src/group/apply.rs:93-94`](../crates/rafter-app/src/group/apply.rs),
[`crates/rafter-app/src/group/snapshot.rs:29-32`](../crates/rafter-app/src/group/snapshot.rs)).
It can never equal a `Noop` index. The barrier therefore reports
`ReadEvent::FreshnessUnavailable` on that step, and on every step after it,
forever. A brand-new cluster is the extreme case: its first-ever entry is the
`Noop` at index 1, so its first-ever linearizable read stalls until somebody
writes.

The fenced-lock consumer pinned the finding rather than assuming it away
([`reference/fenced-lock/tests/adapter_cluster.rs:403-422`](../reference/fenced-lock/tests/adapter_cluster.rs)):

```rust
// API finding, pinned here rather than assumed away. A new leader's only
// entry in its own term is a Raft noop, and a noop never reaches the state
// machine, so the barrier's required applied index is briefly unreachable
// and the query cannot answer yet. Whatever it does, it must never answer
// from state that predates the leader change.
```

The word "briefly" is the consumer being generous. The test can only assert
"fresh answer or no answer", then commits an idempotent `open_session` command
to lift the floor before it may assert an answer at all. Its driver's
freshness arm — "Keep waiting; the next round applies more"
([`reference/fenced-lock/tests/support/cluster.rs:501-503`](../reference/fenced-lock/tests/support/cluster.rs))
— spins the whole `MAX_ROUNDS` budget and then abandons the read
([`:1198-1213`](../reference/fenced-lock/tests/support/cluster.rs)).

Two production paths pay the same bill, and neither is a reference consumer.

- **`rafter-service`.** `handle_linearizable_freshness_gap` abandons the read
  and returns `ReadError::FreshnessUnavailable` as soon as the network drains
  ([`crates/rafter-service/src/driver/read.rs:139-156`](../crates/rafter-service/src/driver/read.rs)).
  After an election the network does drain, because nothing else is happening.
  The managed driver therefore fails every linearizable read after every
  election until a write commits, and it passes `min_applied_index: None`
  ([`:174-206`](../crates/rafter-service/src/driver/read.rs)), so the caller
  cannot work around it.
- **`rafter-maelstrom`.** It has the same defect independently, reached through
  the kernel rather than through `RaftGroup`. `flush_reads` gates on
  `self.app.applied >= read.read_index`
  ([`crates/rafter-maelstrom/src/client.rs:166-182`](../crates/rafter-maelstrom/src/client.rs)),
  and `app.applied` advances only in `apply_committed_command` and on snapshot
  install
  ([`crates/rafter-maelstrom/src/app.rs:174,190`](../crates/rafter-maelstrom/src/app.rs)).
  It is worse there: `flush_reads` is called only from the grant arm, from an
  apply, and from a snapshot install
  ([`crates/rafter-maelstrom/src/raft.rs:100,168`](../crates/rafter-maelstrom/src/raft.rs),
  [`crates/rafter-maelstrom/src/raft/snapshots.rs:46`](../crates/rafter-maelstrom/src/raft/snapshots.rs)),
  so a stalled read is never re-examined at all. `scripts/maelstrom-lin-kv` is
  registered evidence for `RD-04` and `RD-06`
  ([`verification/raft-invariants.yaml:1687-1691`](../verification/raft-invariants.yaml));
  the workload is write-mixed, so the stall shows up as post-partition read
  latency rather than as a failure. The defect is inside the evidence.

### Classification

Raft mechanism, following directly from a documented correctness contract —
and, unusually for this document, from a contradiction between two of them.

The kernel defines what a state machine can ever observe: `Application` entries
produce `Output::Apply`, `Configuration` and `Noop` produce none
([`crates/rafter/src/node/commit/apply.rs:66-69`](../crates/rafter/src/node/commit/apply.rs)).
`ReplicatedStateMachine` has exactly two state-changing entry points,
`apply_batch` and `install_snapshot`
([`crates/rafter-app/src/state_machine.rs:60-97`](../crates/rafter-app/src/state_machine.rs)),
and neither is reachable from a non-application entry. Meanwhile
`ReadConsistency::Linearizable` promises the read "requires the local state
machine to be applied through the returned read index"
([`crates/rafter-app/src/read.rs:14-16`](../crates/rafter-app/src/read.rs)). The
two contracts are jointly unsatisfiable whenever the read index lands on an
entry the first contract guarantees the state machine will never be told about.
That is not a missing feature; it is a requirement the library states and then
makes impossible to meet.

The predicate that replaces it — has this state machine applied every committed
*application* entry at or below the read index — is entirely about Raft log
structure and contains no application policy, exactly as for
[Committed Application Index](#committed-application-index). This entry
generalizes that one; see [Coupled designs](#coupled-designs).

The rule's "at least one other plausible consumer" test is not needed here, but
it is satisfied by demonstration rather than plausibility: `rafter-service` and
`rafter-maelstrom` both contain the defect today, in tree, written independently
of each other and of the consumer that found it.

**Why no verification artifact caught it.** `RD-04` reads "The application
returns a read result only after its local applied index reaches the granted
read index"
([`docs/raft-invariants.md:458`](./raft-invariants.md)). "Local applied index"
names two different quantities in this repository, and every oracle picked the
one that cannot fail. The simulator sources it from `Node::applied_index()`
([`crates/rafter-sim/src/inspection.rs:87-91`](../crates/rafter-sim/src/inspection.rs)),
the kernel cursor, which advances over `Noop` and `Configuration`; its check
`proof.local_applied_index < proof.read_index`
([`crates/rafter-sim/src/model_check/invariants/client.rs:177-188`](../crates/rafter-sim/src/model_check/invariants/client.rs))
is therefore near-vacuous. The TLA model has no `Noop` entry kind at all — only
`Command` and `Configuration`
([`specs/tla/raft/Raft.tla:68-69`](../specs/tla/raft/Raft.tla)) — and its
`Apply` action advances `AppliedThrough` one entry at a time regardless of kind
([`:941-954`](../specs/tla/raft/Raft.tla)), so the model cannot express the gap
either. `RD-04` has no TLA coverage; the only model-checked read invariant is
`RD-03`, which constrains `readIndex >= committedFloor`
([`:546-551`](../specs/tla/raft/Raft.tla)) and is untouched by this change. The
app layer implemented `RD-04`'s words against the quantity the app layer owns,
and it is the only layer where those words are false.

### Design

Three changes, one idea: **the barrier's required floor is the highest
committed application entry at or below the read index, resolved once when the
read index is granted.**

#### The runtime derivation

`PersistedRaftRuntime::committed_application_index` already answers this
question — bounded at the commit index rather than at an arbitrary index. It is
not close enough to reuse, and the reason is precise: **a barrier's read index
is captured at registration and consumed arbitrarily later.**
`read_index_batch` stores `read_index: self.volatile.commit_index` into the
pending round
([`crates/rafter/src/node/read_index.rs:71`](../crates/rafter/src/node/read_index.rs))
and grants it only when a later heartbeat round is quorum-confirmed
([`:108-140`](../crates/rafter/src/node/read_index.rs)); after the grant,
`complete_ready_reads` re-evaluates the barrier on every subsequent step. The
commit index advances throughout. So `commit_index > read_index` is the normal
case on a busy leader, and an uncapped floor would require the read to wait for
a write it is not ordered after.

The required method is therefore generalized to take its bound, and the
existing one becomes a provided method defined in terms of it, so the two can
never disagree and no implementor answers the same question twice.

In `crates/rafter-runtime-api/src/lib.rs`:

```rust
/// Returns the index the local state machine must reach to have consumed
/// every committed application command at or below `index`.
///
/// This is the highest index at or below both `index` and
/// [`PersistedRaftRuntime::commit_index`] whose log entry carries an
/// application payload; when no such entry is retained it is the snapshot
/// boundary, which subsumes every application entry it covers, capped at
/// `index`. It is `LogIndex::ZERO` when the node holds no snapshot and has
/// committed no application entry at or below `index`.
///
/// The result never exceeds `index`. That is the load-bearing property for a
/// read barrier: elections and membership changes commit entries the state
/// machine never sees, so a barrier that required its state machine to reach
/// the read index itself would require an index the kernel guarantees it will
/// never report. Requiring more than this is not conservative — it makes a
/// read wait for a write that is not ordered before it.
///
/// The value is non-decreasing in `index`, and non-decreasing over time for a
/// fixed `index` within one node incarnation: committed entries are never
/// truncated, and compaction can only raise the answer to a boundary the
/// state machine has itself already reached.
///
/// The value is local, and it is not a freshness proof. Pairing it with a
/// granted read index is what makes a read linearizable; on its own it says
/// only what this replica knows.
///
/// Implementations must report the true value for their own log rather than
/// an optimistic bound. A runtime that reports an index below the highest
/// committed application entry at or below `index` lets a barrier grant
/// before the state machine has applied an acknowledged write.
fn committed_application_index_through(&self, index: LogIndex) -> LogIndex;

/// Returns the index the local state machine must reach to have consumed
/// every committed application command.
///
/// This is [`PersistedRaftRuntime::committed_application_index_through`] at
/// the commit index, and it is the readiness predicate: compare it with the
/// state machine's applied index after recovery. Implementations should not
/// override it.
///
/// Elections and membership changes commit entries the state machine never
/// sees, so this is not `commit_index`, and a fully caught-up state machine
/// may trail the committed index forever.
fn committed_application_index(&self) -> LogIndex {
    self.committed_application_index_through(self.commit_index())
}
```

`DurableRaftNode`'s existing backward scan
([`crates/rafter-runtime/src/lib.rs:411-424`](../crates/rafter-runtime/src/lib.rs))
already has the shape; it gains a bound, replacing `commit_index` with
`min(index, commit_index)` in the `find` predicate and returning
`min(snapshot_index, index)` instead of `snapshot_index` when the scan finds
nothing. Its cost profile is unchanged and its documented rationale — O(1) on a
busy group's tail, worst case the committed retained suffix — still holds. The
kernel does not change.

`RaftGroup` gains the matching forwarder beside the one it already has
([`crates/rafter-app/src/group/output.rs:71-74`](../crates/rafter-app/src/group/output.rs)),
because it is the tool a caller needs to turn an arbitrary index into a floor a
state machine can actually reach:

```rust
/// Returns the index this group's state machine must reach to have applied
/// every committed application command at or below `index`.
///
/// Use this to convert a raw log index into a reachable applied floor. A
/// commit index, a read index, or a snapshot boundary may name an entry the
/// state machine will never be told about, and waiting for the state machine
/// to report that index waits forever. An index taken from
/// [`crate::proposal::ProposalEvent::Applied`] never needs the conversion: it
/// already names an application entry.
///
/// This is what [`RaftGroup::read`] applies to a granted read index on the
/// caller's behalf. It is not applied to a caller-supplied
/// `min_applied_index`; see [`crate::read::ReadRequest`].
#[must_use]
pub fn committed_application_index_through(&self, index: LogIndex) -> LogIndex;
```

#### Resolving the floor once, at grant

`PendingRead` currently stores the granted read index alone
([`crates/rafter-app/src/group/types.rs:84-87`](../crates/rafter-app/src/group/types.rs)).
It stores the resolved floor with it:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct GrantedReadIndex {
    /// What the quorum round certified.
    pub(super) read_index: LogIndex,
    /// The highest committed application entry at or below `read_index`, and
    /// therefore the applied index a state machine can actually reach.
    pub(super) application_floor: LogIndex,
}

pub(super) struct PendingRead {
    pub(super) min_applied_index: Option<LogIndex>,
    pub(super) granted: Option<GrantedReadIndex>,
}
```

The `RaftOutput::ReadIndexGranted` arm of `record_raft_output`
([`crates/rafter-app/src/group/output.rs:414-421`](../crates/rafter-app/src/group/output.rs))
resolves the floor there, once per barrier, rather than re-deriving it on every
step for every pending read. It must compute the floor before taking the
`&mut` borrow of the pending entry.

`complete_ready_reads` and `try_complete_pending_query_read`
([`crates/rafter-app/src/group/read.rs:300-302,490-491`](../crates/rafter-app/src/group/read.rs))
then read a stored scalar, so the per-step cost of the read table is exactly
what it is today, and both compute:

```rust
let required_applied_index = match min_applied_index {
    Some(min) => max(granted.application_floor, min),
    None => granted.application_floor,
};
```

The changed default is load-bearing and easy to miss: today the `None` case
folds in `read_index`, and leaving it there would make `max` re-raise the floor
to exactly the value this change removes.

Resolving at grant rather than per evaluation is not only cheaper. It makes the
floor a fixed property of the barrier, so a caller polling toward
`ReadEvent::FreshnessUnavailable { required_applied_index }` sees a stable
target, and a later compaction or commit cannot move it.

#### The correctness argument

The claim to establish is that granting once the state machine has applied
every application entry at or below the read index serves exactly the
linearizable state, and it rests on three facts already in the repository.

1. **The read index is a sound cut.** A granted `read_index` is the leader's
   commit index at registration, confirmed by a quorum round at or after
   registration
   ([`crates/rafter/src/node/read_index.rs:18-21,67-83,108-140`](../crates/rafter/src/node/read_index.rs)),
   with barriers refused until the leader has committed in its own term
   ([`:50-52`](../crates/rafter/src/node/read_index.rs)). Every write that
   completed before the read began is therefore committed at an index at or
   below `read_index`. This is `RD-03`, model-checked as
   `ReadBarrierLinearizability`
   ([`specs/tla/raft/Raft.tla:546-551`](../specs/tla/raft/Raft.tla)), and this
   change does not touch it.
2. **Application state is a function of application entries alone.** The kernel
   dispatches `Output::Apply` only for `LogEntryKind::Application`
   ([`crates/rafter/src/node/commit/apply.rs:66-90`](../crates/rafter/src/node/commit/apply.rs)),
   and the only other input to a `ReplicatedStateMachine` is
   `install_snapshot`, which carries a boundary the snapshot already covers.
   A `Noop` or `Configuration` entry cannot change any state a query can
   observe, because no code path exists to tell the state machine it happened.
3. **`applied_index` is a faithful witness.** The state machine's cursor takes
   values only at application-entry indexes and snapshot boundaries, advances
   in log order, and is verified against the group's floor on every batch
   ([`crates/rafter-app/src/group/apply.rs:110-157`](../crates/rafter-app/src/group/apply.rs));
   a state machine that reports a cursor below the floor, or that is handed an
   already-applied entry, poisons the group. So
   `applied_index >= application_floor(read_index)` holds exactly when every
   application entry at or below `read_index` has been incorporated — "highest"
   is sufficient precisely because applies are ordered and gapless.

(1) and (2) give: every write that completed before the read began is an
application entry at index at or below `read_index`. (3) gives: the floor
predicate is true exactly when all of them are incorporated. The state the
query then reads is the state machine's *current* state, at
`local_applied_index >= required_applied_index` — the barrier certifies a lower
bound on freshness, never a point to rewind to, which is why `ReadBarrier`
carries both numbers
([`crates/rafter-app/src/state_machine.rs:124-129`](../crates/rafter-app/src/state_machine.rs)).
Serving state fresher than the cut is still linearizable; it places the read's
linearization point later within its own interval.

**What breaks if this is wrong.** A stale read: the barrier grants, the query
answers from a state missing a write the client already saw acknowledged, and
`RD-04` and `RD-06` both fail. There are exactly two ways to get there, and
both are worth stating because they are what the negative tests must attack.
The first is a log entry kind that changes application-visible state without
being `LogEntryKind::Application` — impossible today, and the floor is defined
against the same `application_payload()` predicate the kernel's own dispatch
uses
([`crates/rafter/src/types/configuration.rs:178-181`](../crates/rafter/src/types/configuration.rs)),
so the two cannot drift apart without breaking simultaneously. The second is a
state machine whose `applied_index` runs ahead of what it has durably
incorporated; that is already forbidden
([`crates/rafter-app/src/state_machine.rs:26-30`](../crates/rafter-app/src/state_machine.rs))
and is not newly load-bearing here — today's floor trusts the same number.

### Semantics and edge cases

- **Mixed logs: an application entry above the read index must not be
  required.** With `Noop@6` as the read index and an application entry
  committing at 7 while the barrier's round is in flight, the floor is the
  highest application entry at or below 6. Entry 7 is not ordered before this
  read and waiting for it is a defect, not caution. This is the case an
  uncapped `committed_application_index()` gets wrong, and it is the reason the
  method is generalized rather than reused.
- **Caller-supplied floors are honored verbatim.** `min_applied_index` is not
  capped, not lowered, and not snapped to an application entry. A caller may be
  expressing "at least as fresh as the write I already observed", and Rafter
  must not silently weaken that. The natural source of the value —
  `ProposalEvent::Applied { index }` — always names an application entry, so
  the natural usage is always reachable. A caller that sources it from a commit
  index or a read index gets an unreachable floor and a permanent
  `FreshnessUnavailable`; the documented remedy is
  `RaftGroup::committed_application_index_through`, not a silent repair.
- **Local reads are unaffected, and keep the same trap.** `read_local` bases
  its requirement on the local applied index, never on a read index
  ([`crates/rafter-app/src/group/read.rs:347-350`](../crates/rafter-app/src/group/read.rs)),
  so `LocalFreshnessUnavailable` is reachable only through a caller-supplied
  `min_applied_index`. That path is unchanged and gets the same doc pointer.
- **No follower or lease read path exists to fix.** The kernel rejects
  `ReadIndex` on non-leaders
  ([`crates/rafter/src/node/read_index.rs:34-40`](../crates/rafter/src/node/read_index.rs)),
  and `ReadConsistency::LeaseRead` is refused by the app layer
  ([`crates/rafter-app/src/group/read.rs:198-209`](../crates/rafter-app/src/group/read.rs)).
  The kernel's lease fast path grants at the commit index of that moment
  ([`crates/rafter/src/node/read_index.rs:58-65`](../crates/rafter/src/node/read_index.rs)),
  so if lease support is later enabled in this layer it arrives through the
  same `ReadIndexGranted` arm and inherits the fix with no further work.
- **Snapshot boundary above the read index.** The floor is capped at the read
  index, so a boundary above it yields a floor of exactly the read index —
  which a state machine that installed the snapshot has already passed. The
  barrier grants immediately, as it should: the snapshot subsumes every entry
  in the cut.
- **Snapshot boundary below the read index.** The scan covers
  `(snapshot_index, read_index]` and falls back to the boundary. Correct: the
  boundary subsumes everything under it, and the retained window holds every
  committed entry above it.
- **The provided method is exactly today's behavior.** The equivalence rests on
  `snapshot_index <= commit_index`, which the kernel maintains: installing a
  snapshot raises the commit index to the boundary when it sits below
  ([`crates/rafter/src/node/log.rs:209-211`](../crates/rafter/src/node/log.rs)).
  At `index = commit_index` the new `min(snapshot_index, index)` fallback is
  therefore `snapshot_index`, which is what
  `DurableRaftNode::committed_application_index` returns today. The cap only
  ever bites for a bound below the commit index, which is the read-barrier case
  and no existing caller.
- **Compaction after the grant cannot move the floor.** Because the floor is
  resolved at grant and stored, a later compaction is invisible to the barrier.
  Even if it were re-derived, compaction can only raise the answer to a
  boundary at or below the state machine's own applied index — `build_snapshot`
  compacts at the applied index, and an install forces the cursor to the
  boundary — so it can never make a satisfiable barrier unsatisfiable.
- **The proof's two indexes stop being the same number.** `ReadProof` already
  separates `read_index` from `required_applied_index`
  ([`crates/rafter-app/src/read.rs:133-142`](../crates/rafter-app/src/read.rs));
  the type anticipated this distinction and the app layer collapsed it. After
  this change `required_applied_index <= read_index` unless `min_applied_index`
  raised it, and each field means what it says: what the quorum certified, and
  what the state machine had to reach. No type changes.
- **A read barrier certifies application freshness, not membership freshness.**
  `Configuration` entries change kernel state that a query cannot observe
  through `ReplicatedStateMachine`; callers needing committed membership read
  `RaftGroup::metrics().membership`, which is not barrier-gated and never was.
  Worth stating because the TLA model folds membership into its application
  state ([`specs/tla/raft/Raft.tla:148-151`](../specs/tla/raft/Raft.tla)) while
  the implementation does not.
- **Poison and cancellation are unchanged.** The floor changes when a barrier is
  satisfied, not which barriers exist, so rejection, cancellation,
  `cancel_read`, and poison drain paths are untouched.
- **Fakes must answer for whatever they model.** A runtime with no log answers
  from whatever it does model, exactly as for the uncapped method. The contract
  is stated in terms a scripted runtime can meet: a fake that models a set of
  application-entry indexes answers by taking the greatest one at or below the
  bound.

### Blast radius

Breaking: `PersistedRaftRuntime`'s required method changes shape. The kernel
changes one doc comment and no code.

| File | Change |
| --- | --- |
| [`crates/rafter-runtime-api/src/lib.rs:51-77`](../crates/rafter-runtime-api/src/lib.rs) | `committed_application_index_through` required; `committed_application_index` provided |
| [`crates/rafter-runtime/src/lib.rs:397-424,810-812`](../crates/rafter-runtime/src/lib.rs) | Bound the scan; drop the now-provided trait method |
| [`crates/rafter-app/src/group/types.rs:84-87`](../crates/rafter-app/src/group/types.rs) | `GrantedReadIndex`; `PendingRead.granted` |
| [`crates/rafter-app/src/group/output.rs:414-421`](../crates/rafter-app/src/group/output.rs) | Resolve the floor at grant; add the `RaftGroup` forwarder |
| [`crates/rafter-app/src/group/read.rs:300-302,490-491`](../crates/rafter-app/src/group/read.rs) | Use the stored floor; `min_applied_index` defaults to `ZERO`, not `read_index` |
| [`crates/rafter-app/src/read.rs:13-16,120-142`](../crates/rafter-app/src/read.rs) | `ReadConsistency::Linearizable`, `ReadRequest`, and `ReadProof` docs |
| [`crates/rafter/src/node/read_index.rs:1-5`](../crates/rafter/src/node/read_index.rs) | Module doc: "the granted index" becomes "every application entry at or below the granted index" |
| [`crates/rafter-maelstrom/src/client.rs:166-182`](../crates/rafter-maelstrom/src/client.rs) | `flush_reads` gates on the runtime's floor; add a tick-driven retry so a stalled read is re-examined |

Every `PersistedRaftRuntime` implementor changes. There are eight in the tree:
`DurableRaftNode` plus seven others. Two earlier entries said "six" — the
embedder count under [Full-Fidelity Query Reads](#full-fidelity-query-reads) and
the implementor count in [Adoption order](#adoption-order); both were off by one
and are corrected in place. The blast-radius table under
[Committed Application Index](#committed-application-index) was always complete.

| File | Type | Change |
| --- | --- | --- |
| [`crates/rafter-runtime/src/lib.rs:777`](../crates/rafter-runtime/src/lib.rs) | `DurableRaftNode` | Real; bound the scan |
| [`crates/rafter-app/tests/support/mod.rs:185,216-232`](../crates/rafter-app/tests/support/mod.rs) | `KernelRuntime` | Owns a real kernel; bound its scan the same way |
| [`crates/rafter-app/tests/support/mod.rs:333,364-366`](../crates/rafter-app/tests/support/mod.rs) | `ScriptedRuntime` | No log; needs a scripted application-entry index set |
| [`crates/rafter-service/tests/support/mod.rs:242,281-283`](../crates/rafter-service/tests/support/mod.rs) | `ScriptedReadRuntime` | Models application entries through its commit index, so `min(index, commit_index)`; a new `GrantAtNonApplicationIndex` mode models the post-election log and answers `ZERO` |
| [`crates/rafter-service/tests/support/mod.rs:370,409-411`](../crates/rafter-service/tests/support/mod.rs) | `ScriptedWriteRuntime` | Appends but never commits; still `ZERO` |
| [`crates/rafter-service/tests/in_memory_write.rs:236,269-271`](../crates/rafter-service/tests/in_memory_write.rs) | `BatchRecordingRuntime` | Highest recorded index at or below the bound |
| `bench-compare/src/bin/bench-rafter-service.rs:242`, `bench-rafter-multiraft.rs:261` | `RecordingRuntime` | Delegate |

`ScriptedRuntime` is the one that needs real thought: the whole
`group_read*.rs` suite drives it, and it has no log to answer from. Giving it an
explicit set of application-entry indexes is also what makes the mixed-log
negative test writable. The set defaults to "every index is an application
entry", which is what every fixture written before the floor existed assumed, so
those fixtures keep their behavior with no rewrite. It also gains a per-step log
reshape queue, because the group exposes its runtime only by shared reference
and `read_barrier_floor_is_fixed_at_grant` must commit and compact *behind* a
granted barrier.

Two `ScriptedReadRuntime` rows above were corrected against the code. Answering
a flat `ZERO` would have made the driver's freshness path unreachable from every
existing fixture, deleting the coverage in
[`in_memory_read.rs:78-98,100-123`](../crates/rafter-service/tests/in_memory_read.rs)
that the pin table below requires to survive; the fake models a committed
prefix of application entries instead, and a new mode models the post-election
log the regression test needs.

Tests that pin the current floor:

| File | Pin |
| --- | --- |
| [`crates/rafter-app/tests/group_read.rs:43-114,248-334`](../crates/rafter-app/tests/group_read.rs) | Assert `required_applied_index == read_index == 4` on the `min_applied_index: None` path |
| [`crates/rafter-app/tests/group_read.rs:8-40,207-246`](../crates/rafter-app/tests/group_read.rs) | `min_applied_index: Some(5)` above read index 4; the `max` arm still dominates, so these survive |
| [`crates/rafter-app/tests/group_read.rs:569-616,621-660`](../crates/rafter-app/tests/group_read.rs) | Barrier at read index 4 with applied 2 must stay stalled |
| [`crates/rafter-app/tests/group_read_lifecycle.rs:295-389,579-624`](../crates/rafter-app/tests/group_read_lifecycle.rs) | `FreshnessUnavailable { required_applied_index: 4, local_applied_index: 2 }`, then granted after applying an application entry at exactly 4 |
| [`crates/rafter-app/tests/group_read_lifecycle.rs:141-215,218-293`](../crates/rafter-app/tests/group_read_lifecycle.rs) | Proof triples where required equals the read index |
| [`crates/rafter-service/tests/in_memory_read.rs:78-98,100-123`](../crates/rafter-service/tests/in_memory_read.rs) | `ScriptedReadMode::Grant(LogIndex(5))` with applied `ZERO` must still cancel |
| [`crates/rafter-app/tests/support/mod.rs:615-630`](../crates/rafter-app/tests/support/mod.rs) | `begin_pending_read_barrier` asserts the outcome is pending or freshness-stalled |

Most survive because the scripted fixtures already put an application entry at
the read index; each still has to be re-read and re-justified rather than
assumed, since "required equals read index" is now a coincidence of the fixture
rather than a rule.

Verification registry:

| File | Change |
| --- | --- |
| [`verification/raft-invariants.yaml:1658-1691`](../verification/raft-invariants.yaml) | Split `RD-04` into `RD-04.a` (dispatch floor; simulator evidence unchanged) and `RD-04.b` (application floor; new app-layer evidence plus the fixed maelstrom gate) |
| [`docs/raft-invariants.md`](./raft-invariants.md) | Regenerated by `scripts/render-raft-invariants-doc`; not hand-edited |

The `RD-04` restatement is a sharpening, not a weakening. `RD-04.a` keeps the
existing sentence for the layer where "local applied index" means the kernel
cursor, which is what the simulator and its negative fixture
`client_history_detects_completed_read_before_local_apply_floor` actually
measure — naming that quantity explicitly, since the ambiguity is why every
oracle missed the defect. `RD-04.b` states the app-layer obligation the current
wording made unsatisfiable. No TLA predicate is added, so the
`tla_predicates_now: 9` ratchet and the four `.cfg` invariant blocks are
untouched, and no catalog entry is added, so the `total_entries: 44` counts are
untouched too.

The three existing `RD-04` evidence rows stay where they are. The `lin-kv` row
binds `RD-04.a,RD-04.b` rather than being duplicated: one Maelstrom run
evidences both clauses, which is the same shape `RD-03` and `RD-06` already use,
and duplicating it would assert two runs that do not exist. That row is a
clause-bound record, so the reviewed Maelstrom receipt policy in
[`crates/rafter-invariants/src/verification/maelstrom/receipt/mod.rs:32-35`](../crates/rafter-invariants/src/verification/maelstrom/receipt/mod.rs)
counts twenty rather than nineteen; the scenario count stays at six. The
kernel's module-doc edit also rotates the reviewed detector-replay inventory
pin, since the `rafter` lib is one of the two replay targets and the fingerprint
covers file contents.

`rafter-multiraft` and `rafter-sim` need no change: the former only forwards
read events, and the latter measures the kernel cursor. Its `register_value_at`
already selects the highest applied entry at or below the read index
([`crates/rafter-sim/src/model_check/state/client.rs:134-149`](../crates/rafter-sim/src/model_check/state/client.rs))
— the same idea, reached independently, for choosing a value rather than for
gating.

The break is justified on the same ground as the method it generalizes: no
default is both safe and useful. The obvious default,
`min(committed_application_index(), index)`, is wrong; see
[Rejected alternatives](#rejected-alternatives-5).

### Focused-test plan

In `crates/rafter-app/tests/group_read.rs`, over a `ScriptedRuntime` carrying an
explicit application-entry index set:

- `read_barrier_grants_when_the_read_index_is_a_non_application_entry` — commit
  index 6 with application entries at 3 and 4 only, read index 6, state machine
  applied 4. Assert `ReadOutcome::Ready` with
  `ReadProof { read_index: 6, required_applied_index: 4, .. }`. This is the
  headline defect, and it fails today.
- `read_barrier_grants_on_a_cluster_that_has_committed_no_application_entry` —
  the fresh-cluster case: `Noop@1`, nothing else, applied `ZERO`. Assert the
  first read of a cluster's life answers.
- `read_barrier_does_not_require_an_application_entry_above_the_read_index` —
  read index 6, application entries at 4 and 7, commit index 7, applied 4.
  Assert granted at floor 4. This is the mixed-log case and the direct
  refutation of an uncapped floor.
- `read_barrier_floor_is_fixed_at_grant` — grant, then advance commit and
  compact, then assert every later `FreshnessUnavailable` reports the same
  `required_applied_index`.
- `read_barrier_honors_a_caller_supplied_floor_verbatim` — floor 3 from the log,
  `min_applied_index: Some(9)`; assert `required_applied_index == 9` and that no
  capping occurs.
- Negative: **`read_barrier_does_not_grant_while_an_application_entry_below_the_read_index_is_unapplied`**
  — the stale-read attempt the argument must survive. Application entries at 3
  and 5, `Noop@6`, read index 6, state machine applied 3. Assert
  `FreshnessUnavailable { required_applied_index: 5 }` and that the query is not
  served. Then apply 5 and assert the answer includes entry 5's effect. If the
  floor were ever computed as anything but the *highest* application entry in
  the cut, this test serves a state missing an acknowledged write.
- Negative: `read_barrier_floor_never_exceeds_the_read_index` — a
  property-style assertion over the scripted entry sets, so no future
  derivation can reintroduce a floor above the cut.
- Negative: `a_state_machine_that_skips_an_application_entry_poisons_before_a_read_can_grant`
  — assert the ordering guarantee that makes "highest" sufficient is enforced
  rather than assumed, via the existing `validate_apply_floor` path.

In `crates/rafter-runtime/src/tests/recovery.rs`, beside the existing
`committed_application_index_*` tests:

- `committed_application_index_through_ignores_entries_above_its_bound`.
- `committed_application_index_through_uses_the_snapshot_boundary_capped_at_its_bound`
  — both the boundary-above and boundary-below cases.
- `committed_application_index_equals_the_bounded_form_at_the_commit_index` —
  pins the provided method's definition against the required one.

In `crates/rafter-service/tests/in_memory_read.rs`:

- `managed_read_answers_after_an_election_without_an_intervening_write` — the
  production regression. Today the managed driver returns
  `ReadError::FreshnessUnavailable` here.

In `crates/rafter-maelstrom`, as `src/raft/read_tests.rs`:

- `a_read_granted_at_a_noop_index_flushes_without_a_later_apply` — the same
  regression at the kernel-direct gate.
- `a_stalled_read_is_reexamined_by_a_tick_with_no_apply_to_trigger_it` — the
  second half of that coverage, split out because it is a separate claim about
  the tick loop rather than about the floor.

In `reference/fenced-lock/tests/adapter_cluster.rs`:

- `a_linearizable_query_resolves_to_a_non_answer_when_leadership_is_lost` loses
  its pin and its idempotent command; see [After-state](#after-state-5).

### Rejected alternatives

- **Reuse `committed_application_index()` uncapped.** It is bounded at the
  commit index, and a barrier's read index is captured at registration and
  consumed arbitrarily later, so `commit_index > read_index` is normal. The
  read would wait on writes it is not ordered after, and the proof would report
  a `required_applied_index` above the `read_index` the quorum actually
  certified.
- **Default the new method to `min(committed_application_index(), index)`.**
  This is the shortcut that looks right and is not, which is exactly why the
  method must be required rather than provided. It happens to fix the headline
  case, and it fails on mixed logs: with application entries at 3 and 5 and
  `Noop@4`, bounding at 4 yields 4 — an index the state machine will never
  report — so the read waits for entry 5 instead of being served at 3. A
  defaulted implementor would inherit a subtler version of the defect being
  fixed.
- **`Output::AppliedNonApplication { index }` in the kernel (candidate b).**
  Designed and rejected on four grounds, in increasing order of weight. First,
  it is redundant: the app layer can already derive the same fact from a log
  the runtime already exposes, once per barrier, and the derivation is the one
  this document promoted a release earlier. Second, it fails open where the
  chosen design fails closed — the floor would advance from an output the
  caller might mishandle rather than from the state machine's own
  `applied_index()`, and `record_raft_output` runs before `apply_entries` in
  the same step
  ([`crates/rafter-app/src/group/output.rs:324-329`](../crates/rafter-app/src/group/output.rs)),
  so the counter leads the state it stands for and is safe only because a
  failed apply poisons the group first. Third, it fixes nothing for
  `min_applied_index`, for local reads, or for the `rafter-maelstrom` gate,
  each of which still needs an index-to-floor conversion. Fourth, the blast
  radius is large and partly silent. `Output` is deliberately closed — "This
  enum is exhaustive because node steps emit this closed set of side effects"
  ([`crates/rafter/src/node/event/output.rs:32-33`](../crates/rafter/src/node/event/output.rs))
  — and the proposed variant is the first that would announce the *absence* of a
  side effect. Adding it breaks 16 exhaustive matches across 13 files, including
  the deliberate tripwire in
  [`crates/rafter-runtime/src/tests/persistence_contract.rs:1-5,101-133`](../crates/rafter-runtime/src/tests/persistence_contract.rs)
  that forces every new output to be classified for persistence, and it passes
  silently through roughly ten wildcard arms — among them
  [`crates/rafter/tests/annotation_erasure.rs:85`](../crates/rafter/tests/annotation_erasure.rs),
  where an unclassified variant would weaken a Layer-0 invariant instead of
  failing a build. Two things do *not* count against it, and are recorded so the
  argument is not overstated: kernel `Output` is never wire-encoded — the codec
  handles `Message` only
  ([`crates/rafter-codec/src/lib.rs:18-19`](../crates/rafter-codec/src/lib.rs))
  — so no format changes, and the TLA mapping projects the simulator's input
  `Action` enum rather than kernel outputs
  ([`crates/rafter-sim/src/model_check/tla/projection.rs:47-55`](../crates/rafter-sim/src/model_check/tla/projection.rs)),
  so it would not change either.
- **A kernel accessor `Node::committed_application_index_through`.** Tempting,
  because it would place the derivation where the log lives and delete the
  duplicate scan. It is not worth a kernel addition: every consumer that needs
  it — `rafter-app`, `rafter-service`, `rafter-maelstrom`, both reference
  consumers, both benches — reaches the log through `DurableRaftNode`, so there
  is exactly one real implementation site. The only duplicate is
  `KernelRuntime`, a test fake, which already mirrors the unbounded scan
  through the same public `log_entries_slice_from` accessor.
- **Advance the state machine's applied index over non-application entries.**
  Either by having the app layer call a new state-machine hook, or by letting
  the group report a cursor its state machine did not. Both break the contract
  that makes the applied index recoverable: `apply_batch` promises effects
  through the highest returned index are durable
  ([`crates/rafter-app/src/state_machine.rs:8-15`](../crates/rafter-app/src/state_machine.rs)),
  and an index the state machine never persisted cannot survive restart. The
  floor belongs to the reader, not to the state machine.
- **Serve the read at the state as of the read index.** Would require the state
  machine to hold versioned history it has no contract to keep. The barrier is a
  lower bound on freshness, not a snapshot request; serving current state at or
  above the cut is already linearizable.

### After-state

`reference/fenced-lock` un-pins. The finding comment and the "fresh answer or no
answer" match at
[`tests/adapter_cluster.rs:403-422`](../reference/fenced-lock/tests/adapter_cluster.rs)
collapse into a direct assertion that the new leader answers the query with no
intervening write, and the idempotent `open_session` that follows it disappears.
The driver's freshness arm stops being a comment about waiting and becomes a
genuine transient.

`rafter-service` gains a behavior it never had: a linearizable read that is
issued after an election and before any write returns a result instead of
`ReadError::FreshnessUnavailable`. No signature changes there; the driver's
freshness path simply stops being reached in the common case.

`rafter-maelstrom` stops holding reads until an unrelated write arrives, and
its `RD-04` and `RD-06` evidence starts covering the case it was silently
failing.

`RD-04` says two true things where it previously said one thing that was true at
one layer and impossible at another, and
`RaftGroup::committed_application_index` becomes the zero-argument case of a
method that answers the same question at any index — one derivation serving both
readiness and freshness.

## Typed Service Failure Surface

### Origin

The fenced lock cannot tell a refusal from an unknown outcome, and it says so
at the line where it decides
([`reference/fenced-lock/src/adapter/client.rs:179-198`](../reference/fenced-lock/src/adapter/client.rs)):

```rust
/// Everything else falls through, including `UnknownOutcome` and the ambiguous
/// apply/storage/transport/poison errors, which carry only a formatted `String`
/// and so cannot be inspected to narrow the window further.
fn closes_outcome_window(error: &WriteError) -> bool {
    !matches!(
        error,
        WriteError::NotLeader { .. }
            | WriteError::Rejected { .. }
            | WriteError::PayloadTooLarge { .. }
            | WriteError::ShuttingDown
            | WriteError::LocalProposalIdExhausted
    )
}
```

An `Unknown` classification is not free. It obliges the caller to retry under
the *same* request identity and let the replicated session cache decide
([`client.rs:24-43`](../reference/fenced-lock/src/adapter/client.rs)), and it
costs the history checker a terminal fact it could otherwise have recorded
([`tests/support/cluster.rs:1309-1314`](../reference/fenced-lock/tests/support/cluster.rs)).
The window is wider than the facts require, and one case makes that concrete
rather than theoretical: both drivers in this repository report a write
addressed to the wrong group as a transport failure —
`ManagedOperationError::Transport("wrong group".to_owned())`
([`crates/rafter-service/src/driver/state.rs:41`](../crates/rafter-service/src/driver/state.rs))
and `WriteError::Transport { message: "wrong group" }`
([`cluster.rs:602-612`](../reference/fenced-lock/tests/support/cluster.rs)) —
so a command the driver never looked at is classified as one that may have
committed.

The consumer's own driver records where the type is destroyed
([`cluster.rs:1410-1428`](../reference/fenced-lock/tests/support/cluster.rs)):

```rust
/// `GroupError::StateMachine` carries this consumer's own typed
/// [`rafter_reference_fenced_lock::LockAdapterError`], but every `WriteError`
/// variant that could hold it takes a `String`, so the type is lost here. A
/// caller downstream of this driver can only read the rendered message.
```

`LockAdapterError` implements `source()`
([`reference/fenced-lock/src/adapter/mod.rs:122-131`](../reference/fenced-lock/src/adapter/mod.rs)).
So does `GroupError`, for both of its sources
([`crates/rafter-app/src/error.rs:175-200`](../crates/rafter-app/src/error.rs)).
The chain is intact right up to the service boundary, where every link is
replaced by a rendered string:

- `GroupError::StateMachine { operation, source }` becomes
  `format!("{source:?}")`, and the `operation` is folded away into one of two
  variants
  ([`crates/rafter-service/src/driver/mapping.rs:221-233`](../crates/rafter-service/src/driver/mapping.rs)).
- Three catch-alls render an entire `GroupError` with `format!("{error:?}")`
  ([`mapping.rs:237-240,272-275,288-291`](../crates/rafter-service/src/driver/mapping.rs)).
- `ManagedDriverError::Group { message: format!("{error:?}") }` does it a
  fourth time
  ([`mapping.rs:198`](../crates/rafter-service/src/driver/mapping.rs)).

Every `impl Error` in the crate's public error module leaves `source()`
defaulted: [`error.rs:126`](../crates/rafter-service/src/error.rs) (`WriteError`),
`:226` (`ReadError`), `:285` (`TransferLeadershipError`), `:304`
(`MetricsError`), `:325` (`ShutdownError`). Five public error types and fifteen
`String`-carrying variants across them — `WriteError` at `:74,77,80,84,88`,
`ReadError` at `:156,159,162,166,170`, `TransferLeadershipError` at
`:240,243,247`, `MetricsError::Transport` at `:292`, and
`ShutdownError::Transport` at `:310` — with 59 construction sites across the
workspace and the two reference consumers, tests included, plus six more of
`ManagedDriverError::Group`.

Rafter's own tests are the sharpest evidence, because they had nowhere else to
go. One matches on a substring
([`crates/rafter-service/tests/in_memory_read.rs:116-122`](../crates/rafter-service/tests/in_memory_read.rs)):

```rust
matches!(
    &error,
    ReadError::Transport { message } if message.contains("missing node")
)
```

and one pins the rendered `Display` of a `RaftRuntimeError` as an exact string
([`crates/rafter-service/tests/in_memory_write.rs:462-465`](../crates/rafter-service/tests/in_memory_write.rs)):

```rust
Err(WriteError::Storage {
    message: "persisted Raft log diverges from committed state at index 1".to_owned(),
})
```

That regression test asserts a message format. The fact it means to assert —
that a pre-append runtime failure is reported as a runtime failure and could
not have committed — is not expressible.

### Classification

Durable-lifecycle mechanism, following directly from a documented contract that
this crate is the only layer to break.

`PersistedRaftRuntime::Error` is bound to `Error + Send + Sync + 'static`, and
the doc comment states the reason in the terms this entry is about
([`crates/rafter-runtime-api/src/lib.rs:24-28`](../crates/rafter-runtime-api/src/lib.rs)):

```rust
/// Error returned when the runtime cannot step or query local persisted
/// state. Runtime errors are part of the public app/service error stack,
/// so implementations should expose typed errors rather than debug-only
/// strings.
type Error: Error + Send + Sync + 'static;
```

"The public app/service error stack" is named as a thing that exists and that
runtime implementors owe a typed error to. The service half of that stack then
renders those typed errors with `{error:?}` and drops them. Nothing new is
being invented here; a contract written two crates down is being honored.

The other half of the stack has no such bound at all.
`ReplicatedStateMachine::Error` is unconstrained
([`crates/rafter-app/src/state_machine.rs:21`](../crates/rafter-app/src/state_machine.rs)),
which has a consequence inside `rafter-app` that is worth stating on its own:
`impl Error for GroupError` requires `E: Error + 'static`
([`crates/rafter-app/src/error.rs:175-179`](../crates/rafter-app/src/error.rs)),
so `RaftGroup`'s public error type is a `std::error::Error` only for *some*
state machines. Six in-tree implementors declare `type Error = String` and
silently forfeit it. A public error type whose `Error` impl is conditional on a
downstream choice is not a surface a caller can build on.

The promotion rule requires a promoted API to "define resource bounds and typed
failure behavior"
([`docs/reference-consumers.md:392`](./reference-consumers.md)), and the 1.0
production composition requires "structured metrics and failure diagnostics"
([`:360`](./reference-consumers.md)). Neither is reachable from a `String`: a
metrics label taken from a rendered message has unbounded cardinality, because
these messages embed node IDs, indices, and proposal IDs.

Second plausible consumer: the sharded counter, whose contract requires that
"work and failure in one group do not corrupt another" and that a poisoned
group be attributable
([`docs/reference-consumers.md:332-338`](./reference-consumers.md)). The same
defect is already present one crate over, written independently:
`GroupDriver::step` returns `Result<_, String>`
([`crates/rafter-multiraft/src/driver.rs:17-20`](../crates/rafter-multiraft/src/driver.rs))
and `MultiRaftError::Driver { group_id, message: String }`
([`crates/rafter-multiraft/src/error.rs:13`](../crates/rafter-multiraft/src/error.rs))
carries it. That crate is out of scope here — no consumer has exercised it yet,
and the promotion rule defers extraction until one does — but it is the
independent second occurrence the rule asks for.

**That deferral is spent.** A cold audit of `rafter-multiraft` reproduced the
consequence this paragraph predicted, and the reshaping is designed in
[Many-Group Tick Passes, Group Retirement, and a Host Error That Renders](#many-group-tick-passes-group-retirement-and-a-host-error-that-renders).
It reaches the same three answers by the same route, which is the strongest
evidence this section's design was right that the workspace has produced.

### Design

A failed write answers three different questions, and today one `String` is
asked to answer all of them:

| Question | Asked by | Answer |
| --- | --- | --- |
| What kind of failure was this? | a metrics label, a log field, an alert | the variant, projected to a `Copy` category |
| May the command still take effect? | a client deciding whether to retry | a reported fate, never an inference |
| What actually failed? | an operator reading a diagnostic | the typed error, with a real `source()` chain |

The design separates them, and the separation is the whole entry. The three
answers have different types, different lifetimes, and different audiences: the
category is a bounded label, the fate is a two-valued fact the driver observed,
and the cause is an opaque typed error. Collapsing any two of them back
together reproduces some part of the defect.

#### The preserved cause

In `crates/rafter-app/src/error.rs`, beside `GroupError`, because both the app
layer and the service layer need it and `rafter-app` is the lower of the two:

```rust
/// A typed error preserved across a layer boundary.
///
/// A Rafter error names a stable category; the cause names what actually
/// failed. Both are needed: the category is what a caller branches on and what
/// a metric labels, and the cause is what an operator reads. Rendering the
/// cause into the category's message loses the second and does not improve the
/// first.
///
/// The cause is shared rather than owned because one failure fans out to every
/// entry of a write batch, and a `Box<dyn Error>` cannot be cloned. It is
/// type-erased rather than a type parameter because the boundary it crosses is
/// a client boundary: a driver that reaches its group over a network holds its
/// own transport error, not the leader's application error, and a client type
/// parameterized over the leader's error type would be a promise no networked
/// driver can keep.
///
/// This type is deliberately not itself a [`std::error::Error`]. It is a
/// handle, and it is transparent to `source()`: an error carrying a cause
/// returns the *inner* error from its own `source()`, so a chain printer walks
/// one link per real failure rather than one per boundary crossed.
#[derive(Clone)]
pub struct ErrorCause(Arc<dyn Error + Send + Sync + 'static>);

impl ErrorCause {
    /// Preserves `error` as the cause of a Rafter error.
    #[must_use]
    pub fn new<E>(error: E) -> Self
    where
        E: Error + Send + Sync + 'static;

    /// Returns the preserved error.
    #[must_use]
    pub fn as_error(&self) -> &(dyn Error + Send + Sync + 'static);

    /// Preserves an already-shared `error` as the cause of a Rafter error.
    ///
    /// The constructor for a failure with two owners; see the note on
    /// [the poison cause](#the-poison-cause-one-layer-down).
    #[must_use]
    pub fn from_shared<E>(error: Arc<E>) -> Self
    where
        E: Error + Send + Sync + 'static;

    /// Returns the preserved error when it is of type `E`.
    ///
    /// An embedder whose own state machine or runtime produced the failure
    /// recovers its exact type here, which is what makes a typed recovery path
    /// writable. A caller on the far side of a transport recovers whatever
    /// *that* driver preserved, which is that driver's error and not the
    /// leader's — a cause is preserved across one boundary, not serialized
    /// across the network.
    #[must_use]
    pub fn downcast_ref<E>(&self) -> Option<&E>
    where
        E: Error + 'static;
}

impl fmt::Debug for ErrorCause;   // forwards to the preserved error
impl fmt::Display for ErrorCause; // forwards to the preserved error
```

`rafter-service` re-exports it from `crate::error` and from the crate root.

#### The reported fate

In `crates/rafter-service/src/error.rs`:

```rust
/// What a failed managed write proves about the command's fate.
///
/// This is the retry question, and it is the only part of a write error a
/// client may branch on when deciding whether a request identity is still
/// unused. It is separate from the error's category because the two answer
/// different questions: a storage failure before the local append and a
/// storage failure after it are the same fault and different facts.
///
/// A driver reports the fate it observed. It never infers one from a category,
/// and a caller must not either — the category says what broke, not when.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum WriteFate {
    /// The command was refused before it reached the local Raft log. It cannot
    /// commit, now or later, and its request identity is still unused.
    ///
    /// A driver reports this only when it observed the refusal itself.
    NotAppended,
    /// The command may or may not commit and apply.
    ///
    /// Retry only under the same request identity, and only with
    /// application-level idempotency if duplicate effects matter. A driver that
    /// cannot prove [`WriteFate::NotAppended`] reports this.
    Unresolved,
}

impl WriteFate {
    /// Returns whether the command may still take effect.
    ///
    /// Written as the negation of [`WriteFate::NotAppended`] so a future
    /// variant reads as unresolved until a caller is updated to interpret it.
    /// This is the safe direction, and it is the only direction this enum is
    /// meant to be tested in.
    #[must_use]
    pub const fn may_commit(self) -> bool {
        !matches!(self, Self::NotAppended)
    }
}
```

```rust
impl WriteError {
    /// Returns what this error proves about the command's fate.
    ///
    /// Variants that describe a refusal — not leader, rejected, payload too
    /// large, shutting down, wrong group, exhausted local IDs — answer
    /// [`WriteFate::NotAppended`] from the variant alone, because reaching them
    /// is the proof. [`WriteError::UnknownOutcome`] answers
    /// [`WriteFate::Unresolved`] for the same reason. The remaining variants
    /// carry the fate the driver observed, because the same fault can occur on
    /// either side of the local append.
    #[must_use]
    pub const fn fate(&self) -> WriteFate;
}
```

Reads get no fate, and the asymmetry is the point: a read that fails takes no
effect, so there is no later outcome for a client to be uncertain about. This
is why the entry is not "add a fate field to every service error".

#### The category projection

```rust
/// Stable category of a [`WriteError`].
///
/// This is the low-cardinality projection of the error: `Copy`, totally
/// ordered, hashable, and free of payload, so it can be a metric label, a map
/// key, or a structured-log field. The variants themselves carry indices, node
/// IDs, and messages, so neither `Display` nor `Debug` is bounded enough to
/// label with.
///
/// New categories are additive. A caller that aggregates by kind must keep a
/// bucket for kinds it does not recognize rather than dropping them.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum WriteErrorKind {
    NotLeader,
    Rejected,
    PayloadTooLarge,
    UnknownOutcome,
    WrongGroup,
    StateMachine,
    Storage,
    Transport,
    ShuttingDown,
    Poisoned,
    LocalProposalIdExhausted,
    ManagedInvariantViolation,
}

impl WriteError {
    /// Returns this error's stable category.
    #[must_use]
    pub const fn kind(&self) -> WriteErrorKind;
}
```

`ReadErrorKind` and `ReadError::kind` are the same shape.
`TransferLeadershipError` and `ShutdownError` get no projection: they are
control-plane operations a caller performs one at a time and does not
aggregate, and the promotion rule extracts repeated plumbing after a second
consumer shows the same shape, not before.

#### The reshaped variants

```rust
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum WriteError {
    NotLeader { leader_hint: Option<NodeId>, term: Term },
    Rejected { reason: ProposalRejection },
    PayloadTooLarge { max: usize, actual: usize },
    /// The operation may or may not have committed and applied.
    UnknownOutcome {
        local_proposal_id: LocalProposalId,
        client_request_id: Option<ClientRequestId>,
        reason: UnknownOutcomeReason,
    },
    /// The request named a group this driver does not own.
    ///
    /// The command was never handed to a group, so its request identity is
    /// still unused. This was previously reported as a transport failure,
    /// which is both the wrong category and the wrong fate.
    WrongGroup,
    /// The application state machine failed.
    ///
    /// `operation` is the callback that surfaced the failure, and it is
    /// load-bearing: encoding a command, reading an applied index, and applying
    /// a batch fail for unrelated reasons and at unrelated moments.
    StateMachine {
        operation: StateMachineOperation,
        fate: WriteFate,
        cause: ErrorCause,
    },
    /// The Raft runtime failed to persist or query local durable state.
    Storage { fate: WriteFate, cause: ErrorCause },
    /// The driver could not route or deliver the work this write required.
    Transport { fate: WriteFate, cause: ErrorCause },
    ShuttingDown,
    /// The group is permanently poisoned.
    ///
    /// `cause` is the error that poisoned the group, when the poison came from
    /// a typed failure. It is `None` for a poison with no underlying error,
    /// such as a malformed snapshot output.
    Poisoned {
        fate: WriteFate,
        reason: String,
        cause: Option<ErrorCause>,
    },
    LocalProposalIdExhausted,
    /// The driver violated one of its own documented invariants.
    ///
    /// This is the one variant whose message is authored rather than rendered:
    /// a driver reporting its own bug has no underlying error to preserve.
    ManagedInvariantViolation { fate: WriteFate, message: String },
}
```

`ApplyFailed` is gone, replaced by `StateMachine { operation, .. }`. The old
mapping folded six `StateMachineOperation` values into two variants and got one
of them wrong: `EncodeCommand` and `AppliedIndex` were reported as
`WriteError::Storage`
([`mapping.rs:228-232`](../crates/rafter-service/src/driver/mapping.rs)), and
encoding a command touches no storage.

`ReadError` takes the same treatment without the fate:

```rust
    WrongGroup,
    StateMachine { operation: StateMachineOperation, cause: ErrorCause },
    Storage { cause: ErrorCause },
    Transport { cause: ErrorCause },
    Poisoned { reason: String, cause: Option<ErrorCause> },
```

`TransferLeadershipError` gains `WrongGroup` and takes `Storage { cause }`,
`Transport { cause }`, and `Poisoned { reason, cause }`.

`ShutdownError` gains `WrongGroup` and keeps `Transport { cause }`. Both
in-tree drivers currently report a wrong-group shutdown as a transport failure
([`crates/rafter-service/src/driver/in_memory.rs:196-199`](../crates/rafter-service/src/driver/in_memory.rs),
[`cluster.rs:588-591`](../reference/fenced-lock/tests/support/cluster.rs)),
which is the same misclassification as on the write path.

`MetricsError::Transport` is deleted. It has zero construction sites anywhere
in the workspace or in either reference consumer; converting it would add a
cause field no code can set. `MetricsError` therefore keeps
`#[derive(Clone, Copy, Debug, Eq, PartialEq)]`, since after the deletion it
carries no cause.

#### The state-machine error bound

```rust
pub trait ReplicatedStateMachine {
    /// Error returned when this state machine cannot encode, decode, apply,
    /// read, or snapshot.
    ///
    /// Application errors are part of the public app/service error stack, so
    /// implementations expose typed errors rather than debug-only strings —
    /// the same contract
    /// [`rafter_runtime_api::PersistedRaftRuntime::Error`] already states for
    /// the other half of that stack. Without the bound,
    /// [`crate::error::GroupError`] is a [`std::error::Error`] for some state
    /// machines and not others, and every layer above has to render rather than
    /// preserve.
    ///
    /// `Send + Sync` is required because a managed service resolves a client
    /// waiter on a different task from the one that stepped the group.
    type Error: Error + Send + Sync + 'static;
    // ...
}
```

#### The poison cause, one layer down

`WriteError::Poisoned` is the one variant that cannot be repaired inside
`rafter-service`, because the cause is already gone before the service layer
sees it. `poison_with_state_machine_error` receives the typed source and stores
`format!("{operation:?} failed")`
([`crates/rafter-app/src/group/poison.rs:22-28,40`](../crates/rafter-app/src/group/poison.rs)),
so every later error on that group reports the string `"ApplyBatch failed"` and
nothing else. The group retains the cause beside the health state:

```rust
impl<G, A, R> RaftGroup<G, A, R> {
    /// Returns the error that poisoned this group, if it is poisoned and the
    /// poison came from a typed failure.
    ///
    /// [`GroupFatalState`] says *that* a group is poisoned and is published in
    /// every metrics snapshot, so it stays a plain comparable value. The cause
    /// is a diagnostic held beside it and is never published: a metrics
    /// snapshot is cloned and compared on every step and must not carry a
    /// `dyn Error`.
    #[must_use]
    pub fn poison_cause(&self) -> Option<&ErrorCause>;
}
```

`GroupError::Poisoned` gains `cause: Option<ErrorCause>`, and
`RaftGroupParts` gains `poison_cause: Option<ErrorCause>` so decomposition
stays lossless. Both types derive `Debug` only, so neither addition costs a
trait impl.

**One failure, two owners.** `poison_with_state_machine_error` receives the
state machine's error once and has to place it twice: in the group's poison
slot, and in the `GroupError::StateMachine` it returns — which the focused test
below and every existing apply-failure assertion require to keep carrying the
typed source. `ReplicatedStateMachine::Error` is deliberately not `Clone`
(forcing `Clone` on it is a
[rejected alternative](#rejected-alternatives-6) in this very entry), so the two
owners share one allocation: `GroupError::StateMachine` holds
`source: Arc<E>` and the group holds an `ErrorCause` built from the same `Arc`
through `ErrorCause::from_shared`. The alternative — dropping the typed source
from the returned error, or leaving `poison_cause` permanently `None` for the
only poison path that has a typed error — would make `RaftGroup::poison_cause`
dead API. The six construction sites in `group/proposal.rs` and `group/read.rs`
that do *not* poison pay one allocation on an error path, which is the whole
cost.

### Semantics and edge cases

- **Equality is removed, deliberately, and not replaced.** `WriteError`,
  `ReadError`, `TransferLeadershipError`, and `ShutdownError` drop `Eq` and
  `PartialEq`. An error carrying a `dyn Error` has no honest equality, and the
  two dishonest ones are worse than none: comparing `Arc` pointers makes two
  errors built from the same failure unequal, and comparing rendered `Debug` or
  `Display` output rebuilds, inside the comparison operator, exactly the
  stringly-typed semantics this entry deletes. `Clone` is kept, which is what
  the batch path actually needs. `GroupError` and `ManagedOperationError` are
  already `Debug`-only, so the layer below sets the precedent.
- **Why `Arc` and not `Box`.** `repeat_write_error`
  ([`crates/rafter-service/src/driver/write.rs:356-361`](../crates/rafter-service/src/driver/write.rs))
  turns one failure into one error per batch entry, and
  `complete_unresolved_writes` clones a `WriteError` per unresolved state
  ([`:200-203`](../crates/rafter-service/src/driver/write.rs)). A `Box<dyn Error>`
  cannot do that.
- **Fate is reported, never inferred.** The driver already tracks the
  observation: `BatchWriteState::saw_local_append`
  ([`write.rs:8`](../crates/rafter-service/src/driver/write.rs)) is set from
  `ProposalEvent::Appended` (`:268-270`) and is read on exactly one path today,
  to choose `UnknownOutcomeReason::PostAppendDriverError` (`:109`). Every other
  failure path discards it — most visibly `finish_failed_write_batch`, which
  clones one mapped error across every state without consulting it
  (`:200-203`). After this change that field feeds `fate` on every path, which
  is why the change is a plumbing fix rather than new bookkeeping.
- **An observed append is unresolved, and that is the truth rather than
  caution.** An entry that reached the local log and then hit a failure is on
  disk. A node reopened over the same durable log can still replicate and commit
  it under a later incarnation, so `NotAppended` is not merely risky there — it
  is unprovable. The symmetric half is what makes `NotAppended` reportable at
  all: a group step report is the complete record of its step, so an entry the
  driver never saw appended, in a step whose report it did see, was not
  appended. Both halves are the driver reporting an observation; neither is a
  margin of safety added on top of one.
- **Non-exhaustive policy.** The error enums stay `#[non_exhaustive]` at the
  enum level and their variants stay constructible, which is the policy already
  settled under
  [Leader Hint on Proposal Rejection](#leader-hint-on-proposal-rejection):
  downstream tests and drivers build expected values, and new variants are the
  more common evolution. `WriteFate`, `WriteErrorKind`, `ReadErrorKind`, and
  `UnknownOutcomeReason` are `#[non_exhaustive]` for the same reason, and each
  documents its safe default — unresolved for a fate, an unrecognized bucket
  for a kind. The variants that gain fields break every exhaustive destructure
  that does not already use `..`; that is unavoidable and is the smaller half of
  this entry's break.
- **A cause crosses one boundary.** `ErrorCause::downcast_ref` recovers the
  error the driver preserved. For an in-process driver that is the state
  machine's or the runtime's own error. For a driver that reaches its group over
  a network it is that driver's error, because nothing serializes a
  `dyn Error`. The doc comment says so rather than leaving a caller to discover
  it.
- **`source()` is transparent.** `WriteError::source` returns the preserved
  error, not the `ErrorCause` wrapper, so a chain printer shows one link per
  real failure. This is the reason `ErrorCause` is not itself an `Error`.
- **`Display` does not repeat the cause.** Each variant's `Display` states the
  category and the typed fields; the cause is reached through `source()`. A
  `Display` that interpolated the cause would reproduce today's message in a
  place a caller cannot parse and a chain printer would print twice.
- **`ManagedInvariantViolation` keeps its `String`.** It is a driver reporting
  its own broken invariant, with no underlying error. It gains a fate, because
  some of those violations are provably pre-append — the non-monotonic local-ID
  check at
  [`mapping.rs:211-220`](../crates/rafter-service/src/driver/mapping.rs)
  refuses before the group proposes.
- **`ManagedDriverError::Group` carries a cause, not a message, and the type
  loses its equality with it.** The rule that no free-text message field remains
  in this surface applies to the driver's construction error too: the category
  is the variant and the detail is the preserved cause. Carrying an
  `ErrorCause` costs `ManagedDriverError` its `Eq`/`PartialEq` derives for
  exactly the reason the four client-facing errors lose theirs, so its in-tree
  assertions move to `matches!` in the same pass.
- **The driver authors two errors about itself.** `ManagedOperationError` gains
  `WrongGroup` and `DriveBoundReached` so a driver fact stops being smuggled
  through `Transport(String)`, and a private `DriverRoutingError` is the typed
  cause the driver preserves when the failure is its own routing rather than
  somebody else's fault. It is the one place this layer constructs an error
  object instead of preserving one, which is the same licence
  `ManagedInvariantViolation` already has.
- **`into_write_error` takes the fate as a parameter.** The mapper knows the
  category; only its caller knows which side of the local append the failure was
  on. Passing it in is what keeps the fate reported rather than inferred, and it
  is why a batch can restamp one mapped error with a different fate per entry.
- **`TransferLeadershipError::NotLeader` is left alone.** It has zero
  construction sites, like `MetricsError::Transport`, but it is not one of the
  variants this entry has to convert, and deleting it is a break with no
  evidence behind it. Recorded here so the next API-review pass finds it.
- **`StateMachineOperation` becomes part of the service surface.** It is
  already public in `rafter-app`
  ([`crates/rafter-app/src/error.rs:14-21`](../crates/rafter-app/src/error.rs))
  and already imported by the driver
  ([`crates/rafter-service/src/driver/mod.rs:11`](../crates/rafter-service/src/driver/mod.rs));
  it now appears in a public `rafter-service` signature and must be re-exported
  from that crate's root under the rule in
  [Driver Boundary Re-exports](#driver-boundary-re-exports).
- **The bound does not reach the kernel.** `rafter` and `rafter-runtime-api`
  are untouched. `rafter-runtime-api` already carries the stricter of the two
  bounds.

### Blast radius

Breaking at three layers: a trait bound in `rafter-app`, field and variant
changes in `rafter-service`, and the loss of `Eq` on four public error types.

| File | Change |
| --- | --- |
| [`crates/rafter-app/src/error.rs`](../crates/rafter-app/src/error.rs) | Add `ErrorCause`; `GroupError::Poisoned` gains `cause`; `GroupError::StateMachine` shares its `source` as `Arc<E>` |
| [`crates/rafter-app/src/state_machine.rs:21`](../crates/rafter-app/src/state_machine.rs) | `type Error: Error + Send + Sync + 'static` |
| [`crates/rafter-app/src/group/poison.rs:22-40`](../crates/rafter-app/src/group/poison.rs) | Retain the cause when entering poison |
| [`crates/rafter-app/src/group/types.rs:9-16,204-215`](../crates/rafter-app/src/group/types.rs) | `RaftGroupParts.poison_cause`; `RaftGroup::poison_cause` |
| [`crates/rafter-service/src/error.rs`](../crates/rafter-service/src/error.rs) | The whole module: variants, `fate`, `kind`, `source`, derives |
| [`crates/rafter-service/src/driver/mapping.rs:198,204-292`](../crates/rafter-service/src/driver/mapping.rs) | Preserve instead of render; carry `operation`; thread `fate` |
| [`crates/rafter-service/src/driver/write.rs:98-118,173-206`](../crates/rafter-service/src/driver/write.rs) | `saw_local_append` feeds `fate` on every failure path |
| [`crates/rafter-service/src/driver/state.rs:36-44,90-101`](../crates/rafter-service/src/driver/state.rs) | `WrongGroup` instead of `Transport("wrong group")`; poison cause |
| [`crates/rafter-service/src/driver/read.rs:60,129,183,217`](../crates/rafter-service/src/driver/read.rs) | Reshaped read errors |
| [`crates/rafter-service/src/driver/in_memory.rs:196-199`](../crates/rafter-service/src/driver/in_memory.rs) | `ShutdownError::WrongGroup` |
| [`crates/rafter-service/src/lib.rs:30-33`](../crates/rafter-service/src/lib.rs) | Re-export `ErrorCause`, `WriteFate`, `WriteErrorKind`, `ReadErrorKind`, `StateMachineOperation` |

Every implementor of `ReplicatedStateMachine` that declares
`type Error = String` must declare a typed error. There are six, all examples
and in-tree fakes:

| File | Type |
| --- | --- |
| [`crates/rafter-app/tests/support/mod.rs:68`](../crates/rafter-app/tests/support/mod.rs) | `RecordingStateMachine` |
| [`crates/rafter-app/examples/replicated_kv_manual.rs:288`](../crates/rafter-app/examples/replicated_kv_manual.rs) | `KvStateMachine` |
| [`crates/rafter-app/examples/snapshot_install.rs:175`](../crates/rafter-app/examples/snapshot_install.rs) | `KvStateMachine` |
| [`crates/rafter-multiraft/examples/real_raft_groups.rs:260`](../crates/rafter-multiraft/examples/real_raft_groups.rs) | `KvStateMachine` |
| [`crates/rafter-service/examples/replicated_kv_service.rs:96`](../crates/rafter-service/examples/replicated_kv_service.rs) | `KvStateMachine` |
| [`crates/rafter-service/tests/support/mod.rs:161`](../crates/rafter-service/tests/support/mod.rs) | `KvStateMachine` |

The four remaining implementors already satisfy it: `LedgerAdapterError`,
`LockAdapterError`, and both `BenchStateMachineError`s implement
`std::error::Error`. That the reference consumers and the benchmarks all
independently chose typed errors, while every example chose `String`, is itself
the finding: the bound codifies what a real consumer already does.

Test and consumer sites that assert on whole errors:

| File | Change |
| --- | --- |
| [`crates/rafter-service/tests/in_memory_write.rs:21,52,152-153,198,218,380,391,415,439,463,485`](../crates/rafter-service/tests/in_memory_write.rs) | 12 assertions move from `assert_eq!` to `matches!` plus typed field checks; `:463` becomes a downcast |
| [`crates/rafter-service/tests/in_memory_read.rs`](../crates/rafter-service/tests/in_memory_read.rs) | 9 assertions, same treatment; `:119` stops matching a substring and `:160` stops pinning a message |
| [`crates/rafter-service/tests/adoption.rs:107`](../crates/rafter-service/tests/adoption.rs), [`tests/metrics.rs:41,45`](../crates/rafter-service/tests/metrics.rs), [`tests/transfer.rs:30,44,58`](../crates/rafter-service/tests/transfer.rs) | Reshaped expectations |
| [`crates/rafter-service/src/handle/tests.rs:205-260,324-356,404-420`](../crates/rafter-service/src/handle/tests.rs) | The mock sender builds errors and the tests compare them |
| [`crates/rafter-service/src/error.rs:345-381`](../crates/rafter-service/src/error.rs) | Unit tests, extended below |
| [`reference/fenced-lock/src/adapter/client.rs:31-43,67-77,179-198`](../reference/fenced-lock/src/adapter/client.rs) | `SubmitOutcome` and `QueryOutcome` lose their `Eq`/`PartialEq` derives; `closes_outcome_window` collapses |
| [`reference/fenced-lock/tests/support/cluster.rs:1393-1478`](../reference/fenced-lock/tests/support/cluster.rs) | The four `*_error_from_group` mappers; deleted outright by the next entry |

`reference/` is outside the root workspace and `bench-compare/` is not a
member, so `cargo check --workspace` covers neither; both must be built for
this step. `bench-compare` needs no source change — its state machines already
declare typed errors — but it must be built to prove it.

The break is justified on the ground the contract already states. There is no
additive path: a `String` field cannot be widened, and a parallel `cause` field
beside a retained `message` would give a caller two representations of one fact
and leave the string as the one that is easiest to match on.

### Focused-test plan

In `crates/rafter-service/src/error.rs`:

- `write_error_exposes_the_preserved_error_as_its_source` — build a
  `WriteError::Storage` around a `RaftRuntimeError`, walk `source()`, assert the
  runtime error is reached in one link.
- `a_preserved_cause_downcasts_to_the_error_the_driver_kept`.
- `display_states_the_category_without_repeating_the_cause` — assert the
  rendered message does not contain the cause's `Display`, so a chain printer
  does not print it twice.
- `every_write_error_kind_is_distinct_from_every_other` and
  `fate_is_not_appended_for_every_refusal_variant` — property-style, so a new
  variant cannot silently join the wrong side.

In `crates/rafter-service/tests/in_memory_write.rs`:

- `a_pre_append_runtime_error_preserves_the_runtime_error_and_reports_not_appended`
  — the replacement for the string pin at `:463`. Downcast to
  `RaftRuntimeError`, assert the variant, assert
  `fate() == WriteFate::NotAppended`.
- `a_write_for_the_wrong_group_is_not_appended` — the case both drivers get
  wrong today.
- `a_state_machine_failure_reaches_the_client_as_its_own_type` — a `RaftGroup`
  over a state machine with a typed error; assert the client downcasts to that
  exact type through `WriteError`. This is the two-boundary case, and it is the
  one the bound exists for.
- `a_state_machine_error_reports_the_operation_that_surfaced_it` — an
  `EncodeCommand` failure is `StateMachine { operation: EncodeCommand, .. }`,
  not `Storage`.
- Negative: **`an_apply_failure_after_a_local_append_is_not_reported_as_not_appended`**
  — the safety case. A command that appended and then hit an apply failure must
  report `Unresolved`; a driver that derived the fate from the category would
  fail this.
- Negative: `a_batch_failure_reports_a_per_entry_fate` — one entry appended, one
  not, one failure; assert the two entries get different fates. The old code
  cloned one error across the batch.

In `crates/rafter-service/tests/in_memory_read.rs`:

- `an_unroutable_read_step_reports_a_typed_transport_cause` — the replacement
  for the substring match at `:119`.

In `crates/rafter-app/tests/group_lifecycle.rs`:

- `a_poisoned_group_reports_the_error_that_poisoned_it`, and the same value
  arriving in `RaftGroupParts::poison_cause`.
- Negative: `a_group_poisoned_by_a_malformed_snapshot_reports_no_cause` — the
  `Option` is not decoration; `poison_with_malformed_snapshot`
  ([`crates/rafter-app/src/group/poison.rs:31-37`](../crates/rafter-app/src/group/poison.rs))
  has no typed source and must not invent one.
- Negative: `a_metrics_snapshot_does_not_carry_the_poison_cause` — pins the
  split that keeps `RaftGroupMetrics` comparable.

### Rejected alternatives

- **Parameterize the service errors over `A::Error` and `R::Error`.** The
  finding's first option, and the one that looks most faithful. It fails at the
  boundary it has to cross. `DriverCommandSender` and `RaftHandle` are the
  *client* surface, and the crate documents drivers that are not backed by a
  local group at all: "Production transports can implement
  [`DriverCommandSender`] directly"
  ([`crates/rafter-service/src/driver/in_memory.rs:9-11`](../crates/rafter-service/src/driver/in_memory.rs)).
  A client type parameterized over the leader's application error names a value
  a remote client will never hold. It also forces `Clone` onto `A::Error`, which
  the batch path requires and which `GroupError` deliberately does not demand,
  and it adds two parameters to a handle that already carries six. The typed
  recovery it would buy is available anyway through
  `ErrorCause::downcast_ref` for the in-process case, which is the only case
  where the type exists.
- **`Box<dyn Error + Send + Sync>` with no sharing.** The right erasure, the
  wrong container: a write batch fans one failure out to every entry, and a
  boxed trait object cannot be cloned. `Arc` is the same erasure that survives
  the fan-out.
- **Keep `Eq` by comparing rendered `Display` or `Debug` output.** This would
  keep every existing assertion compiling and would make the comparison
  operator itself string-typed — the defect, moved into `PartialEq`, where it is
  harder to see. Two distinct error types whose `Debug` output coincides would
  compare equal.
- **Add `cause` beside the existing `message`.** Non-breaking, and it leaves the
  string as the field a caller reaches for first, so nothing that matches on
  messages today has any reason to stop.
- **Put the `Error + Send + Sync` bound only on `rafter-service`'s impl
  blocks.** Half the break — two `type Error = String` sites instead of six —
  and it leaves `GroupError`'s `Error` impl conditional on a downstream choice,
  so `rafter-app`'s own public error type stays a `std::error::Error` only
  sometimes. The bound belongs where the error type is declared.
- **A `retryable: bool` field.** Conflates two different questions: whether a
  retry is *safe* (the fate) and whether it is *likely to help* (the category).
  A caller that retries an unsafe operation because a boolean said "retryable"
  has been misled by the API.
- **Make `ErrorCause` implement `Error`.** Adds a link to every chain that
  renders identically to the link below it.
- **Put `ErrorCause` in `rafter-runtime-api`.** That crate "owns only the
  application-facing runtime contract"
  ([`crates/rafter-runtime-api/src/lib.rs:3-5`](../crates/rafter-runtime-api/src/lib.rs))
  and needs no cause type of its own. `rafter-app::error` is the lowest crate
  with a type that has to carry one.

### After-state

`closes_outcome_window` and its explanatory comment collapse into the question
it was always trying to ask
([`reference/fenced-lock/src/adapter/client.rs:179-198`](../reference/fenced-lock/src/adapter/client.rs)):

```rust
fn closes_outcome_window(error: &WriteError) -> bool {
    error.fate().may_commit()
}
```

That is the acceptance evidence for this entry, and it is checkable rather than
rhetorical: every write the driver observed as refused moves from
`SubmitOutcome::Unknown` to `SubmitOutcome::Refused`, which means the lock's
history records a terminal refusal where it previously recorded an unknown, and
a retrying client stops burning a request identity it never used.
`write_error_from_group` and its comment about a lost type disappear with the
rest of the consumer's driver in the next entry; until then the type survives
the mapping. Both have since happened: the mappers are gone with the driver,
and `closes_outcome_window` is the two-line body above in the tree.

**Corrected during adoption.** This paragraph named "a wrong-group write, a
payload-too-large write, and a pre-append runtime failure" as the three cases
that move. `PayloadTooLarge` is not one of them: it is in the old enumeration's
own refusal list, so it was already a refusal and moves nothing. The cases that
actually move are `WriteError::WrongGroup`, which the old list could not name
because the variant did not exist, and every fate-carrying variant the driver
observed before the local append — `Storage`, `Transport`, `StateMachine`,
`Poisoned`, and `ManagedInvariantViolation` with `WriteFate::NotAppended`. That
last group is the real width of the change: it is the set the old comment
described as carrying "only a formatted `String`" and therefore uninspectable,
and it is now five variants rather than one. The lock pins the resulting
equivalence — a refusal is recorded as `HistoryEvent::NotCommitted` exactly
when the fate is `NotAppended` — in one test over one representative error per
`WriteErrorKind`, so the client's classification and the history's terminal
vocabulary cannot drift apart.

`rafter-service` stops rendering. `mapping.rs` becomes a category assignment
plus a `fate`, with the `GroupError` moved into an `ErrorCause` rather than
formatted, and `ManagedDriverError::Group` carries the same. The two tests that
matched on message text assert on types. And `rafter-app` reports a poisoned
group's actual cause rather than the string `"ApplyBatch failed"`.

## Transport-Attached Group Driver

### Origin

`rafter-service` ships a transport contract and no code that uses it.
`RaftTransport` and `AsyncRaftTransport`
([`crates/rafter-service/src/transport.rs:73-104,117-147`](../crates/rafter-service/src/transport.rs))
carry sixty lines of delivery semantics — drops, duplicates, reordering,
bounded queues, what a successful `send` does and does not mean — and
`validate_inbound_peer_envelope` (`:158-167`) is the documented gate an inbound
frame passes before a group sees it. Neither trait is named as a bound anywhere
in `crates/`. Their only mentions outside their own module are the crate-root
re-exports at
[`lib.rs:37-41`](../crates/rafter-service/src/lib.rs) and one implementor: a
reference consumer's test support
([`reference/fenced-lock/tests/support/transport.rs:223`](../reference/fenced-lock/tests/support/transport.rs)).
The workspace's own concrete transport does not implement them —
`InsecureTcpTransport` exposes `send(&self, peer: NodeId, message: &Message)`
and `receive()`
([`crates/rafter-transport-tcp-insecure/src/lib.rs:353,389`](../crates/rafter-transport-tcp-insecure/src/lib.rs))
and never mentions `RaftTransport`.

The shipped driver cannot be that seam. `InMemoryRaftDriver` owns every replica
in a private map and moves frames through a private queue
([`crates/rafter-service/src/driver/state.rs:5-15`](../crates/rafter-service/src/driver/state.rs)):

```rust
pub(super) struct InMemoryRaftState<G, A, R> {
    pub(super) group_id: G,
    pub(super) primary_node_id: NodeId,
    pub(super) groups: BTreeMap<NodeId, RaftGroup<G, A, R>>,
    pub(super) network: VecDeque<PeerEnvelope<G>>,
    // ...
}
```

`new` takes the groups by value
([`driver/adoption.rs:29-32`](../crates/rafter-service/src/driver/adoption.rs)),
nothing returns them, and no method cuts a link, isolates a node, steps one
node, or attaches a transport. Its own doc comment states the gap and then
leaves it to the reader
([`driver/in_memory.rs:7-11`](../crates/rafter-service/src/driver/in_memory.rs)):

```rust
/// Production transports can implement
/// [`DriverCommandSender`] directly or wrap the same group-driving logic around
/// an authenticated network boundary.
```

"The same group-driving logic" is precisely what the crate does not ship.

The fenced lock had to write it, and its module docs name the reason
([`reference/fenced-lock/tests/support/cluster.rs:3-5`](../reference/fenced-lock/tests/support/cluster.rs)):

```rust
//! `rafter-service` ships one driver, and it owns every replica behind a
//! private queue with no way to cut a link, so a consumer that needs
//! partitions has to supply its own [`DriverCommandSender`].
```

and again in the transport module
([`tests/support/transport.rs:3-5`](../reference/fenced-lock/tests/support/transport.rs)):

```rust
//! `rafter-service` ships transport *traits* and an inbound validator, but no
//! driver that consumes them, so this module is what an external embedder has
//! to write.
```

The pair is 1,838 lines. That number needs splitting rather than quoting,
because only part of it is a workaround. The deterministic network in
`transport.rs` is legitimately consumer-owned: a real deployment supplies a real
network, and a test supplies a controllable one. What is not consumer-owned is
`cluster.rs:90-789` — `WriteStart`, `ReadStart`, `WriteWaiter`, `ReadWaiter`,
`NodeState`, the `DriverCommandSender` impl,
`begin_write`/`poll_write`/`take_write`,
`begin_read`/`poll_read`/`take_read`/`drive_read`, `record_report`, `send_all`,
`observe_proposal_event`, `observe_read_event`, `resolve_write`,
`resolve_read`, `abandon_all_waiters`, and the two ID allocators — plus the
four group-error mappers and the unknown-reason mapper at
`cluster.rs:1393-1478`. That is roughly
790 lines of pure mechanism, and every line of it has a counterpart inside
`rafter-service` already: `observe_batch_report`
([`crates/rafter-service/src/driver/write.rs:245-307`](../crates/rafter-service/src/driver/write.rs))
and `handle_read_outcome`
([`driver/read.rs:65-137`](../crates/rafter-service/src/driver/read.rs))
are the same correlation logic against a different waiter representation.

Teardown is blocked separately. `RaftGroup::into_parts` exists and is the
documented in-process restart path, but a group handed to a driver is
unreachable: `groups` is private and no accessor returns it. The entry that
promoted `into_parts` said so at the time and left it open —
"`rafter-service` still has no teardown … a driver-level release is a separate
need with no consumer behind it yet"
([Group and Runtime Decomposition](#group-and-runtime-decomposition)). It has
one now.

### Classification

Raft mechanism, and a durable-lifecycle mechanism in its teardown half.

Nothing in the missing driver is application policy. Routing a report's peer
messages, correlating proposal and read events back to local waiters,
allocating monotonic `LocalProposalId`s and `ReadId`s, retrying a pending
barrier with the same read ID and freshness, cancelling a barrier through
`RaftGroup::cancel_read` before dropping its waiter, refusing input after
shutdown, and resolving every outstanding waiter when the group is released —
each of these is a rule the app layer's own contract states, and each is a rule
a consumer gets wrong silently. The clearest case is proposal correlation: the
app layer requires local proposal IDs to be strictly monotonic and rejects a
reused one with `GroupError::NonMonotonicLocalProposalId`, and the hazard of
reusing one across incarnations is documented in
[Group and Runtime Decomposition](#group-and-runtime-decomposition) as
silently completing a new waiter with an older proposal's result. A driver is
where that discipline lives.

The transport half follows directly from a documented contract in the same
sense: `validate_inbound_peer_envelope` documents what must happen to a frame
before a group sees it, and no code path in the workspace connects the two. A
contract with a validator, a delivery-semantics essay, and no caller is a
contract that has never been executed.

Second plausible consumer: the sharded counter service. Its contract requires
"group creation, draining, removal, reopening, and tombstoning", "messages
arriving after removal", "a poisoned group cannot stop unrelated groups", and
"removed groups cannot be resurrected by late traffic"
([`docs/reference-consumers.md:308-317,332-338`](./reference-consumers.md)). Every one
of those is a per-group driver with its own release and its own inbound gate.
The many-group host it will run on cannot express them today:
`MultiRaftHost::open_group` exists
([`crates/rafter-multiraft/src/host.rs:39-55`](../crates/rafter-multiraft/src/host.rs))
and there is no `close_group`, no `remove_group`, and no way to get a driver
back out of the `BTreeMap<G, Box<dyn GroupDriver<G>>>` it was inserted into
(`:17`). A host that can only open groups cannot drain one.

### Design

One new driver and one new method on the existing one, sharing a single rule:
**a driver owns a movable slot for its group, and hands back exactly what it
was given.**

#### The driver

In a new `crates/rafter-service/src/driver/transport.rs`:

```rust
/// Managed driver for one local Raft group over an attached transport.
///
/// This is the driver an embedder writes when frames leave the process.
/// [`InMemoryRaftDriver`] owns every replica of a group and moves frames
/// between them itself, which makes it a complete cluster and an unusable
/// node; this driver owns exactly one replica, hands its outbound frames to a
/// [`RaftTransport`], and receives inbound frames from whatever loop the
/// embedder runs. Rafter opens no sockets and spawns no tasks: the embedder
/// calls [`TransportRaftDriver::tick`] and
/// [`TransportRaftDriver::deliver`], and this type owns everything between
/// those calls and a resolved client future.
///
/// Cloning shares the driver. Handles obtained from
/// [`TransportRaftDriver::handle`] stay valid across a group release and
/// re-adoption, because a handle names a service rather than a node
/// incarnation.
pub struct TransportRaftDriver<G, A, R, T, V> { /* Arc<Mutex<_>> */ }
```

Both bounds default to 1024, matching the shipped driver's own
`max_drive_steps`: all three count local steps taken on behalf of one
operation. Both are validated at construction and fail closed on zero, which is
meaningless rather than merely small — zero retries never collects a granted
barrier and zero waiters refuses every write, so a driver built with either is
one that cannot serve anything. Because the struct is `#[non_exhaustive]`, an
embedder outside the crate cannot use struct-update syntax, so it gets
`with_max_read_retries` and `with_max_pending_waiters` setters beside `new`.

```rust
/// Bounds on one driver's local work.
///
/// Every bound is a refusal rather than an unbounded wait, so a stalled
/// protocol surfaces as a typed error instead of a hang.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct TransportDriverOptions {
    /// Retries a pending read barrier at most this many times within one
    /// [`TransportRaftDriver::drive_pending_reads`] call before leaving it
    /// pending for the next one.
    pub max_read_retries: usize,
    /// Refuses to enqueue more than this many unresolved client waiters of
    /// each kind, so a driver whose transport is down fails closed rather than
    /// growing.
    pub max_pending_waiters: usize,
}
```

`max_read_retries` and `with_max_read_retries` did not survive adoption:
[Revision after adoption](#revision-after-adoption) removes the retry bound in
favour of a grant-gated retry, so the shipped struct carries
`max_pending_waiters` alone and the paragraph above reads as a design that had
two bounds rather than as a description of the type.

```rust
impl<G, A, R, T, V> TransportRaftDriver<G, A, R, T, V>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine + Send + 'static,
    // ... the same associated-type bounds InMemoryRaftDriver carries ...
    R: PersistedRaftRuntime + Send + 'static,
    T: RaftTransport<G>,
    V: AuthenticatedPeerValidator<G, T::PeerPrincipal> + Send + Sync + 'static,
{
    /// Builds a driver over one already-configured group.
    ///
    /// The group must be quiescent — no pending proposals and no reserved
    /// reads — for the same reason [`InMemoryRaftDriver::new`] requires it: the
    /// driver correlates outcomes to waiters it created, and a waiter it did
    /// not create can never be resolved. Generated IDs start above the group's
    /// adopted watermarks.
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError`] when the group is poisoned, holds
    /// undrained poisoned waiters, is not quiescent, or has exhausted a local
    /// ID space.
    pub fn new(
        group: RaftGroup<G, A, R>,
        transport: T,
        validator: V,
        options: TransportDriverOptions,
    ) -> Result<Self, ManagedDriverError>;

    /// Returns a cloneable handle connected to this driver.
    #[must_use]
    pub fn handle(&self)
        -> RaftHandle<G, A::Command, A::Query, A::CommandResult, A::QueryResult, Self>;

    /// Steps the group with a tick and routes everything the step produced.
    ///
    /// This is one of the two entry points that advance the protocol. Call it
    /// on the embedder's own timer; the app layer's election and heartbeat
    /// timing is measured in ticks, not in wall time, so the tick interval is
    /// the embedder's policy and Rafter does not choose it.
    ///
    /// The step's report is routed before this returns: peer messages go to
    /// the transport, proposal and read events resolve waiters, and the metrics
    /// snapshot is published. A terminal event resolves its waiter whichever
    /// step observed it, which is why a client future can complete inside a
    /// tick it has no other relationship to.
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError`] when the driver has released its group,
    /// is shutting down, or the group step fails.
    pub fn tick(&self) -> Result<(), ManagedDriverError>;

    /// Validates one inbound authenticated envelope and steps the group with
    /// it.
    ///
    /// Validation is [`validate_inbound_peer_envelope`] against this driver's
    /// validator, and it happens before the group is touched. A frame that
    /// fails it is refused here, exactly where a production embedder refuses
    /// it, and the group never sees it. Rejection is not a driver failure: an
    /// unauthorized or fenced peer sending frames is an expected condition, and
    /// the caller decides whether to log it, count it, or drop the connection.
    ///
    /// # Errors
    ///
    /// Returns [`InboundEnvelopeError::Rejected`] when validation refuses the
    /// frame, leaving the group untouched, and
    /// [`InboundEnvelopeError::Driver`] when the group step itself fails.
    pub fn deliver(
        &self,
        envelope: AuthenticatedPeerEnvelope<G, T::PeerPrincipal>,
    ) -> Result<(), InboundEnvelopeError>;

    /// Retries every unresolved read barrier.
    ///
    /// A granted barrier is consumed by a later read call rather than
    /// announced by an event, so a driver that only ticks and delivers leaves
    /// granted proofs uncollected. Call this after each batch of deliveries and
    /// after each tick. It is a no-op when no barrier is outstanding, and it is
    /// safe to call at any time: the app layer's contract for a pending helper
    /// read is to retry with the same read ID, freshness requirement, and
    /// context until it resolves.
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError`] when the driver has released its group or
    /// a read step fails for a reason that is not attributable to one barrier.
    pub fn drive_pending_reads(&self) -> Result<(), ManagedDriverError>;

    /// Returns how many outbound frames the attached transport refused.
    ///
    /// A refused frame is not a failure. Raft tolerates drops and the protocol
    /// re-sends, so the driver counts refusals rather than propagating them —
    /// a write must not fail because one heartbeat could not be delivered. The
    /// count is how an operator tells a cut link from an idle cluster, and a
    /// driver that discarded it would leave nothing to tell them apart.
    #[must_use]
    pub fn refused_sends(&self) -> u64;
}
```

#### Release and re-adoption

```rust
impl<G, A, R, T, V> TransportRaftDriver<G, A, R, T, V> {
    /// Retires the running incarnation and returns its group.
    ///
    /// This is the driver-level half of decomposition.
    /// [`rafter_app::group::RaftGroup::into_parts`] consumes the group it
    /// retires, and a driver's group lives behind the lock its cloned handles
    /// share, which nothing can move out of. The driver owns the movable slot
    /// so an embedder does not have to build one, which is the friction the
    /// decomposition entry recorded after adoption.
    ///
    /// Every outstanding waiter resolves before this returns. Writes resolve as
    /// [`WriteError::UnknownOutcome`] with
    /// [`UnknownOutcomeReason::DriverReleased`], because a proposal already
    /// appended may still commit and apply under the next incarnation. Reads
    /// resolve as [`ReadError::Abandoned`] with
    /// [`ReadAbandonReason::DriverReleased`], and their barriers are cancelled
    /// through the group first so the retired group is quiescent.
    ///
    /// The driver refuses every operation until
    /// [`TransportRaftDriver::adopt_group`] installs a new incarnation. It does
    /// not close the transport: the same link serves the next incarnation, and
    /// closing it is the embedder's decision.
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError::NoGroup`] when the driver has already
    /// released its group.
    pub fn release_group(&self) -> Result<RaftGroup<G, A, R>, ManagedDriverError>;

    /// Installs a new incarnation and routes its recovery outputs.
    ///
    /// `recovery_outputs` are the outputs the recovered runtime released, and
    /// the driver applies them itself rather than accepting an already-applied
    /// group. That is deliberate: the recovery report carries peer messages and
    /// snapshot directives that must be routed, and a caller that applied them
    /// outside the driver would drop exactly the effects a restart depends on.
    ///
    /// The new group must hold no reserved reads, and its local ID watermarks
    /// must be at or above the retired incarnation's when the two share a
    /// runtime; see [`rafter_app::group::RaftGroupParts`]. A driver that
    /// rebuilt its runtime from durable storage may restart its IDs at zero.
    ///
    /// Unlike `new`, this accepts a group that still tracks appended
    /// proposals; see the note below on why a released group has them.
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError::GroupAlreadyAdopted`] when the driver
    /// still holds a group, the same validation errors as
    /// [`TransportRaftDriver::new`], and a group error when the recovery
    /// outputs fail to apply.
    pub fn adopt_group(
        &self,
        group: RaftGroup<G, A, R>,
        recovery_outputs: Vec<RaftOutput>,
    ) -> Result<(), ManagedDriverError>;
}
```

```rust
impl<G, A, R> InMemoryRaftDriver<G, A, R> {
    /// Shuts the driver down and returns every group it owns.
    ///
    /// The counterpart to [`InMemoryRaftDriver::new`], which takes its groups
    /// by value. Waiters resolve exactly as they do for
    /// [`TransportRaftDriver::release_group`], undelivered frames in the
    /// in-memory network are dropped, and the driver refuses every later
    /// operation — there is no re-adoption, because this driver's constructor
    /// builds a whole cluster rather than installing one node.
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError::ShuttingDown`] when the driver has already
    /// shut down or released its groups.
    pub fn release_groups(&self)
        -> Result<BTreeMap<NodeId, RaftGroup<G, A, R>>, ManagedDriverError>;
}
```

#### Supporting types

```rust
/// Why an inbound peer envelope did not reach a group.
#[derive(Debug)]
#[non_exhaustive]
pub enum InboundEnvelopeError {
    /// The envelope failed inbound validation and was dropped. The group was
    /// not stepped and no state changed.
    Rejected { source: AuthenticatedPeerEnvelopeError },
    /// The group step failed after the envelope was accepted.
    Driver { source: ManagedDriverError },
}
```

`ManagedDriverError` gains `NoGroup` and `GroupAlreadyAdopted`.

`RaftTransport::Error` and `AsyncRaftTransport::Error` gain the bound
`Error + Send + Sync + 'static`, matching `PersistedRaftRuntime::Error` and the
state-machine bound from
[Typed Service Failure Surface](#typed-service-failure-surface). Without it a
driver cannot preserve a send failure as an `ErrorCause`, and a transport that
returns an untyped error would force the same rendering this cluster removes.
The one implementor in the tree already satisfies it
([`reference/fenced-lock/tests/support/transport.rs:87`](../reference/fenced-lock/tests/support/transport.rs)).

### Semantics and edge cases

- **Why a separate driver rather than generalizing the existing one.**
  `InMemoryRaftDriver` is a cluster: it has a `primary_node_id`, it drains its
  own network inside every operation
  ([`driver/state.rs:125-136`](../crates/rafter-service/src/driver/state.rs)),
  and it resolves a client future before the call returns. That synchronous
  completion is the property that makes it useful in examples and impossible
  over a transport, where the answer arrives on a later `deliver`. The two
  drivers have different lifecycles, not different configurations of one
  lifecycle.
- **The waiter table is the promoted mechanism.** A client future registered by
  `write` cannot complete until some later step observes a terminal proposal
  event, and that step may be a tick, a delivery, or another client's read. The
  driver therefore registers the waiter *before* stepping, so an event inside
  the very step that started the proposal resolves it rather than arriving
  before anything is listening — the consumer discovered this and wrote it down
  ([`cluster.rs:302-312`](../reference/fenced-lock/tests/support/cluster.rs)).
  That ordering is part of the promoted contract.
- **Bounded waiters.** `max_pending_waiters` fails closed rather than growing,
  which is what the transport contract already demands of transports
  ("Use bounded queues or another explicit backpressure policy rather than
  unbounded memory growth",
  [`transport.rs:59-61`](../crates/rafter-service/src/transport.rs)) and what
  the promotion rule demands of a promoted API. The refusal is
  `WriteError::Transport` with `WriteFate::NotAppended`: nothing was proposed.
- **A released group carries its appended proposals, and `adopt_group` takes
  them.** `release_group` cancels every read barrier through
  `RaftGroup::cancel_read`, so the retired group reports no reserved reads. It
  cannot do the same for proposals, and must not: the app layer has no
  `cancel_proposal`, because an appended entry is in the durable log and will
  commit or not on its own. That is exactly why the client's write resolves as
  *unknown* rather than refused. So the returned group is quiescent in reads and
  not in proposals, and `adopt_group` accepts what `new` refuses — a proposal
  whose waiter this driver already resolved is safe to carry, and its later
  `Applied` event correctly resolves nothing. Requiring full quiescence here
  would make the restart case this entry exists for unreachable, which its own
  focused test demonstrates.
- **A released driver refuses; it does not panic.** Every operation on a
  driver with no group returns `ManagedDriverError::NoGroup`, and every client
  future resolves with the corresponding service error. The consumer's
  `Option<LockGroup>` with two `expect`ing accessors
  ([`cluster.rs:276-287`](../reference/fenced-lock/tests/support/cluster.rs)) is
  the shape this replaces, and the difference is that the promoted slot has a
  typed empty state instead of a panic.
- **Release is not shutdown.** `DriverCommandSender::shutdown` marks the driver
  closed and resolves waiters; `release_group` also hands the group back and
  leaves the driver re-adoptable. A supervisor restarting a replica calls
  release; a supervisor stopping one calls shutdown and then release. Both
  resolve waiters the same way, because from a client's perspective they are the
  same event.
- **The transport survives a release.** A restart that reopened its links would
  turn an in-process incarnation swap into a network event for every peer. The
  driver keeps `T` and `V` across the gap and the embedder decides otherwise.
- **Inbound validation is not a driver error.** A rejected frame is an ordinary
  outcome on an authenticated boundary, which is why `InboundEnvelopeError`
  separates it from a step failure rather than folding both into
  `ManagedDriverError`.
- **`AsyncRaftTransport` gets no driver in this entry.** Its `recv` returns a
  future, so a driver over it owns a receive loop and therefore an executor —
  which this crate does not have and deliberately does not choose. The
  synchronous seam is the one both reference consumers and both benches can
  drive, and it is enough to make the async trait implementable by an embedder
  that owns its own loop. Promoting an async driver before a consumer has
  demonstrated one would be exactly the generalization the promotion rule
  defers.

  The trait itself did not survive: still no driver, still no implementor, and
  still no consumer a release later, so
  [Fourth revision after adoption](#fourth-revision-after-adoption-2026-07-26)
  removes `AsyncRaftTransport`, `InboundEnvelopeFuture`, and `TransportFuture`
  rather than publishing them. The reasoning above is what removal follows from
  — the argument for deferring the driver was always an argument against
  shipping the trait ahead of one.
- **Group identity stays caller-defined.** The driver serves one group and
  refuses every other group ID with `WriteError::WrongGroup`. A many-group host
  owns one driver per group and demultiplexes inbound frames by
  `AuthenticatedPeerEnvelope::group_id` before calling `deliver`.
- **Read retries are bounded per call, not per barrier.** A barrier that is
  still pending after `max_read_retries` stays pending and is retried on the
  next call, so the driver never spins and never abandons a barrier that the
  network could still resolve. Within a call the driver retries only where a
  retry can change the answer: a granted barrier whose state machine is behind
  it retries, because a read step also steps the group and a committed entry can
  apply between attempts, while a barrier still waiting on its quorum round does
  not, because only `deliver` can bring the frame it needs. Abandoning is the caller's decision, and its
  vocabulary is
  [Terminal Driver Vocabulary](#terminal-driver-vocabulary).

### Blast radius

Additive apart from one trait bound. No existing signature changes.

| File | Change |
| --- | --- |
| `crates/rafter-service/src/driver/transport.rs` | New: `TransportRaftDriver`, `TransportDriverOptions`, `InboundEnvelopeError` |
| [`crates/rafter-service/src/driver/mod.rs:33-49`](../crates/rafter-service/src/driver/mod.rs) | Declare and re-export the module |
| [`crates/rafter-service/src/driver/in_memory.rs`](../crates/rafter-service/src/driver/in_memory.rs) | Add `release_groups` |
| [`crates/rafter-service/src/driver/state.rs:5-15`](../crates/rafter-service/src/driver/state.rs) | `groups` becomes releasable |
| [`crates/rafter-service/src/driver/mapping.rs:11-47`](../crates/rafter-service/src/driver/mapping.rs) | `ManagedDriverError::{NoGroup, GroupAlreadyAdopted, InvalidOptions}` |
| [`crates/rafter-service/src/transport.rs:73-76,117-120`](../crates/rafter-service/src/transport.rs) | `type Error: Error + Send + Sync + 'static` on both traits |
| [`crates/rafter-service/src/lib.rs:26-41`](../crates/rafter-service/src/lib.rs) | Re-export the new types |

Breaking only for out-of-tree implementors of `RaftTransport` or
`AsyncRaftTransport` whose error type is not a `std::error::Error`. The single
in-tree implementor already satisfies the bound. `ManagedDriverError` is
`#[non_exhaustive]`, so its two new variants break no downstream match.

`bench-compare` is unaffected — it drives `InMemoryRaftDriver`
([`bench-compare/src/bin/bench-rafter-service.rs:26,30`](../bench-compare/src/bin/bench-rafter-service.rs))
and adds no transport — but it is outside the root workspace and must still be
built for this step because of the error-surface changes it inherits from the
previous entry.

### Focused-test plan

In a new `crates/rafter-service/tests/transport_driver.rs`, over a test-support
transport that is a queue with an explicit `take_deliverable`, so no test
depends on timing:

- `a_write_completes_through_two_drivers_over_a_transport` — the base case the
  crate has never had: two `TransportRaftDriver`s, frames moved by the test,
  one write committing and applying.
- `an_outbound_frame_reaches_the_transport_rather_than_a_private_queue` — the
  structural claim. Assert the transport observed exactly the report's peer
  messages.
- `a_client_future_resolves_inside_a_tick_it_did_not_start` — the waiter
  property: a write registered on one call completes on a later `tick`.
- `a_read_barrier_resolves_through_drive_pending_reads` — and its companion,
  `a_granted_barrier_is_not_collected_by_tick_alone`, which pins why the third
  entry point exists.
- `release_returns_the_group_the_driver_was_built_with`, and
  `a_released_driver_refuses_every_operation`.
- `adopt_routes_the_recovery_outputs_it_was_given` — assert the recovery
  report's peer messages reached the transport. A driver that accepted an
  already-applied group would drop them, which is why the signature takes
  outputs.
- `a_handle_survives_release_and_re_adoption` — the same `RaftHandle` writes
  successfully against the new incarnation.
- Negative: `an_unauthorized_peer_is_refused_before_the_group_is_stepped` —
  assert `InboundEnvelopeError::Rejected` *and* that the group's metrics are
  unchanged. Validation that happens after the step is not validation.
- Negative: `a_fenced_peer_is_refused_after_fencing` — the same frame accepted,
  then fenced through `RaftTransport::fence_peer`, then refused.
- Negative: `a_frame_for_another_group_is_refused`.
- Negative: `a_refused_send_does_not_fail_the_write_that_produced_it` — cut the
  link, assert the write proceeds toward its own outcome and `refused_sends`
  increments. A driver that propagated transport refusals would fail writes on
  every heartbeat drop.
- Negative: `waiters_are_bounded` — fill to `max_pending_waiters` and assert the
  next write is refused with `WriteFate::NotAppended` rather than enqueued.
- Negative: `release_resolves_outstanding_waiters_before_returning` — start a
  write, release, assert the future is already resolved with
  `UnknownOutcomeReason::DriverReleased` and that the returned group reports no
  pending proposals.

In `crates/rafter-service/tests/in_memory_write.rs`:

- `release_groups_returns_every_group_the_driver_adopted`, and
  `a_released_in_memory_driver_refuses_every_operation`.

### Rejected alternatives

- **Generalize `InMemoryRaftDriver` with a pluggable network.** The in-memory
  driver completes client futures synchronously inside the call, which a
  transport cannot. Making the network pluggable would make that completion
  conditional on the network implementation, so the same type would have two
  incompatible contracts.
- **Ship an async driver over `AsyncRaftTransport` instead.** It needs a receive
  loop and therefore an executor. `rafter-service` states that "the
  deterministic kernel, runtime API boundary, and synchronous app driver stay
  free of async runtime dependencies"
  ([`crates/rafter-service/src/lib.rs:5-7`](../crates/rafter-service/src/lib.rs));
  choosing an executor for the service layer is a larger decision than this
  evidence supports.
- **Have the driver own a receive loop over a `recv`-shaped sync trait.** Would
  require the driver to block a thread it did not create, which is the one thing
  a sans-IO library must not do.
- **Fold the validator into `RaftTransport`.** The crate separates
  authenticating a principal from deciding which Raft replica that principal is,
  and the consumer's own transport documents why: collapsing them erases the
  check `AuthenticatedPeerEnvelopeError::AuthenticatedPeerMismatch` exists for
  ([`reference/fenced-lock/tests/support/transport.rs:42-47`](../reference/fenced-lock/tests/support/transport.rs)).
  One type may still implement both traits.
- **`into_group(self)` instead of `release_group(&self)`.** The driver is
  `Clone` and shared behind an `Arc`, so a `self`-taking method would have to
  return `Result<_, Self>` on every extra handle and would give a caller no way
  to release a driver whose handles are held by live client futures. The
  `&self` form is what a supervisor can actually call.
- **A generic `MultiRaftHost` group-removal API instead.** That is the right
  eventual home for many-group lifecycle, and it needs a per-group driver with a
  release first. Extracting the host-level shape now would be the general
  framework the delivery plan forbids before a consumer demonstrates it.
- **Leave the transport traits unused and delete them.** They encode a real
  contract that the fenced lock implemented as written and found sufficient.
  The defect is the missing caller, not the traits.

### After-state

`reference/fenced-lock/tests/support/cluster.rs` loses `cluster.rs:90-789` and
`1393-1478` — roughly 790 of its 1,496 lines, and every line of driver
mechanism in the consumer. `NodeDriver` becomes a type alias for a
`TransportRaftDriver` over the lock's own types, and `LockCluster` keeps what a
deployment genuinely owns: which nodes tick, which frames are delivered, which
links are cut, when a replica restarts, and what the history records.
`transport.rs` is unchanged, which is the correct outcome — a deterministic
network is test infrastructure, not a workaround.

**Corrected during adoption.** Three of those sentences were optimistic, and the
measurements are these.

`cluster.rs` fell from 1,496 lines to 778, a deletion of 718 rather than 790,
and the difference is not rounding: what stayed is the client-side machinery the
driver does not offer a term for. `transport.rs` is *not* unchanged. It loses
`NodeTransport::accept_inbound`, because `TransportRaftDriver::deliver` performs
inbound validation itself, which is the entry working as designed; the module's
premise sentence — that the crate "ships transport *traits* and an inbound
validator, but no driver that consumes them" — also stops being true.

`NodeDriver` is a `TransportRaftDriver` over *wrappers* of the lock's own types
rather than over the types themselves. The driver takes its `RaftGroup` by value
and exposes no accessor for what is inside it, and `RaftGroupMetrics` carries
neither the state machine, nor the durable log, nor
`committed_application_index`. `release_group` is not a way to look, because it
resolves every outstanding waiter. So the consumer supplies a 232-line
`tests/support/observe.rs` holding a shared state machine and a shared runtime,
each implementing the public trait the group requires. That module is new
workaround, written to satisfy this entry, and the support directory's honest
net is 1,947 lines to 1,446 — **-501**, not -790.

`LockCluster` also keeps two things this paragraph did not anticipate: client
futures whose caller stopped waiting, because nothing abandons one waiter and
the fact the driver eventually reports has nowhere else to go, and a slot for a
barrier the driver could not carry forward. See
[Terminal Driver Vocabulary](#terminal-driver-vocabulary) for the first and the
note below for the second.

**Two defects the adoption found.** `TransportRaftDriver` never inspects a step
report's `read_events`. A barrier the cluster rejects or cancels during a `tick`
or a `deliver` is therefore never reported to its client; the group drops that
barrier's state, and the driver's next `drive_pending_reads` asks the group to
re-reserve a spent `ReadId`, which it refuses with
`GroupError::NonMonotonicReadId`. That error propagates out of
`drive_pending_reads` while the client waiter stays unresolved forever, and it
repeats on every later call. The lock reproduces this in
`a_linearizable_query_resolves_to_a_non_answer_when_leadership_is_lost`. Second,
`TransportRaftDriver::new` takes a group and no recovery outputs, so a first
incarnation cannot route them the way `adopt_group` does — the asymmetry is
harmless only while the first incarnation recovers nothing.

**Closed by the revision.** Everything the four paragraphs above record as
outstanding has since landed; see
[Revision after adoption](#revision-after-adoption). `route_report` routes read
events, so a barrier the cluster ends during a `tick` or a `deliver` resolves
its client instead of stranding it behind `GroupError::NonMonotonicReadId`;
`new` takes recovery outputs; and `with_group` reads a running replica under
the driver's own lock. `tests/support/observe.rs` is deleted with the gap it
existed for, so `NodeDriver` is now a `TransportRaftDriver` over the lock's own
state machines rather than over wrappers of them, and the two waiters
`LockCluster` had to keep alive are retired through `abandon_write` and
`abandon_read`. The **-501** measurement is the net at step 15 and is not the
current one: the support directory stands at 1,532 lines, having lost
`observe.rs` and then gained the durable slice's own modules.

Three of the consumer's own notes disappear with the code. The comment that
`rafter-service` "ships one driver, and it owns every replica behind a private
queue"; the comment that it "ships transport *traits* and an inbound validator,
but no driver that consumes them"; and the `Option<LockGroup>` slot with its two
`expect`ing accessors, whose doc comment explains that a shared, lock-guarded
group has nowhere to be moved out of. The last of these was recorded as an
unanticipated friction in
[Group and Runtime Decomposition](#group-and-runtime-decomposition); this entry
is its answer.

`rafter-service` gains the composition the crate has been describing since it
was written: an embedder holds a `RaftGroup`, a transport, and a validator,
calls `tick` and `deliver`, and gets a `RaftHandle`. The sharded counter builds
one of these per group.

### Revision after adoption

The findings above are one bug and four gaps. This subsection designs the fixes
and lands before them, so the design is reviewable against the evidence that
produced it rather than against the code that answered it. Nothing here reopens
the entry's shape: the driver still owns one replica, one movable slot, and the
waiter tables, and every fix is inside that.

A later adversarial review found seven more, including three the fixes below
did not touch and one this subsection's own fix 1 created. They are designed in
[Second revision after adoption (2026-07-25)](#second-revision-after-adoption-2026-07-25),
which is current truth wherever the two disagree; the two places they disagree
are named there.

#### 1. A step report's read events are routed

`route_report` handles `peer_messages` and `proposal_events`
([`driver/transport/state.rs:110-119`](../crates/rafter-service/src/driver/transport/state.rs))
and drops `read_events` on the floor. That is not a missing refinement; the app
layer documents the exact hazard against the method the driver calls
([`crates/rafter-app/src/group/read.rs:161-168`](../crates/rafter-app/src/group/read.rs)):

```rust
/// A terminal read event clears local waiter state, so a caller that keeps
/// retrying after observing [`ReadEvent::Rejected`] or [`ReadEvent::Canceled`]
/// in the report receives [`GroupError::NonMonotonicReadId`] rather than a
/// second statement of the rejection. Check the read events of every report
/// the caller records before retrying — a barrier is most often ended by
/// the tick or delivery step that observes a leadership change, so its
/// terminal event arrives in that step's report rather than in one this
/// method returned.
```

The driver is the caller that keeps retrying. `route_report` gains a read-event
pass that reaches the same terminal mapping `drive_pending_reads` already
reaches through `handle_read_outcome`, so a barrier resolves identically
whichever step observed its end:

| `ReadEvent` | Waiter |
| --- | --- |
| `Rejected { read_id, reason, leader_hint }` | `ReadError::Rejected { read_id: Some(read_id), reason, leader_hint }` |
| `Canceled { read_id, reason, leader_hint }` | `ReadError::Canceled { read_id, reason, leader_hint }` |
| `Granted { read_id, .. }` | Not terminal. The proof is now cached in the group; the driver records that this barrier is ready to collect, which is what fix 5 waits on. |
| `FreshnessUnavailable { .. }` | Not terminal. The barrier is still reserved and a later `Granted` follows it. |

The event carries the answer for the first two rows and nothing the group still
needs, so the driver resolves them without touching the group again — which is
the point, because the group has already dropped that barrier's state.

**The invariant this creates, and what breaks it.** Once terminal events resolve
their waiters, a read that `drive_pending_reads` still tracks and the group does
not is no longer a reachable state; it is a driver invariant violation. The
driver must not report that as a permanent client hang, which is what it does
today: `GroupError::NonMonotonicReadId` propagates out of `drive_pending_reads`,
the waiter is never resolved, and every later call raises the same error while
the client waits forever. Instead, a per-barrier group error resolves that
barrier's waiter with `ReadError::ManagedInvariantViolation` naming the
invariant, and `drive_pending_reads` continues with the others. Two reasons for
that shape rather than propagation. A client learning that its read produced no
answer can act; a client that hangs cannot. And `drive_pending_reads` serves
every barrier, so one barrier's fault must not deny service to the rest — which
is the same rule the entry already states for the group as a whole, one level
down. The method's error contract is unchanged and now true: it returns
`ManagedDriverError` when the driver has released its group, or a read step
fails for a reason not attributable to one barrier.

#### 2. Observation without release

`release_group` is the only way to see inside a running driver, and it resolves
every outstanding waiter, so looking costs the caller its clients. The consumer
paid 232 lines to avoid that, in a
`reference/fenced-lock/tests/support/observe.rs` this fix deleted: a shared
state machine and a shared runtime, each re-implementing the public trait the
group requires, held on the side so the harness can still read what the driver
took.

```rust
impl<G, A, R, T, V> TransportRaftDriver<G, A, R, T, V> {
    /// Reads the adopted group under the driver's own lock.
    ///
    /// The closure receives a shared borrow for its own duration and nothing
    /// outlives the call: no guard, no owned escape, no way to keep the group
    /// after the lock is released. `&RaftGroup` rather than `&mut` is the whole
    /// policy — the driver correlates outcomes to waiters it created, and a
    /// group stepped, read, or cancelled from outside would break that
    /// correspondence silently.
    ///
    /// The closure runs with the driver locked, so it must not call back into
    /// this driver. A shared borrow of the group offers no way to.
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError::NoGroup`] when the driver has released its
    /// group.
    pub fn with_group<U>(
        &self,
        read: impl FnOnce(&RaftGroup<G, A, R>) -> U,
    ) -> Result<U, ManagedDriverError>;

    /// Returns the index this replica's state machine must reach to have
    /// applied every application command it knows to be committed.
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError::NoGroup`] when the driver has released its
    /// group.
    pub fn committed_application_index(&self) -> Result<LogIndex, ManagedDriverError>;
}
```

`RaftGroup` already exposes `state_machine`, `runtime`, `metrics`, and
`committed_application_index`
([`crates/rafter-app/src/group/types.rs:396,415`](../crates/rafter-app/src/group/types.rs),
[`group/output.rs:18,77`](../crates/rafter-app/src/group/output.rs)), so the
closure needs no new app-layer surface. That is the test of whether this is the
right seam: the missing thing was reachability, not vocabulary.

**One forwarder, not two.** `committed_application_index` gets a direct method
because it is a zero-argument scalar that the promoted decomposition recipe
names as the readiness gate, and `with_group(|group| group.committed_application_index())`
is pure ceremony around it. The state machine gets no forwarder, because every
real state-machine read is a projection the closure has to express anyway, and
the consumer's own three call sites prove it: one clones the machine, one
derives an applied index from that clone, and one decodes durable log payloads
through the machine's decoder while holding the runtime's log — which no
forwarder over `&A` alone could serve.

**Rejected: a guard type.** `fn group(&self) -> Result<GroupGuard<'_, ...>>`
reads better at a call site and hands a caller a lock it can hold across
arbitrary code, including a call back into the driver that deadlocks. The
closure makes the lock's extent a syntactic fact.

#### 3. Per-waiter abandon

`release_group` and `shutdown` resolve every waiter; nothing retires one. A
client that stops waiting drops its future, and its waiter stays unresolved,
counting against `max_pending_waiters` until something the client is not
listening for fills it.

```rust
impl<G, A, R, T, V> TransportRaftDriver<G, A, R, T, V> {
    /// Stops waiting for one write and resolves its client.
    ///
    /// Returns whether a waiter was retired, so abandoning an ID this driver
    /// no longer holds is a no-op rather than an error: a caller racing its own
    /// completion is not a fault.
    pub fn abandon_write(&self, local_proposal_id: LocalProposalId) -> bool;

    /// Stops waiting for one read, cancelling its barrier through the group
    /// first.
    pub fn abandon_read(&self, read_id: ReadId) -> bool;

    /// Returns every write this driver has not resolved.
    pub fn pending_writes(&self) -> Vec<PendingWrite>;

    /// Returns the read IDs of every barrier this driver has not resolved.
    pub fn pending_reads(&self) -> Vec<ReadId>;
}

/// One unresolved write, named both ways a caller can name it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct PendingWrite {
    /// The ID the driver allocated, which [`TransportRaftDriver::abandon_write`]
    /// takes.
    pub local_proposal_id: LocalProposalId,
    /// The ID the caller supplied in [`WriteOptions`], which is how a caller
    /// with several writes in flight tells them apart.
    pub client_request_id: Option<ClientRequestId>,
}
```

**Vocabulary: the one that exists.** A write resolves as
`UnknownOutcome { local_proposal_id, client_request_id, reason: DriveBoundReached }`
and a read as `Abandoned { read_id, reason: DriveBoundReached }`, with
`cancel_read` called first so the retired group stays quiescent — the same
ordering `release_group` uses and the same guarantee
[Terminal Driver Vocabulary](#terminal-driver-vocabulary) makes for the variant.
No new reason is minted. That entry described this producer when it named "a
caller's own round budget expires", and a caller's round budget is a drive bound
reached by the caller; its after-state note records that the producer was then
lost, and this is it returning.

**Resolve, do not remove.** An abandoned waiter keeps its resolved outcome in
the table until its future is polled, exactly as every other resolution does. A
caller that abandons may still hold the future — the lock does, one line later —
and a future that answered `ManagedInvariantViolation` because its own caller
abandoned it would be a worse answer than the one it asked for. The slot is
freed regardless, because `max_pending_waiters` counts *unresolved* waiters.

**Late events: the first outcome wins.** `resolve_write` and `resolve_read`
already refuse to overwrite a waiter that has one, and abandonment is an
outcome, so a later `Applied`, `Rejected`, `Canceled`, or `UnknownOutcome` for
an abandoned ID resolves nothing and changes nothing. That direction is the
correct one and not merely the convenient one: the client already holds a
terminal answer, and on the write side that answer is *unknown*, which is
exactly the statement that the proposal may still commit. The consequence is
worth naming plainly, because it decides when a caller should abandon at all: a
client that abandons stops hearing. A caller that wants the eventual fact keeps
its future and does not abandon. Those are two different situations and only one
of them is abandonment.

**Learning the ID.** `DriverCommandSender::write` and `read` return a future and
nothing else, so today the allocated ID exists only inside the driver until the
future resolves — which is too late for a caller that wants to stop waiting.
`pending_writes` and `pending_reads` close that from the driver side rather than
through the trait, which is shared with `InMemoryRaftDriver`, whose waiters are
not addressable this way and whose futures resolve inside the call that made
them. Reading the driver's own unresolved table is also the honest surface: it
answers "what is this driver still holding", which is the question a supervisor
draining one actually asks.

#### 4. `new` takes recovery outputs

```rust
pub fn new(
    group: RaftGroup<G, A, R>,
    recovery_outputs: Vec<RaftOutput>,
    transport: T,
    validator: V,
    options: TransportDriverOptions,
) -> Result<Self, ManagedDriverError>;
```

Symmetric with `adopt_group`, and for the reason `adopt_group` already gives: a
recovery report carries peer messages and snapshot directives that must be
routed, and a caller that applied them outside the driver drops exactly the
effects a restart depends on. A first incarnation over empty storage passes
`Vec::new()`. The consumer proved the asymmetry by writing the workaround and
then asserting its own escape hatch was safe
([`cluster.rs:203-218`](../reference/fenced-lock/tests/support/cluster.rs)) —
`assert!(report.peer_messages.is_empty())`, which holds only because that
replica had never started.

Ordering inside `new` is the ordering `adopt_group` uses: validate the group and
take its watermarks, install it, then apply and route the outputs. A group the
driver refuses never leaves a half-built driver behind.

This breaks a constructor one commit old with two in-tree call sites, which is
the cheapest this correction will ever be.

#### 5. Retries become event-driven, and `max_read_retries` goes

The selective-retry rule rests on a premise the app layer contradicts. The entry
says a granted-but-stale barrier is worth retrying "because a read step also
steps the group, so a committed entry can apply between one attempt and the
next". That is true of the call that *starts* a barrier and false of every
retry: `RaftGroup::read` against a barrier the group already tracks returns
through `unstepped_read_report`
([`crates/rafter-app/src/group/read.rs:384-404,433-440`](../crates/rafter-app/src/group/read.rs)),
so nothing steps and no index moves. Every waiter `drive_pending_reads` iterates
had its barrier submitted by `begin_read`, so that is the only shape the loop
ever has, and every attempt after the first is a spin against a group whose
state nothing in between can change. The consumer measured this and worked
around it with `max_read_retries: 1`
([`cluster.rs:81-90`](../reference/fenced-lock/tests/support/cluster.rs)).

**Chosen: event-driven, off the read events fix 1 now routes.** A reserved
barrier has exactly one transition that changes its answer, and after fix 1 the
driver sees it: `ReadEvent::Granted` says the proof is cached and the next read
call will consume it. So `drive_pending_reads` attempts a barrier when a grant
has arrived for it and leaves it alone otherwise. `Pending` and
`FreshnessUnavailable` are waits, not retries — and `FreshnessUnavailable` is
re-emitted on each step until the applied index catches up, at which point the
same code path emits `Granted`
([`group/read.rs:303-338`](../crates/rafter-app/src/group/read.rs)), so nothing
is lost by waiting.

**Rejected: step the group once per retry.** The only step a driver could take
is a tick, election and heartbeat timing is measured in ticks, and this entry
already states that the tick interval is the embedder's policy and Rafter does
not choose it. A driver that injected ticks to service a read would move an
election to answer a query. There is no neutral step to take, which is why the
event is the right signal.

**`max_read_retries` is removed rather than defaulted.** With grants announced,
a second attempt within one call is provably useless, and a bound that cannot
change any behavior is a bound that lies about having one. The field and
`with_max_read_retries` go together; `TransportDriverOptions` keeps
`max_pending_waiters`, which is a real refusal. This is the same one-commit-old
sanction fix 4 takes, and it removes a knob rather than adding one.

**`drive_pending_reads` stays, and stays required.** The grant is announced, but
the *proof* is still consumed by a read call, and that call runs the state
machine — which the driver will not do inside a tick the embedder asked for on
its own timer. What changes is that the call is now a no-op unless a grant
arrived, instead of a bounded spin on every barrier every time.

#### Blast radius of the revision

| File | Change |
| --- | --- |
| [`crates/rafter-service/src/driver/transport.rs`](../crates/rafter-service/src/driver/transport.rs) | `new` takes recovery outputs; `with_group`, `committed_application_index`, `abandon_write`, `abandon_read`, `pending_writes`, `pending_reads`; `max_read_retries` and its setter removed |
| [`crates/rafter-service/src/driver/transport/state.rs`](../crates/rafter-service/src/driver/transport/state.rs) | Read events routed; grant-gated retry; per-barrier group errors resolve their own waiter |
| [`crates/rafter-service/src/lib.rs`](../crates/rafter-service/src/lib.rs) | Re-export `PendingWrite` |
| [`crates/rafter-service/tests/transport_driver.rs`](../crates/rafter-service/tests/transport_driver.rs) | The regression test below, the new surfaces, and every `new` call site |
| [`reference/fenced-lock/tests/support/`](../reference/fenced-lock/tests/support/) | `observe.rs` deleted; the read-side workarounds and the options override go with it |

Additive apart from `new`, `max_read_retries`, and the read-event routing, which
changes when a client future resolves — earlier, and to the answer the cluster
gave rather than to nothing.

#### Focused-test plan for the revision

In `crates/rafter-service/tests/transport_driver.rs`:

- `a_barrier_canceled_during_a_delivery_resolves_its_client` — the regression
  test, written first and failing first. Start a linearizable read on a leader,
  make it lose leadership through a delivered frame, and assert the client
  future resolves to `ReadError::Canceled`. Before the fix it stays pending
  forever and the next `drive_pending_reads` errors with
  `GroupError::NonMonotonicReadId`; the test asserts the resolution, so both
  halves of the defect are pinned by one assertion.
- No direct test for the invariant violation, and the reason is the point of
  having named it: once terminal events are routed there is no public sequence
  that reaches the state. The driver's `ReadId`s are monotonic across release
  and re-adoption, its retry request is byte-identical to the one that started
  the barrier, and `with_group` hands out a shared borrow, so nothing outside
  the driver can cancel or re-reserve a barrier it holds. The regression test
  above closes the only route that existed, and its final
  `drive_pending_reads().expect(...)` is where a regression would land.
- `with_group_reads_a_running_replica_without_releasing_it` — read the state
  machine and the applied index through the closure, then assert the driver
  still serves a write, which is the property `release_group` cannot offer.
- `with_group_refuses_after_release` — `ManagedDriverError::NoGroup`.
- `abandoning_a_write_resolves_its_client_and_frees_its_slot` — fill to
  `max_pending_waiters`, abandon one, assert the next write is admitted.
- `a_late_proposal_event_does_not_overwrite_an_abandoned_write` — abandon, then
  drive the proposal to commit, and assert the client still holds
  `DriveBoundReached` and nothing panics.
- `abandoning_a_read_cancels_its_barrier` — assert `ReadError::Abandoned` and
  `reserved_reads` back to its prior value.
- `a_pending_write_is_addressable_before_it_resolves` — `pending_writes` names
  the in-flight write with the `client_request_id` the caller supplied, and
  stops naming it once resolved.
- `new_routes_the_recovery_outputs_it_was_given` — the counterpart to
  `adopt_routes_the_recovery_outputs_it_was_given`, asserting the transport saw
  the recovery report's peer messages.
- `a_zero_bound_is_refused_at_construction` — narrowed to
  `max_pending_waiters`, the only bound left.

### Second revision after adoption (2026-07-25)

An adversarial review of the shipped driver reproduced seven findings, each
with a test that passes only while its defect stands. Two are critical and
change what a client is told about a write it may have committed; one is
critical and silently disables snapshot transfer; the rest are a denial of
service across barriers, a bypassed error vocabulary, an unbounded table, and
four smaller shapes. One of them —
[fix 4](#4-a-barriers-own-fault-resolves-that-barrier-and-only-that-barrier) —
is a defect the previous subsection's own fix 1 introduced, which is recorded
here rather than corrected in place because a design block is immutable as
approved.

A second adversarial hunt attacked these fixes and reproduced five more, four of
them a fix below failing on its own written terms. They are designed in
[Third revision after adoption (2026-07-25)](#third-revision-after-adoption-2026-07-25),
which is current truth wherever the two disagree; the three places they disagree
are fix 3's all-or-nothing justification, fix 2's list of drained call sites, and
fix 6's claim that a drop-vs-poll race cannot exist, and each is named there.

Nothing below reopens the entry's shape. The driver still owns one replica, one
movable slot, and two waiter tables. What changes is that every statement the
driver makes about a client's operation is now one it observed, and every
stream a step produces is either routed or has a written reason why not.

One rule governs the whole subsection, and it is the entry's own rule stated
strictly enough to be checkable: **a driver reports what it observed, and says
"unknown" for everything else.** Six of the seven findings are the same
violation of it seen from different sides.

#### 1. A failing step reports a fate it observed, or none at all

`begin_write` derives the reported fate from a flag:

```rust
if let Err(error) = self.step(GroupInput::Proposal { proposal }) {
    let fate = if self.write_waiters.get(&local_proposal_id)
        .is_some_and(|waiter| waiter.saw_local_append)
    { WriteFate::Unresolved } else { WriteFate::NotAppended };
```

`saw_local_append` is set by `observe_proposal_event`, which runs from
`route_report`, which `step` reaches only on the success path — and this branch
is the failure path. `step_with_options` returns `Result<Report, GroupError>`,
so on failure there is no report at all, and the flag is not merely unset but
structurally unreachable. Every failing step therefore reports
`WriteFate::NotAppended`, and `WriteFate::NotAppended` is documented as
"refused before it reached the local Raft log. It cannot commit, now or later,
and its request identity is still unused"
([`error.rs:122-127`](../crates/rafter-service/src/error.rs)).

The review reproduced the worst case it licenses. A single-voter leader
appends, commits, and applies inside the step that proposes; when the state
machine refuses that apply the group poisons and the step returns `Err`. The
driver told the client `NotAppended` for an entry the same test then read out of
the group at `last_log_index = 2` and `commit_index = 2`. A caller obeying the
documented meaning retries under a fresh identity and double-applies a
committed write.

The same branch is reached with nothing appended at all — a runtime that
returns no proposal lifecycle event yields `GroupError::ProposalDidNotStart`,
which states that the app layer does not know what the runtime did — and the
client is told `NotAppended` there too. One answer for two opposite facts is
the proof that the answer is not an observation.

**The fate rule, stated so it can be checked.** `WriteFate::NotAppended` is
reported only where the refusal is the thing that happened:

| Situation | Fate | Why it is an observation |
| --- | --- | --- |
| The driver refused before proposing — wrong group, shutting down, no group, waiter bound reached, local IDs exhausted | `NotAppended` | Nothing was handed to a group. The refusal is the whole event. |
| `ProposalEvent::Rejected` | n/a | `write_error_from_rejection` produces `NotLeader`, `PayloadTooLarge`, or `Rejected`, none of which carry a fate field: the variant *is* the proof. |
| `GroupError::NonMonotonicLocalProposalId` | `NotAppended` | The group refuses before it proposes, and `write_error_from_group` already stamps this variant rather than taking the passed fate. |
| Any other group error from the proposing step | `Unresolved` | The driver asked a group to propose and did not learn what happened. |

The last row is the change, and `saw_local_append` goes with it. The flag was
not just unreachable here; it could not be made into a proof if it were
reachable. `false` means "no append was observed", and the entry's own
`WriteFate` doc is explicit that unobserved is not refused: "A locally appended
entry that was never sent is unresolved rather than refused, and that is the
truth rather than caution." `InMemoryRaftDriver` keeps its `saw_local_append`
because it drives to completion inside one call and its batch mapper needs a
per-entry discriminator across a shared error; this driver resolves each waiter
from the event that named it, so the only thing the flag could add is an
inference.

**The two shapes `InMemoryRaftDriver` already answers, adopted verbatim.** Its
`finish_failed_write_batch`
([`driver/write.rs:178-215`](../crates/rafter-service/src/driver/write.rs)) is
a three-way decision, and this driver takes the same three in the same order:

1. The group captured this proposal in `poisoned_waiters` →
   `UnknownOutcome { reason: GroupPoisoned }`. This is fix 2, and it is first
   for the reason it is first there: the poison is a more specific fact about
   this proposal than the step error is.
2. `GroupError::ProposalDidNotStart` →
   `UnknownOutcome { reason: RuntimeDroppedProposal }`. The app layer's variant
   name says the driver does not know what the runtime did, and
   `RuntimeDroppedProposal` is the service layer's word for exactly that.
3. Otherwise → `write_error_from_group(error, WriteFate::Unresolved)`, which is
   fix 5.

The single-voter poisoning apply lands in row 1 and answers
`UnknownOutcome { GroupPoisoned }` — the same answer `InMemoryRaftDriver`
gives for the identical fault, which is the outcome the review asked for by
running both drivers against one fixture.

#### 2. Poisoned waiters are drained wherever a step can poison

`RaftGroup::enter_poisoned` moves every pending proposal and every reserved
read out of the group's live tables and into `poisoned_waiters`
([`crates/rafter-app/src/group/poison.rs:54-70`](../crates/rafter-app/src/group/poison.rs)).
After that the group emits no further event for them: they are not dropped,
they are handed over. `TransportRaftDriver` never reads that table, so every
in-flight client future at the moment of a poison waits forever, and every
later `tick` and `drive_pending_reads` raises the same refusal without ever
resolving anybody. The review reproduced it on two voters: the follower's
acknowledgement commits, the apply fails, the group poisons, and thirty-two
further ticks later the write is still pending and the group is still holding
its captured waiter.

There is no API gap here. The driver owns the group mutably and
`drain_poisoned_waiters` is public and `#[must_use]`; the previous revision
simply never called it.

```rust
/// Resolves every waiter the group handed over when it poisoned.
///
/// A poison is not an event stream: `enter_poisoned` moves pending proposals
/// and reserved reads into `poisoned_waiters` and emits nothing for them, so a
/// driver that only routes reports leaves their clients waiting forever. This
/// runs after every group interaction that can poison, on both the success and
/// the failure path, because a step that poisons can still return `Ok`.
fn drain_poisoned_waiters(&mut self);
```

Vocabulary, matched to what each side can prove:

| Waiter | Resolution |
| --- | --- |
| Proposal | `WriteError::UnknownOutcome { local_proposal_id, client_request_id, reason: GroupPoisoned }`. The entry may be in the durable log; a later incarnation over the same log can still commit it. |
| Read | `ReadError::Poisoned { reason, cause }`, the same pair `InMemoryRaftState::poisoned_read_error_from_primary` produces ([`driver/state.rs:91-103`](../crates/rafter-service/src/driver/state.rs)). |

`client_request_id` comes from the captured waiter when the group carried one
and from the driver's own `WriteOptions` otherwise, which is the same
`or(...)` the in-memory driver uses.

`UnknownOutcomeReason::GroupPoisoned` becomes reachable through this driver for
the first time. It was already reachable through `InMemoryRaftDriver`, so this
is not a new reason; it is a producer that was missing.

**Call sites, chosen by "can this poison".** `step` (tick, delivery, and the
proposing step), `apply_recovery_outputs`, `attempt_read`, and the
leadership-transfer step. Every one of them calls into a group method that can
reach `enter_poisoned`, and the drain runs after each on both paths. The
alternative — draining inside `route_report` — is wrong precisely for the case
that produced the finding: `route_report` does not run when the step fails, and
a failing step is the most likely place for a poison to have happened.

**Ordering against fix 1.** The drain runs before the step error is mapped for
the client, so a proposal that was captured resolves as `GroupPoisoned` and the
generic mapping never reaches it. `resolve_write` already refuses to overwrite
a waiter that has an outcome, so the ordering is the whole mechanism.

#### 3. Every report stream is routed or argued, one by one

`GroupStepReport` has eight streams. `route_report` handles three. This is the
per-stream disposition, and a stream with no row here would be a defect in this
subsection rather than a simplification of it.

**`peer_messages` — routed** (unchanged). `transport.send`, refusals counted.

**`proposal_events` — routed** (unchanged), with fix 1's mapping.

**`read_events` — routed** (unchanged), from the previous revision's fix 1.

**`snapshot_events::SendChunk` — routed, through a new transport method.**
`RaftOutput::SendSnapshotChunk` is the leader's only snapshot-send path: for
any follower whose progress is in `ProgressMode::Snapshot`,
`replicate_snapshot_to_follower` emits exactly one of these and nothing else
([`crates/rafter/src/node/replication/send.rs:147-153`](../crates/rafter/src/node/replication/send.rs)),
and the app layer records it as `SnapshotEvent::SendChunk`
([`crates/rafter-app/src/group/output.rs:474-480`](../crates/rafter-app/src/group/output.rs)).
Dropping it does not degrade a transfer; it removes the only mechanism by which
a follower below the leader's snapshot boundary can ever be caught up, on a
transport whose own contract already says snapshot transfers use
`InstallSnapshotChunk` frames
([`crates/rafter-service/src/transport.rs:69-74`](../crates/rafter-service/src/transport.rs)).

The directive is not a frame. `SnapshotChunkSend` carries `offset` and `len`
and no bytes, because the kernel never holds an application snapshot payload,
and the kernel names the layer that closes the gap: "The transport resolves
each directive against a source with `SnapshotChunkSend::resolve` before
putting the chunk on the wire, so payload bytes flow from the application's
snapshot store to the network without entering kernel state"
([`crates/rafter/src/types/snapshot/source.rs:9-19`](../crates/rafter/src/types/snapshot/source.rs)).
So the driver hands the directive to the transport and the transport resolves
it, which is what the kernel already says happens:

```rust
/// One leader snapshot chunk directive addressed to a peer.
///
/// A directive rather than a message, because the kernel holds no application
/// snapshot payload: `chunk` names the bytes by transfer, offset, and length,
/// and the transport reads them from the snapshot store with
/// [`rafter::SnapshotChunkSend::resolve`] before framing them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotChunkEnvelope<G> {
    pub group_id: G,
    pub from: NodeId,
    pub to: NodeId,
    pub chunk: SnapshotChunkSend,
}

pub trait RaftTransport<G>: Send + Sync + 'static {
    // ...
    /// Resolves one leader snapshot chunk directive and sends it.
    ///
    /// Implementations resolve `envelope.chunk` against their own
    /// [`rafter::SnapshotChunkSource`] and send the resulting
    /// [`rafter::Message::InstallSnapshotChunk`]. A directive the source
    /// cannot serve is dropped like a lost message, exactly as
    /// [`rafter::SnapshotChunkSend::resolve`] documents: the transfer resumes
    /// from the follower's acknowledged offset once the source and the kernel
    /// agree on the current snapshot again.
    ///
    /// # Errors
    ///
    /// Returns the transport implementation's error when the chunk cannot be
    /// sent. Like [`RaftTransport::send`], a refusal is counted rather than
    /// propagated to a client.
    fn send_snapshot_chunk(
        &self,
        envelope: SnapshotChunkEnvelope<G>,
    ) -> Result<(), Self::Error>;
}
```

**No default body.** The only default expressible here returns `Ok(())` and
drops the chunk, which is the defect with a signature on it. A required method
costs the two in-tree implementors one body each and makes it impossible for a
transport to disable snapshot transfer by omission. `AsyncRaftTransport` gets
the same method returning `TransportFuture<(), Self::Error>`, because its
delivery-semantics paragraph already claims parity with the synchronous trait
on exactly this point — "Snapshot chunk and message-size expectations are also
the same as the synchronous trait" — and a trait that made the claim without
the method would make the sentence false. It has no in-tree implementor, so the
cost is the sentence staying true.

**Why the driver does not resolve the chunk itself.** It would need a
`SnapshotChunkSource`, which means a sixth type parameter or a `dyn` field, and
the source belongs to the snapshot store, which belongs to the embedder's
storage — the same object that builds the transport. `DurableRaftNode` resolves
directives inside `step` because it owns its store
([`crates/rafter-runtime/src/lib.rs:301,707-727`](../crates/rafter-runtime/src/lib.rs)),
which is why a driver over the shipped runtime never sees a `SendChunk` at all
and why this routing exists for embedders with their own
`PersistedRaftRuntime` — of which this workspace has several.

**`snapshot_events::StageChunk` — argued out, with a contract citation.** The
receive side is already durable when the event exists.
`PersistedRaftRuntime` requires that "outputs that depend on hard-state, log,
snapshot, or compaction changes must not be released until those changes are
durably reflected"
([`crates/rafter-runtime-api/src/lib.rs:16-22`](../crates/rafter-runtime-api/src/lib.rs)),
and staging a chunk *is* that output's durability obligation.
`DurableRaftNode` discharges it in `persist_snapshot_outputs_for_step` before
it returns any output
([`crates/rafter-runtime/src/lib.rs:286,663-685`](../crates/rafter-runtime/src/lib.rs)).
A driver that staged again would write a second copy into a store the kernel
never reads back: recovery resumes a transfer from
`current_pending_snapshot_transfer` on the *runtime's* store
([`construction.rs:148-176`](../crates/rafter-runtime/src/construction.rs)),
so a driver-side staging area could only diverge from it. The event is a
notification of a completed write, and the honest routing of a notification the
driver has no second use for is none. `InMemoryRaftDriver` reaches the same
disposition by the same fact, which is why its `route_report` handles peer
messages alone.

This is the one place the fix is a written reason rather than a call, so it is
also the one place with a test whose job is to pin the reason: a report
carrying `StageChunk` steps cleanly, reaches no transport, and leaves the
driver healthy.

**`snapshot_events::Apply` — argued out.** The group installs the snapshot into
the state machine and advances its own applied floor before pushing the event
([`crates/rafter-app/src/group/snapshot.rs:38-67`](../crates/rafter-app/src/group/snapshot.rs)),
and the runtime promoted the staged snapshot and compacted the log before
releasing the output. Nothing is left to do; the boundary move is visible in
the metrics snapshot the driver publishes after every step.

**`membership_events` — routed, to `update_peers` and `fence_peer`.** The
transport's peer set is the link layer's copy of who may speak, and a driver
that never updates it leaves a joined replica unable to be heard and a removed
replica able to speak forever. The review reproduced the second half directly:
no `TransportRaftDriver` in the tree has ever called either method.

Routing needs a `NodeId → PeerPrincipal` direction the crate does not have.
`AuthenticatedPeerValidator` has the forward direction,
`node_for_authenticated_peer`, and the entry's own rejected alternative
explains why that direction lives on the validator rather than the transport:
"the crate separates authenticating a principal from deciding which Raft
replica that principal is". Naming the principal for a replica is that same
decision read the other way, and the one implementor in the tree proves the
object already holds both — `PeerDirectory` keeps a
`BTreeMap<PeerPrincipal, NodeId>` and answers the forward direction by lookup
([`reference/fenced-lock/tests/support/transport.rs:259-267`](../reference/fenced-lock/tests/support/transport.rs)).
So the inverse joins it:

```rust
pub trait AuthenticatedPeerValidator<G, P> {
    // ...
    /// Returns the principal this deployment issues to `node_id`, when it can
    /// name one.
    ///
    /// The inverse of
    /// [`AuthenticatedPeerValidator::node_for_authenticated_peer`], and the
    /// same policy: a validator that decides which replica a principal is, is
    /// the object that knows which principal a replica has. A driver
    /// needs this direction to publish a group's membership as a transport
    /// peer set and to fence a removed replica.
    ///
    /// `None` means this deployment cannot name a principal for `node_id`. A
    /// caller must not treat that as an empty peer set: see the all-or-nothing
    /// rule in the transport driver's membership routing.
    fn principal_for_node(&self, group_id: &G, node_id: NodeId) -> Option<P>;
}
```

This is the subsection's one change outside `rafter-service`, and it is a
transport-surface addition rather than a group-surface one: no `RaftGroup`
method, type, or report changes. It is required rather than defaulted, for the
reason `send_snapshot_chunk` is: the only possible default returns `None` for
every node, which disables membership routing silently. Four in-tree
implementors, three of them test fixtures.

The driver keeps the set it last published and moves it on two events:

| Event | Action |
| --- | --- |
| `Appended { membership }` — the effective configuration changed and has not committed | Publish the **union** of the current set and `membership.replica_ids()`. Never fence. A replica joining under joint consensus must be able to speak before the change commits, or it can never catch up and the change can never commit; and an uncommitted change can still be reverted, so nothing may be taken away here. |
| `Applied { membership }` — the committed configuration changed | Publish exactly `membership.replica_ids()`, then fence every principal that was in the previous set and is not in this one. Committed removal is the only fact that licenses fencing. |
| `Rejected { .. }` | Nothing. The change never entered the log. |

**Publishing at adoption, not only on change.** `new` and `adopt_group`
publish the group's current membership before serving anything. A driver that
published only on change would leave the transport's peer set undefined for the
whole first incarnation, which is the state the reproduction found: a
single-voter group whose membership never changes never told its transport
anything. The invariant this creates is checkable and is the one worth having —
*the transport's peer set is the driver's last-known membership, from
construction onward.*

**All-or-nothing.** A membership the validator cannot fully name is not
published. A partial peer set authorizes fewer replicas than the membership
has, which is a quorum-splitting configuration change performed by accident;
refusing to publish leaves the previous set in place, which is merely stale.
The refusal is counted:

```rust
/// Returns how many membership updates this driver could not publish.
///
/// Counted, not propagated, for the reason a refused send is: a peer-set
/// update that failed is a link-layer condition, and a write must not fail
/// because of one. A non-zero value means either the transport refused the
/// update or the validator could not name a principal for some replica in the
/// membership — two different faults with one consequence, that the link
/// layer's peer set is behind the group's.
#[must_use]
pub fn refused_peer_updates(&self) -> u64;
```

It is separate from `refused_sends` because a dropped frame is routine and Raft
re-sends, while a peer set that never updated does not repair itself.

**`leadership_transfer_events` — argued out, with the limitation named.**
`transfer_leadership` already reads them, from the report of the step it
issued, and resolves its own future
([`driver/transport.rs:683-700`](../crates/rafter-service/src/driver/transport.rs)).
There is no waiter table for transfers, because the operation is created and
resolved inside one call. A `Rejected` arriving in a later tick's report
belongs to a transfer whose future has already returned `Ok`, meaning "the
driver accepted the request" — which the trait's own doc says is all it means,
and which it explicitly tells callers to follow with a metrics observation
("callers that need completion semantics should observe metrics until the
target is reported as leader",
[`driver/trait.rs:37-48`](../crates/rafter-service/src/driver/trait.rs)). The
metrics snapshot the driver publishes after every step carries the role and the
leader hint, so the later fact reaches the caller through the surface the
contract already points at. Adding a transfer waiter table would be a new
promoted mechanism with no consumer behind it, which is the generalization the
promotion rule defers.

**`applied` — argued out.** The `ApplyResult`s were produced by applying them
to the state machine; by the time the report exists the effect has happened.
The client-visible subset is re-reported as `ProposalEvent::Applied` carrying
the same index, term, and result, which is the path that resolves a waiter, and
routing the stream again would resolve nothing twice. An embedder that wants
the full apply stream, including entries no local client proposed, reads it
through `with_group`.

**`metrics` — argued out, structurally.** The driver steps with
`StepReportOptions::without_metrics()`, so this field is always `None` in every
report this driver ever sees. It publishes `group.metrics()` itself after each
step, which is strictly fresher than the in-report snapshot would be: it is
taken after the report was routed rather than during the step.

#### 4. A barrier's own fault resolves that barrier, and only that barrier

The previous revision's `fail_read` attributes exactly two group errors to
their barrier and propagates everything else:

```rust
if !matches!(
    error,
    GroupError::NonMonotonicReadId { .. } | GroupError::DuplicateReadId { .. }
) {
    return Err(ManagedDriverError::Group { cause: ErrorCause::new(error) });
}
```

That list is not the set of per-barrier faults; it is the set of faults that
subsection anticipated. The review found the obvious omission by making a state
machine refuse a query. `GroupError::StateMachine { operation: Read }` escapes
`drive_pending_reads`, and three things go wrong at once. The failing barrier's
client is not resolved. Every other barrier in the same pass is skipped,
including ones that never failed — the reproduction has a second barrier,
granted and ready, that is simply not served. And the failure is
unrecoverable in a way the client is then lied to about: `read_linearizable`
removes the completed proof from `completed_query_reads` *before* running the
state-machine read
([`crates/rafter-app/src/group/read.rs:384-392`](../crates/rafter-app/src/group/read.rs)),
so the proof is consumed and gone, the next pass re-reserves a spent `ReadId`,
and `fail_read`'s surviving arm resolves the client with
`ReadError::ManagedInvariantViolation` whose message reads "a terminal read
event was not routed". No terminal event was ever emitted; the state machine
refused the query, and the cause that says so is discarded.

**Every group error from a read call is attributable to that read's barrier.**
`RaftGroup::read` is called with one `ReadRequest` naming one `ReadId`, so
there is no second barrier the error could be about. `fail_read` therefore
resolves the barrier with `read_error_from_group(error)` — fix 5 — and returns
`Ok(())` so the loop continues. The two anticipated arms are not special-cased
away: `read_error_from_group` already maps `NonMonotonicReadId` and
`DuplicateReadId` to `ManagedInvariantViolation` with a message naming the ID
invariant, which is the correct report for those and only those.

`ReadError::StateMachine { operation: Read, cause }` becomes the answer to the
reproduction, with the state machine's own error preserved under `source()`.

**What still leaves `drive_pending_reads`.** Only
`ManagedDriverError::NoGroup`. Nothing else in the method can fail without
being about one barrier, so the doc comment's error clause narrows to that, and
becomes true rather than aspirational. The previous revision argued for exactly
this shape one level up — "one barrier's fault must not deny service to the
rest" — and then implemented it for two error variants; this is that argument
applied to the rest.

**The invariant-violation message survives, and its claim narrows.** It is now
reached only from the two ID variants, where "this driver holds a waiter for a
barrier its group no longer tracks" is literally what happened. It is no longer
reachable for a state-machine refusal, which is the misattribution the review
named.

#### 5. Group failures reach clients through the typed mapping

`write_error_from_group` and `read_error_from_group` exist, are used by
`InMemoryRaftDriver`, and are the reason
[Typed Service Failure Surface](#typed-service-failure-surface) can claim that
`Poisoned`, `Storage`, and `StateMachine` are categories rather than strings.
`TransportRaftDriver` calls neither. Every group failure it reports arrives as
`WriteError::Transport` or `ReadError::Transport` wrapping a
`ManagedDriverError` wrapping the `GroupError` — so on the driver the entry
exists to make usable, a
poisoned group is reported as a transport fault, and the reproduction shows
both sides of it at once:

```
write=Transport { fate: NotAppended, cause: Group { cause: Poisoned { .. } } }
read=Transport  { cause: Group { cause: Poisoned { .. } } }
```

Three call sites change, and none of them is a new mapping — each is the
existing one:

| Site | Was | Becomes |
| --- | --- | --- |
| `begin_write`'s failing step | `Transport { fate: inferred, cause: ManagedDriverError }` | fix 1's three-way decision, ending in `write_error_from_group(error, WriteFate::Unresolved)` |
| `attempt_read`'s failing read | propagated, or `ManagedInvariantViolation` | `read_error_from_group(error)` |
| The poison drain | nothing | fix 2's vocabulary |

To reach them, the state's `step` splits: an inner form returning the
`GroupError` unchanged, and the outer `ManagedDriverError` form that `tick` and
`deliver` keep. `WriteError::Transport` and `ReadError::Transport` stay for what
they name — a driver that could not route or deliver, which is the waiter
bound, the missing group, and a genuine transport fault.

#### 6. A dropped client future reclaims its waiter

`poll_write` and `poll_read` remove a waiter when a poll takes its outcome.
Nothing removes one whose outcome was taken by nobody. The documented
supervisor drain is exactly that shape — a client times out and drops its
future, the supervisor abandons the waiter to free its slot — and abandonment
resolves rather than removes, deliberately, so the future can still answer. The
review filled a bounded driver four times over with dropped futures and found
four resolved waiters that nothing will ever poll, each holding its cloned
`ReadRequest`, permanently.

The slot accounting is not what leaks: `max_pending_waiters` counts unresolved
waiters, so the bound is respected. The `BTreeMap` entries are what leak, and
they leak once per timed-out client for the life of the driver.

**Chosen: the future owns its waiter, and dropping it is the reclamation.**
Each client future carries a guard whose `Drop` removes its own waiter from the
table if the future did not already poll it out.

```rust
/// Removes a waiter whose client future was dropped.
///
/// The future is the only thing that can consume a resolved outcome, so a
/// dropped future is the moment the waiter provably has no reader. A read that
/// was still unresolved has its barrier cancelled through the group first, so
/// `reserved_reads` returns to its previous value: a client that stopped
/// listening must not leave a barrier reserved in the group any more than it
/// leaves a waiter in the driver.
fn discard_write(&mut self, local_proposal_id: LocalProposalId);
fn discard_read(&mut self, read_id: ReadId);
```

This composes with the previous revision's rule rather than replacing it, and
the composition is the whole design:

- **Resolve, do not remove** still holds for abandonment. `abandon_write` and
  `abandon_read` write an outcome and leave the entry in place, so a caller
  that abandons and still holds its future gets `DriveBoundReached` on the next
  poll. The lock does exactly this, one line later.
- **The late poll is safe** because the entry is still there. There is no
  window: only the future's own `Drop` removes it, and a future cannot be
  dropped and polled.
- **A dropped future frees everything**, table entry included, without anyone
  having to call `abandon_*` at all.
- **Abandoning after a drop returns `false`.** The waiter is gone, so nothing
  is retired. That is the honest answer — abandonment resolves a client, and
  there is no client — and it is the one behavioural difference a caller can
  observe. It is recorded in `abandon_write`'s and `abandon_read`'s doc
  comments, which already promise "returns whether a waiter was retired".
- **`pending_writes` and `pending_reads` sharpen.** They now answer "what is
  this driver holding for a client that is still listening", which is a
  strictly better answer to the question a draining supervisor asks than "what
  is this driver holding, including for clients that left".

**Rejected: reclaim on a later sweep.** A sweep needs a rule for when a
resolved-and-unpolled waiter is dead, and there is no such rule: a future may
be parked for an arbitrary time before its executor polls it. Any time-based or
count-based sweep either drops an answer a live client was about to read, or
does not bound the table. Drop is the exact signal, and it is free.

**Rejected: remove on resolution and keep a terminal ledger.** It answers the
late poll from a side table, and the side table is the original problem with an
extra hop.

#### 7. Four smaller corrections

**Fabricated IDs are replaced by the refusal they were standing in for.** A
released driver answers `begin_write` with
`UnknownOutcome { local_proposal_id: LocalProposalId(0), .. }` and `begin_read`
with `Abandoned { read_id: ReadId(0), .. }`. Both IDs name operations that do
not exist, and `LocalProposalId(0)` is a value a caller can compare against a
real allocation. Neither operation started, so neither is an outcome to be
unknown about: they are refusals, and the driver already has a refusal shape
for "could not route this" — the one the waiter bound uses,
`WriteError::Transport { fate: NotAppended, cause: DriverRoutingError::_ }`.
`DriverRoutingError` gains `NoGroup`, and reads take the same shape without a
fate. No public variant is added; `DriverRoutingError` is internal and reaches
callers as a preserved `source()`.

**`adopt_group` refuses after a completed shutdown.** Today it clears
`shutting_down`, so `shutdown` → `release_group` → `adopt_group` produces a
driver serving again — which the review recorded as observed behaviour, because
the vocabulary calls shutdown terminal and `shutdown` itself refuses a second
call with `ShutdownError::AlreadyShutDown`. The entry's own sentence is "a
supervisor restarting a replica calls release; a supervisor stopping one calls
shutdown and then release", which makes shutdown the stopping path. A stopping
path that can be walked backwards is not a stopping path. `adopt_group`
therefore returns `ManagedDriverError::ShuttingDown` when the driver has shut
down, and stops clearing the flag; a supervisor that wants to serve again
builds a driver. The restart path — `release_group` then `adopt_group`, without
a shutdown — is untouched, which is the path the entry exists for.

**`min_applied_index` carries the caller's floor.** Both drivers hardcode
`None`, and the app layer documents what that discards: a floor is "honored
verbatim … A caller may be expressing 'at least as fresh as the write I already
observed', and Rafter must not silently weaken that", with
`ProposalEvent::Applied` named as its natural source
([`crates/rafter-app/src/read.rs:49-59`](../crates/rafter-app/src/read.rs)). A
client that just received a `WriteReceipt` and wants to read its own write has
no way to say so, and no workaround: the floor is a property of the barrier and
cannot be applied afterwards.

The shape is the one `WriteOptions` already established, so reads stop being
the asymmetric half of the pair:

```rust
/// Per-read options.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct ReadOptions {
    /// The applied index this read must observe, if the caller has one.
    ///
    /// Honored verbatim by the app layer: it is not capped at the read index
    /// and not lowered. The natural source is the `index` of a
    /// [`WriteReceipt`] the caller already holds, which always names an
    /// application entry. An index taken from a commit index, a read index, or
    /// a snapshot boundary may name an entry the state machine is never told
    /// about; convert it with
    /// [`TransportRaftDriver::committed_application_index`] first.
    pub min_applied_index: Option<LogIndex>,
}

pub trait DriverCommandSender<G, C, Q, R, QR>: Clone + Send + Sync + 'static {
    fn read(
        &self,
        group_id: G,
        query: Q,
        consistency: ReadConsistency,
        options: ReadOptions,
    ) -> DriverFuture<Result<QueryReceipt<G, QR>, ReadError>>;
}

impl<G, C, Q, R, QR, S> RaftHandle<G, C, Q, R, QR, S> {
    /// Runs a query that must observe at least the caller's own floor.
    pub async fn read_with_options(
        &self,
        query: Q,
        consistency: ReadConsistency,
        options: ReadOptions,
    ) -> Result<QueryReceipt<G, QR>, ReadError>;
}
```

`RaftHandle::read` keeps its signature and passes `ReadOptions::default()`, so
every call site that does not want a floor is unchanged — the trait has four
implementors in the tree, two of them test fixtures. Both drivers thread the
value into the `ReadRequest` they build; the transport driver stores it on the
waiter, because the app layer requires a retry to present the same freshness or
be refused with `GroupError::DuplicateReadId`
([`group/read.rs:384-400`](../crates/rafter-app/src/group/read.rs)), and
`drive_pending_reads` is that retry.

**`metrics()` after a release is stale, and says so.** The watch stays open
across a release, which is correct — a handle names a service rather than an
incarnation, and re-adoption republishes — but the last snapshot describes the
retired incarnation and nothing refreshes it in between. Closing the watch
would break re-adoption; publishing an empty snapshot would require inventing a
`RaftGroupMetrics` for a group that does not exist. So the boundary is
documented on `release_group` rather than signalled: the last published
snapshot is the retired incarnation's until `adopt_group` publishes the new
one, and the surface that distinguishes "released" from "idle" is
`with_group`/`committed_application_index`, which answer
`ManagedDriverError::NoGroup` and are the reason those methods exist.

#### Blast radius of the second revision

| File | Change |
| --- | --- |
| [`crates/rafter-service/src/driver/transport/state.rs`](../crates/rafter-service/src/driver/transport/state.rs) | Fate mapping; poison drain; snapshot and membership routing; per-barrier read failures; typed mapping; `discard_write`/`discard_read`; `min_applied_index` on the waiter |
| [`crates/rafter-service/src/driver/transport.rs`](../crates/rafter-service/src/driver/transport.rs) | Waiter guards on the client futures; `refused_peer_updates`; `adopt_group` refuses after shutdown; membership published at construction and adoption |
| [`crates/rafter-service/src/driver/mapping.rs`](../crates/rafter-service/src/driver/mapping.rs) | `DriverRoutingError::NoGroup` |
| [`crates/rafter-service/src/driver/options.rs`](../crates/rafter-service/src/driver/options.rs) | `ReadOptions` |
| [`crates/rafter-service/src/driver/trait.rs`](../crates/rafter-service/src/driver/trait.rs) | `DriverCommandSender::read` takes `ReadOptions` |
| [`crates/rafter-service/src/driver/read.rs`](../crates/rafter-service/src/driver/read.rs), [`in_memory.rs`](../crates/rafter-service/src/driver/in_memory.rs) | The in-memory driver threads the floor |
| [`crates/rafter-service/src/handle.rs`](../crates/rafter-service/src/handle.rs) | `read_with_options` |
| [`crates/rafter-service/src/transport.rs`](../crates/rafter-service/src/transport.rs) | `SnapshotChunkEnvelope`; `send_snapshot_chunk` on both transport traits |
| [`crates/rafter-app/src/transport.rs`](../crates/rafter-app/src/transport.rs) | `AuthenticatedPeerValidator::principal_for_node` |
| [`crates/rafter-service/src/lib.rs`](../crates/rafter-service/src/lib.rs) | Re-export `ReadOptions` and `SnapshotChunkEnvelope` |
| [`reference/fenced-lock/tests/support/transport.rs`](../reference/fenced-lock/tests/support/transport.rs) | The two new trait methods |

Breaking for implementors of `RaftTransport`, `AsyncRaftTransport`,
`AuthenticatedPeerValidator`, and `DriverCommandSender`, and for direct callers
of `DriverCommandSender::read`. Every break is a required method or parameter
whose default would be a silent wrong answer, which is the criterion the
pre-1.0 rule asks for. `RaftHandle`'s surface is additive.

The behavioural breaks are the ones worth naming, because they change what a
correct caller sees: a failing write step now reports `Unresolved` where it
reported `NotAppended`, a poisoned group now reports `Poisoned` and
`UnknownOutcome` where it reported `Transport`, a per-barrier read failure now
resolves its client where it returned an error to the driver, and
`abandon_write`/`abandon_read` now return `false` for a waiter whose future was
already dropped.

#### Focused-test plan for the second revision

Every reproduction the review supplied becomes a regression test with its
assertion inverted, keeping its fixture and its name where the name still
describes the scenario.

In `crates/rafter-service/tests/transport_driver.rs`:

- `a_poisoning_apply_reports_unknown_for_an_entry_that_is_committed` — the
  fate reproduction. Single voter, failing apply; assert the client receives
  `UnknownOutcome { GroupPoisoned }` and that the entry really is committed, so
  the test still proves the fate mattered.
- `both_drivers_call_the_same_poisoning_apply_unknown` — the review's
  two-driver fixture, now asserting the answers agree.
- `a_write_with_no_proposal_lifecycle_event_is_unresolved_rather_than_refused`
  — `ProposalDidNotStart` maps to `UnknownOutcome { RuntimeDroppedProposal }`.
- `a_poison_resolves_every_in_flight_waiter` — the two-voter reproduction;
  assert the write resolves as `GroupPoisoned`, a concurrent read as
  `ReadError::Poisoned`, and that the group's `poisoned_waiters` is drained.
- `a_poisoned_group_reports_poisoned_rather_than_transport` — the vocabulary
  reproduction, for both a write and a read.
- `a_snapshot_chunk_directive_reaches_the_transport` — the snapshot
  reproduction, over a transport that resolves against an in-memory source;
  assert an `InstallSnapshotChunk` frame is observed and a refused directive
  increments `refused_sends`.
- `a_staged_chunk_is_the_runtimes_obligation_and_reaches_no_transport` — the
  argued-out half, pinned.
- `membership_reaches_the_transports_peer_set` — the membership reproduction;
  assert the peer set is published at construction, widened on `Appended`,
  narrowed on `Applied`, and that a removed replica is fenced.
- `a_membership_the_validator_cannot_name_is_not_published` — all-or-nothing,
  with `refused_peer_updates` incremented.
- `a_dropped_read_future_reclaims_its_waiter_and_its_barrier` — the
  unbounded-growth reproduction; four reads started and dropped leave no
  waiters, no reserved reads, and a full budget.
- `an_abandoned_waiter_still_answers_a_late_poll` — the composition rule:
  abandon, then poll the still-held future, and get `DriveBoundReached`.
- `abandoning_a_waiter_whose_future_was_dropped_retires_nothing`.
- `a_released_driver_refuses_without_fabricating_an_id` — both sides.
- `adoption_does_not_reverse_a_completed_shutdown` — the review's
  `observed_` probe, inverted to `ManagedDriverError::ShuttingDown`.
- `a_read_floor_is_honored_verbatim` — a read whose `min_applied_index` is
  above the local applied index stalls rather than answering, and answers once
  the state machine catches up.

In `crates/rafter-service/tests/read_waiters.rs`, from the review's second
file:

- `one_barriers_state_machine_failure_resolves_only_that_barrier` — the denial
  reproduction; assert the failing client gets
  `ReadError::StateMachine { operation: Read }` with its cause preserved, the
  second barrier is served in the same pass, and `drive_pending_reads` returns
  `Ok`.
- `a_refused_query_is_reported_as_a_state_machine_failure` — the
  misattribution reproduction, asserting the cause downcasts to the state
  machine's own error.
- The review's seven held-under-attack probes are kept as written; they pass
  before and after, and their job is to fail if a fix moves something it was
  not meant to move.

### Third revision after adoption (2026-07-25)

A second adversarial hunt attacked the subsection above and reproduced five
findings. Four of them are one of those fixes failing on its own written terms —
a fence the fix exists to apply, dropped; a call site the fix enumerated by
name, missed; a sentence the fix wrote into a doc comment, made false by the
fix's own mechanism; a rule the fix stated, applied to one of the three
situations that satisfy it. The fifth is the same fate rule read one step
further. Nothing here reopens the entry's shape either.

A third adversarial hunt attacked these fixes and reproduced two more, both of
them fix 1 below failing on its own written terms at the one call site it never
visited, and a craft pass over the same surface found five. They are designed in
[Fourth revision after adoption](#fourth-revision-after-adoption-2026-07-26),
which is current truth wherever the two disagree; the one place they disagree is
fix 1's scope — it establishes a rule about publishing a membership and reaches
two of the three publishers that owe it — and it is named there.

The rule the second revision stated still governs — *a driver reports what it
observed, and says "unknown" for everything else* — and this subsection adds the
one the hunt makes unavoidable: **a promise in a doc comment is a claim about
the code under it, so the test that carries the fix's name covers every branch
the claim covers.** Three of the five findings were reachable because a named
test covered a fraction of its stated scope, and each fix below says which
branches its test now exercises.

#### 1. A committed removal is fenced whether or not the peer set could be published

`publish_membership` decides two things and had one exit. It computes the
principals for the new peer set, and on the first replica the validator cannot
name it counts a refusal and returns — before the fencing loop, which is the
other half of the method. So a membership event that both narrows the set and
licenses a fence applies neither, and the replica the cluster committed the
removal of is fenced by nothing.

The second revision justified the refusal like this: "refusing to publish leaves
the previous set in place, which is merely stale." Both halves are false as the
code implements them. The previous set is *not* left in place — `known_members`
is assigned before the loop that refuses, so the driver's own record has already
advanced past the removal, and no later `Applied` can re-derive it because the
difference it would be derived from is gone. And the consequence is not
staleness. `update_peers` is the admission boundary in the consumer that adopted
this driver, so both of the controls a committed removal is supposed to install
— a peer set that no longer lists the replica, and a fence against it — are
missed at once, permanently, by a driver that reports its group healthy. That is
the exact hazard the fix exists to close, reached through the fix.

**The sentence is true of the peer set and false of the fence, so the two
separate.**

```rust
/// Publishes the current membership as a peer set, or publishes nothing.
///
/// All or nothing, for the reason the second revision gives: a partial peer
/// set authorizes fewer replicas than the cluster has.
fn update_transport_peers(&mut self);

/// Fences every principal a committed removal took out of the set.
///
/// Per principal rather than all-or-nothing, because the two statements have
/// different shapes. A peer set is one statement about a whole cluster, and a
/// partial one is a quorum-splitting configuration change made by accident. A
/// fence is one statement about one replica, and fencing three of four removed
/// replicas is strictly better than fencing none of them.
fn fence_removed_peers(&mut self, removed: Vec<NodeId>);
```

`publish_membership` calls both, in that order, on every path. A replica the
validator cannot name is still counted — `refused_peer_updates` already
documents itself as covering "either the transport refused the update or the
validator could not name a principal", and an unfenceable removal is the second
of those.

`known_members` still advances when publication was refused, and that is not the
defect. Its own doc comment gives the reason and the reason is right: it is the
driver's record of what the group says, not of what the link layer accepted, so
a later committed removal is computed against the membership the cluster had.
Once the fence runs unconditionally, advancing it is what makes the next removal
correct too.

**This fix was applied to the two publishers it reached through an event and
not to the third.** `publish_adopted_membership` derives no membership from an
event, and neither half of the split above visits it; it read the effective
configuration and withheld the fence, which is both of the hazards this
subsection names, at a call site reachable from two public constructors. The
correction, and the caller enumeration that would have caught it, are
[Fourth revision after adoption](#fourth-revision-after-adoption-2026-07-26).

**The named test covered a quarter of its stated scope.** The second revision
asked `membership_reaches_the_transports_peer_set` to "assert the peer set is
published at construction, widened on `Appended`, narrowed on `Applied`, and
that a removed replica is fenced". What shipped was
`membership_is_published_to_the_transport_at_construction`, which asserts the
first clause; `fence_peer` had no caller in any test in the workspace, which is
how a fix whose whole subject is fencing shipped without fencing anything. The
test returns to its name and its four clauses.

One of the four cannot be reached the way the others can, and the honest place
to say so is here. `MembershipEvent::Appended` is emitted only when the step
carried a membership *request*
([`crates/rafter-app/src/group/membership.rs:60-63`](../crates/rafter-app/src/group/membership.rs)),
which means `GroupInput::Membership` — an input `TransportRaftDriver` has no
public method that produces, and deliberately: a membership-change API on this
driver is a promoted mechanism with no consumer behind it. So the widening arm
is live code with no public route to it today, and it is pinned by an in-crate
test that hands the router the event directly. Recording that is the point: the
arm is not dead, it is waiting for the entry point, and a test that quietly
skipped it is how this finding was possible.

#### 2. The drain runs on the leadership-transfer step, which fix 2 named

Fix 2 chose its call sites by "can this poison" and listed four: "`step` (tick,
delivery, and the proposing step), `apply_recovery_outputs`, `attempt_read`, and
the leadership-transfer step." The first three route through the state's
`step_group`, which drains on both paths. The fourth calls
`group_mut().step_with_options(...)` directly and drains on neither.

The reproduction poisons on the transfer step: a tracked proposal is appended
and pending, the transfer step commits and applies it, the state machine refuses
the apply, and the group poisons — capturing the proposal on the way down. When
`transfer_leadership` returns, the client is unresolved and the group is still
holding what it captured.

The strand is a deferral rather than a permanent loss, because any later call
through `step_group` drains on its error path. Naming that is what keeps the
severity honest, and it is also what makes the fix worth making rather than
merely tidy: the rescue is incidental, and the one supervisor behaviour the
entry actually documents does not produce it. A supervisor whose reaction to a
failed transfer is `release_group` gets `DriverReleased` — the driver retired
the incarnation — for a client whose group had already poisoned under it. Two
different facts, and the driver reports the one it did not observe first.

The transfer step needs one thing out of its report before the report is routed,
which is why it stepped the group itself: whether the target was rejected. That
is a reason to have a second entry point, not a reason to bypass the drain, so
the state grows one:

```rust
/// Steps the group with a leadership transfer, reporting the rejection it saw.
///
/// Everything else is [`TransportDriverState::step_group`], the poison drain on
/// both paths included. The only difference is the return value: a transfer has
/// no waiter table, so the one fact its caller needs is read out of the report
/// before the report is routed and consumed.
fn step_transfer(
    &mut self,
    target: NodeId,
) -> Result<Option<TransferLeadershipError>, StepFailure<A::Error, R::Error>>;
```

**Ordering, which is fix 2's own.** The drain runs before the step error becomes
the transfer's error, and `resolve_write` keeps the first outcome, so a proposal
the poison captured answers `GroupPoisoned` and the transfer's own failure
reaches only the transfer's caller. The transfer future is unaffected: it still
resolves from the rejection event or from `transfer_error_from_group`.

#### 3. A dropped client future reclaims its waiter without re-entering the lock

Fix 6 made a future's `Drop` the reclamation, and reclamation takes the driver's
lock. `with_group` documents the rule the other way round:

```rust
/// The closure runs with the driver locked, so it must not call back into
/// this driver. A shared borrow of the group offers no way to.
```

The second sentence is now false, and not because a closure found a way to call:
dropping a value the closure already owns is not a call, needs no borrow of
anything, and reaches `discard_write`/`discard_read` through `Drop`.
`std::sync::Mutex` is not reentrant, so the thread stops there. The reproduction
hangs on both sides — a write future and a read future — and needs a watchdog to
report rather than to hang the suite.

`with_group` is only the *documented* place a caller's code runs under this
lock. The driver runs an embedder's code under it in four more:
`RaftTransport::send` and `send_snapshot_chunk` and `update_peers` and
`fence_peer` from `route_report`, `ReplicatedStateMachine::apply` and `read`
from inside a group step, and `Waker::wake` from `resolve_write` and
`resolve_read` — the last of which reaches an arbitrary executor. Any of them
may own a client future: a transport that kept one to retry, a task a waker
resumes inline. Each is the same deadlock, and each is invisible until it
happens.

**Chosen: reclamation never blocks.** `Drop` asks for the lock and does not wait
for it. A guard that cannot have it puts its waiter on a side queue that the
next lock acquisition drains, so the reclamation is deferred rather than lost
and nothing on the drop path can block.

```rust
/// One driver's shared state, and the reclamations that could not run yet.
///
/// Two locks rather than one, because the second exists to be takeable when the
/// first is not. A client future's `Drop` may run on a thread that already
/// holds `state` — inside `with_group`, inside a transport call, inside a state
/// machine apply, inside a waker — and a `Drop` that waited for `state` there
/// would stop that thread forever.
pub(super) struct DriverShared<G, A, R, T, V> {
    state: Mutex<TransportDriverState<G, A, R, T, V>>,
    /// Waiters whose client futures were dropped while `state` was held.
    ///
    /// Locked only across a push and a take. No group method, no transport
    /// call, and no embedder code ever runs under it, so it is a leaf and
    /// cannot be the next revision's version of this finding.
    deferred: Mutex<Vec<WaiterId>>,
}

impl<G, A, R, T, V> DriverShared<G, A, R, T, V> {
    /// Locks the driver, reclaiming anything a drop had to defer first.
    fn lock(&self) -> MutexGuard<'_, TransportDriverState<G, A, R, T, V>>;

    /// Reclaims one dropped future's waiter now, or leaves it for the next
    /// acquisition.
    fn reclaim(&self, waiter: WaiterId);
}
```

**Why `try_lock` decides it, and why it cannot decide wrongly.** `try_lock`
hands out a `MutexGuard`, so it cannot succeed on a thread that already holds
one: the two guards would alias the same `&mut`. A re-entrant drop therefore
always takes the deferred path, by the type system rather than by a platform
detail. The converse mistake costs nothing — deferring because another thread
happened to hold the lock reclaims at the next acquisition, and every public
method of the driver acquires.

**The drain takes batches until the queue is empty.** Reclaiming a read
publishes metrics, publishing metrics wakes watchers under the lock, and a woken
task may drop another future, which defers onto the queue the current holder is
already draining. So the drain loops. It terminates for the reason the table is
bounded at all: each entry comes from one guard, and a guard reclaims once.

**What `with_group` can promise now.** The rule stays — the closure must not
call into this driver, because a second `lock` on the same thread is a deadlock
and `with_group` cannot prevent one: the closure is an `impl FnOnce` and may
capture a driver clone, which the deleted sentence was wrong about too. What
replaces that sentence is the guarantee a caller needs and can rely on:
**dropping a value is not calling in.** A client future of either kind, resolved
or not, may be dropped inside the closure — and inside a transport call, a state
machine apply, or a waker — and its waiter is reclaimed with no lock taken on
the dropping thread. Polling one is still a call, and still forbidden.

**Corrects fix 6's "no window" claim, without weakening it.** That fix argued
the late poll is safe "because the entry is still there. There is no window:
only the future's own `Drop` removes it, and a future cannot be dropped and
polled." Both sentences hold, and a deferred entry opens no window, because a
deferred entry belongs to a future that has already been dropped and can
therefore never be polled. What the argument did not say is the thing this fix
supplies: `Drop` must be able to *run* everywhere a future can be dropped.

**Rejected: remove the waiter without the lock.** The tables are `BTreeMap`s
inside the state. Making them concurrent maps would move the waiter tables
outside the invariant that everything one step observes is decided under one
lock, to buy a removal path for a case the deferral already handles.

**Rejected: a reentrant lock.** It would let a `with_group` closure call `tick`
and step the group from inside a `route_report`, which is the aliasing the
`&RaftGroup` borrow exists to prevent. The defect is not that the lock refuses
re-entry; it is that `Drop` asked for it.

#### 4. A refusal the group made before it proposed is `NotAppended`

The second revision's fate table ends with "any other group error from the
proposing step → `Unresolved`, because the driver asked a group to propose and
did not learn what happened." That row is right for a group error that is
genuinely opaque, and two of the errors falling into it are not opaque at all.
They are the same shape as the row above them —
`GroupError::NonMonotonicLocalProposalId`, which the table already recognises as
"the group refuses before it proposes" — so the rule does not change; two more
situations are recognised as satisfying it.

| Group error | Fate | Why the refusal is the whole event |
| --- | --- | --- |
| `GroupError::Poisoned` | `NotAppended` | `reject_if_poisoned()` is `step_with_options`'s first statement and the only producer of the variant ([`crates/rafter-app/src/group/poison.rs:14-21`](../crates/rafter-app/src/group/poison.rs)). A step that *becomes* poisoned reports `StateMachine` or `MalformedSnapshot` instead, so this variant means the group refused before it looked at the proposal. |
| `GroupError::StateMachine { operation: EncodeCommand }` | `NotAppended` | `step_proposal` encodes before it inserts into `pending_proposals` and before it calls `raft.step` ([`group/proposal.rs:230-242`](../crates/rafter-app/src/group/proposal.rs)), and the service layer's own mapping comment already says encoding touches no storage. |

Every other `StateMachine` operation stays `Unresolved`. `Apply` and
`ApplyBatch` run after the append, on an entry the log already holds, and that
is precisely the case fix 1 exists for.

The poisoned row is worth naming separately because of how common it is: it is
the fate of *every write after the first* on a poisoned replica, which is the
most-travelled failing-write path the driver has. Telling those callers
`Unresolved` says their request identity may be spent and forecloses the retry
under a fresh identity that is, here, exactly the safe thing to do.

#### 5. Two statements the public surface makes that the code stopped honoring

**`drive_pending_reads` still describes the driver it replaced.** Its doc opens
"Retries every unresolved read barrier. A granted barrier is consumed by a later
read call rather than announced by an event, so a driver that only ticks and
delivers leaves granted proofs uncollected." The first revision's fix 1 made
grants announced and its fix 5 made the retry grant-gated: the method now
attempts exactly the barriers a routed `ReadEvent::Granted` named, and is a
no-op for the rest. Its error clause is stale for the same reason one level
down — the second revision's fix 4 narrowed what can leave this method to
`ManagedDriverError::NoGroup`, and said so ("the doc comment's error clause
narrows to that, and becomes true rather than aspirational"), and the doc
comment was not narrowed. Both are corrected against the private
`drive_pending_reads` beneath them, which already says the true thing.

**`refused_sends` counts one thing it does not name, and that is the right
place for it.** It reads "how many outbound frames the attached transport
refused", and since the second revision it also counts a refused
`send_snapshot_chunk` — which is a directive, not a frame, as that same
subsection is at pains to establish.

The question the split of `refused_peer_updates` raises is whether the chunk
directive deserves its own counter too, and that subsection states the criterion
that answers it: the two counters are separate "because a dropped frame is
routine and Raft re-sends, while a peer set that never updated does not repair
itself". A refused chunk directive repairs itself, and the kernel says so in the
contract the driver routes it under — "the transfer resumes from the follower's
acknowledged offset once the source and the kernel agree on the current snapshot
again". So a refused directive belongs with the refused frames by the criterion
already written down, and `refused_sends` is corrected to name both producers
rather than split. A third counter would assert a distinction the contract
denies.

#### Blast radius of the third revision

| File | Change |
| --- | --- |
| [`crates/rafter-service/src/driver/transport/state.rs`](../crates/rafter-service/src/driver/transport/state.rs) | Membership publication splits into peer-set and fence halves; `step_transfer`; `DriverShared` |
| `crates/rafter-service/src/driver/transport/state/tests.rs` | New: the membership branch with no public entry point. Split out rather than inlined because the file-size guard's hard limit is a thousand lines and `state.rs` was at 1,023 with the module in it |
| [`crates/rafter-service/src/driver/transport.rs`](../crates/rafter-service/src/driver/transport.rs) | The shared state becomes `DriverShared`; the guard reclaims through it; the transfer routes through `step_transfer`; `with_group`, `drive_pending_reads`, and `refused_sends` doc comments |
| [`crates/rafter-service/src/driver/transport/waiters.rs`](../crates/rafter-service/src/driver/transport/waiters.rs) | Two more `NotAppended` situations in `write_failure` |
| [`crates/rafter-service/tests/transport_streams.rs`](../crates/rafter-service/tests/transport_streams.rs) | `membership_reaches_the_transports_peer_set` at its stated scope, plus the fence reproductions |
| [`crates/rafter-service/tests/transport_waiters.rs`](../crates/rafter-service/tests/transport_waiters.rs) | The re-entrancy reproductions and the concurrency probes |
| [`crates/rafter-service/tests/transport_failures.rs`](../crates/rafter-service/tests/transport_failures.rs) | The two fate reproductions and the transfer-drain reproductions |

Entirely internal. No public signature, variant, or trait method changes, and
the three behavioural changes a correct caller can see are: a committed removal
now fences even when the peer set could not be published, a write refused by an
already-poisoned group or by a failing encoder now reports `NotAppended` where
it reported `Unresolved`, and dropping a client future under the driver's lock
now returns instead of hanging.

#### Focused-test plan for the third revision

Every reproduction becomes a regression test with its assertion inverted,
keeping its fixture and keeping its name wherever the name still describes the
scenario rather than the defect.

In `crates/rafter-service/tests/transport_streams.rs`:

- `membership_reaches_the_transports_peer_set` — the named test at its stated
  scope: published at construction, widened on `Appended` and never narrowed
  there, narrowed on `Applied`, and the removed replica fenced. The widening
  clause is the one with no public route, so it is pinned in-crate; the other
  three run against a scripted runtime whose committed membership shrinks.
- `a_committed_removal_is_fenced_even_when_the_peer_set_cannot_be_published` —
  the reproduction. One replica unnameable, the removed replica nameable;
  assert the fence, and assert `refused_peer_updates` still counts the
  publication that did not happen.
- `a_removed_replica_cannot_speak_after_the_publication_was_refused` — the
  consequence, at the boundary a consumer sees: `deliver` refuses the frame.
- `an_unfenceable_removal_is_counted_rather_than_silent` — the removed
  replica is the one the validator cannot name; nothing is fenced, and
  `refused_peer_updates` says so.

In `crates/rafter-service/tests/transport_failures.rs`:

- `a_poison_on_the_leadership_transfer_step_resolves_its_client` and
  `the_leadership_transfer_step_drains_the_groups_poisoned_waiters` — the two
  halves of the transfer reproduction, on the `Err` path.
- `releasing_after_a_transfer_poison_reports_the_poison` — the supervisor
  reaction the entry documents, asserting `GroupPoisoned` rather than
  `DriverReleased`.
- `a_transfer_that_is_rejected_still_resolves_its_own_future` — the `Ok` path
  through the new entry point, so routing the rejection is not lost to the
  drain.
- `a_write_to_an_already_poisoned_group_is_not_appended` and
  `an_encode_failure_is_not_appended` — the two fate reproductions, each
  asserting the evidence that makes the fate provable rather than the fate
  alone: no proposal tracked, and the log index unmoved.

In `crates/rafter-service/tests/transport_waiters.rs`:

- `dropping_an_unresolved_write_future_inside_with_group_reclaims_it` and its
  read counterpart — the re-entrancy reproductions, keeping their watchdog so a
  regression reports a deadlock instead of hanging the suite.
- `dropping_a_future_inside_a_transport_call_reclaims_it` — the same hazard at
  the site an embedder reaches without reading `with_group`'s doc at all.
- `a_deferred_reclamation_is_taken_by_the_next_lock_acquisition` — the deferral
  itself, asserted rather than inferred: drop under the lock, then observe the
  waiter and its barrier gone after the next driver call.
- The hunt's five concurrency probes and its four ordering probes are kept as
  written, for the reason the second revision kept its own: they pass before and
  after, and their job is to fail if a fix moves something it was not meant to
  move.

### Fourth revision after adoption (2026-07-26)

A third adversarial hunt attacked the subsection above and reproduced two
findings, and a craft pass over the same surface found five more. Both of the
correctness findings are the third revision's fix 1 failing on its own written
terms — at a call site the fix did not visit. The fix split membership
publication into a peer-set half and a fence half, argued that "a driver that
dropped one of them because the other failed would leave a committed-removed
replica able to speak with nothing reporting why", and then left the one
publisher that reaches neither half through an event doing exactly that.

The third revision's own rule was *a promise in a doc comment is a claim about
the code under it, so the test that carries the fix's name covers every branch
the claim covers*. It held; the named test does now cover its four clauses. The
rule this subsection adds is the one it was missing, and it is about scope
rather than depth: **a fix that establishes a rule enumerates the rule's
callers, and the enumeration is written down.** Both findings below are the same
caller — reachable from two public constructors — and neither the fix nor its
tests named it.

#### 1. The membership authority is one fact, and one derivation reads it

`publish_adopted_membership` got both of its two decisions wrong, in opposite
directions, and for one reason: it took them.

It read `group.runtime().membership()` — the *effective* configuration — and
published it as the peer set. An appended-but-uncommitted removal makes that
narrower than the committed membership, so adoption took transport
authorization away for a change that had not committed and could still be
reverted. That is precisely what the `Appended` arm four lines above exists to
refuse: "an uncommitted change can still be reverted, so nothing may be taken
away for it." And nothing repairs it if the change does revert. No `Applied`
fires, because the committed membership never moved. No `Appended` fires,
because this driver has no input that carries a membership request. The replica
is cut off for as long as the incarnation runs.

It also passed `Fencing::Withhold`. The supervisor pattern this entry documents
is release, rebuild the runtime from durable storage, adopt, and a rebuilt
runtime's committed membership can have advanced past a removal while the driver
held no group. The driver still holds `known_members` from before the release,
so the difference is computed — and then discarded, with nothing counting what
was withheld. One committed removal, two ways of observing it, two answers: a
routed `Applied` event narrowed *and* fenced; the same removal across an
adoption narrowed and did not.

**The two mistakes are one mistake.** `publish_membership(members, fencing)`
took a set and a flag as independent arguments, so a caller could answer "what
may the link layer authorize" and "what must it fence" inconsistently, and this
one did. The fix removes the choice rather than correcting it. Callers supply
the *fact* they have, and the publisher derives both answers:

```rust
/// The membership fact one publication is derived from.
pub(super) enum MembershipFact {
    /// A configuration that is effective and may still be uncommitted.
    ///
    /// It may only widen.
    Effective(BTreeSet<NodeId>),
    /// A committed configuration, and the effective one beside it.
    ///
    /// `committed` is the only fact that licenses narrowing the set and fencing
    /// what left it. `effective` is what keeps an in-flight change's joiner
    /// able to speak across the same publication.
    Committed {
        committed: BTreeSet<NodeId>,
        effective: BTreeSet<NodeId>,
    },
}
```

`Effective` publishes the union of what is already known and what is effective,
and fences nothing — the union is a superset of `known_members`, so the fence
set it computes is empty by construction rather than by a flag. `Committed`
publishes the union of the committed and effective sets and fences
`known_members` minus that union. The two properties that make this safe are
consequences of the derivation rather than obligations on a caller:

- **The published set always contains the committed membership**, so no
  publication narrows past what the cluster committed.
- **Every fenced replica is absent from the committed membership**, because the
  fence set is the complement of a superset of it.

The effective half of `Committed` is load-bearing and not symmetry. A replica
that rebuilt its runtime from durable storage can hold an appended-uncommitted
*addition*, and publishing the committed set alone would take the joiner's
authorization away and stall the change that needs it — the mirror of the first
finding, reachable through the obvious over-correction. It is also what makes
one report's two events compose: `rafter-app` emits an `Appended` for a newly
appended change *before* the `Applied` for the one that just committed, so an
`Applied` arm that replaced the set with the committed configuration alone would
undo the widening two lines earlier and fence the replica it had just
authorized.

**The full caller list, which is the part the third revision did not write
down.** Every site that publishes a membership to the transport, and the
authority each derives from:

| Publisher | Fact supplied | Source of the fact | Verdict before |
| --- | --- | --- | --- |
| `route_membership_event`, `Appended` arm | `Effective` | the event's `membership`, which `rafter-app` builds from `raft.membership()` ([`crates/rafter-app/src/group/membership.rs:57,70`](../crates/rafter-app/src/group/membership.rs)) | correct |
| `route_membership_event`, `Applied` arm | `Committed` | the event's `membership`, which `rafter-app` builds from `raft.committed_membership()` (`:58,89`), plus the runtime's effective membership | correct on the fence, missing the widening half |
| `publish_adopted_membership` | `Committed` | the runtime's `committed_membership()` and `membership()` | **both halves wrong** |

`publish_adopted_membership` has exactly two callers, and both are public
constructors: `TransportRaftDriver::new`
([`driver/transport.rs:259`](../crates/rafter-service/src/driver/transport.rs))
and `TransportRaftDriver::adopt_group` (`:612`). Below the three publishers,
`update_transport_peers` and `fence_removed_peers` are the only callers of
`RaftTransport::update_peers` and `RaftTransport::fence_peer` in the crate, and
`publish_membership` is the only caller of either. The list is closed at both
ends.

**Test scope.** `membership_reaches_the_transports_peer_set` grows from four
clauses to five — published at adoption, widened on `Appended` and never
narrowed there, narrowed on `Applied`, the removed replica fenced, and no
replica the committed membership still names fenced with it. The two findings
get four tests of their own, and the fourth is the one the third revision
needed: `a_committed_removal_fences_the_same_way_on_both_publication_paths`
asserts the two paths *agree*, rather than asserting each separately and
leaving a divergence to be discovered adversarially.

#### 2. `PersistedRaftRuntime::committed_membership` becomes required

Fix 1 fences on `committed_membership`. The trait shipped it with a provided
body that returns the effective membership, so an implementor that inherited it
reported an appended-but-uncommitted change as committed — and the layer above
now fences on that answer. The default is the first finding again, one crate
down, available to every runtime that does not write the method.

The last revision's response was a warning in the doc comment: "**Any
implementation that can be mid-change must override this.**" A warning is not a
mechanism. `ReplicatedStateMachine::SNAPSHOT_SUPPORT` set the precedent and
states the general rule this is an instance of: there is no default because "a
default would make the claim on their behalf, and it would be wrong for
whichever of the two they meant." The two claims here are "my effective and
committed memberships are always the same" and "I have not thought about it",
and only the implementor can tell them apart.

```rust
/// Returns the latest committed Raft membership.
///
/// There is no default ... A runtime that genuinely cannot be mid-change says
/// so in one line by forwarding to [`PersistedRaftRuntime::membership`]. That
/// is the same body the default had, and it now carries a signature: the
/// implementor asserted it, rather than receiving it.
fn committed_membership(&self) -> MembershipConfig;
```

Breaking, and the cost is proportionate: twelve implementors exist workspace-
wide and six of them already write the method, including the one that matters
(`DurableRaftNode`, which forwards to the kernel's real committed view) and both
`bench-compare` binaries. The six that inherited it are all fixed-membership
test fakes in `crates/rafter-service/tests`, and each now says so in a line of
prose above a one-line body. No `reference/` consumer implements this trait, so
the break costs the reference consumers nothing.

#### 3. `AsyncRaftTransport` is removed rather than published

`AsyncRaftTransport` and `InboundEnvelopeFuture` have zero implementors and zero
consumers workspace-wide. Their only mentions outside their own module are the
crate-root re-export. The entry's own Design subsection already recorded the
gap — "**`AsyncRaftTransport` gets no driver in this entry.** Its `recv` returns
a future the driver would have to own a receive loop for" — and one release
later there is still no driver, no implementor, and no consumer asking for one.

Publishing it converts an unvalidated design sketch into a compatibility
obligation. Every one of its five methods states delivery semantics this crate
has never executed: what a resolved send future means, how `recv` interacts with
cancellation, whether `update_peers` may be reordered against `send`. A
synchronous trait with a real driver behind it earns those sentences; an async
twin with nothing behind it is a specification whose only evidence is that it
compiles.

Removal is also the smaller claim, because the seam it would occupy is already
covered. `RaftTransport::send` means "accepted or enqueued", not "delivered", so
an embedder whose link layer is async owns the queue and the task that drains
it and hands this crate a handle to the queue — which is what
`reference/fenced-lock`'s test transport does and what any real deployment would
do. Rafter opens no sockets and spawns no tasks, which leaves it nothing to say
about how the frames get there. Adding the trait back when an implementor exists
is additive; removing it after a release is not.

`TransportFuture` goes with them. It is the return type of the four methods
being removed and of `InboundEnvelopeFuture`, and nothing else in the workspace
names it, so leaving it behind would keep exactly the defect this fix is about.

#### 4. `WriteOptions` and `ReadOptions` are a pair, and now behave like one

`ReadOptions` is `#[non_exhaustive]` with a `with_min_applied_index` setter, and
says why: "an embedder outside this crate cannot name every field, and a later
field must not break their construction." `WriteOptions` — which the same
module documents as its counterpart, "the read counterpart of [`WriteOptions`]"
— has neither. Any field added to it is a breaking change today, to every caller
that built one with a struct literal.

The asymmetry is an accident of order: reads gained their options later, under a
rule that writes predate. The correction is `#[non_exhaustive]` plus a
`with_client_request_id` setter, and **only the setter lands in this revision.**

The reason is worth recording, because it is the one place this pass left a
finding half-closed. `#[non_exhaustive]` invalidates struct literals, and there
is exactly one outside this crate:
[`reference/fenced-lock/src/adapter/client.rs:162`](../reference/fenced-lock/src/adapter/client.rs),
which builds `WriteOptions { client_request_id: request_metadata(&command) }`.
Converting it is a two-line change — `request_metadata` returns an `Option`, so
it becomes a `map_or_else` over `WriteOptions::default` — and it belongs in the
same slice as the attribute, not ahead of it. So the setter ships first and
alone: it is purely additive, it gives every caller a construction that survives
the attribute, and it makes closing the gap a one-file change with no consumer
edit left in it.

Until then the type's own doc comment states the gap in the imperative rather
than implying the pair is level, because a `WriteOptions` that documents itself
as `ReadOptions`' equal while a field addition still breaks it is the failure
mode this document keeps naming: prose ahead of the code. The remaining work is
one attribute and one consumer call site.

**Closed later in the same pass, not deferred past it.** The two paragraphs
above are a scheduling argument that the pass they were written for outran.
`#[non_exhaustive]` and the consumer conversion landed together a few commits
after the setter, so `WriteOptions` carries the attribute today and the one
struct literal at
[`reference/fenced-lock/src/adapter/client.rs`](../reference/fenced-lock/src/adapter/client.rs)
builds through the setter. The argument was right about the order — setter
first, then the attribute with its call site, never the attribute alone — and
wrong only about how many slices that order needed. Nothing in fix 4 is
outstanding. Read the deferral sentences here, the blast-radius row for
`options.rs`, and the `both_option_types_are_buildable_from_outside_the_crate`
note in the test plan as the design as approved; the attribute is current state.

#### 5. `TransferLeadershipError` and `ShutdownError` project a category

The error module's header makes category projection one of the three questions a
failed operation answers: "*What kind of failure was this?* — the variant,
projected to a `Copy` category through [`WriteError::kind`] and
[`ReadError::kind`]. A metric label, a map key, or a structured-log field." Four
operations on this driver can fail. Two of them cannot answer the question, so
an operator aggregating driver failures gets two buckets and two rendered
strings.

`TransferLeadershipErrorKind` and `ShutdownErrorKind` are the same
low-cardinality, `#[non_exhaustive]`, `Ord + Hash` projections the other two
carry, with the same rule for unrecognized values. Additive. The property test
the other two already have — every variant maps to a distinct kind — extends to
them, and one more asserts what the projections are *for*: all four surfaces
aggregate by the same shape of key.

#### 6. `MetricsPublisher::publish` keeps its `bool`, with `#[must_use]`

Both production call sites discard the result with `let _ =`, which is the
finding: a returned value that every caller drops is either the wrong return
type or a missing `#[must_use]`.

It is the second. The tempting argument for `()` is symmetry with
`MetricsPublisher::close`, which has the identical already-closed early return
and returns nothing — and that argument is what settles it the other way.
`close` returning early leaves the publisher closed, so its caller's intent is
satisfied either way and there is nothing to report. `publish` returning `false`
leaves the caller's intent *unmet*: the snapshot is dropped, no watcher sees it,
and none ever will. A method that can silently fail to do what it was asked
should say so where the compiler can see it. `MetricsPublisher` is `Clone`, so
"did I close it?" is not always answerable locally — one clone can close while
another publishes.

Both of this crate's call sites keep their `let _ =` and gain the sentence that
makes discarding correct there: this driver owns its publisher and is the only
thing that closes it, so `false` means the driver is already down, and a metrics
snapshot from a driver that is already down is the one nobody is waiting for.

#### Blast radius of the fourth revision

| File | Change |
| --- | --- |
| [`crates/rafter-runtime-api/src/lib.rs`](../crates/rafter-runtime-api/src/lib.rs) | `committed_membership` loses its provided body |
| [`crates/rafter-service/src/driver/transport/state.rs`](../crates/rafter-service/src/driver/transport/state.rs) | `Fencing` becomes `MembershipFact`; `publish_membership` derives both answers; `publish_adopted_membership` reads the committed membership; `effective_members` |
| `crates/rafter-service/src/driver/transport/state/tests.rs` | The `Applied` arm's composition with a live `Appended` |
| [`crates/rafter-service/src/transport.rs`](../crates/rafter-service/src/transport.rs) | `AsyncRaftTransport`, `InboundEnvelopeFuture`, and `TransportFuture` removed |
| [`crates/rafter-service/src/driver/options.rs`](../crates/rafter-service/src/driver/options.rs) | `WriteOptions` gains `with_client_request_id`; the `#[non_exhaustive]` half is deferred with its one consumer call site |
| [`crates/rafter-service/src/error.rs`](../crates/rafter-service/src/error.rs) | `TransferLeadershipErrorKind`, `ShutdownErrorKind`, and the two `kind()` methods |
| [`crates/rafter-service/src/watch.rs`](../crates/rafter-service/src/watch.rs) | `publish` gains `#[must_use]` |
| [`crates/rafter-service/src/lib.rs`](../crates/rafter-service/src/lib.rs) | Re-export list: two kinds added, three transport names removed |
| `crates/rafter-service/src/driver/state.rs`, `driver/transport/state.rs` | The two `let _ = publish(...)` sites say why |
| `crates/rafter-service/tests/{support/mod,read_waiters,in_memory_write,transport_failures,transport_streams}.rs` | Six fakes write `committed_membership`; the membership fixture separates effective from committed |
| `crates/rafter-service/tests/transport_streams.rs` | The named test at five clauses, plus the four adoption reproductions |
| `crates/rafter-service/tests/{public_surface,transport_driver}.rs` | The new kinds and the options builders |

Breaking two ways, both argued above and both one release old:
`PersistedRaftRuntime::committed_membership` becomes required, and
`AsyncRaftTransport` with its two future aliases is removed. The third break
this pass designed — `WriteOptions` becoming `#[non_exhaustive]` — is deferred
for the reason fix 4 gives, and is the only piece of the revision that does not
land with it.

`bench-compare` is outside the root workspace and must be built explicitly; it
needs no source change for either break, since both of its runtimes already
write `committed_membership` and neither names the async trait — but it must be
built to prove it. `reference/` needs no change for either break: no consumer
implements `PersistedRaftRuntime` and none names `AsyncRaftTransport`. Holding
`#[non_exhaustive]` back is what keeps that true, and it is why the deferral is
a scheduling decision rather than a retreat.

The behavioural change a correct caller can see is one: a driver's adoption now
publishes the committed membership widened by the effective one, and fences what
neither names, where it previously published the effective membership and fenced
nothing.

#### Focused-test plan for the fourth revision

Every reproduction becomes a regression test with its assertion inverted.

In `crates/rafter-service/tests/transport_streams.rs`, over a scripted runtime
whose effective and committed memberships move independently — the fixture the
old one could not be, because it had a single membership:

- `membership_reaches_the_transports_peer_set` — the named test at its stated
  scope, now five clauses, with the fifth being the one fix 1 could have broken:
  no replica the committed membership still names is fenced.
- `an_uncommitted_removal_does_not_narrow_the_peer_set_at_adoption` — the first
  reproduction.
- `an_uncommitted_addition_widens_the_peer_set_at_adoption` — the mirror, and
  the test that fails if the fix is over-corrected into "publish the committed
  set". Without it, "read `committed_membership` instead of `membership`" passes
  every other test here and stalls every join across a restart.
- `a_committed_removal_across_release_and_adopt_narrows_and_fences` — the second
  reproduction, at the supervisor pattern the entry documents.
- `a_committed_removal_fences_the_same_way_on_both_publication_paths` — the
  control, and the shape the third revision's test plan lacked: it asserts the
  two paths agree rather than asserting each alone.

In `crates/rafter-service/src/driver/transport/state/tests.rs`, which stays the
home of the branch with no public entry point:

- `an_appended_membership_widens_the_published_peer_set` and
  `an_appended_membership_never_narrows_and_never_fences` — kept as written.
- `a_committed_change_does_not_narrow_past_the_membership_in_effect` — replaces
  the old composition test, which scripted an `Applied` the fixture's own
  runtime contradicted. This is the one-report ordering case: `Appended` widens,
  `Applied` commits an older configuration, and the replica the append
  authorized keeps speaking.

In `crates/rafter-service/src/error/tests.rs`:

- `every_transfer_leadership_error_kind_is_distinct_from_every_other` and its
  shutdown counterpart — the property the other two surfaces already carry.
- `all_four_operation_surfaces_project_to_a_category` — the point of the
  projection rather than the mechanism: four surfaces, four complete
  projections, asserted together so a fifth surface added without one is
  visible.

In `crates/rafter-service/tests/public_surface.rs`, where compilation is the
assertion:

- `every_operation_surface_projects_to_a_category_nameable_from_the_root` — the
  two new kinds on the re-export list, pinned by type annotations rather than by
  a list.
- `both_option_types_are_buildable_from_outside_the_crate` — the options pair
  through its setters. It passes today and its job is the deferral: it pins the
  construction that must keep working when `WriteOptions` becomes
  `#[non_exhaustive]`, so the attribute lands against a test that already
  requires the shape it enforces.

`AsyncRaftTransport` needs no test. Its removal is asserted by the workspace
compiling without it, which is the same evidence that made it removable.

## Terminal Driver Vocabulary

### Origin

A write that the driver stops waiting for has a typed reason. A read does not.

`UnknownOutcomeReason` gives the write side five diagnoses
([`crates/rafter-service/src/error.rs:16-34`](../crates/rafter-service/src/error.rs)):
`EmptyNetwork`, `DriveBoundReached`, `PostAppendDriverError`,
`RuntimeDroppedProposal`, `GroupPoisoned`. `ReadError` has
`Rejected`, `Canceled`, `FreshnessUnavailable`, and no way to say "this driver
gave up"
([`error.rs:131-173`](../crates/rafter-service/src/error.rs)), so Rafter's own
driver borrows a transport failure and writes the reason into its message
([`crates/rafter-service/src/driver/read.rs:57-62`](../crates/rafter-service/src/driver/read.rs)):

```rust
if let Some(read_id) = read_id {
    self.abandon_read(read_id);
}
Err(ManagedOperationError::Read(ReadError::Transport {
    message: format!("managed read stalled after {} steps", self.max_drive_steps),
}))
```

The consumer mirrors it, and names the gap where it does
([`reference/fenced-lock/tests/support/cluster.rs:186-202`](../reference/fenced-lock/tests/support/cluster.rs)):

```rust
/// The local waiter is cleared through the documented group API. The read
/// resolves to a non-answer; there is no typed "the caller stopped waiting"
/// read error, so this mirrors the shipped driver's own stalled-read
/// vocabulary.
```

It needs the same term twice more. Once when a driver releases its group
(`abandon_all_waiters`,
[`cluster.rs:768-775`](../reference/fenced-lock/tests/support/cluster.rs)),
which invents a second message —
`ReadError::Transport { message: format!("lock read barrier {read_id} lost its driver") }`.
And once when a caller's own round budget expires
([`cluster.rs:1256-1264`](../reference/fenced-lock/tests/support/cluster.rs)),
which reaches for `abandon_read` and therefore reports a driver's internal
step bound to a client that simply stopped waiting. Three distinct facts, one
variant, two message strings, and nothing a caller can match on.

The write side has the same gap one level in. `UnknownOutcomeReason` has no
variant for a driver that stops owning its node, so both of the consumer's
release paths borrow `RuntimeDroppedProposal`:
`LockCluster::restart`
([`cluster.rs:1127-1129`](../reference/fenced-lock/tests/support/cluster.rs))
and `NodeState::shutdown`
([`cluster.rs:587-600`](../reference/fenced-lock/tests/support/cluster.rs))
both call `abandon_all_waiters(UnknownOutcomeReason::RuntimeDroppedProposal)`.
That reason means something else and says so:
"The app/runtime layer reported that local proposal tracking was dropped before
the final proposal result was known"
([`error.rs:28-30`](../crates/rafter-service/src/error.rs)). In the restart case
the app layer reported nothing; the driver retired the incarnation while the
proposal was still live, and the proposal may well commit under the next one.
An operator reading `RuntimeDroppedProposal` after a restart is being pointed at
the wrong layer.

### Classification

Typed failure behavior, which the promotion rule requires of every promoted API
([`docs/reference-consumers.md:392`](./reference-consumers.md)), and — after
the reshaping in
[Typed Service Failure Surface](#typed-service-failure-surface) — a
prerequisite rather than a refinement. `ReadError::Transport` gains a required
`cause: ErrorCause`, and an abandoned read has no cause: nothing failed. There
is no honest value to put in that field, so the stalled-read path cannot keep
borrowing the variant. The two entries are one change to the read error surface
and must land together; see [Coupled designs](#coupled-designs).

The distinction being typed is a real one, not a naming preference. A caller
must tell three things apart:

- **Rejected or canceled** — the *cluster* refused or invalidated the barrier.
  The read has an authoritative negative answer and a leader hint.
- **Freshness unavailable** — the barrier was granted and this replica has not
  applied through its floor. The read may succeed on a later attempt with the
  same barrier.
- **Abandoned** — nobody refused anything. The driver stopped waiting, the
  barrier is cancelled, the `ReadId` is spent, and a retry needs a fresh read.

Today the third collapses into a transport failure, which a caller reasonably
reads as "the network broke", and retries against the same replica.

Second plausible consumer: the sharded counter, whose contract requires
draining and removing groups while work is in flight
([`docs/reference-consumers.md:308-317`](./reference-consumers.md)). Draining a
group with outstanding reads produces exactly this outcome, per group, and
"a poisoned group cannot stop unrelated groups" requires telling a released
group's reads apart from a broken link.

### Design

Two variants and one small enum, in `crates/rafter-service/src/error.rs`.

```rust
/// Why a managed read barrier was abandoned without an answer.
///
/// Abandonment is the driver's own decision, so every variant names something
/// the driver did. None of them says anything about the cluster: a read that
/// was refused reports [`ReadError::Rejected`], and a barrier the cluster
/// invalidated reports [`ReadError::Canceled`].
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ReadAbandonReason {
    /// The managed read reached its configured drive-step bound before the
    /// barrier resolved.
    DriveBoundReached,
    /// The driver released the group that held this barrier.
    DriverReleased,
}
```

```rust
pub enum ReadError {
    // ...
    /// The driver stopped waiting for this barrier and released it.
    ///
    /// The barrier was cancelled through
    /// [`rafter_app::group::RaftGroup::cancel_read`] before this error was
    /// returned, so no local read state leaks and no later step will report an
    /// outcome for `read_id`. That `ReadId` is spent: a retry issues a new
    /// read, and reusing this one is
    /// [`rafter_app::error::GroupError::NonMonotonicReadId`].
    ///
    /// Unlike an abandoned write, an abandoned read has no outcome that can
    /// still occur — a read takes no effect — so this is a terminal error
    /// rather than an unknown outcome. A caller learns nothing about the
    /// queried state, which is the correct result when freshness cannot be
    /// proved.
    Abandoned {
        read_id: ReadId,
        reason: ReadAbandonReason,
    },
}
```

```rust
pub enum UnknownOutcomeReason {
    // ...
    /// The driver released the group that held this proposal's waiter before
    /// the final proposal result was known.
    ///
    /// This is the in-process restart and shutdown case. The incarnation that
    /// accepted the proposal is gone; a proposal already appended is still in
    /// the durable log and may commit and apply under the next incarnation,
    /// which is exactly what an unknown outcome means.
    ///
    /// It is distinct from [`UnknownOutcomeReason::RuntimeDroppedProposal`],
    /// which reports that the app or runtime layer itself declared local
    /// proposal tracking lost while the driver kept running. The two point at
    /// different layers and lead to different investigations.
    DriverReleased,
}
```

Two enums rather than one shared "the driver stopped" vocabulary, because the
write side's enum answers *why the outcome is unknown* and the read side's
answers *why there is no answer*. Three of the five existing
`UnknownOutcomeReason` variants have no read meaning at all —
`PostAppendDriverError` and `RuntimeDroppedProposal` are about a proposal, and
`GroupPoisoned` is already `ReadError::Poisoned` — so a merged enum would be
mostly unreachable from one side.

`ReadAbandonReason::DriveBoundReached` deliberately reuses the write side's
wording, because it is the same event: the driver's bounded loop ran out.

### Semantics and edge cases

- **The barrier is always cancelled first.** `Abandoned` is returned only after
  `cancel_read` has cleared the group's waiter, so a caller can rely on the
  group's `reserved_reads` metric returning to its previous value. The existing
  driver already does this
  ([`driver/read.rs:57-59`](../crates/rafter-service/src/driver/read.rs),
  [`driver/state.rs:144-152`](../crates/rafter-service/src/driver/state.rs));
  the change makes it part of the documented contract of the variant rather
  than an implementation detail.
- **`FreshnessUnavailable` is not abandonment and stays separate.** The shipped
  driver reaches it when the network is quiet and nothing can advance the
  applied index
  ([`driver/read.rs:139-170`](../crates/rafter-service/src/driver/read.rs)).
  That is a statement about the replica's state, and it carries the two indexes
  that explain it. Folding it into `Abandoned` would discard them.
- **The read side gains no `EmptyNetwork`.** The write side has it because an
  appended proposal with nothing left to drive is genuinely unresolvable. A read
  in the same position has an answer — `FreshnessUnavailable` — so a third
  reason would have no producer.
- **Non-exhaustive.** Both enums stay `#[non_exhaustive]`; a caller that does
  not recognize an abandon reason still knows the read produced no answer,
  which is the whole decision.
- **`DriverReleased` is not `ShuttingDown`.** `WriteError::ShuttingDown` and
  `ReadError::ShuttingDown` refuse *new* operations against a closed driver.
  These two reasons resolve operations that were already in flight when the
  driver let go. Both events happen during a shutdown, in that order.

### Blast radius

Additive to two `#[non_exhaustive]` enums. Breaking only for callers that
matched the message text these variants replace, and for the one in-tree test
that pins it.

| File | Change |
| --- | --- |
| [`crates/rafter-service/src/error.rs:16-34,131-173`](../crates/rafter-service/src/error.rs) | `ReadAbandonReason`, `ReadError::Abandoned`, `UnknownOutcomeReason::DriverReleased`, `Display` arms |
| [`crates/rafter-service/src/driver/read.rs:57-62`](../crates/rafter-service/src/driver/read.rs) | Return `Abandoned { reason: DriveBoundReached }` |
| `crates/rafter-service/src/driver/transport.rs` | `release_group` resolves waiters with `DriverReleased` on both sides |
| [`crates/rafter-service/src/lib.rs:30-33`](../crates/rafter-service/src/lib.rs) | Re-export `ReadAbandonReason` |
| [`crates/rafter-service/tests/in_memory_read.rs:153-169`](../crates/rafter-service/tests/in_memory_read.rs) | `in_memory_driver_cancels_stalled_linearizable_reads` stops asserting `"managed read stalled after 1024 steps"` |
| [`reference/fenced-lock/tests/support/cluster.rs:186-202,768-775,1256-1264`](../reference/fenced-lock/tests/support/cluster.rs) | Three facts become one variant; deleted outright with the driver |

### Focused-test plan

In `crates/rafter-service/tests/in_memory_read.rs`:

- `a_read_that_exhausts_the_drive_bound_is_abandoned` — the replacement for the
  string pin, asserting the variant and
  `ReadAbandonReason::DriveBoundReached`.
- `an_abandoned_read_leaves_no_reserved_read` — assert `reserved_reads` returns
  to its prior value, so the cancellation half of the contract is pinned
  separately from the error half.
- Negative: `a_freshness_gap_is_not_reported_as_abandonment` — the driver's
  freshness path must keep reporting the two indexes.
- Negative: `a_rejected_barrier_is_not_reported_as_abandonment` — the cluster's
  refusal must not be reported as the driver's decision.
- Negative: `an_abandoned_read_id_is_not_reusable` — reissue the same `ReadId`
  through the group and assert `GroupError::NonMonotonicReadId`, so the "spent"
  claim in the doc comment is executable.

In `crates/rafter-service/tests/transport_driver.rs`:

- `releasing_a_group_abandons_its_outstanding_reads` — assert
  `ReadAbandonReason::DriverReleased`.
- `releasing_a_group_resolves_outstanding_writes_as_driver_released` — assert
  `UnknownOutcomeReason::DriverReleased`, and assert it is *not*
  `RuntimeDroppedProposal`, which is the misattribution this entry removes.
- `a_write_released_after_local_append_may_still_apply_under_the_next_incarnation`
  — release with a proposal appended, re-adopt over the same durable storage,
  drive to commit, and assert the command took effect. This is what makes
  `DriverReleased` an *unknown* outcome rather than a failure, and it is the
  claim its doc comment makes.

### Rejected alternatives

- **One shared `DriverStopReason` for reads and writes.** Three of the five
  write reasons have no read meaning, and the read side's terminal semantics
  differ: an abandoned read is final, an abandoned write is not.
- **Reuse `ReadError::Canceled` with a new `ReadIndexCancelReason`.**
  `ReadIndexCancelReason` is a kernel type
  ([`crates/rafter/src/node/event/rejection.rs:29-38`](../crates/rafter/src/node/event/rejection.rs))
  whose variants are all cluster events — leadership lost, leader state reset,
  transfer started. A driver's local bound is not a kernel cancellation, and
  adding it to a kernel enum would put a service-layer concept in the kernel's
  vocabulary.
- **`ReadError::Transport` with a typed marker cause.** Keeps the wrong
  category and invents an error type whose only purpose is to occupy a field
  that should not exist.
- **A single `UnknownOutcomeReason::DriverStopped` covering shutdown and
  restart separately.** Both are the same fact from the proposal's point of
  view: the incarnation that held its waiter is gone. Splitting them would make
  a caller branch on a distinction it cannot act on.

### After-state

The fenced lock's `abandon_read` and its comment about a missing typed error
disappear, and so do the two other message strings it needed for the same idea.
`rafter-service` stops describing its own bounded loop in prose that a caller
can only read, and an operator investigating a restart is pointed at the driver
rather than at the runtime.

**Corrected during adoption.** `abandon_read` disappears, and so does the
consumer's ability to abandon anything. `TransportRaftDriver` resolves waiters
in bulk on `release_group` and `shutdown`, and offers no way to retire one; a
client that stops waiting drops its future, and the driver's waiter stays in the
table until something resolves it and nobody reads the answer. So the two
`DriveBoundReached` reasons still have exactly one producer each — the shipped
in-memory driver's loop — and the case this entry described as "a caller's own
round budget expires" is now authored by the caller, outside any driver, with a
`ReadId(0)` and a `LocalProposalId(0)` it has no way to learn. That is a smaller
version of the same defect the entry set out to fix: the vocabulary is right and
the client cannot reach it. The lock keeps its abandoned write futures alive
solely so a late `UnknownOutcomeReason::RuntimeDroppedProposal` is still
observed by somebody.

**And returned.** That correction described one release only. Per-waiter
abandon is now part of the transport driver — see
[Revision after adoption](#revision-after-adoption), fix 3 — so
`ReadAbandonReason::DriveBoundReached` and
`UnknownOutcomeReason::DriveBoundReached` have the producer this entry designed
them for, and the ID a caller needs in order to reach it is readable from
`pending_writes` and `pending_reads` rather than unlearnable. The lock's own
round budget now expires into `TransportRaftDriver::abandon_write` and
`abandon_read`, and it authors no terminal outcome of its own;
`RuntimeDroppedProposal` survives there only as a predicate over an error the
app layer produced, which is the layer that reason names.

More concretely, this entry is what lets
[Transport-Attached Group Driver](#transport-attached-group-driver) document
`release_group` at all: "every outstanding waiter resolves before this returns"
needs a vocabulary for what they resolve to, and before this entry the read half
of that sentence had none.

## Driver Boundary Re-exports

### Origin

`rafter-service` re-exports its public surface from the crate root
([`crates/rafter-service/src/lib.rs:26-42`](../crates/rafter-service/src/lib.rs)),
and the re-export list is incomplete in a way that shows up in the first
consumer's import block
([`reference/fenced-lock/tests/support/cluster.rs:48-53`](../reference/fenced-lock/tests/support/cluster.rs)):

```rust
use rafter_service::{
    driver::DriverFuture, DriverCommandSender, MetricsError, MetricsPublisher, MetricsWatch,
    PeerEnvelope, QueryReceipt, RaftHandle, RaftTransport, ReadConsistency, ReadError,
    ShutdownError, TransferLeadershipError, UnknownOutcomeReason, WriteError, WriteOptions,
    WriteReceipt,
};
```

Sixteen names from the root; one from a module path. `DriverFuture` is the
return type of four of the five `DriverCommandSender` methods
([`crates/rafter-service/src/driver/trait.rs:22-62`](../crates/rafter-service/src/driver/trait.rs)),
so every implementor of that trait names it, and the root exports
`DriverCommandSender` (`lib.rs:26-29`) without it.

The omission is provably an oversight rather than a decision: the root already
re-exports `TransportFuture`
([`lib.rs:37-41`](../crates/rafter-service/src/lib.rs)), and
`TransportFuture` is defined as an alias *of the type the root does not export*
([`crates/rafter-service/src/transport.rs:25`](../crates/rafter-service/src/transport.rs)):

```rust
pub type TransportFuture<T, E> = DriverFuture<Result<T, E>>;
```

The same audit finds one item in the other direction.
`driver::metrics_watch_from_current`, in a
`crates/rafter-service/src/driver/metrics.rs` this entry deleted,
is re-exported from `driver`
([`driver/mod.rs:48`](../crates/rafter-service/src/driver/mod.rs)), is not
re-exported from the root, has zero callers anywhere in the workspace or in
either reference consumer, and its entire body is `MetricsWatch::new(metrics)` —
a public constructor on a type the root does re-export
([`crates/rafter-service/src/watch.rs:98`](../crates/rafter-service/src/watch.rs),
[`lib.rs:42`](../crates/rafter-service/src/lib.rs)).

### Classification

This is the smallest entry in this document, and it earns its place by settling
a rule rather than by adding a capability.

`rafter-app` exposes modules and re-exports nothing from its root
([`crates/rafter-app/src/lib.rs:17-33`](../crates/rafter-app/src/lib.rs)). That
is a coherent policy: every path is `rafter_app::group::RaftGroup`, and there is
no second way to name anything. `rafter-service` chose the opposite policy and
applied it incompletely, which produces a surface with two conventions and no
rule for choosing between them. A consumer discovers which applies to a given
type by trying one and reading the compiler error.

The rule this entry adopts is checkable: **every type or trait named in the
signature of a public item must be reachable from the crate root of the crate
that declares that item.** Under it, `DriverFuture` must be exported because
`DriverCommandSender` is. So must `StateMachineOperation`, which
[Typed Service Failure Surface](#typed-service-failure-surface) puts into a
public `rafter-service` error variant while it is declared in `rafter-app`. And
`metrics_watch_from_current` is not a type in any signature, has no caller, and
duplicates an exported constructor, so it is deleted rather than exported.

The promotion rule's "usable by at least one other plausible consumer" test is
satisfied by demonstration: the fenced lock reaches into `rafter_service::driver`
today, and the sharded counter will implement `DriverCommandSender` per group.

### Design

In `crates/rafter-service/src/lib.rs`:

```rust
pub use driver::{
    DriverCommandSender, DriverFuture, InMemoryRaftDriver, InboundEnvelopeError,
    ManagedDriverError, QueryReceipt, TransportDriverOptions, TransportRaftDriver,
    WriteBatchEntry, WriteOptions, WriteReceipt,
};
pub use error::{
    ErrorCause, MetricsError, ReadAbandonReason, ReadError, ReadErrorKind, ShutdownError,
    StateMachineOperation, TransferLeadershipError, UnknownOutcomeReason, WriteError,
    WriteErrorKind, WriteFate,
};
```

`StateMachineOperation` is re-exported from `rafter_app::error`, not
redeclared. A caller must be able to compare the value it receives in a
`WriteError::StateMachine` with the one `rafter-app` produced, so there can be
only one type.

`driver::metrics_watch_from_current` and its module are deleted.

The shipped `driver` list carries one name beyond the block above, `PendingWrite`,
added under this rule when [Revision after adoption](#revision-after-adoption)
put it in a public driver signature. That is the rule working: a later entry
extended the list instead of re-deciding what belongs on it.

### Semantics and edge cases

- **Module paths keep working.** These are re-exports, not moves, so
  `rafter_service::driver::DriverFuture` remains valid. Nothing that compiles
  today stops compiling because of this entry.
- **The rule is one-directional.** It says every type in a public signature must
  be *reachable* from the root; it does not say every public item must be
  re-exported. `crate::watch::MetricsPublisher` is re-exported because it
  appears in a driver's construction path; `crate::membership`'s internals are
  not, because nothing outside that module names them.
- **`metrics_watch_from_current` is a removal, and small.** It is public API,
  and pre-1.0 removal of an uncalled function that duplicates an exported
  constructor is the cheapest kind of break there is. It was not reachable from
  the documented root, so a consumer following the crate's own convention never
  found it.
- **This entry is why the surrounding entries stay honest.** Both new error
  types and both new driver types in this cluster have to be added to these two
  lists; without the rule, each would be an independent judgement call made
  while writing a different entry.

### Blast radius

Additive apart from one deletion.

| File | Change |
| --- | --- |
| [`crates/rafter-service/src/lib.rs:26-42`](../crates/rafter-service/src/lib.rs) | Complete both re-export lists |
| `crates/rafter-service/src/driver/metrics.rs` | Deleted |
| [`crates/rafter-service/src/driver/mod.rs:38,48`](../crates/rafter-service/src/driver/mod.rs) | Drop the module and its re-export |
| [`reference/fenced-lock/tests/support/cluster.rs:48-53`](../reference/fenced-lock/tests/support/cluster.rs) | One import path |

No in-tree caller of `metrics_watch_from_current` exists, so its removal touches
nothing else.

### Focused-test plan

Compilation is the test, and it is worth writing as one rather than leaving it
to a consumer to discover.

In a new `crates/rafter-service/tests/public_surface.rs`:

- `the_driver_boundary_is_nameable_from_the_crate_root` — implement
  `DriverCommandSender` for a trivial type using only `rafter_service::*` paths,
  with no `rafter_service::driver::` path anywhere in the file. This does not
  compile today, which is the entry.
- `a_service_error_is_matchable_from_the_crate_root` — destructure a
  `WriteError::StateMachine` and compare its `StateMachineOperation` against a
  root-imported value, so the "only one type" claim is pinned rather than
  assumed.

### Rejected alternatives

- **Adopt `rafter-app`'s module-only policy instead.** Coherent, and a much
  larger break: it would remove sixteen root re-exports the consumers use
  today, for a consistency gain with no consumer behind it. The crate has
  already chosen; the defect is that it did not finish.
- **Export `DriverFuture` and leave `metrics_watch_from_current` where it is.**
  Leaves dead public surface that the crate's own convention hides, which is
  worse than either exporting or deleting it.
- **Re-declare `StateMachineOperation` in `rafter-service`.** Two types with the
  same name and no conversion between them, in a chain where the value crosses
  the boundary unchanged.

### After-state

The fenced lock's import block has one path, and the rule that produced it is
written down where the next crate-by-crate API review can apply it: a public
signature that names an unreachable type is a defect, and the check is a test
rather than a reviewer's memory.

## Declared Snapshot Support

### Origin

A state machine cannot say it has no snapshot format yet, so the fenced lock
says it as a failure
([`reference/fenced-lock/src/adapter/mod.rs:239-251`](../reference/fenced-lock/src/adapter/mod.rs)):

```rust
fn build_snapshot(&mut self, at: LogIndex) -> Result<ApplicationSnapshot, Self::Error> {
    Err(LockAdapterError::DurableSnapshotUndefined {
        snapshot_index: at,
        applied_index: self.applied_index,
    })
}
```

`LockAdapterError` is documented as meaning "the adapter was asked for
something the application contract cannot express, and the group layer is
expected to treat it as fatal"
([`adapter/mod.rs:55-60`](../reference/fenced-lock/src/adapter/mod.rs)). A
declared limitation is filed under the same heading as a malformed frame and an
applied-index regression, and the variant needs a nine-line comment to explain
that it is not really a fault
([`:78-90`](../reference/fenced-lock/src/adapter/mod.rs)), ending with the
sentence that gives the game away: "A driver that never compacts never reaches
this path." The consumer is relying on nobody calling the method.

That reliance is only half safe, and the halves are asymmetric.

- **`build_snapshot` is never called by `rafter-app`.** The only mentions in
  `crates/rafter-app/src` are the trait declaration
  ([`state_machine.rs:83`](../crates/rafter-app/src/state_machine.rs)) and a
  doc reference
  ([`group/output.rs:69`](../crates/rafter-app/src/group/output.rs)). Snapshot
  creation is entirely caller-driven: the ledger calls
  `state_machine_mut().build_snapshot(applied_index)` itself and hands the
  result to the runtime
  ([`reference/ledger/tests/adapter_cluster.rs:461-464`](../reference/ledger/tests/adapter_cluster.rs),
  [`crates/rafter-runtime/src/lib.rs:533`](../crates/rafter-runtime/src/lib.rs)).
  Nothing in the trait's own documentation says so, so a reader has to grep to
  find out that the layer never invokes it.
- **`install_snapshot` is called by the protocol.** `RaftOutput::ApplySnapshot`
  reaches `apply_snapshot_output`
  ([`crates/rafter-app/src/group/output.rs:465-466`](../crates/rafter-app/src/group/output.rs)),
  which calls `install_snapshot` and poisons the group on any error
  ([`group/snapshot.rs:24-28`](../crates/rafter-app/src/group/snapshot.rs)).
  A follower that falls behind the leader's compacted prefix reaches it with no
  caller involved. The fenced lock is safe only because its deterministic
  driver never compacts.

The audit that this finding prompted turned up something worse than the missing
declaration. Of the ten `ReplicatedStateMachine` implementors in the workspace
and the reference consumers, two implement snapshots for real
([`reference/ledger/src/adapter/mod.rs:256,270`](../reference/ledger/src/adapter/mod.rs),
[`crates/rafter-app/examples/snapshot_install.rs:239,247`](../crates/rafter-app/examples/snapshot_install.rs)),
one is the fenced lock's pair of refusals, one is a recording fake with a fault
switch
([`crates/rafter-app/tests/support/mod.rs:142,150`](../crates/rafter-app/tests/support/mod.rs)),
and six return an empty payload from `build_snapshot` and set an applied index
in `install_snapshot`. Two of those six are shipped examples that install a
snapshot by discarding the state it carries
([`crates/rafter-app/examples/replicated_kv_manual.rs:359-363`](../crates/rafter-app/examples/replicated_kv_manual.rs),
[`crates/rafter-multiraft/examples/real_raft_groups.rs:325-329`](../crates/rafter-multiraft/examples/real_raft_groups.rs)):

```rust
fn install_snapshot(&mut self, snapshot: ApplicationSnapshot) -> Result<(), Self::Error> {
    self.applied_index = snapshot.applied_index;
    self.values.clear();
    Ok(())
}
```

That returns `Ok`, which the contract reads as "the state machine must be able
to recover with all snapshot effects and applied-index progress through the
installed snapshot boundary"
([`state_machine.rs:87-89`](../crates/rafter-app/src/state_machine.rs)). It
reports an applied index whose effects it has just deleted. A required method
with no way to decline is a method every implementor answers, and six of ten
answered with something untrue.

### Classification

Durable-lifecycle mechanism, following directly from a documented contract that
the trait currently makes unstatable.

`ReplicatedStateMachine`'s own preamble already draws the line this entry
formalizes: "A state machine that cannot persist application effects and
applied-index progress together strongly enough for that guarantee should not
be used with the higher-level group or service APIs"
([`state_machine.rs:15-17`](../crates/rafter-app/src/state_machine.rs)). The
trait states a capability requirement and then provides no way to declare
whether it is met, so the requirement is enforced by review.

The declaration is Raft-adjacent rather than application policy: whether a
replica can install a leader's snapshot determines whether it can rejoin after
falling behind a compacted prefix, which is a protocol capability, not a
product decision. Where an application's snapshot bytes come from stays entirely
in the application.

Second plausible consumer: the sharded counter, whose workload must include
"a snapshot-heavy group" alongside groups that are not
([`docs/reference-consumers.md:308-317`](./reference-consumers.md)), and whose
host must decide per group whether a replica that has fallen behind can
recover. And the reference program itself, which forbids treating an in-memory
demo as application-durability evidence
([`docs/reference-consumers.md:59`](./reference-consumers.md)) — a rule that
becomes mechanically checkable for the first time here.

### Design

A required declaration with no default, plus provided method bodies that the
declaration makes safe.

In `crates/rafter-app/src/state_machine.rs`:

```rust
/// Whether a state machine implements application snapshots.
///
/// A state machine that cannot install a snapshot cannot rejoin the cluster
/// after falling behind the leader's compacted log prefix, so this is a
/// statement about replication capability rather than about an application
/// feature.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum SnapshotSupport {
    /// The state machine builds and installs application snapshots, and both
    /// [`ReplicatedStateMachine::build_snapshot`] and
    /// [`ReplicatedStateMachine::install_snapshot`] are implemented.
    Supported,
    /// The state machine has no snapshot representation.
    ///
    /// A group over such a state machine refuses a Raft-driven install before
    /// the state machine is touched, and poisons — a replica that cannot
    /// install the snapshot it was sent has no way forward, and pretending
    /// otherwise is how an empty payload gets reported as an applied index.
    ///
    /// This is a development state, not a deployment one. A durable
    /// application declares [`SnapshotSupport::Supported`]; nothing here makes
    /// snapshots optional for one, and a durability test that admits an
    /// `Unsupported` state machine as evidence is testing something else.
    Unsupported,
}
```

```rust
/// Failure of an application snapshot operation.
///
/// The refusal is part of the trait's vocabulary rather than the
/// application's, so a state machine that has no snapshot format does not have
/// to invent an error variant that reads as a fault.
#[derive(Debug)]
#[non_exhaustive]
pub enum ApplicationSnapshotError<E> {
    /// The state machine declared [`SnapshotSupport::Unsupported`].
    Unsupported,
    /// The state machine failed to build or install the snapshot.
    StateMachine(E),
}

impl<E> From<E> for ApplicationSnapshotError<E>;
impl<E: Display> Display for ApplicationSnapshotError<E>;
impl<E: Error + 'static> Error for ApplicationSnapshotError<E>; // source() is the inner error
```

It gets `Display` and `Error` because it is a public error type returned from a
public trait method, and this cluster's whole argument is that such a type must
be a real `std::error::Error` a caller can walk. It deliberately gets no
`PartialEq`: an implementor's own error may have one, but a wrapper that exists
to carry a refusal is compared with `matches!`, not `assert_eq!`.

```rust
pub trait ReplicatedStateMachine {
    // ...

    /// Whether this state machine implements application snapshots.
    ///
    /// There is no default. Every implementor states this, because "this
    /// application has no snapshot format yet" and "this application does not
    /// need snapshots" are different claims and only the implementor can make
    /// either one — a default would make the claim on their behalf, and it
    /// would be wrong for whichever of the two they meant.
    ///
    /// A state machine that declares [`SnapshotSupport::Supported`] must
    /// implement both snapshot methods. Inheriting a provided body while
    /// declaring support is a contract violation, and a group detects it: the
    /// provided body returns [`ApplicationSnapshotError::Unsupported`], which
    /// contradicts the declaration and poisons the group with a distinct
    /// error rather than a generic install failure.
    const SNAPSHOT_SUPPORT: SnapshotSupport;

    /// Builds an application snapshot at `at`.
    ///
    /// Rafter never calls this. Snapshot creation is caller-driven: an
    /// embedder decides when to compact, calls this method, and passes the
    /// result to its runtime's compaction API — see
    /// [`rafter_runtime::DurableRaftNode::compact_log_with_snapshot`]. `at`
    /// must be the state machine's own applied index; compacting above it
    /// raises the group's committed application index past a value the state
    /// machine will ever report.
    ///
    /// # Errors
    ///
    /// Returns [`ApplicationSnapshotError::Unsupported`] when this state
    /// machine declared [`SnapshotSupport::Unsupported`], and
    /// [`ApplicationSnapshotError::StateMachine`] when snapshot construction
    /// or persistence fails.
    fn build_snapshot(
        &mut self,
        at: LogIndex,
    ) -> Result<ApplicationSnapshot, ApplicationSnapshotError<Self::Error>> {
        let _ = at;
        Err(ApplicationSnapshotError::Unsupported)
    }

    /// Installs an application snapshot and makes its effects durable.
    ///
    /// Rafter calls this when the local node accepts a leader's snapshot. After
    /// it returns `Ok`, the state machine must be able to recover with all
    /// snapshot effects and applied-index progress through the installed
    /// snapshot boundary; returning `Ok` without incorporating the payload
    /// reports an applied index the state machine does not reflect, and every
    /// later read and every readiness gate believes it.
    ///
    /// # Errors
    ///
    /// Returns [`ApplicationSnapshotError::Unsupported`] when this state
    /// machine declared [`SnapshotSupport::Unsupported`], and
    /// [`ApplicationSnapshotError::StateMachine`] when the snapshot is invalid
    /// for this state machine or cannot be installed durably.
    fn install_snapshot(
        &mut self,
        snapshot: ApplicationSnapshot,
    ) -> Result<(), ApplicationSnapshotError<Self::Error>> {
        let _ = snapshot;
        Err(ApplicationSnapshotError::Unsupported)
    }
}
```

In `crates/rafter-app/src/error.rs`, two `GroupError` variants:

```rust
    /// A Raft-driven snapshot install reached a state machine that declared
    /// [`crate::state_machine::SnapshotSupport::Unsupported`].
    ///
    /// The state machine was not called. This replica has fallen behind the
    /// leader's compacted prefix and cannot catch up, so the group poisons.
    SnapshotsUnsupported { snapshot_index: LogIndex },
    /// A state machine that declared
    /// [`crate::state_machine::SnapshotSupport::Supported`] refused the
    /// install as unsupported, which means it inherited a provided method body
    /// while declaring support.
    SnapshotSupportMisdeclared { snapshot_index: LogIndex },
```

`apply_snapshot_output`
([`crates/rafter-app/src/group/snapshot.rs:13-38`](../crates/rafter-app/src/group/snapshot.rs))
checks `A::SNAPSHOT_SUPPORT` before building the `ApplicationSnapshot`, so an
unsupported state machine is never handed a payload it cannot use, and maps
`ApplicationSnapshotError::Unsupported` from a `Supported` state machine to the
misdeclaration error.

### Semantics and edge cases

- **The const and the defaults are one design.** The const without the defaults
  leaves the consumer writing two bodies it cannot honestly write. The defaults
  without the const are the loophole the finding warns about: a production state
  machine would inherit "no snapshots" silently and pass every test that does
  not force an install. Together, declaring nothing does not compile, and
  declaring `Unsupported` is a sentence in the source that a reviewer, a
  release gate, and the type system can all read.
- **No obligation is weakened.** A durable consumer declares `Supported` and
  implements both methods exactly as today. The reference program's rule that an
  in-memory demo is not durability evidence
  ([`docs/reference-consumers.md:59`](./reference-consumers.md)) becomes
  checkable rather than reviewed: a durability lane can assert
  `A::SNAPSHOT_SUPPORT == SnapshotSupport::Supported` at compile time for the
  state machines it admits.
- **`Unsupported` poisons rather than degrading.** A follower that cannot
  install the snapshot it was sent is stuck, and the honest report is a fatal
  group state with a specific reason. The improvement over today is not that the
  outcome is softer — it is that the group refuses before touching the state
  machine and names why, instead of reporting a generic
  `StateMachineOperation::InstallSnapshot` failure whose cause is an application
  error variant that a reader must interpret.
- **`build_snapshot`'s caller-driven nature becomes documented.** It is true
  today and stated nowhere. A reader of the trait has every reason to assume the
  layer calls a method the layer declares, and the fenced lock's comment
  ("A driver that never compacts never reaches this path") is a consumer having
  worked it out from the source.
- **The misdeclaration check has a real failure mode.** A state machine that
  declares `Supported`, implements `build_snapshot`, and forgets
  `install_snapshot` compiles today and fails at the worst moment. After this
  change it still compiles — Rust has no way to require one provided method
  given a const — but it fails with an error that names the mistake.
- **`SnapshotSupport` is `#[non_exhaustive]`.** A future third state — a state
  machine that installs but does not build, for instance — is additive. The
  group must treat an unrecognized value as unsupported, which fails closed.
- **The recording fake keeps its fault switch.**
  `crates/rafter-app/tests/support/mod.rs` declares `Supported` and keeps
  `fail_install_snapshot`, because injecting an install failure is exactly the
  coverage that must survive.

### Blast radius

Breaking: a required associated const with no default, and a changed error type
on two trait methods. Every `ReplicatedStateMachine` implementor changes; seven
of the ten get shorter.

| File | Type | Change |
| --- | --- | --- |
| [`crates/rafter-app/src/state_machine.rs:16-98`](../crates/rafter-app/src/state_machine.rs) | trait | `SnapshotSupport`, `ApplicationSnapshotError`, `SNAPSHOT_SUPPORT`, two provided bodies, `build_snapshot`'s caller-driven contract |
| [`crates/rafter-app/src/error.rs`](../crates/rafter-app/src/error.rs) | `GroupError` | Two variants and their `Display` arms |
| [`crates/rafter-app/src/group/snapshot.rs:13-38`](../crates/rafter-app/src/group/snapshot.rs) | group | Check the declaration before installing; map the misdeclaration |
| [`crates/rafter-app/src/group/mod.rs`](../crates/rafter-app/src/group/mod.rs) | — | Import the two new types from `state_machine`. Not a re-export: `rafter-app` exposes modules and gives every type exactly one path, so `SnapshotSupport` is named `rafter_app::state_machine::SnapshotSupport` and nothing else |

| File | Type | Declaration | Bodies |
| --- | --- | --- | --- |
| [`reference/ledger/src/adapter/mod.rs:193,256,270`](../reference/ledger/src/adapter/mod.rs) | `LedgerStateMachine` | `Supported` | Both kept; error type wrapped |
| [`crates/rafter-app/examples/snapshot_install.rs:170,239,247`](../crates/rafter-app/examples/snapshot_install.rs) | `KvStateMachine` | `Supported` | Both kept; error type wrapped |
| [`crates/rafter-app/tests/support/mod.rs:63,142,150`](../crates/rafter-app/tests/support/mod.rs) | `RecordingStateMachine` | `Supported` | Both kept; the fault switch survives |
| [`reference/fenced-lock/src/adapter/mod.rs`](../reference/fenced-lock/src/adapter/mod.rs) | `LockStateMachine` | `Unsupported` | Both kept for now, error type wrapped; the deletion, and `DurableSnapshotUndefined`'s, belong to step 15 |
| [`crates/rafter-app/examples/replicated_kv_manual.rs`](../crates/rafter-app/examples/replicated_kv_manual.rs) | `KvStateMachine` | `Supported` | Both rewritten to round-trip the map |
| [`crates/rafter-multiraft/examples/real_raft_groups.rs`](../crates/rafter-multiraft/examples/real_raft_groups.rs) | `KvStateMachine` | `Supported` | Same |
| [`crates/rafter-service/examples/replicated_kv_service.rs`](../crates/rafter-service/examples/replicated_kv_service.rs) | `KvStateMachine` | `Unsupported` | Both deleted |
| [`crates/rafter-service/tests/support/mod.rs`](../crates/rafter-service/tests/support/mod.rs) | `KvStateMachine` | `Unsupported` | Both deleted; no service test drives an install |
| `bench-compare/src/bin/bench-rafter-service.rs` | `BenchStateMachine` | `Unsupported` | Both deleted |
| `bench-compare/src/bin/bench-rafter-multiraft.rs` | `BenchStateMachine` | `Unsupported` | Both deleted |

"Re-examine" was the honest instruction, and the answers above are what it
produced. The two examples that installed a snapshot by clearing their state
now encode and decode their whole map, because an example is what a user
copies: an `install_snapshot` that reports the snapshot's applied index while
discarding the data it carries claims durability through a boundary whose
effects it just deleted. The four that never see an install — a demo, a service
test fake, and two benches — declare `Unsupported` and inherit the provided
bodies, which is the sentence the const exists to make writable. This is the
only step in this cluster whose adoption changed behavior in files nobody set
out to touch.

`reference/` and `bench-compare/` are outside the root workspace and must be
built for this step.

The break is justified because no default is both safe and useful, which is the
same argument
[Committed Application Index](#committed-application-index) made for its
required method. A default of `Supported` makes every silent stub a lie the type
system endorses. A default of `Unsupported` makes a production state machine
that forgets the declaration unable to accept a snapshot, discovered when a
follower falls behind. An implementor must answer for its own durability.

### Focused-test plan

In `crates/rafter-app/tests/group_apply.rs`, or a new
`crates/rafter-app/tests/group_snapshot_support.rs`:

- `an_unsupported_state_machine_refuses_a_raft_driven_install` — drive
  `RaftOutput::ApplySnapshot` into a group whose state machine declares
  `Unsupported`; assert `GroupError::SnapshotsUnsupported` and that the group is
  poisoned.
- **`an_unsupported_state_machine_is_not_called_before_the_refusal`** — the
  load-bearing negative. The state machine records every call; assert
  `install_snapshot` was never invoked. Refusing after the call would leave the
  application having seen a payload it declared it cannot interpret.
- `a_supported_state_machine_installs_as_before` — the existing install
  coverage, unchanged in behavior.
- Negative: `a_state_machine_that_declares_support_and_inherits_the_default_is_misdeclared`
  — a fixture declaring `Supported` with no bodies; assert
  `GroupError::SnapshotSupportMisdeclared`, distinct from
  `SnapshotsUnsupported`. This is the loophole check, and it is the reason the
  provided bodies are safe.
- Negative: `an_install_failure_still_poisons_with_the_state_machine_error` —
  the recording fake's `fail_install_snapshot` path must still report
  `GroupError::StateMachine { operation: InstallSnapshot, .. }` with the
  application's own error preserved, per
  [Typed Service Failure Surface](#typed-service-failure-surface).

In `crates/rafter-app/tests/group_lifecycle.rs`:

- `snapshot_support_is_readable_without_an_instance` — assert
  `<A as ReplicatedStateMachine>::SNAPSHOT_SUPPORT` is usable in a const
  context, so a release gate can assert it at compile time rather than at run
  time. That is the property that makes the reference program's durability rule
  checkable.

### Rejected alternatives

- **A separate `SnapshottingStateMachine` supertrait.** `RaftGroup` would need
  the bound to handle a protocol-driven install, and installs are not optional
  for any real cluster. It would force either two group types or a runtime
  refusal, and the runtime refusal is what this design does without the second
  trait.
- **Provided bodies with no required const.** The loophole the finding names: a
  production state machine inherits "no snapshots" and nothing says so.
- **A required const with no provided bodies.** Leaves the consumer writing two
  error stubs and inventing an application error variant for a Rafter concept,
  which is exactly the state the finding reports. The two halves are only
  correct together.
- **`Result<Option<ApplicationSnapshot>, Self::Error>`, with `None` meaning
  unsupported.** Conflates "no snapshot format" with "nothing to snapshot", and
  makes the capability a per-call answer rather than a declaration, so nothing
  can be checked before the call.
- **Leave the methods required and only document that `build_snapshot` is
  caller-driven.** Fixes the documentation gap and none of the rest. Six of ten
  implementors would still answer a question they should have been able to
  decline, and two would keep returning `Ok` from an install that discards
  state.
- **Have `RaftGroup` skip the install and keep going.** A replica that ignores a
  snapshot it needs is a replica whose applied index and log diverge silently.
  Poison is the honest outcome, and it is what happens today — the change is
  that it happens with a reason and before the state machine is touched.

### After-state

`LockStateMachine` deletes both snapshot methods and the
`DurableSnapshotUndefined` variant with its nine-line apology, and declares one
line:

```rust
const SNAPSHOT_SUPPORT: SnapshotSupport = SnapshotSupport::Unsupported;
```

`LockAdapterError` drops to three variants, all of which are genuine faults, and
its module comment — "Every variant here means the adapter was asked for
something the application contract cannot express" — becomes true. When the
lock's durable slice lands, the declaration flips to `Supported` and the two
methods come back, which is a diff a reviewer can read as the durability claim
it is.

**The flip has happened, and it read exactly that way.** The lock's durable
slice moved `LockStateMachine` to
`const SNAPSHOT_SUPPORT: SnapshotSupport = SnapshotSupport::Supported`
([`reference/fenced-lock/src/adapter/mod.rs`](../reference/fenced-lock/src/adapter/mod.rs)),
restored both methods over the adapter's own snapshot frame, and added a second
declaring implementor,
[`DurableLockStateMachine`](../reference/fenced-lock/src/adapter/durable.rs).
`LockAdapterError` did not stay at three variants: implementing snapshots is
what gave it honest snapshot faults to name — an index it cannot reproduce, an
install that would move the applied floor backwards, a missing or mismatched
payload, and a payload that violates a service invariant. Those are the
variants the `Unsupported` state had no way to earn, and the module comment is
still true with them in it.

`rafter-app` gains a question it can answer before it needs to, and the
workspace gains a list of six state machines that have been quietly answering a
question they should have declined — two of them in examples that install a
snapshot by deleting the data it carries.

## Lease Reads After an Authorized Deposition

### Origin

A leader that has asked another voter to depose it keeps serving leader-lease
reads after the request stops being visible locally, and answers them at an
index a newer leader has already committed past. The kernel's own two documents
state the contradiction between them.

`NodeConfig::with_lease_reads` justifies the lease's safety with a refusal:

> The lease also relies on voters refusing to depose a live leader. The request
> therefore becomes effective only while pre-vote and check-quorum are both
> effective.

([`crates/rafter/src/node/config/options.rs:91-95`](../crates/rafter/src/node/config/options.rs))

`Node::handle_timeout_now` documents the waiver of exactly that refusal:

> A `TimeoutNow` from the current leader instructs this node to campaign
> immediately: the real, term-incrementing election, bypassing pre-vote and
> leader stickiness — that bypass is the message's entire purpose (thesis 3.10).

([`crates/rafter/src/node/transfer.rs:101-104`](../crates/rafter/src/node/transfer.rs))

The only thing standing between the two is the local transfer record.
`read_index_batch` rejects barriers while `leader.pending_transfer.is_some()`
([`crates/rafter/src/node/read_index.rs:47-52`](../crates/rafter/src/node/read_index.rs)),
and `tick_leadership_transfer` deletes that record after one election timeout so
an unreachable target cannot wedge the leader
([`crates/rafter/src/node/transfer.rs:51-59`](../crates/rafter/src/node/transfer.rs)):

```rust
transfer.ticks_remaining = transfer.ticks_remaining.saturating_sub(1);
if transfer.ticks_remaining == 0 {
    self.leader.pending_transfer = None;
}
```

The record is local. The authorization is a `TimeoutNow` already on the wire,
and Raft bounds no message's delay. Deleting the record does not recall the
message; it only stops the leader from remembering that it sent one. The lease
fast path then grants at `self.volatile.commit_index`
([`crates/rafter/src/node/read_index.rs:64-70`](../crates/rafter/src/node/read_index.rs)),
which is the deposed leader's stale index.

The repro is `crates/rafter/tests/gen6_kernel_lease.rs`. Node 1 authorizes a
transfer to node 2, the `TimeoutNow` is held on the wire, node 1 heartbeats
healthily for four election timeouts — renewing its lease on every acknowledged
round — and then the message is delivered:

```
STALE LEASE READ: deposed leader NodeId(1) granted read_index [LogIndex(2)]
while the term-Term(2) leader had already committed through 4
```

No clock skew is involved. Nodes 2 and 3 receive zero ticks in the repro, which
is the most favorable direction for the documented bounded-tick-rate assumption:
the leader's clock runs infinitely fast relative to theirs, and the violation
still happens. This is not the lease's skew assumption failing. It is the
lease's *other* assumption — the refusal — having been waived by the leader
itself.

A second, smaller finding sits on the same surface. `Node::read_lease_active`
claims

> Whether a read barrier requested right now would grant from the leader lease
> without a quorum round trip.

and consults neither `pending_transfer` nor the current-term commit rule
([`crates/rafter/src/node/observe.rs:92-102`](../crates/rafter/src/node/observe.rs)),
so it answers `true` in states where the barrier is rejected outright:

```
read_lease_active() promised true but the barrier produced
[ReadIndexRejected { read_id: ReadId(1),
                     reason: LeadershipTransferInProgress { target: NodeId(2) } }]
```

### Classification

Raft mechanism, and a linearizability violation rather than an ergonomics gap.
The lease is thesis 6.4.2's optimization, and its entire safety argument is that
no one will take leadership away inside the window. `TimeoutNow` is thesis
3.10's instrument for taking leadership away *without asking* — and the kernel
confirms it takes nothing else away with it. Leader stickiness lives only in
`handle_pre_vote`
([`crates/rafter/src/node/election.rs:196-205`](../crates/rafter/src/node/election.rs));
`handle_request_vote` grants on term, membership, log freshness, and vote
uniqueness alone
([`crates/rafter/src/node/election.rs:126-159`](../crates/rafter/src/node/election.rs)).
A `TimeoutNow` recipient skips the poll that stickiness guards and goes straight
to the vote that has no stickiness at all. Nothing is left in the protocol that
would make a voter refuse.

So the leader's obligation is not "remember the transfer for a while". It is:
*a leader that has put a deposition authorization on the wire has spent the
assumption its lease rests on, and no local event can un-spend it.*

### Design

Two changes, both in `crates/rafter`.

**1. A waiver that only a new term clears.** `LeaderState` gains one field, set
at the single point where the authorization escapes:

```rust
/// Whether this leadership has put a `TimeoutNow` on the wire.
///
/// A `TimeoutNow` authorizes its recipient to depose this leader by the
/// path that skips pre-vote and leader stickiness, and the network bounds
/// no message's delay. The lease's safety argument is that voters refuse
/// to depose a live leader; emitting the message waives that refusal for
/// the rest of this term, and no local event can recall it. Abandoning
/// the transfer record therefore does not restore the lease — only a new
/// term does, and `LeaderState` is rebuilt per term.
pub deposition_authorized: bool,
```

`send_timeout_now` is the only producer of `Message::TimeoutNow`
([`crates/rafter/src/node/transfer.rs:79-90`](../crates/rafter/src/node/transfer.rs)),
reached from `transfer_leadership` when the target is already caught up and from
`maybe_complete_leadership_transfer` when it catches up later. It sets the flag
in the same statement that marks the transfer as sent. Nothing else sets it, and
nothing clears it: `become_leader` and `become_follower` both assign
`LeaderState::default()`
([`crates/rafter/src/node/lifecycle.rs:24,39`](../crates/rafter/src/node/lifecycle.rs)),
which is the whole clearing rule.

Arming at emission rather than at request is deliberate and load-bearing. A
transfer to a target that never catches up sends no `TimeoutNow`, authorizes
nothing, and times out with the leader's lease intact. Arming at
`transfer_leadership` would void the lease for a message that was never written.

**2. One predicate, two callers.** The lease fast path and `read_lease_active`
stop being two independent conditions. A private

```rust
fn lease_grant_available(&self) -> bool
```

answers "would a barrier requested right now be granted from the lease, without
a quorum round trip" — leader role, effective `lease_reads`, no live transfer
record, no authorized deposition, a current-term commit, and a lease that covers
the current tick. `read_index_batch` consults it where it consulted
`lease.holds` alone; `read_lease_active` returns it. The observability method
cannot disagree with the decision it describes, because it *is* the decision.

The waiver disables the fast path only. It does not reject reads. A leader whose
transfer was abandoned is fully operational, and the quorum `ReadIndex` round
trip remains available and remains sound: its evidence is a quorum acknowledging
a round broadcast *after* the barrier was registered, which is a log-and-term
argument, not a time argument, and the authorized deposition cannot forge it.
`pending_transfer` keeps rejecting reads while a transfer is live, which is a
different rule for a different reason (thesis 3.10: the leader is stepping aside
and has stopped accepting work).

### Semantics and edge cases

- **The waiver's bound.** The exposure ends with the term, not before and not
  after. A `TimeoutNow(T)` is inert once its recipient's term exceeds `T`
  ([`crates/rafter/src/node/transfer.rs:106`](../crates/rafter/src/node/transfer.rs)
  returns empty for `term < self.current_term()`), and this node can only grant
  lease reads again in some term `T' > T`, which it can only reach by winning an
  election that puts a majority at `T'`. A campaign the stale authorization
  starts proposes `T+1 <= T'`, and cannot collect a majority of voters already
  at `T'`. So a term-scoped waiver is not merely sufficient — it is exactly
  co-extensive with the authorization's power.
- **Abandoned before emission.** Target not caught up, transfer times out: no
  `TimeoutNow`, no waiver, lease unaffected. Tested.
- **Transfer completes.** The recipient's higher term reaches this node, it
  becomes a follower, `LeaderState` is discarded with the waiver in it.
- **Re-elected in a later term.** Fresh `LeaderState`, fresh lease, no waiver.
- **Reads after the waiver.** Granted through the quorum round trip, not
  rejected. Tested, and the repro's control test asserts the round trip is
  actually taken rather than short-circuited.
- **`read_lease_active` and the single-voter shortcut.** A single-voter
  membership grants barriers immediately through `has_quorum_with_self`
  ([`crates/rafter/src/node/read_index.rs:87-89`](../crates/rafter/src/node/read_index.rs)),
  which is a quorum grant, not a lease grant — its evidence is "I am the
  quorum". `read_lease_active` stays `false` there, and its rustdoc says so,
  because a caller reading it to decide whether the lease is carrying its reads
  would otherwise attribute a quorum grant to the lease. This is the one place
  the method deliberately does not predict "granted without a round trip", and
  it is the direction that under-claims.
- **Lease active but no current-term commit.** Reachable: the lease confirms on
  a round *sequence* acknowledgement
  ([`crates/rafter/src/node/replication/authority.rs:15-43`](../crates/rafter/src/node/replication/authority.rs)),
  while the commit index advances on `match_index`, so a follower that
  acknowledges a round while still catching up confirms the lease before the
  term's `Noop` commits. Pre-fix `read_lease_active` answered `true` and the
  barrier was rejected `NoCommitInCurrentTerm`. Now both say no.

### Blast radius

`crates/rafter` only, and no wire, storage, or output shape changes.

- `LeaderState` gains a field. It is `pub(in crate::node)` and appears in the
  kernel's `Clone + Debug + Eq + Hash + PartialEq` derives, so model-checked
  state equality now distinguishes a leadership that has authorized a deposition
  from one that has not. That is the intended distinction: the two are not the
  same state and were previously conflated.
- `Node::read_lease_active` changes its answer in three situations: while a
  transfer record is live, after a `TimeoutNow` has been emitted in this term,
  and before this term's first commit. All three move from `true` to `false`,
  and in all three the barrier was already not granted from the lease. No caller
  can observe a `false → true` change.
- `Node::read_index` / `read_index_batch` change behavior in exactly one
  situation: a leader that emitted `TimeoutNow` this term takes the quorum round
  trip instead of granting from the lease. Latency, not semantics — and the
  semantics it stops offering were wrong.
- Nothing outside the kernel is edited. `rafter-service`, `rafter-app`, and both
  reference stores route reads through the same outputs.

### Focused-test plan

Every guard added here is mutation-tested: the guard is removed or inverted, and
the named test must fail.

1. `crates/rafter/tests/gen6_kernel_lease.rs`, adopted from the hunt, three
   tests: the pre-existing guard while the record lives; the control showing the
   quorum path does not short-circuit; and the violation itself, which now
   asserts no grant is produced.
2. `crates/rafter/src/node/tests/transfer/lease_waiver.rs`, at the kernel's own
   granularity — one test per boundary in *Semantics and edge cases*: waiver
   armed at emission and not at request; abandoned-before-emission leaves the
   lease intact; reads after the waiver go through the quorum round trip and are
   granted, not rejected; a new term restores the lease.
3. `crates/rafter/src/node/tests/read/lease.rs` gains the prediction-agreement
   tests for `read_lease_active`, one per clause of the predicate.

### Rejected alternatives

**Void the lease until fresh quorum contact re-establishes it.** This is the
proportionate-sounding option and it is unsound. A quorum acknowledgement proves
a majority followed this leader at tick *t*; the lease needs "a majority will
refuse to depose me through *t + window*", and those are different claims that
coincide only under the refusal assumption. The authorization on the wire is a
standing waiver of that assumption held by one voter who does not have to ask
anyone, and — per `handle_request_vote` above — the voter it *does* have to ask
applies no stickiness either. Contact does not un-send a message. This option
narrows the window and leaves the violation reachable inside it.

**Hold the lease void for one election timeout (or one lease window) after the
last `TimeoutNow`.** The same mistake as the code being fixed, with a larger
constant. The delay on the wire is unbounded by assumption; any finite local
timer is a guess about the network.

**Make `TimeoutNow` carry an expiry the recipient honors.** This would bound the
authorization at its source and is the only alternative that attacks the real
quantity. It is rejected on scope and on cost: it is a wire-format change to a
protocol message, it requires the recipient's clock to be comparable to the
sender's — reintroducing exactly the cross-node time assumption the sans-I/O
kernel refuses to make — and it buys nothing the term scope does not already
give, because the authorization is already inert past the term.

**Reject reads instead of falling back to the quorum path.** Over-corrects. The
leader is not stepping aside; it tried to and the attempt was abandoned. Reads
are still linearizably answerable through the round trip, and a `ReadIndex`
rejection would push a correctness problem into every caller's availability
budget for no safety gain.

**Fix only `read_lease_active`.** It is the visible symptom and the smaller
half; correcting the predictor while the predicted path stays wrong would make
the observability method faithfully report an unsound grant.

### After-state

`crates/rafter/src/node/state/leader.rs` records the waiver next to the lease it
constrains, and the two doc comments that contradicted each other now point at
one another: `with_lease_reads` names the transfer waiver as the second way the
lease can be suspended, and `handle_timeout_now`'s bypass paragraph says what the
bypass costs the sender.

`Node::read_lease_active` becomes usable as what its name suggests — a
precondition check a caller can branch on — instead of a lease-timer readout
that agrees with the read path by coincidence.

The kernel keeps one lease-suspension rule with two arms, both stated on the
public configuration method: the window can lapse, and the leader can waive it.
Nothing else suspends it.

## Declared Applied Floor Below the Snapshot Boundary

### Origin

`Node::from_bootstrap_applied_through` takes the application's own statement of
what it has durably applied and, when that statement falls below the node's
snapshot boundary, replaces it
([`crates/rafter/src/node/construction.rs:97`](../crates/rafter/src/node/construction.rs)):

```rust
let floor = node.volatile.applied_index.max(applied_through);
```

`node.volatile.applied_index` is the snapshot boundary at this point
([`:40-52`](../crates/rafter/src/node/construction.rs)). The method validates a
floor that is too high — twice, with two typed errors — and silently raises one
that is too low. The entries between the declared floor and the boundary are
compacted out of the log, so they are never re-emitted as `Output::Apply`, and
nothing anywhere reports that they were skipped. The state machine is left
believing it applied them and the kernel is left believing it delivered them.

Both reference stores reach the state, by unrelated routes, and both are
documented as recoverable on the strength of a promise this seam does not make.

**Fenced lock.** `LockStore::discard_and_reseed` empties the application
directory and justifies it with:

> What refills it is mostly this replica's own retained log rather than the
> group: this call empties the application store and touches nothing else, so
> the Raft log and snapshot beside it survive, the reopened store reports
> `LogIndex::ZERO`, and the entries replay. The group supplies only what local
> compaction has dropped, as a snapshot.

Over a log that has been compacted, the re-seeded store's `LogIndex::ZERO` is
raised to the snapshot boundary and the dropped prefix is supplied by nobody —
the follower's log already matches the leader's, so no snapshot is ever sent.
The outcome is the one that same method gives as its reason for *refusing* to
repair a `NoReadableImage`:

```
a fencing token must never be reissued: the store handed out 1 for a resource
whose guarded downstream has already accepted 2
the replay must carry the re-seeded replica back to the floor it deleted:
reported 5, reached 0
```

**Ledger.** `CONTRACT.md` names one escape from its re-apply claim:

> the entries above it are re-applied on the next recovery. That second half is
> a fact about the composition, so it is tested end to end rather than asserted,
> and **it stops holding exactly when the group can no longer supply the
> entries.**

There is a second escape and it is local. A zero-filled tail — crash rule two,
on the ordinary `open`, with no operator flag — moves the application's applied
index below the boundary of a snapshot the replica compacted itself, and one
acknowledged transaction is gone from that replica for good while the group
still holds every entry:

```
the replay stopped at LogIndex(4) anyway
  left: LogIndex(4)
 right: LogIndex(5)
```

One kernel seam, two stores, two symptoms.

### Classification

Raft mechanism, and specifically a *composition* mechanism: who is responsible
for the gap between an application's durable state and a Raft node's compacted
prefix. It is not application policy, because neither store can express the
repair — the entries the state machine needs do not exist in any form either
store can reach.

The layer above has already made the ruling this seam contradicts.
`ReplicatedStateMachine::build_snapshot` states
([`crates/rafter-app/src/state_machine.rs:196-205`](../crates/rafter-app/src/state_machine.rs)):

> `at` must be the state machine's own applied index; compacting above it raises
> the group's committed application index past a value the state machine will
> ever report.

and `install_snapshot` states that after `Ok`, the state machine recovers "with
all snapshot effects and applied-index progress through the installed snapshot
boundary". Together those say: **a replica's durable application floor is never
below its own snapshot boundary.** That is an invariant of the composition, not
a preference. The kernel's `max` does not enforce it, does not check it, and
hides every violation of it — including the two that the reference stores can
produce without doing anything the app layer forbids.

This also settles a question that looks like a false-positive risk and is not. A
snapshot boundary that lands on a `Noop` or a configuration entry does *not* put
a conforming state machine below its boundary, because the boundary is the state
machine's own applied index by the contract above; an embedder that compacts at
the kernel's applied index instead is already violating `build_snapshot`'s
stated precondition, and now finds out at the next restart instead of silently.

### Design

**The kernel refuses; it does not re-feed.** `from_bootstrap_applied_through`
gains a third validation, alongside the two it already performs:

```rust
/// The declared applied floor lies below this node's own snapshot
/// boundary: the entries between the two are compacted out of the log and
/// cannot be re-emitted, so the floor cannot be honored.
AppliedFloorBelowSnapshot {
    applied_through: LogIndex,
    snapshot_index: LogIndex,
},
```

and the `max` becomes a plain assignment, because after the check the declared
floor is provably at or above the boundary.

Re-feeding was considered and rejected on a fact about the kernel rather than a
preference: the kernel holds a snapshot *descriptor*, never payload bytes, and
`DurableRaftNode::with_storage_and_snapshot` will construct a descriptor over an
empty payload
([`crates/rafter-runtime/src/construction.rs:59-72`](../crates/rafter-runtime/src/construction.rs)).
A kernel that emitted `Output::ApplySnapshot` here would be directing a layer to
restore from bytes the kernel cannot know exist — the precise shape this review
programme exists to remove. Worse, the layer it would direct is allowed to
decline: `SnapshotSupport::Unsupported` is a state a `ReplicatedStateMachine`
may legally declare
([`crates/rafter-app/src/state_machine.rs:26-35`](../crates/rafter-app/src/state_machine.rs)),
and for such a state machine no re-feed exists at any layer. A typed refusal is
the only answer the kernel can make that is true for every caller.

**"No declaration" stops being spelled `LogIndex::ZERO`.** The check is total
over the argument, including zero — the fenced lock's re-seeded store reports
exactly `LogIndex::ZERO` and means it. That is only sound because the kernel
already has a separate constructor for callers who are *not* declaring:
`Node::from_bootstrap` takes no floor and starts at the snapshot boundary, which
is the honest reading of "replay everything you can". The runtime's two
non-declaring constructors currently reach the declaring one with a zero
argument
([`crates/rafter-runtime/src/construction.rs:90-96,117-123`](../crates/rafter-runtime/src/construction.rs)),
and are rerouted to a private helper that takes `Option<LogIndex>` and calls
`from_bootstrap` for `None`. Behavior for those two is unchanged; what changes is
that they stop making a declaration on their caller's behalf.

The runtime's declaring constructors surface the new error as
`RaftRuntimeError::Bootstrap`, which they already do for the other two floor
errors, so no new runtime variant is needed.

### Semantics and edge cases

- **No snapshot.** The boundary is `LogIndex::ZERO` and every floor is at or
  above it; the check is inert, which is the overwhelmingly common case.
- **Floor exactly at the boundary.** Accepted — the state machine restored from
  this snapshot, or built it.
- **Floor above the boundary, at or below commit.** Accepted, unchanged: the
  entries above the floor are still retained and replay.
- **Floor above commit, or beyond the log.** Still the two existing errors,
  checked first, so a floor that is both too high and nonsensical reports the
  most specific existing reason rather than the new one.
- **`from_bootstrap` (no declaration).** Unchanged in every case.
- **The error is not repairable by retrying with a different floor.** The
  message says which two indexes disagree; the repair is at the layer that owns
  the application store — restore it from the snapshot, or delete the Raft state
  beside it so the replica rejoins empty and is sent one.

### Blast radius

- `BootstrapValidationError` gains a variant. It is documented as exhaustive by
  design ("bootstrap validation is closed over these persisted-state
  invariants"), so this is a breaking change for anyone matching it
  exhaustively. Justified: the alternative is an invariant with no name.
- `crates/rafter/tests/properties.rs:960-1010` models the constructor's outcomes
  and gains the third arm.
- `crates/rafter-sim` restarts nodes through the declaring constructor with the
  simulator's durable application floor. Any place the simulator lets that floor
  lag a snapshot boundary it installed is now an error rather than a silent
  raise, and is fixed in the simulator rather than masked in the kernel.
- Both reference stores stop losing data silently and start failing to open.
  Neither store's *code* changes; both stores' prose does, because both made a
  cross-layer claim this seam never supported.

### Focused-test plan

1. `crates/rafter/src/node/tests/bootstrap/application.rs` gains the boundary
   pair: a floor one below the snapshot boundary is refused with the new error;
   a floor exactly at the boundary is accepted. Mutation: delete the check, and
   both the kernel test and both consumer suites fail.
2. `crates/rafter/tests/properties.rs` extends its outcome model, which is the
   coverage for "no other input reaches the raise".
3. `crates/rafter-runtime` gains a test that the non-declaring constructors open
   a compacted node unchanged, and that the declaring ones report
   `RaftRuntimeError::Bootstrap` for a low floor — the boundary between the two
   constructor families, which is the thing the design moved.
4. `reference/fenced-lock/tests/gen6_reseed_compaction.rs` and
   `reference/ledger/tests/gen6_zero_tail_compaction.rs`, adopted from the hunt,
   assert the refusal at the composition seam and that the damage the pre-fix
   run produced — a reissued token, a lost transaction — is not reachable.

### Rejected alternatives

**Re-feed via `Output::ApplySnapshot`.** Rejected above on two independent
grounds: the kernel does not know the payload exists, and the state machine is
allowed to have no snapshot representation at all. Either alone is fatal.

**Accept the floor and report the effective one.** A struct return carrying "you
asked for 4, you got 5" is the silent raise with a receipt. The caller still
cannot obtain index 5, and a receipt nobody is obliged to read is how this defect
survived six generations.

**Refuse only for a non-zero floor.** Keeps `LogIndex::ZERO` as "no declaration"
and leaves the fenced-lock symptom fully open, since a re-seeded store's honest
floor is zero. The ambiguity is the defect; preserving it is not a smaller fix.

**Fix it in the reference stores.** Neither store can. The bytes the state
machine needs are in a snapshot the store cannot address and in log entries that
no longer exist. A store-level fix would be a refusal to open written twice, in
two consumers, for a condition the kernel is the only layer that can see.

### After-state

The kernel states one rule about the applied floor and enforces all of it: a
declared floor must lie in `[snapshot_index, commit_index]`, and each of the
three ways out of that interval has a typed error naming the two indexes that
disagree.

`docs/reference-consumers.md`'s composition story gains the missing case, and
both `CONTRACT.md` files stop claiming a repair the layer beneath never promised
— the ledger's re-apply escape clause names the local half, and the fenced
lock's `discard_and_reseed` says what it requires of the Raft state beside the
store it empties.

### Revision after implementation (2026-07-26)

A seventh adversarial hunt attacked the guard this subsection designs and
reproduced seven probes against its placement. They are answered in
[Second revision after implementation (2026-07-26)](#second-revision-after-implementation-2026-07-26),
which is current truth wherever the two disagree; the two places they disagree
are the call this subsection routes the refusal through —
`RaftGroup::apply_raft_outputs`, which now takes no verdict — and the
`ApplySnapshot` skip it introduces, which is withdrawn rather than repaired.

**The refusal is right and the kernel is the wrong layer for it.** The design
above was implemented as written, and `rafter-maelstrom`'s production recovery
path falsified its central claim within one test run:

```
production reopen restores the promoted application snapshot:
Bootstrap(AppliedFloorBelowSnapshot { applied_through: LogIndex(0), snapshot_index: LogIndex(3) })
```

`open_application_node` loads its durable application state, opens the Raft node
with that state's applied index as the floor, and *then* — if the reopened node
carries a snapshot the application is behind — reads the payload out of the
snapshot store and restores the application to the boundary
([`crates/rafter-maelstrom/src/runtime.rs:45-61`](../crates/rafter-maelstrom/src/runtime.rs)).
That is not a bug. It is the repair for the crash window an inbound snapshot
install necessarily has: the snapshot is promoted durably *before* the
application installs it, so a crash between those two writes leaves an
application legitimately short of a boundary its Raft state already carries.
The floor is below the boundary at the moment of construction, and the
composition fixes it a few lines later.

So the Classification section's ruling — "a replica's durable application floor
is never below its own snapshot boundary" — is true of a *settled* replica and
false of a recovering one, and the constructor sees exactly the recovering one.
The kernel cannot tell the two apart: whether the gap is about to be repaired is
a fact about what the caller does next.

This is the programme's own failure mode, committed by the fix for it. The
design verified its invariant against the app layer's **prose**
(`build_snapshot`'s stated precondition) and did not verify it against the code
of a layer that composes the kernel differently. The binding rule this
subsection adds: **an invariant is enforced at the layer that can observe every
state reaching it, and "every state" is established by reading the callers, not
the contracts.**

**What changes.** The refusal moves to `rafter-app`'s `RaftGroup`, which is the
only object in the workspace that holds a runtime and a state machine together
and therefore the only one that can compare them:

```rust
AppliedIndexBelowSnapshotBoundary {
    app_applied_index: LogIndex,
    snapshot_index: LogIndex,
},
```

`reject_if_below_snapshot_boundary` runs at the top of `step_with_options` and
of `apply_raft_outputs`, poisons the group, and returns that error. It compares
against the state machine's *current* applied index rather than the floor the
group was constructed with, which is what lets a maelstrom-shaped caller restore
after opening and pass. It skips a batch that carries an `ApplySnapshot`, which
is the one batch whose own contents lift the state machine to the boundary. It
costs one integer comparison in the common case — the state machine is consulted
only when the boundary is above the group's own floor.

`BootstrapValidationError::AppliedFloorBelowSnapshot` is withdrawn; the kernel's
enum is unchanged from before this entry. `Node::from_bootstrap_applied_through`
keeps `max(snapshot_index, applied_through)` and now documents it as a
behavior with an owner: the asymmetry against the two errors beside it, why
neither refusing nor re-feeding is available to the kernel, that the gap is
never emitted in any form, and that comparing `applied_index()` against
`snapshot_index()` after construction is the supported way to see that a
declaration was raised. The runtime constructors are unchanged, and the
`Option<LogIndex>` split the design proposed for them is withdrawn with the
refusal that motivated it — with the kernel permissive, "declaring zero" and
"declaring nothing" have the same meaning again, and a split that changes no
behavior is churn.

Both consumer symptoms still close, through the same call the consumers already
make: `RaftGroup::apply_raft_outputs(recovery_outputs)`, one line after
construction in both drivers. The mutation evidence is unchanged in kind —
removing the guard reproduces the reissued token and the lost transaction
verbatim.

**One fixture moved with it.**
[`crates/rafter-app/tests/group_read.rs:975`](../crates/rafter-app/tests/group_read.rs)
scripted a runtime that compacts to boundary 4 while its state machine sits at
2, to exercise a read barrier's floor being fixed at grant. No embedder can
produce that shape — `build_snapshot` requires the state machine's own applied
index as the boundary — so the fixture now compacts to 2, which exercises the
same reshape without scripting a composition the group refuses to run.

### Second revision after implementation (2026-07-26)

**The refusal is right, the layer is right, and the placement was wrong in both
directions.** The subsection above moved the check out of the kernel because the
kernel cannot see whether a caller is about to repair the gap. It then put the
check on `RaftGroup::apply_raft_outputs` — the one call a recovering caller
makes *before* it repairs anything — and left it off `RaftGroup::read`, the one
call that hands a truncated state machine's contents to a client. A seventh
adversarial hunt reproduced both.

**The severe direction: the accommodation does not fire.** The skip written to
let a recovering replica through tests whether the batch contains an
`Output::ApplySnapshot`. A recovery batch is
`RecoveredDurableRaftNode::into_parts`' `recovery_outputs`, which comes from
`drain_committed_outputs` → `apply_committed_into`
([`crates/rafter/src/node/commit/apply.rs:58-92`](../crates/rafter/src/node/commit/apply.rs)),
and that function pushes an `Output::Apply` per committed application entry and,
if the committed configuration removed this leader, whatever stepping down
emits. It never pushes an `Output::ApplySnapshot`: the kernel holds a snapshot
descriptor rather than payload bytes, which is the same fact that put this
refusal in the app layer to begin with. A recovery batch therefore *cannot*
carry an install, so
the skip written for it can never fire for it, and the crash-window replica the
subsection above exists to accommodate is poisoned by the very call both
reference drivers make one line after construction:

```
AppliedIndexBelowSnapshotBoundary { app_applied_index: LogIndex(3), snapshot_index: LogIndex(5) }, group poisoned
```

A recoverable crash window became an unopenable replica. Both consumers declare
`SnapshotSupport::Supported`, so both are reachable.

Two further shape faults sat in the same skip. It was **presence-keyed rather
than effect-keyed** — `apply_raft_outputs([ApplySnapshot@4])` against a boundary
of 5 returned `Ok` and left the group `Healthy`, because the variant was present
and not because the install cleared anything. And it was **not composable**: the
same runtime state passed or poisoned depending only on how the caller chunked
one runtime step's outputs into calls, so an empty leading chunk poisoned a
group whose install was in the next one. Neither is a fact about the replica.

**The other direction: reads were never covered.** `RaftGroup::read` with a
`ReadRequest::Local` never reached the guard at all — the group served the
truncated state machine and stayed `Healthy` — and `read_linearizable`'s
proof-consuming and pending-retry branches returned unstepped reports past it
too. Only a *fresh* linearizable read was covered, and only incidentally,
because it routes through `step`. The local path is the one reached in
production, at
[`reference/ledger/src/bin/ledger-node/replica.rs:477`](../reference/ledger/src/bin/ledger-node/replica.rs).
The regression test's own doc comment claimed the group "refuses rather than
answering later applies **and reads**" — wider than the code, which is the
programme's recurring shape and is why the sentence is part of the fix.

**The invariant, stated once.** *At every moment the group would let its state
machine answer for this replica, that state machine is at or above its own Raft
snapshot boundary.* The subject is a moment, not a vector. Being below the
boundary is a fault when the group would let the state machine speak for the
replica, and a legitimate transient otherwise — which is exactly the
recovering-versus-settled distinction the subsection above found the kernel
could not draw, applied one level finer. The group can draw it, because it is
the caller of both.

**What changes.** `reject_if_below_snapshot_boundary` is unchanged. Its two call
sites become:

| Call | Verdict | Why |
| --- | --- | --- |
| `step_with_options` | taken | A step advances the protocol on this replica's behalf — voting, acknowledging replication, granting read indexes — for state it does not hold. Unchanged from the subsection above. `step` and `begin_read_barrier` inherit it. |
| `begin_proposal`, `begin_proposal_batch` | taken (new) | They step the runtime *without* routing through `step_with_options`, so they inherited nothing. A proposal is how a state machine's contents are extended. |
| `read` | taken (new) | Every consistency, every branch, before the group-id check. Serving a query hands out the contents of a state machine short of acknowledged entries; that is the damage, not a stale answer. |
| `apply_raft_outputs` | withdrawn | The pump a recovering caller drains before restoring. A verdict here refuses the recovery rather than the fault, and would depend on the caller's chunking. |
| `metrics` | never had one | `applied_index` beside `snapshot_index` is the supported way to *see* a raised declaration. An observability call that poisoned the group would destroy the evidence it was called for. |

That is every public method of `RaftGroup` that reaches the runtime's `step*`
family or the state machine's `read`, which is the closure claim the scope
statement in `reject_if_below_snapshot_boundary`'s rustdoc now makes.

The two proposal entry points were not in the hunt's report. They were found by
checking the scope sentence this revision wanted to write against the code
rather than against the one entry point that spells the word "step", and the
probe that found them returned `ProposalDidNotStart` from a runtime that had
already been stepped. Writing "every step is refused" while `begin_proposal`
was not would have repeated the exact fault this revision exists to correct — a
doc sentence wider than the code beneath it.

Placing the verdict on `read` rather than on `read_local` and the two unstepped
branches of `read_linearizable` is deliberate: one entry covers every branch by
construction, including branches added later, and there is nothing for a future
reader to keep in sync. The proposal pair cannot be collapsed the same way
without changing their error semantics, so they are listed by name and tested by
name.

Nothing is given up by deferring past the pump. A replica that drains its
recovery outputs and never restores must step, propose, or read before it can do
anything observable, and all three refuse. The severe direction is closed
because the *only* call that now accepts a below-boundary state machine is the
one that cannot produce an effect on its own.

**What was deliberately not added.** No new guard, no new checked-closure
mechanism, no source-text scan, no gate. The `ApplySnapshot` skip is withdrawn
rather than made effect-keyed, because with no verdict at that seam there is
nothing left to key. No contiguity check on `apply_raft_outputs` — that a batch
may apply entry 6 onto a state machine at 3 is a different invariant, owned by
`validate_apply_floor`, and this entry does not touch it. `metrics` keeps
`unwrap_or(self.last_applied_index)`: a `#[must_use]` observability call cannot
return a `Result`, and the fallback only matters when the state machine itself
is failing, which is not this entry's subject.

**Blast radius.**

- `RaftGroup::apply_raft_outputs` stops returning
  `GroupError::AppliedIndexBelowSnapshotBoundary`. A caller matching on it there
  now never matches; the same error arrives from the next `step` or `read`.
- `RaftGroup::read` starts returning it, and poisons. This is a new failure mode
  for a method that previously could not fail this way — and it is the point.
- Neither reference store's *code* changes. Both regression suites move the call
  they observe the refusal on, and the ledger's gains the read that would have
  reported the short balance.
- `GroupError::AppliedIndexBelowSnapshotBoundary`'s own rustdoc, `read`'s,
  `apply_raft_outputs`', and the regression test's doc comment are corrected to
  the code beneath them.

**Focused-test plan.**
[`crates/rafter-app/tests/gen7_boundary_probe.rs`](../crates/rafter-app/tests/gen7_boundary_probe.rs)
adopts all seven probes, each converted to assert the intended behaviour, plus
one test per scope boundary: the two unstepped `read_linearizable` branches, the
two proposal entry points, the install that does and does not clear the
boundary, the chunking equivalence, `metrics` reporting the gap without taking a
verdict, and the crash-window replica that restores and the one that does not.
Nine of its thirteen tests fail against the pre-fix code. Mutation: with the
guard's remaining call sites removed, the fenced lock reissues token 1 over a
downstream that accepted 2 and the ledger reads back a balance short of the
acknowledged one — the same two symptoms, verbatim.

## Two Kernel Contract Corrections

Both come from the same hunt and neither changes a protocol decision; each
removes a sentence the code does not implement.

### 1. `install_local_snapshot` has a precondition and now states it

`Node::install_local_snapshot`'s rustdoc is one line — "Installs a local
snapshot descriptor and compacts covered log entries"
([`crates/rafter/src/node/log.rs:177-178`](../crates/rafter/src/node/log.rs)) —
and the body force-raises both indexes
([`:206-212`](../crates/rafter/src/node/log.rs)):

```rust
if self.volatile.commit_index < boundary_index {
    self.volatile.commit_index = boundary_index;
}
```

A follower holding three replicated-but-uncommitted entries jumps from commit 0
to commit 3 because the application handed it a descriptor:

```
install_local_snapshot advanced the commit index with no quorum evidence
  left: LogIndex(3)
 right: LogIndex(0)
```

The raise is correct for the *other* caller of the same helper.
`install_snapshot_state` runs the leader-sent install path, and a leader only
snapshots committed state, so the boundary carries quorum evidence. A local
descriptor carries none.

The workspace's own only caller already knows this.
`DurableRaftNode::compact_log_with_snapshot` validates the boundary against the
commit index and returns `RaftRuntimeError::SnapshotAheadOfCommit` before it
reaches the kernel
([`crates/rafter-runtime/src/lib.rs:644-655`](../crates/rafter-runtime/src/lib.rs)).
The precondition exists; it is enforced one layer up; the public kernel method
that needs it says nothing. A caller that uses `rafter` without `rafter-runtime`
— the sans-I/O audience this crate is published for — gets the unguarded
version.

**Design.** The signature becomes

```rust
pub fn install_local_snapshot(
    &mut self,
    snapshot: RaftSnapshot,
) -> Result<Vec<Output>, LocalSnapshotInstallError>
```

with one variant, `BoundaryAheadOfCommit { snapshot_index, commit_index }`, and
rustdoc that states the precondition, states that the applied-index raise
follows from the application having applied through the boundary in order to
build the snapshot, and points at the runtime's compaction API as the shipped
caller. `install_snapshot_state` is untouched: the leader-driven path keeps
raising the commit index, which is what makes an installed snapshot commit
evidence.

Breaking, pre-1.0, one in-repo call site (`rafter-runtime`, twice), which
already computes the same check and now propagates the kernel's error instead of
duplicating the rule.

**Rejected:** documenting the precondition without enforcing it. Unlike the
`max` in the entry above, there is no legitimate caller on the other side of
this one: a boundary beyond the committed prefix is a misuse under every
composition, so the kernel can refuse for all of them.

**One consequence, recorded rather than smoothed over.** With the precondition
enforced, a tracked local proposal can no longer be covered by a *local*
install: the boundary is at or below the commit index, everything at or below
the commit index has been applied in the same step that committed it, and apply
clears the tracker. `LocalProposalDropReason::SnapshotCovered` therefore
survives on one narrower path — a local descriptor whose boundary term
contradicts the retained entry at that index, which discards the suffix above
it along with the proposals tracked there. That path is reachable only for
callers using `rafter` without `rafter-runtime`, which validates the boundary
term first, and it is what
`local_snapshot_covering_tracked_proposal_emits_dropped_event` now exercises.
The variant keeps its coverage; the shape that covers it is narrower and is
written down here rather than discovered later.

### 2. `leader_replication_progress` reports followers, and says so

The method claims "leader-side replication progress for every effective replica"
([`crates/rafter/src/node/observe.rs:65-67`](../crates/rafter/src/node/observe.rs))
and iterates `progress.iter_followers()`, which filters out the leader's own
slot by construction
([`crates/rafter/src/node/state/membership/progress.rs:124-136`](../crates/rafter/src/node/state/membership/progress.rs)):

```
effective replicas [NodeId(1), NodeId(2), NodeId(3)] but progress reported
[NodeId(2), NodeId(3)]; missing [NodeId(1)]
```

**Design.** Correct the claim, not the code. The returned rows are
`ReplicationProgress { follower_id, .. }`; the leader has no `next_index` toward
itself and no send mode, and the `Probing`/`Replicating`/`Snapshotting` states
are meaningless for it. Inserting a synthetic self-row so the sentence becomes
true would put a value into a metrics stream that every existing consumer would
then have to filter back out.

The rustdoc instead names the scope in the direction the code uses it — every
effective *follower*, learners included — and states what falls outside it and
where to get it: the leader's own match index is `last_log_index()` by
construction, which is the fact a caller doing quorum math needs. A test pins
both halves.

**Rejected:** including the leader. It changes a published observability shape to
satisfy a sentence, and the field name would then lie instead of the doc.

## Many-Group Tick Passes, Group Retirement, and a Host Error That Renders

### Origin

`MultiRaftHost::tick_all` is four lines, and the fourth is the defect
([`crates/rafter-multiraft/src/host.rs:96-102`](../crates/rafter-multiraft/src/host.rs),
mirrored verbatim at
[`typed.rs:153-159`](../crates/rafter-multiraft/src/typed.rs)):

```rust
let group_ids = self.groups.keys().cloned().collect::<Vec<_>>();
group_ids
    .into_iter()
    .map(|group_id| self.step_group(&group_id, GroupInput::Tick))
    .collect()
```

`collect()` into `Result<Vec<_>, _>` short-circuits. Every `Ok` produced before
the first `Err` is dropped on the floor, and the layer below states exactly what
is in them
([`crates/rafter-app/src/group/types.rs:209-213`](../crates/rafter-app/src/group/types.rs)):

> Results the state machine returned from applying committed entries.
> An entry reaching this list has committed and applied. This is the only
> list that proves a write took effect.

`peer_messages` survives a dropped report because Raft re-sends. `applied` does
not: nothing re-emits it. So a two-group host whose second group's driver fails
its tick advances group 1's `applied_index` to 1, hands the caller a bare
`MultiRaftError::Driver { group_id: 2 }`, and the client future waiting on
group 1's write waits forever. There is no recovery path, because there is no
second copy.

The same line has a second consequence with a longer blast radius. `BTreeMap`
iterates in key order, so the pass always fails at the same group, and every
group behind it is never stepped at all — no election timeout, no heartbeat, no
replication, no commit. Over 100 `tick_all` passes with a broken group 1 and a
healthy group 2, group 1 is stepped 100 times and group 2 is stepped **0**
times. And `rafter-app`'s poison is permanent by construction
([`GroupError::Poisoned`](../crates/rafter-app/src/error.rs)), so a poisoned
lowest-keyed group ends all Raft activity in the process until restart.

That is a stated 1.0 acceptance criterion, negated
([`docs/reference-consumers.md:364-366`](./reference-consumers.md)):

> - work and failure in one group do not corrupt another;
> - a poisoned group cannot stop unrelated groups;

The host also has no way out of it. There is no `remove_group`, `close_group`,
or eviction of any kind anywhere in the crate: a driver handed to `open_group`
is owned until the host is dropped. `open_group` will not even hand back the
driver it *refuses* — both error paths take `driver: D` by value and drop it,
so a caller that misroutes an open loses the storage handles the driver owned.

The rustdoc does not say any of this. `host.rs:87` says "Ticks every open group
in deterministic key order", which the code stops doing at the first failure,
and the `# Errors` block below it never mentions that the pass ends.

The third finding is the error surface itself.
[`crates/rafter-multiraft/src/error.rs`](../crates/rafter-multiraft/src/error.rs)
is thirteen lines with no `impl` block at all: `MultiRaftError` is the only
public error type in the workspace implementing neither `Display` nor
`std::error::Error`. Upstream of it, the crate's only shipped blanket driver
impl — the one on the path of every real embedder — throws the typed error away
([`typed.rs:56`](../crates/rafter-multiraft/src/typed.rs)):

```rust
RaftGroup::step(self, input).map_err(|error| format!("{error:?}"))
```

`GroupError` has twenty `#[non_exhaustive]` variants, a `Display`, a `source()`
chain, and an `ErrorCause` a caller can `downcast_ref`. All of it becomes a
`Debug` string, so a caller cannot tell `Poisoned` — permanent, retire the group
— from `Runtime` — which may not recur. And because `MultiRaftError` derives
`Eq`, two unrelated failures that render alike compare *equal*, which
`crates/rafter-service/src/error.rs:243-247` forbids in as many words:

> Equality is deliberately absent. An error carrying a `dyn Error` has no
> honest equality: comparing `Arc` pointers makes two errors built from the
> same failure unequal, and comparing rendered output rebuilds the
> stringly-typed semantics this surface exists to remove.

This document already named that defect and deferred it
([Typed Service Failure Surface](#typed-service-failure-surface)): the
`String` in `GroupDriver::step` was "the independent second occurrence the rule
asks for", left alone because no consumer had exercised it. That deferral is
now spent.

### Classification

Raft and durable-lifecycle mechanism, on all three counts, and the first two are
not close. Losing a committed apply result is a durability defect in the layer
whose entire job is to hand committed effects to a caller. Starving a group of
its tick is denying it elections and heartbeats, which is Raft liveness. Neither
is application policy, and neither is expressible by a consumer working around
the host: `tick_all`'s reports are gone before any caller sees them, and a
consumer cannot evict a group from a map it has no handle to.

Typed failure behavior is required of every promoted API
([`docs/reference-consumers.md:462`](./reference-consumers.md)), and the 1.0
production composition requires "structured metrics and failure diagnostics"
([`:311`](./reference-consumers.md)). Neither is reachable from a `String`: a
metrics label taken from a rendered `GroupError` has unbounded cardinality,
because those messages embed node IDs, log indices, and proposal IDs.

**The consumer, and the honesty about it.** The sharded counter is the stated
acceptance workload for the managed multi-Raft scheduler
([`docs/reference-consumers.md:300-317`](./reference-consumers.md)), and it
declares no Rafter dependency yet
([`:390`](./reference-consumers.md)). So this entry has no workaround to cite
and no line count to delete — the promotion rule's usual evidence. What it has
instead is a written contract the current code demonstrably fails, and the
audit's repros are that failure made executable. Where a shape below follows
from a clause in that contract, the clause is quoted. Where it does not, the
entry says the shape is judgement and states what would change it.

One boundary is drawn on the other side deliberately. The counter's workload
lists "group creation, draining, removal, reopening, and tombstoning" and
"messages arriving after removal". Creation, removal, and reopening are group
lifecycle and land here. **Tombstoning does not**, and the reason is the
classification rule: a tombstone is a retention decision — how long after a
group is removed may its traffic still arrive — and that horizon is a
deployment property, not a Raft one. A host that kept every removed key forever
would grow without bound on behalf of a policy it cannot see. The host reports
`UnknownGroup` for a removed key, exactly as for a key that never existed, and a
caller that must tell those apart holds its own tombstone map. This is stated in
the rustdoc and pinned by a test rather than left to be discovered.

### Design

Five changes in `crates/rafter-multiraft`, and none in any other crate. The
typed and untyped hosts are line-for-line mirrors today and stay mirrors after.

#### 1. A tick pass visits every group

`tick_all` stops returning a `Result`. It returns the pass.

```rust
/// One complete pass over every group a host held when the pass began.
///
/// This is the executable form of the scheduler contract's unit of fairness:
/// "every continuously ready group receives a scheduling opportunity within
/// one complete pass over the ready set"
/// (`docs/reference-consumers.md`). A pass carries one outcome per open group,
/// in the host's key order, whatever any individual group did — a failing
/// group consumes its own opportunity and nobody else's.
#[derive(Debug)]
#[must_use = "a tick pass carries every group's report, and a report's `applied` \
              list is the only proof a write took effect; it is never re-emitted"]
pub struct TickPass<G, R> {
    outcomes: Vec<GroupOutcome<G, R>>,
}

impl<G, R> TickPass<G, R> {
    /// Every outcome, one per group, in the host's key order.
    #[must_use]
    pub fn outcomes(&self) -> &[GroupOutcome<G, R>];

    /// Takes the outcomes, which is how a caller routes what the pass proved.
    #[must_use]
    pub fn into_outcomes(self) -> Vec<GroupOutcome<G, R>>;

    /// The number of groups this pass stepped.
    ///
    /// Equal to the host's open-group count at the moment the pass began. A
    /// caller asserting fairness compares this against `MultiRaftHost::len`.
    #[must_use]
    pub fn visited(&self) -> usize;

    /// The reports of the groups that stepped successfully.
    pub fn reports(&self) -> impl Iterator<Item = &GroupStepReport<G, R>>;

    /// The groups that failed, with the host key each failed under.
    pub fn failures(&self) -> impl Iterator<Item = (&G, &MultiRaftError<G>)>;

    /// Whether every group in this pass stepped successfully.
    #[must_use]
    pub fn is_complete(&self) -> bool;
}

/// One group's outcome within a pass.
///
/// `group_id` is the *host key* that was stepped, not the group ID the driver
/// reported. The two can disagree — that disagreement is
/// [`MultiRaftError::InvalidReport`] — and only the host key is authoritative.
#[derive(Debug)]
pub struct GroupOutcome<G, R> {
    pub group_id: G,
    pub result: Result<GroupStepReport<G, R>, MultiRaftError<G>>,
}
```

The named type rather than a bare `Vec<GroupOutcome<_, _>>` earns its place on
one argument: the fairness bound "must remain a deterministic assertion rather
than a latency impression from benchmarks"
([`docs/reference-consumers.md:355-356`](./reference-consumers.md)), and
`visited()` is where that assertion is written down and checked. A `Vec` can
carry the same data and cannot carry the promise.

Only two error variants can appear inside a pass — `Driver` and
`InvalidReport` — because the keys come from the host's own map and a `Tick`
carries no group ID to mismatch. The rustdoc says so rather than leaving a
caller to infer it from four unreachable arms.

#### 2. A host can retire a group

```rust
/// Retires `group_id`, returning its driver.
///
/// The driver is returned rather than dropped so a caller can drain it — step
/// it to quiescence, read its final metrics, close what it owns — after the
/// host has stopped scheduling it. Retiring a group is how a poisoned group
/// stops consuming a scheduling opportunity it can never use.
///
/// Idempotent: retiring a key that is not open returns `None`.
///
/// This host keeps no record of a retired key. A later input for it is
/// [`MultiRaftError::UnknownGroup`], indistinguishable from a key that never
/// existed, and `open_group` will reopen it. A caller that must fence late
/// traffic against a removed group holds that tombstone itself; see the
/// classification note above for why the horizon is not this crate's to pick.
pub fn remove_group(&mut self, group_id: &G) -> Option<Box<dyn GroupDriver<G>>>;
```

With four accessors the host never had, because a host that cannot be asked
what it holds cannot be scheduled over: `len`, `is_empty`, `contains_group`, and
`group_ids`.

#### 3. The driver reports a typed, categorized failure

`GroupDriver::step` and `TypedGroupDriver::step` stop returning `String`.

```rust
/// Why a group driver could not complete a step.
///
/// This kind answers **permanence and nothing else**: may this group be
/// stepped again, or is it finished? It deliberately does not answer what a
/// failed proposal's fate was. That question is answered by
/// `ProposalEvent` in the report, and a second answer here would be a second
/// place to disagree with it.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum DriverErrorKind {
    /// The group will never make progress again. Retire it.
    Poisoned,
    /// The step failed and the group has not declared itself permanently
    /// unusable. This is the absence of a poison, not a promise that a retry
    /// succeeds.
    Transient,
}

impl DriverErrorKind {
    /// Whether this failure retires the group.
    ///
    /// Written as a positive test for `Poisoned` so an unrecognized future
    /// kind reads as *not* permanent. That is the safe direction: continuing
    /// to tick a dead group wastes a scheduling opportunity, while retiring a
    /// live one destroys a driver that still owned committed state.
    #[must_use]
    pub const fn is_permanent(self) -> bool { matches!(self, Self::Poisoned) }
}

/// A group driver's failure: what kind, and what actually failed.
#[derive(Clone, Debug)]
pub struct DriverError {
    kind: DriverErrorKind,
    cause: ErrorCause,
}

impl DriverError {
    pub fn new(kind: DriverErrorKind, cause: ErrorCause) -> Self;
    #[must_use] pub const fn kind(&self) -> DriverErrorKind;
    #[must_use] pub const fn cause(&self) -> &ErrorCause;
}

impl fmt::Display for DriverError;
impl std::error::Error for DriverError;  // source() -> the preserved cause
```

`ErrorCause` is re-exported from `rafter-app`, not redeclared, under the rule
this document already set: "a caller must be able to compare the value it
receives here with the one `rafter-app` produced, so there can be only one
type" ([`crates/rafter-service/src/error.rs:29-33`](../crates/rafter-service/src/error.rs)).

The blanket impl on `RaftGroup` then preserves what it used to render, and —
this is the part that matters — **reports the permanence it observed rather
than inferring it from the variant**:

```rust
Err(error) => {
    let kind = match self.fatal_state() {
        GroupFatalState::Poisoned { .. } => DriverErrorKind::Poisoned,
        GroupFatalState::Healthy => DriverErrorKind::Transient,
    };
    Err(DriverError::new(kind, ErrorCause::new(error)))
}
```

A failure that *causes* a poison does not return `GroupError::Poisoned` — it
returns the underlying fault, and the group is poisoned afterwards. Classifying
by variant would therefore call the first poisoning failure transient and only
the second one permanent. Asking `fatal_state()` after the step is the same
discipline `WriteFate` states one crate over: report what was observed, never
what a category implies.

#### 4. The host error renders, chains, and projects

```rust
/// Errors returned by a many-group host.
///
/// Equality is deliberately absent, for the reason `rafter-service` gives:
/// an error carrying a `dyn Error` has no honest equality, and comparing
/// rendered output rebuilds the stringly-typed semantics this reshaping
/// removes.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum MultiRaftError<G> {
    GroupAlreadyOpen { group_id: G },
    UnknownGroup { group_id: G },
    /// The *caller's input* named another group. Nothing was stepped.
    WrongGroup { expected: G, actual: G },
    /// The driver stepped, and then returned a report naming another group.
    ///
    /// The driver has already mutated itself; whatever the report described
    /// has happened. The report is discarded because a driver that cannot
    /// name its own group has forfeited the contract that made its `applied`
    /// list mean anything — so this is a data loss, it is not silent, and the
    /// repair is to retire the group.
    InvalidReport { group_id: G, field: &'static str, reported: G },
    /// The report carried a `#[non_exhaustive]` variant this host does not
    /// recognize. The host failed to understand it; the driver did nothing
    /// wrong.
    UnrecognizedEvent { group_id: G, field: &'static str },
    Driver { group_id: G, kind: DriverErrorKind, cause: ErrorCause },
}

impl<G: Debug> fmt::Display for MultiRaftError<G>;
impl<G: Debug> std::error::Error for MultiRaftError<G>;  // source() -> cause

/// Stable, payload-free category of a [`MultiRaftError`].
///
/// `MultiRaftError<G>` is generic over a caller-defined key and carries those
/// keys in its payloads, so it is neither a bounded metric label nor a map
/// key. This is. A host running thousands of groups aggregates failures by
/// this and by nothing else.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum MultiRaftErrorKind {
    GroupAlreadyOpen,
    UnknownGroup,
    WrongGroup,
    InvalidReport,
    UnrecognizedEvent,
    DriverPoisoned,
    DriverTransient,
}

impl<G> MultiRaftError<G> {
    #[must_use] pub const fn kind(&self) -> MultiRaftErrorKind;
}
```

`Display` and `Error` are bounded on `G: Debug`, not `G: Display`, because
`Debug` is what the hosts already require of a key and what the crate's own
`ShardId`-shaped examples implement. Requiring `Display` would make the error
type unusable for the keys the crate ships examples for.

The split between `WrongGroup` and `InvalidReport` is the whole of finding M4a.
Today both are `WrongGroup { expected: 1, actual: 2 }` — byte-identical — and
they are opposite facts: one means nothing happened, the other means something
happened and was dropped. That is the same conflation `WriteFate` exists to
prevent, arriving independently in a second crate.

`Driver` carries `kind` and `cause` flat rather than a nested `DriverError`, so
`source()` walks one link per real failure: host error → the preserved
`GroupError` → the state machine's own error. `DriverError` remains the driver
trait's return type, since a trait needs one error type to return.

#### 5. `metrics` stops being all-or-nothing, and `open_group` gives the driver back

```rust
/// Metrics for every group whose driver reported the key it is open under.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct MultiRaftMetrics<G> {
    pub groups: Vec<RaftGroupMetrics<G>>,
    /// The groups excluded, and why.
    ///
    /// A driver that reports another group's identity is excluded rather than
    /// published under a key it disowns — publishing it would put a
    /// fabricated `group_id` into a metrics stream. It is listed here rather
    /// than dropped, so an operator sees a gap and its reason instead of a
    /// shorter list.
    pub failures: Vec<MultiRaftError<G>>,
}

pub fn metrics(&self) -> MultiRaftMetrics<G>;   // no longer fallible
```

and

```rust
/// A refused `open_group`, carrying the caller's driver back.
#[derive(Debug)]
pub struct OpenGroupRejected<G, D> {
    pub error: MultiRaftError<G>,
    pub driver: D,
}

pub fn open_group<D>(&mut self, group_id: G, driver: D)
    -> Result<(), OpenGroupRejected<G, D>>;
```

A driver owns a runtime, a state machine, and open storage handles. Destroying
one because the caller passed the wrong key is a data-loss bug wearing a
validation error's clothes; `std` returns the value on `mpsc::SendError` and
`String::from_utf8` for the same reason.

### Semantics and edge cases

**A pass is a snapshot of the key set.** `tick_all` collects keys, then steps
them. A driver cannot reach the host to open or retire a group mid-pass — the
host is `&mut`-borrowed for the duration — so `visited()` equals `len()` at
entry, always. A caller that retires a group in response to a pass does it
after the pass, and the next pass is one shorter.

**Retiring a group inside a pass's own results.** A pass may report
`DriverErrorKind::Poisoned` for a group whose report is also in the pass —
`into_outcomes` hands back both, and routing the report before retiring the
group is the caller's obligation, not the host's. The host never retires
anything on its own: a host that evicted on poison would destroy a driver whose
storage the caller may still need to inspect. This is stated where `remove_group`
is documented.

**`InvalidReport` does not retire the group either.** The host has no basis to
decide that a misreporting driver is unusable rather than merely wrong once;
the rustdoc says retirement is the repair and leaves the decision with the
caller.

**Five of seven `GroupInput` variants carry no group ID.** `PeerMessage` and
`ReadBarrier` carry one and are cross-checked against the host key. `Tick`,
`Proposal`, `ProposalBatch`, `Membership`, and `TransferLeadership` do not, so a
caller's shard-map bug routes a command into the wrong group and **this host
cannot detect it** — the information required to detect it is not in the input.
Nothing here can fix that; the rustdoc names the two inputs the check covers
instead of describing a check that sounds total, and a test pins both halves,
including the uncomfortable half that proves a misrouted proposal is accepted.

**`Default` is hand-written.** Deriving it put `G: Default` on the untyped host
and `G: Default, C: Default, R: Default` on the typed one — `C` and `R` are the
command and result types, which the host never stores by value — so
`TypedMultiRaftHost::<u64, MyCommand, MyResult>::default()` did not compile for
any real command type. The hand-written impl forwards to `new` and carries only
`new`'s bounds.

### Blast radius

Entirely within `crates/rafter-multiraft` plus the three examples it ships.

- **Breaking:** `tick_all` (return type), `metrics` (no longer `Result`),
  `open_group` (error type), `GroupDriver::step` and `TypedGroupDriver::step`
  (error type), `MultiRaftError` (loses `Eq`/`PartialEq`, gains two variants and
  `#[non_exhaustive]`, `Driver`'s payload changes), `MultiRaftMetrics` (gains a
  field and `#[non_exhaustive]`, loses `Eq`/`PartialEq`).
- **Additive:** `remove_group`, `len`, `is_empty`, `contains_group`,
  `group_ids`, `TickPass`, `GroupOutcome`, `DriverError`, `DriverErrorKind`,
  `MultiRaftErrorKind`, `OpenGroupRejected`, the `ErrorCause` re-export, and
  every `Display`/`Error` impl.
- **No change:** `rafter-app`, and every crate below it. This was checked rather
  than assumed: the reshaping needs `GroupError<E, R>` to be an
  `Error + Send + Sync + 'static` so `ErrorCause::new` accepts it, and it
  already is — `ReplicatedStateMachine::Error` and
  `PersistedRaftRuntime::Error` both carry that exact bound today. The
  permanence classification needs `RaftGroup::fatal_state`, which is public. So
  the argument for touching `rafter-app` fails on its own terms, and it is not
  touched.
- `bench-compare` uses `open_group` and `step_group` on real `RaftGroup`s and
  needs no source change, but is outside the root workspace and must be built.

Dropping `Eq` is the change most likely to hide a regression, for the reason
step 13 gave: it rewrites a pile of assertions mechanically. Every converted
assertion should say more than it did, not less — an `assert_eq!` on a whole
error becomes a `kind()` check *and* a field check, not a `matches!` that
accepts any payload.

### Focused-test plan

The audit's eight repros are adopted verbatim as the regression suite, at
`crates/rafter-multiraft/tests/audit_adversarial.rs`, and every one of them
fails against the pre-fix tree. Added to them, one per boundary this entry
draws:

1. A pass over a host with a failing group still carries every healthy group's
   `applied` list — untyped and typed.
2. Over 100 passes, a broken group and a healthy group are each stepped 100
   times.
3. `visited()` equals `len()` for a pass in which every group fails.
4. A retired group's driver comes back, its key stops being stepped, and the
   key reopens.
5. Retiring an absent key returns `None` twice.
6. A message for a retired key is `UnknownGroup` — the tombstone boundary,
   asserted in the direction the crate actually behaves.
7. A refused input and a refused report have different `kind()`s.
8. A misreporting driver is excluded from `metrics().groups` and named in
   `metrics().failures`, while every healthy group stays visible.
9. A refused `open_group` returns a driver that has not been dropped.
10. A poisoning `RaftGroup` reaches the caller as `DriverErrorKind::Poisoned`
    with a `GroupError` recoverable by `downcast_ref` — the blanket impl's own
    path, through a real group.
11. `MultiRaftError` and `DriverError` render non-empty `Display` output and
    chain to the preserved cause through `source()`.
12. A `Proposal` for the wrong shard is accepted, because the input carries no
    group ID to check.

Every new guard is mutation-tested: each check is inverted or deleted in turn
and the test that must fail is recorded.

### Rejected alternatives

**Keep `tick_all -> Result` and document the short-circuit.** This is what the
current rustdoc almost does. It cannot work: the caller receives an `Err` and
the successful reports do not exist any more, so no amount of documentation
gives them a way to route what already committed.

**Have the host retire a poisoned group automatically.** Tempting, and wrong in
the same direction as every other silent recovery: the driver owns storage the
caller may need, and "poisoned" is reported by the driver, which is the party
whose judgement is in question when it is misbehaving. The host surfaces the
kind and the caller retires.

**Return the invalid report alongside `InvalidReport`.** It would need
`MultiRaftError<G, R>` — a second type parameter on an error type — to carry
`GroupStepReport<G, R>`, and it would invite a caller to route apply results
produced by a driver that just proved it does not know which group it is. The
loss is real and is documented as a loss.

**`remove_group` returning the caller's concrete `D`.** Requires `Any` on the
driver trait, or a second generic method that cannot be object-safe. The
returned `Box<dyn GroupDriver<G>>` still steps and still reports metrics, which
is what draining needs. Revisit if a consumer shows a drain that needs the
concrete type.

**A `fate` on `DriverError`, mirroring `WriteFate`.** The write fate exists
because `rafter-service` answers a client that asked "may I retry?". This crate
answers a caller that already holds `GroupStepReport::proposal_events`, which
carries per-proposal outcomes. Adding a second, coarser answer creates a place
for the two to disagree. Not until a consumer shows the report is insufficient.

**Tombstoning removed group keys in the host.** Covered above: unbounded state
in service of a retention horizon the host cannot see.

### After-state

`rafter-multiraft` stops being the crate that claims what it does not do. The
crate docs said the host retained "explicit control over routing, storage,
authorization, recovery, metrics" — it has no authorization and no recovery, in
any sense — and the README's twelve lines never used the words "manual",
"scheduler", or "fairness". Both now state, first, that this is a manual host:
it steps what it is told to step, it does no scheduling, no fairness, no
admission control, no backpressure, and no queueing, and the component that
does is the managed scheduler this document's consumer contract describes and
that does not exist yet.

What the crate does not do that a managed scheduler would, stated for the
counter consumer: it does not decide *when* to step anything, so ticks arrive
only as often as the caller loops; it enforces no per-group work quota, so a
group with slow storage blocks the pass for as long as its driver takes; it has
no queues and therefore no queue limits and no backpressure; it does not
prioritize control traffic over bulk replication; it does not retire a poisoned
group on its own; and it keeps no tombstones. `TickPass::visited` is a fairness
*measurement*, not a fairness *mechanism* — it proves the pass reached every
group, which is the weakest of the properties the scheduler contract lists and
the only one a manual host can offer.

## Coupled designs

The eleven promotions form seven surfaces, not eleven independent additions.
The first six form four; the service-layer cluster adds three more, one of
which reaches back into the app layer.

**Step reporting — the read report and the rejection hint.** Both establish the
same rule: *a step report is the complete record of its step, and every event in
it carries what the immediate outcome would have carried.* The read report makes
the record reachable from the one operation that hid it; the leader hint makes
one of its events self-sufficient. They must land together in the consumer's
view, because a driver that switches to `RaftGroup::read` in order to observe
proposal events is switching to observe events that would still be missing their
redirect. Implement the hint first: it changes a type carried inside the report,
so the read-report tests can then assert final shapes.

**Restart — decomposition and the owned pending transfer.** The ledger's restart
path is blocked twice. `into_parts` and `into_storage` hand back the state
machine and the stores; the owned pending transfer makes a handed-back store
implementable over anything that is not a plain owned field. A consumer adopting
only decomposition still needs a mirror for any store it wraps. A consumer
adopting only the owned transfer still cannot get the store back. Together they
delete the consumer's entire storage-support layer.

**Readiness completes the restart story.** `into_storage` produces a new
incarnation; `committed_application_index` says when that incarnation may serve.
The two are used in the same sequence and documented as a pair: decompose,
recover, apply recovery outputs, then gate on the index and on
`RaftGroup::fatal_state`.

**The application floor — readiness and freshness are one derivation.** The
read-barrier floor is `committed_application_index` bounded at a read index
instead of at the commit index, so the two are one method with two callers, not
two methods. They must land as one change to the runtime trait: introducing the
unbounded form first and generalizing it later would rewrite all eight
implementors twice, and would leave a released signature whose obvious use in a
read barrier is wrong. Implement the bounded form as the required method from
the start and derive the readiness accessor from it. (History spent this rule
the expensive way: the unbounded form shipped in the first wave and the
read-barrier work rewrote all eight implementors — Adoption order step 3
records it. The rule stands for the next occasion.)

**Failure typing — the error surface and the read vocabulary are one change to
one type.** `ReadError::Transport` gains a required `ErrorCause`, and an
abandoned read has no cause to put in it: nothing failed, the driver stopped
waiting. So the stalled-read path cannot keep borrowing that variant, and
`ReadError::Abandoned` is not a refinement of the error work but the thing that
lets the error work land without leaving one path unable to construct its own
error. They are one edit to `ReadError`, made once.

**The transport driver is the first producer of the vocabulary the other two
entries define.** `TransportRaftDriver::release_group` is the first caller of
`UnknownOutcomeReason::DriverReleased` and `ReadAbandonReason::DriverReleased`,
and the first place a transport's send failure needs an `ErrorCause` — which is
why `RaftTransport::Error` gains its bound in the driver entry rather than the
error entry, alongside the code that needs it. The three entries share one
rule: *a driver reports what it observed, in the vocabulary of the layer it
observed it in.* A driver that renders has stopped reporting.

**Two entries change `ReplicatedStateMachine`, and the same ten implementors
pay for each.** [Typed Service Failure Surface](#typed-service-failure-surface)
raises `type Error` to `Error + Send + Sync + 'static`, which six implementors
fail today; [Declared Snapshot Support](#declared-snapshot-support) adds a
required const and changes two method signatures, which all ten must answer.
The arguments are independent and the edits overlap almost completely, so they
land as one `rafter-app` step. Splitting them across releases would open the
same ten files twice to make one decision each.

**The re-export rule is a constraint on the other four, adopted once.** Two new
error types, two new driver types, a new reason enum, a new fate enum, two kind
projections, and one type promoted out of `rafter-app` all have to be reachable
from `rafter_service`'s root. [Driver Boundary Re-exports](#driver-boundary-re-exports)
states the rule and adds the test that enforces it, so the other entries extend
a list rather than each re-deciding what belongs on it.

**The many-group host's three findings are one surface.**
[Many-Group Tick Passes, Group Retirement, and a Host Error That Renders](#many-group-tick-passes-group-retirement-and-a-host-error-that-renders)
reads as three unrelated defects — a lost report, an absent eviction, a
stringly-typed error — and they meet in one type. A tick pass carries a
`MultiRaftError` per group, so the pass shape cannot be settled until the error
shape is; the error's permanence category is what tells a caller to retire a
group, so retirement without it is an API nobody can decide to call; and
retirement is what makes the pass's fairness promise recoverable rather than
merely observable. Fixing any one of the three and stopping leaves the host
still unable to survive a poisoned group. They land in three steps for
bisectability, not because they are separable.

## Adoption order

The sequence minimizes churn by moving from the lowest crate upward, so no
change is written twice and every step ends green.

1. **`RaftSnapshotStore::current_pending_snapshot_transfer` returns owned.**
   `rafter-storage` first, with its two concrete stores, its own tests, the two
   `rafter-runtime` construction call sites, and the three fault-injection
   implementors. Nothing above the storage crate changes behavior.
2. **`DurableRaftNode::into_storage`.** Additive in `rafter-runtime`, and now
   able to return stores that any consumer can wrap.
3. **`committed_application_index_through` and `committed_application_index`,
   whole.** `rafter-runtime-api` with the bounded form as the required method
   and the unbounded form provided from it, then `DurableRaftNode`, then the
   seven other test and bench implementors, then both one-line `RaftGroup`
   forwarders. The bounded form lands here rather than in step 7 so no
   implementor is written twice; see
   [Coupled designs](#coupled-designs). Do this before any other `rafter-app`
   change so the app layer compiles once against a complete runtime trait.

   **This is the one step the tree did not follow, and the cost was the
   predicted one.** The unbounded method shipped first, across all eight
   implementors, before the read-barrier floor was designed; the bounded form
   then landed as its own change after step 9's consumer adoption and rewrote
   the same eight. So every implementor *was* written twice, which is exactly
   what this step and [Coupled designs](#coupled-designs) set out to avoid.
   Nothing about the end state is wrong — the required method is the bounded
   form and the unbounded one is provided from it — but the sequencing
   rationale above describes a plan rather than a record, and the ordering
   argument stands unspent for the next generalization of a required trait
   method.
4. **`ProposalEvent::Rejected { leader_hint }`.** `rafter-app`, plus
   `proposal_begin_from_report`, plus the `rafter-service` write path that stops
   reconstructing the hint.
5. **`RaftGroup::read` / `read_outcome` and `ReadReport`.** `rafter-app`, then
   the `rafter-service` read path, the manual example, and the app-layer read
   tests. Landing after step 4 means the new report tests assert the final
   `ProposalEvent` shape.
6. **`RaftGroup::into_parts` and `RaftGroupParts`.** Additive, and last of the
   additive app-layer changes because its tests exercise the rebuilt-group path
   that steps 3 and 5 also touch.
7. **The read-barrier application floor.** `PendingRead` and the grant arm in
   `rafter-app`, then `complete_ready_reads` and
   `try_complete_pending_query_read`, then the kernel's read-index module doc,
   then the `rafter-maelstrom` gate. It consumes step 3's bounded method and
   lands after step 5 so its new tests assert the final `ReadReport` shape.
   Re-read every test in the step-5 suites that asserts a proof triple: they
   mostly survive, but only because their fixtures place an application entry
   at the read index.
8. **The `RD-04` restatement.** Split the clause in
   [`verification/raft-invariants.yaml`](../verification/raft-invariants.yaml)
   and regenerate [`docs/raft-invariants.md`](./raft-invariants.md) with
   `scripts/render-raft-invariants-doc`. Separate from step 7 so the code change
   and the evidence change are reviewable apart, and last of the two because the
   new clause's evidence is the tests step 7 adds.
9. **Reference-consumer adoption.** Delete the five workarounds in
   [`reference/ledger/tests/support/`](../reference/ledger/tests/support) and
   un-pin the read-cancellation test in
   [`reference/fenced-lock/tests/adapter_cluster.rs`](../reference/fenced-lock/tests/adapter_cluster.rs),
   then re-run both source mode and package-consumer mode. The consumers are the
   acceptance evidence for the promotions, so they are adopted last and re-run in
   full.

Steps 1, 3, 4, and 5 are breaking. Step 7 changes observable behavior without
changing a signature, which is the harder kind to review: it is the only step
whose correctness argument lives in prose rather than in a type, and its
negative tests are the acceptance evidence. Each break is confined to one crate
and the crates above it in the same step, and `reference/` and `bench-compare/`
are outside the root workspace — they must be built explicitly for every one of
those steps.

Steps 1–9 are the first wave and are complete in the tree. The numbering
continues below for the service-layer cluster, which moves the same way: lowest
crate upward, so no change is written twice and every step ends green.

10. **`ErrorCause` and the app-layer poison cause.** `rafter-app` only, and
    additive: the new type, `GroupError::Poisoned { cause }`,
    `RaftGroup::poison_cause`, `RaftGroupParts::poison_cause`, and the retention
    slot in `enter_poisoned`. `GroupError` and `RaftGroupParts` derive `Debug`
    only, so no implementor and no trait impl changes. Doing it first means the
    two crates above it have a cause type to preserve into before either of them
    needs one.

    The retention is plumbing here and only plumbing: `ErrorCause::new` requires
    `Error + Send + Sync + 'static`, and the only poison path with a typed error
    to preserve is `poison_with_state_machine_error`, whose source is
    `A::Error` — unbounded until step 11. Supplying a bound in this step would
    propagate it to every `RaftGroup` method and *be* step 11's break, in the
    wrong place. So every caller passes `None` here, the two poisons that
    genuinely have no underlying error keep passing `None` forever, and step 11
    wires the state-machine path as the first producer.
11. **`ReplicatedStateMachine`, both changes at once.** The `Error` bound, the
    required `SNAPSHOT_SUPPORT` const, the two provided snapshot bodies and
    `ApplicationSnapshotError`, `GroupError`'s two new snapshot variants, the
    declaration check in `apply_snapshot_output`, and — now that the bound
    exists — the state-machine poison cause step 10 left unsupplied. Then one
    pass over all ten implementors: six declare a typed error, seven delete or
    rewrite their snapshot methods, and the two examples that install a snapshot
    by clearing their state are fixed rather than carried forward. Breaking, and
    the only step whose adoption is expected to change behavior in files nobody
    set out to touch; see [Coupled designs](#coupled-designs) for why it is not
    split.
12. **The `rafter-service` re-export completion.** `DriverFuture`,
    `StateMachineOperation`, the `metrics_watch_from_current` deletion, and the
    `public_surface.rs` test. Small, independent, and placed first in the
    service crate so every later step extends a list that is already correct
    rather than re-deciding what belongs on it.
13. **The reshaped service errors.** `ErrorCause`-carrying variants, `fate`,
    `kind`, `WrongGroup`, `StateMachine` replacing `ApplyFailed`, the dropped
    `Eq` derives, the `MetricsError::Transport` deletion,
    `ReadError::Abandoned` with `ReadAbandonReason`, and
    `UnknownOutcomeReason::DriverReleased`, together with every call site in
    `mapping.rs`, `write.rs`, `read.rs`, `state.rs`, and `in_memory.rs`. The
    largest step in the cluster, and the one whose review matters most: it is
    the only step that changes what a client may conclude from an error.
    Breaking.
14. **The transport seam.** `RaftTransport::Error` and
    `AsyncRaftTransport::Error` gain their bound, then `TransportRaftDriver`,
    `TransportDriverOptions`, `InboundEnvelopeError`,
    `ManagedDriverError::{NoGroup, GroupAlreadyAdopted, InvalidOptions}`, and
    `InMemoryRaftDriver::release_groups`. Additive apart from the bound, and
    after step 13 so the new driver builds its errors in their final shape and
    never carries a rendering path that step 13 would have to delete.

    `InMemoryRaftDriver::release_groups` needs no waiter release of its own: that
    driver resolves every client future inside the call that created it, so
    there is never an outstanding waiter when it is called. It drops undelivered
    frames, closes its metrics, and refuses afterwards, which is what its
    documented counterpart promises.
15. **Reference-consumer adoption.** Collapse
    `reference/fenced-lock/src/adapter/client.rs`'s `closes_outcome_window`,
    delete `reference/fenced-lock/tests/support/cluster.rs:90-789,1393-1478` in
    favour of `TransportRaftDriver`, delete `LockStateMachine`'s two snapshot
    methods and `LockAdapterError::DurableSnapshotUndefined`, and re-run both
    source mode and package-consumer mode. The narrowing in
    `closes_outcome_window` is the acceptance evidence for step 13 and the
    ~790-line deletion is the acceptance evidence for step 14, so the consumer
    is adopted last and re-run in full.
16. **The transport driver's revision.** Not planned here — step 15's adoption
    produced it. `route_report` routes read events, `with_group` and the
    `committed_application_index` forwarder replace observation-by-release,
    `abandon_write` and `abandon_read` retire one waiter, `new` takes recovery
    outputs, and `max_read_retries` is removed in favour of a grant-gated
    retry; the design is
    [Revision after adoption](#revision-after-adoption). Then the consumer
    adoption that step 15 could not yet make: `tests/support/observe.rs` and
    the read-side workarounds are deleted. Breaking for `new` and
    `max_read_retries`, both one release old, which is the cheapest that
    correction was ever going to be.

Steps 10–16 are the service-layer cluster and are complete in the tree, as are
steps 1–9. The reference consumers have moved on past both waves — the ledger
through its transactional backend and its durable process suite, the lock
through its durable backend and crash points — so an entry's After-state
describes the shape a promotion left behind, not the consumer's current line
count.

Steps 11, 13, and 14 are breaking. `reference/` and `bench-compare/` are
outside the root workspace and must be built explicitly for steps 11, 13, 14,
and 15 — `bench-compare` needs no source change for step 13, since its state
machines already declare typed errors, but it must be built to prove it.

Two things about this cluster differed from the first wave and were worth
stating before the work started. Step 13 removes `Eq` from four public error
types, so its adoption is a broad, mechanical rewrite of assertions across two
crates and one reference consumer, and the rewrite is where a real regression
could hide in the noise — every converted assertion should say more than it
did, not less. And step 11 is the only step in this document whose adoption was
expected to *find* defects rather than only remove workarounds; the six state
machines it forced to declare their snapshot support did not all answer the
same way, and the two that were wrong — examples installing a snapshot by
discarding the state it carried — were fixed rather than carried forward.

The many-group host is a third wave of one entry, and it moves the other way
round from both: there is no lower crate to start from, because it changes no
crate but its own. The sequence is by defect severity instead, so that the
repro suite is green a finding at a time and a bisect lands on one finding.

17. **The design, alone.** This document's entry, with no code. It is a public
    API change to the crate the counter consumer is meant to validate, and the
    argument for the shape is longer than the diff that implements it.
18. **The tick pass and group retirement.** `TickPass`, `GroupOutcome`,
    `tick_all`'s return type, `remove_group`, and the four host accessors, in
    both hosts, plus the three examples. This is M1 and M2 — the two findings
    that lose committed data — and it lands first for that reason alone.
19. **The error surface.** `DriverError`, `DriverErrorKind`,
    `MultiRaftErrorKind`, the `ErrorCause` re-export, the `Display`/`Error`
    impls, the dropped `Eq`, `InvalidReport`, `UnrecognizedEvent`, and the
    blanket impl on `RaftGroup` that stops rendering its typed error. Breaking,
    and the step whose adoption rewrites the most assertions.
20. **The remaining corrections and the claims.** `metrics` becoming
    infallible, `open_group` returning its driver, the hand-written `Default`,
    and then the README and `lib.rs` — every claim in them true of the code or
    deleted.

Step 17 is documentation. Steps 18, 19, and 20 are all breaking, all confined
to one crate, and `bench-compare` must be built for each of them even though it
needs no source change — it is outside the root workspace, and "needs no
change" is a claim to check rather than assume.
