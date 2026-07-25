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
([`crates/rafter-app/src/group/types.rs:242`](../crates/rafter-app/src/group/types.rs),
[`crates/rafter-runtime/src/lib.rs:89`](../crates/rafter-runtime/src/lib.rs)),
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
| [`crates/rafter-app/src/error.rs`](../crates/rafter-app/src/error.rs) | Add `ErrorCause`; `GroupError::Poisoned` gains `cause` |
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
rhetorical: a wrong-group write, a payload-too-large write, and a
pre-append runtime failure all move from `SubmitOutcome::Unknown` to
`SubmitOutcome::Refused`, which means the lock's history records a terminal
refusal where it previously recorded an unknown, and a retrying client stops
burning a request identity it never used. `write_error_from_group` and its
comment about a lost type disappear with the rest of the consumer's driver in
the next entry; until then the type survives the mapping.

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
    /// The new group must be quiescent, and its local ID watermarks must be at
    /// or above the retired incarnation's when the two share a runtime; see
    /// [`rafter_app::group::RaftGroupParts`]. A driver that rebuilt its runtime
    /// from durable storage may restart its IDs at zero.
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
- **Group identity stays caller-defined.** The driver serves one group and
  refuses every other group ID with `WriteError::WrongGroup`. A many-group host
  owns one driver per group and demultiplexes inbound frames by
  `AuthenticatedPeerEnvelope::group_id` before calling `deliver`.
- **Read retries are bounded per call, not per barrier.** A barrier that is
  still pending after `max_read_retries` stays pending and is retried on the
  next call, so the driver never spins and never abandons a barrier that the
  network could still resolve. Abandoning is the caller's decision, and its
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
| [`crates/rafter-service/src/driver/mapping.rs:11-47`](../crates/rafter-service/src/driver/mapping.rs) | `ManagedDriverError::{NoGroup, GroupAlreadyAdopted}` |
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
`driver::metrics_watch_from_current`
([`crates/rafter-service/src/driver/metrics.rs:3-7`](../crates/rafter-service/src/driver/metrics.rs))
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
| [`crates/rafter-service/src/driver/metrics.rs`](../crates/rafter-service/src/driver/metrics.rs) | Deleted |
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
```

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
| [`crates/rafter-app/src/error.rs:39-97`](../crates/rafter-app/src/error.rs) | `GroupError` | Two variants and their `Display` arms |
| [`crates/rafter-app/src/group/snapshot.rs:13-38`](../crates/rafter-app/src/group/snapshot.rs) | group | Check the declaration before installing; map the misdeclaration |
| [`crates/rafter-app/src/group/mod.rs`](../crates/rafter-app/src/group/mod.rs) | — | Re-export the two new types from `state_machine` |

| File | Type | Declaration | Bodies |
| --- | --- | --- | --- |
| [`reference/ledger/src/adapter/mod.rs:193,256,270`](../reference/ledger/src/adapter/mod.rs) | `LedgerStateMachine` | `Supported` | Both kept; error type wrapped |
| [`crates/rafter-app/examples/snapshot_install.rs:170,239,247`](../crates/rafter-app/examples/snapshot_install.rs) | `KvStateMachine` | `Supported` | Both kept; error type wrapped |
| [`crates/rafter-app/tests/support/mod.rs:63,142,150`](../crates/rafter-app/tests/support/mod.rs) | `RecordingStateMachine` | `Supported` | Both kept; the fault switch survives |
| [`reference/fenced-lock/src/adapter/mod.rs:177,239,246`](../reference/fenced-lock/src/adapter/mod.rs) | `LockStateMachine` | `Unsupported` | Both deleted; `DurableSnapshotUndefined` deleted |
| [`crates/rafter-app/examples/replicated_kv_manual.rs:283,351,359`](../crates/rafter-app/examples/replicated_kv_manual.rs) | `KvStateMachine` | Re-examine | The lossy install is a defect, not a simplification |
| [`crates/rafter-multiraft/examples/real_raft_groups.rs:255,317,325`](../crates/rafter-multiraft/examples/real_raft_groups.rs) | `KvStateMachine` | Re-examine | Same |
| [`crates/rafter-service/examples/replicated_kv_service.rs:91,144,152`](../crates/rafter-service/examples/replicated_kv_service.rs) | `KvStateMachine` | Re-examine | Vacuous both ways |
| [`crates/rafter-service/tests/support/mod.rs:156,212,220`](../crates/rafter-service/tests/support/mod.rs) | `KvStateMachine` | Re-examine | Vacuous both ways |
| `bench-compare/src/bin/bench-rafter-service.rs:130,174,182` | `BenchStateMachine` | Re-examine | Vacuous both ways |
| `bench-compare/src/bin/bench-rafter-multiraft.rs:149,193,201` | `BenchStateMachine` | Re-examine | Vacuous both ways |

"Re-examine" is the honest instruction and the point of the entry: each of those
six must decide whether it models snapshots or declares that it does not, and
the two that clear their state on install must not answer `Supported` while
doing so. This is the only step in this cluster whose adoption is expected to
change behavior in files nobody set out to touch.

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

`rafter-app` gains a question it can answer before it needs to, and the
workspace gains a list of six state machines that have been quietly answering a
question they should have declined — two of them in examples that install a
snapshot by deleting the data it carries.

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
the start and derive the readiness accessor from it.

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
    `ApplicationSnapshotError`, `GroupError`'s three new variants, the
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
    `ManagedDriverError::{NoGroup, GroupAlreadyAdopted}`, and
    `InMemoryRaftDriver::release_groups`. Additive apart from the bound, and
    after step 13 so the new driver builds its errors in their final shape and
    never carries a rendering path that step 13 would have to delete.
15. **Reference-consumer adoption.** Collapse
    `reference/fenced-lock/src/adapter/client.rs`'s `closes_outcome_window`,
    delete `reference/fenced-lock/tests/support/cluster.rs:90-789,1393-1478` in
    favour of `TransportRaftDriver`, delete `LockStateMachine`'s two snapshot
    methods and `LockAdapterError::DurableSnapshotUndefined`, and re-run both
    source mode and package-consumer mode. The narrowing in
    `closes_outcome_window` is the acceptance evidence for step 13 and the
    ~790-line deletion is the acceptance evidence for step 14, so the consumer
    is adopted last and re-run in full.

Steps 11, 13, and 14 are breaking. `reference/` and `bench-compare/` are
outside the root workspace and must be built explicitly for steps 11, 13, 14,
and 15 — `bench-compare` needs no source change for step 13, since its state
machines already declare typed errors, but it must be built to prove it.

Two things about this cluster differ from the first wave and are worth stating
before the work starts. Step 13 removes `Eq` from four public error types, so
its adoption is a broad, mechanical rewrite of assertions across two crates and
one reference consumer, and the rewrite is where a real regression could hide
in the noise — every converted assertion should say more than it did, not less.
And step 11 is the only step in this document so far whose adoption is expected
to *find* defects rather than only remove workarounds; the six state machines
it forces to declare their snapshot support are not all going to answer the same
way, and two of them are wrong today.
