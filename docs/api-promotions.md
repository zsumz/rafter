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
behavior rather than exposing a missing one. The couplings are recorded in
[Coupled designs](#coupled-designs), and the implementation sequence in
[Adoption order](#adoption-order).

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
observed rejection arrives — but it watches them in the report it already
records, not in a private map.

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
refresh calls; the method becomes a one-line delegation and `reopen()` becomes
`self.clone()`. Any store the durable slices later introduce — pooled, locked,
or lazily loading — is implementable without a cache.

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

`LedgerCluster::committed_application_floor` and
`committed_application_entries` disappear, and `settle()` compares two public
numbers. `committed_commands` keeps its log walk, because it needs the decoded
payloads rather than the floor — an honest remainder, not a workaround.

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
| [`crates/rafter-service/tests/support/mod.rs:242,281-283`](../crates/rafter-service/tests/support/mod.rs) | `ScriptedReadRuntime` | Commits no application entry; still `ZERO`, now bounded |
| [`crates/rafter-service/tests/support/mod.rs:370,409-411`](../crates/rafter-service/tests/support/mod.rs) | `ScriptedWriteRuntime` | Appends but never commits; still `ZERO` |
| [`crates/rafter-service/tests/in_memory_write.rs:236,269-271`](../crates/rafter-service/tests/in_memory_write.rs) | `BatchRecordingRuntime` | Highest recorded index at or below the bound |
| `bench-compare/src/bin/bench-rafter-service.rs:242`, `bench-rafter-multiraft.rs:261` | `RecordingRuntime` | Delegate |

`ScriptedRuntime` is the one that needs real thought: the whole
`group_read*.rs` suite drives it, and it has no log to answer from. Giving it an
explicit set of application-entry indexes is also what makes the mixed-log
negative test writable.

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
measure. `RD-04.b` states the app-layer obligation the current wording made
unsatisfiable. No TLA predicate is added, so the `tla_predicates_now: 9` ratchet
and the four `.cfg` invariant blocks are untouched.

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

In `crates/rafter-maelstrom`:

- `a_read_granted_at_a_noop_index_flushes_without_a_later_apply` — the same
  regression at the kernel-direct gate, plus coverage that a stalled read is
  re-examined without an apply to trigger it.

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

## Coupled designs

The six promotions form four surfaces, not six independent additions.

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
