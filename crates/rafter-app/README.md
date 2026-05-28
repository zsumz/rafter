# rafter-app

Synchronous embedded replicated-state-machine support for Rafter.

`rafter-app` sits above the `rafter-runtime-api` contract and exposes an
explicit application group layer. It helps callers dispatch peer messages,
apply committed entries, publish metrics, handle snapshots, and manage
proposal/read outcomes while leaving the concrete runtime policy in the
caller's hands.

Use this crate for embedded databases, replicated services, and systems that
want a practical group abstraction without adopting a global async runtime or
network stack.

Restart recovery must pair the durable Raft applied floor with the
application state's durable applied floor, and application state writes must be
atomic before their corresponding Raft outputs are considered handled.

## Restart recovery

On restart, recover the Raft runtime and the application state machine with the
same durable applied floor:

```rust,ignore
let applied = app.applied_index()?;
let recovered = DurableRaftNode::recover_with_storage_and_snapshot_store_applied_through(
    config,
    hard_state,
    log,
    snapshots,
    applied,
)?;
let (raft, recovery_outputs) = recovered.into_parts();
let mut group = RaftGroup::with_applied_index(group_id, node_id, raft, app, applied);
let recovery_report = group.apply_raft_outputs(recovery_outputs)?;
```

`RaftGroup` validates this boundary before every apply batch. If the runtime
ever emits a committed entry already covered by `app.applied_index()`, the
group poisons itself before calling `apply_batch` so the application cannot
silently double-apply a durable command after restart.

## Advanced direct output path

Most callers should drive groups with `RaftGroup::step`,
`begin_proposal`, `begin_read_barrier`, or `read`. Use
`RaftGroup::apply_raft_outputs` only when you own the lower-level
`PersistedRaftRuntime` drive loop. Pass the runtime outputs in the exact order
they were emitted; raw kernel output ordering is part of the persistence and
snapshot-transfer contract. A poisoned group rejects this direct path before
handling any additional outputs.

`ReadConsistency::LeaseRead` is reserved for future app-layer lease support and
currently returns `UnsupportedReadConsistency`; use linearizable reads for safe
freshness today.
