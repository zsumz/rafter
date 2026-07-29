# rafter-multiraft

Manual and bounded managed many-group hosting for Rafter.

`rafter-multiraft` holds many caller-defined Raft groups in one process and
steps the one you name. It provides untyped and typed host APIs so sharded
systems can route messages, step proposals, retire groups, and collect metrics
while keeping group identity separate from node identity.

Use `MultiRaftHost` when groups are dynamic or heterogeneous. Use
`TypedMultiRaftHost` when groups share one command type and one apply result
type. Use `ManagedScheduler` or `ManagedTypedMultiRaftHost` when work must pass
through deterministic ready-set turns, explicit quotas, bounded admission, and
exact completion accounting.

## Manual host

The host steps what the caller tells it to step. `tick_all` walks every open
group once, in key order, and returns one outcome per group; nothing else here
decides when work happens. In particular this crate does **not**:

- decide when to step anything — ticks arrive only as often as the caller
  loops;
- enforce a per-group work quota, so a group with slow storage occupies the
  pass for as long as its driver takes;
- queue anything, and therefore has no queue limits and no backpressure;
- prioritize control traffic over bulk replication;
- retire a group on its own, even one whose driver reports a permanent
  failure; or
- keep tombstones, so a retired key is reopenable and late traffic for it is
  reported as an unknown group.

`TickPass::visited` is a fairness *measurement* — it proves the pass reached
every group — not a fairness *mechanism*.

## Managed scheduler

The `managed` module provides the bounded mechanism above the manual host:

- deterministic one-opportunity-per-ready-group passes;
- per-group and global queue limits with lossless refusal;
- per-group quotas and fixed work-class ordering;
- explicit worker occupancy and exact-once completion permits;
- failure isolation and queue-conservation metrics; and
- typed composition through `ManagedTypedMultiRaftHost`.

It remains sans-I/O. The caller owns threads, clocks, storage, network,
application retry policy, group-incarnation/tombstone policy, and readiness
signals.
