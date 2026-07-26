# rafter-multiraft

Manual many-group host layer for Rafter.

`rafter-multiraft` holds many caller-defined Raft groups in one process and
steps the one you name. It provides untyped and typed host APIs so sharded
systems can route messages, step proposals, retire groups, and collect metrics
while keeping group identity separate from node identity.

Use `MultiRaftHost` when groups are dynamic or heterogeneous. Use
`TypedMultiRaftHost` when groups share one command type and one apply result
type.

## This is a manual host, not a scheduler

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
every group — not a fairness *mechanism*. The managed multi-Raft scheduler that
bounds fairness, isolates failure, and enforces quotas is a separate 1.0
component described in `docs/reference-consumers.md`, and it does not exist
yet.
