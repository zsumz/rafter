# rafter-runtime

Persist-before-output runtime wrapper for the Rafter core.

`rafter-runtime` wraps `rafter::Node` with hard-state, log, and snapshot
persistence. It withholds peer and application outputs until required durable
work has completed, and it supports batched `step_batch` calls for group
commit.

Use this crate when an embedding needs a durable node but still wants to own
its application state machine, transport, scheduling, recovery policy, and
authorization boundary.

Pair it with `rafter-storage` for file-backed or in-memory stores.

On restart, feed runtime recovery outputs through the application layer only
after matching them against the application's durable applied floor. Application
state durability remains the caller's responsibility above this runtime.

The opt-in `pipelined::PipelinedRaftNode` can release eligible leader append
messages while one owned persistence job runs. Its generation/operation receipt
must complete before another consensus input runs. Follower acknowledgments,
term/vote changes, commits, and client application results retain their fences.

`pipelined::PersistenceWorker` is the optional standard-thread executor for
that owned job. Its single credit covers queued, executing, and unconsumed
completion state. A refused submission returns the exact work, and explicit
shutdown refuses while the worker still owns an operation. Embeddings still
choose proposal batching, route the eligible messages, return the completion to
the originating node, and durably apply committed application entries before
acknowledging clients.

`application::ApplicationWorker` supplies that application-side durability
fence without importing benchmark code. It applies contiguous committed items
on one ordered thread, combines only work already waiting, and bounds retained
entries and bytes until each completion is consumed. Store success is accepted
only when it returns one outcome per item and reports the matching durable
applied floor. Refusals and failures preserve owned work; a failure or worker
panic closes admission and requires recovery before the service continues.
Snapshot creation and installation remain application policy and must be
coordinated at a consumed durable floor.

The `pipelined_durable_service` example is the tested reference composition for
that fast path. It uses the shared WAL, one-credit persistence worker, bounded
ordered application worker, bounded peer queue, durable application records
carrying their applied floor, snapshot compaction, lagging-follower catch-up,
restart recovery, and explicit worker shutdown. It uses only public Rafter APIs
and no benchmark crate:

```text
cargo run -p rafter-runtime --example pipelined_durable_service
```

Its in-process message queue is intentionally not an authenticated production
transport. Real services must supply peer authentication and choose an explicit
overload policy for their bounded client and peer queues.
