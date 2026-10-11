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

`pipelined::ThreadedPipelinedRaftNode` is the supported bounded composition of
that node and its standard-thread executor. Its single credit covers queued,
executing, and unconsumed completion state. It refuses consensus input while
persistence owns the node, accepts only the originating completion, and runs
returned work through the same synchronous fence if worker submission is
unavailable. Thread-start failure returns the unchanged durable node. The
lower-level `PipelinedRaftNode`, `PersistenceWork`, and `PersistenceWorker`
remain public for embeddings with another executor. Embeddings still choose
proposal batching, route eligible messages, and durably apply committed
application entries before acknowledging clients.

`application::ApplicationWorker` supplies that application-side durability
fence without importing benchmark code. It applies contiguous committed items
on one ordered thread, combines only work already waiting, and bounds retained
entries and bytes until each completion is consumed. A separate per-entry work
estimate bounds one application call without conflating storage batch geometry
with completion-retained memory. Store success is accepted only when it returns
one outcome per item and reports the matching durable applied floor. Refusals
and failures preserve owned work; a failure or worker panic closes admission
and requires recovery before the service continues.
An item normally covers one log position. `ApplicationEntry::first_log_index`
can include a known protocol-only prefix with the next command, so long runs
of no-ops or membership entries need no allocated placeholders. The worker
rejects reversed, overlapping, or discontinuous ranges before taking ownership;
the embedding remains responsible for including every application command.
One outcome and one entry credit belong to each item regardless of range length,
and success must durably publish the final covered index with the matching bytes.
`ApplicationWorker::try_submit_prefix` sizes one submission to the current
entry, retained-byte, and per-batch work budgets. It removes only accepted
items from a caller-owned `VecDeque`; every refusal restores the entire queue.
The owner bounds that queue and consumes completions before retrying `Full`.
The durable reference converts at most 64 consecutive application outputs at
a time and waits for one completion when needed, so recovery can exceed the
worker's total credits without dropping committed commands or treating queue
pressure as a fatal storage failure. The admission operation itself never
waits; asynchronous embeddings can poll events on their chosen scheduler.
Snapshot creation and installation remain application policy and must be
coordinated at a consumed durable floor. `shutdown_into_store` returns the
application store after an idle join, so maintenance can take exclusive
ownership without a side channel; a prior application failure still requires
application-defined recovery before the store is reused.

`service::DurableService` now coordinates both workers through a poll-driven
owner. `recover` takes the actual application store, derives its durable floor,
and retains replay outputs until application recovery finishes. `try_step`
returns the original input if admission is closed; proposal batches have entry
and payload limits. `poll` dispatches at most one application command or event,
retains a refused command under credit pressure, and exposes client-eligible
`Applied` completions only after both durability fences. Read grants wait for
the corresponding application prefix, including barriers at protocol-only
positions. Peer sends use the embedding's existing authenticated transport.

An `InstallSnapshot` event transfers the quiescent application store and the
exact durable descriptor. The embedding installs its bytes and floor atomically,
then calls `resume_snapshot`; later outputs and new input remain fenced until
the floor matches. `begin_shutdown` closes admission while polling continues;
`finish_shutdown` joins only idle workers and returns the application store.
Unresolved proposals remain unknown outcomes, since draining does not create a
quorum decision. Worker failures, adapter errors, and invalid completion floors
terminally fence the incarnation for recovery.

This first owner has no input mailbox. Its retained work is one existing runtime
output batch, one converted command, and bounded worker credits; a runtime or
recovery batch itself can be large. The caller still bounds input frames,
client/read waiters, transport queues, and consumed events. Ordinary consensus
steps remain synchronous, and the caller provides periodic completion polling.
Local snapshot creation/compaction and executor wake integration remain on the
lower-level APIs pending their own composition slices. The independent ledger's
`tests/service_owner.rs` uses this API over real Raft files and its transactional
journal, exercising backlog recovery, tracked durable completion, shutdown,
descriptor installation, and reopen in source and exact-package lanes.

`LogRetirementWorker` optionally releases entries retired by a successfully
published local snapshot away from the consensus-owner thread. Entry and
payload-byte credits include queued and currently dropping work. Submission
never waits, and refusal returns the complete retired prefix for safe inline
destruction. Snapshot publication, durable WAL compaction, and the kernel
transition remain synchronous; explicit shutdown drains accepted work.

The `pipelined_durable_service` example is the tested reference composition for
that fast path. It uses the shared WAL, `ThreadedPipelinedRaftNode`, bounded
ordered application worker, bounded peer queue, durable application records
carrying their applied floor, snapshot compaction, bounded retired-log release,
current-only file retention, startup cleanup of recognized
interrupted-publication files, lagging-follower catch-up, restart recovery, and
explicit worker shutdown. It uses only public Rafter APIs and no benchmark crate:

```text
cargo run -p rafter-runtime --example pipelined_durable_service
```

Its in-process message queue is intentionally not an authenticated production
transport. Real services must supply peer authentication and choose an explicit
overload policy for their bounded client and peer queues.
