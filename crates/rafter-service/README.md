# rafter-service

Async managed service layer for Rafter.

`rafter-service` builds an application-facing handle API on top of
`rafter-app`. It provides a managed in-memory driver, read/write receipts,
membership helpers, metrics watches, and transport traits for production
integrations.

Use this crate when you want an async handle over a Rafter group while keeping
transport authentication, peer routing, and durable application storage
explicit.

`TransportDriverOptions` bounds unresolved writes and reads separately through
`max_pending_waiters` (1024 each by default). A second, shared
`max_retained_waiters` limit (2048 by default) counts every registered waiter,
including completed results that clients have not collected. Set it with
`with_max_retained_waiters`. Both limits must admit a new operation; refusal
happens before an ID is allocated, a write is proposed, or a read barrier is
reserved. Refused writes report `NotAppended` through the existing transport
error surface.

Polling a future to completion or dropping it returns its retained credit.
Abandonment resolves the future and releases its pending slot, but keeps the
terminal answer and its retained credit until the client consumes or drops it.
Local reads register no waiter. These are entry-count bounds; embeddings must
also bound application query/result sizes and receipts they have collected.

Each pending `MetricsWatch::changed` call retains one current task wakeup.
Repolling replaces that wakeup; canceling the future unregisters it immediately,
even while the watch stays open and no further metrics are published. Canceled
waits do not consume later updates. Registered task wakeups and their cleanup
run outside the watch's locks, so waking or destroying a task can cancel a wait
or inspect the publisher. Registration storage follows live pending waits;
embeddings still control how many concurrent watches they open.

## Optional durable write owner

Enable `features = ["durable"]` to use `durable::DurableClientOwner` and its
cloneable encoded-write sender. It owns the existing asynchronous
`rafter-runtime::service::DurableService`; the feature is disabled by default.
Construct it with `recover`, encode commands independently of the busy
application worker, and drive `poll_next` until `Pending` before waiting on the
supplied notification. Route authenticated peer inputs and returned transport
and snapshot events using the existing embedding APIs.

A successful write receipt follows durable application completion. Admission
bounds cover queued writes, in-flight writes, and completed results clients have
not collected; the byte bound charges encoded `Vec` capacities. Canceling a
queued write lets the owner reclaim its credit. Canceling an accepted write does
not revoke the command or release credit until the owner consumes its outcome.
Application outcome sizes and the kernel log need separate embedding bounds.

Cloned senders do not retain the backend. Stop admission with `begin_shutdown`,
poll existing work, then call `finish_shutdown`. Discard and recover the owner
after fatal errors or caught callback panics. Destruction joins workers and
releases protocol state before remaining clients receive refusal or uncertainty;
it can wait for a blocked application transaction. An uncertain result requires
application-defined retry identities, not a new unconditional mutation.

This initial adapter manages writes. Managed reads, membership operations, and
`DriverCommandSender` integration remain follow-up work. The independent ledger
consumer exercises it over real TLS and file stores, including paused durable
application work, cancellation, shutdown, and cold reopen.

## Transport Delivery Semantics

Rafter transports are allowed to drop, duplicate, reorder, reconnect, and
deliver messages non-FIFO without violating Raft safety. The protocol validates
terms, log indices, membership, and snapshot metadata before accepting effects.

For liveness, production transports should use bounded queues or another
explicit backpressure policy, provide eventual delivery between healthy
authorized peers, and treat per-peer FIFO as optional but beneficial. A
successful send means the transport accepted or enqueued a message; it does not
mean the peer received it, committed it, or applied it.

Append-entries frames are sized by the configured append budget plus codec
headers. Snapshot transfer should use chunk messages with at most 64 KiB of
payload plus metadata; the current peer wire format does not serialize
unbounded whole-snapshot payloads.

This pre-release service layer assumes the current `rafter-codec` peer wire
format. It does not negotiate peer wire versions or provide downgrade helpers.
After Rafter has a public wire compatibility promise, any incompatible wire bump
must ship with an explicit compatibility or migration plan.

Production embeddings own peer authentication, removed-peer fencing, queue
bounds, replay protection, and the durable application state policy around this
service layer.
