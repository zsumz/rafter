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

## Optional durable client owner

Enable `features = ["durable"]` to use `durable::DurableClientOwner` and its
cloneable encoded-write sender. `recover_with_reads` additionally configures an
encoded-query adapter and a read sender. It owns the existing asynchronous
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

`recover_with_reads` accepts `DurableClientOptions` with independent read and
write limits. The configured query adapter borrows an immutable verified
application view on the owner thread; it must be bounded, read-only, and
nonreentrant. Request encoding does not borrow the application. The read sender
serves local and linearizable queries and refuses lease reads. Linearizable
queries obtain an actual Raft grant, translate its protocol position to the
required application floor, and preserve any higher caller `min_applied_index`.
An unavailable floor is refused; it is never lowered. The proof keeps the term
captured at the grant, even if leadership changes before the query runs.

Read credits cover queued requests, quorum rounds, and uncollected results,
charging encoded `Vec` capacity. Canceling a caller cannot free a quorum round's
credit: the owner retains it until the kernel grants/cancels the round or the
backend is released. Queued and already-granted canceled reads are reclaimed on
an owner turn. Query-result sizes need an application bound. A local read has no
quorum proof; the single-owner proof uses `()` as its group identifier, leaving
authenticated group routing with the embedding.

Cloned senders do not retain the backend. Stop admission with `begin_shutdown`,
poll existing work, then call `finish_shutdown`. Discard and recover the owner
after fatal errors or caught callback panics. Destruction joins workers and
releases protocol state before remaining clients receive refusal or uncertainty;
it can wait for a blocked application transaction. An uncertain result requires
application-defined retry identities, not a new unconditional mutation.
Snapshot resumption errors and caught floor-validation panics close both client
endpoints immediately. Outstanding requests retain their ownership until the
backend is released; correcting the floor cannot revive a failed incarnation.
The same admission fence spans storage and command-adapter callbacks during
consensus input and polling. A caught panic requires recovery; an ordinary busy
refusal preserves the live owner and its client endpoints.

Managed membership operations and `DriverCommandSender` integration remain
follow-up work. The independent ledger consumer exercises writes and reads over
real TLS and file stores, including withheld quorum, paused durable application
work, cancellation, shutdown, and read-your-writes after cold reopen.

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
