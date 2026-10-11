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
reserved. Capacity refusals report `WriteError::Backpressure` or
`ReadError::Backpressure`, with matching error kinds. Refused writes prove
`NotAppended`; actual transport failures retain their separate category.

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
embeddings still control how many concurrent watches they open. A panicking
subscriber wakeup does not suppress later notifications: publication or closure
remains visible, all remaining tasks are notified, then the first panic resumes.

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

Durable read/write admission also reports `Backpressure` when either its count
or encoded-capacity budget cannot fit a request. Consume or drop completed
futures to return credit. A buffer larger than the entire byte budget needs a
smaller retained capacity or a larger configured budget; waiting alone cannot
make it fit. No automatic retry or retry delay is implied by this category.

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

`metrics()` returns the existing `RaftGroupMetrics<()>` observation type. Its
applied index is the verified durable application floor. Pending work is distinct
from completed linearizable proofs that callers have not collected; local reads
and completed errors remain visible through retained credits. Metrics return
`None` while persistence owns the node, application snapshot installation owns
the store, or the backend has been released. Reading metrics drives no work and
proves no quorum. Collecting or dropping a result notifies the owner after
releasing the future's ownership; credits still held by the owner remain charged.

`durable::DurableRaftDriver::attach(group_id, owner, transport)` adds the existing
`RaftHandle` API through `DurableCommandSender`. Use the configured transport's
sender (for example, `TlsPeerTransport::sender()`). Attachment needs an observable
live owner and returns both resources on refusal. Commands and queries remain
encoded bytes; each operation validates its group before admission, and read
proofs carry that group identity. Polling routes outgoing frames through the
existing `RaftTransport`; refused sends are counted as Raft message loss.

`DurableCommandSender::retained()` exposes write/query count and byte charges
plus the shared control count and payload charge through `DurableCommandStats`.
Queries are `None` when no query adapter was configured. Observation drives no
work, invokes no callbacks, and remains available after backend release while
uncollected results still retain credits. The counters can change independently
during concurrent admission or release; this view does not reserve capacity.
Submission can still report backpressure. These charges exclude application
results, kernel logs, transport queues and other embedding-owned memory.

Use `DurableRaftDriver::attach_with_membership` with `DurableMembershipLimits`
constructed from a `NonZeroUsize` byte budget to enable bounded membership
proposal admission. Submit an app-layer `MembershipChange` through
`driver.sender().propose_membership(&group_id, change)` and continue polling the
driver. Default `attach` keeps manually submitted membership inputs available;
opt-in attachment requires the sender for membership proposals.

Membership and leadership transfer share one retained control credit, including
queued work, submitted work whose caller canceled, and uncollected results.
Target and promotion-barrier vectors are normalized and charged by capacity;
`NodeInfo` metadata is discarded. This is a retained request-payload bound;
transient dispatch allocations and the kernel log require separate budgets.
An occupied credit or excess request payload reports
`DurableMembershipError::Backpressure` before submission. Protocol refusals
retain their exact `ProposalRejection` and leader hint. Backend release returns
`ShuttingDown` for a queued request or `Unconfirmed` for a submitted request
whose result was not established; the latter may have appended or committed.

A `DurableMembershipReceipt` identifies a durably appended index and term after
its immediate effects drain. The proposal can still be replaced after leader
loss. Observe the matching `Output::ConfigurationCommitted` before advancing a
plan; a later commit index alone cannot establish that this particular entry
committed. Voter changes need a committed joint configuration followed by a
stable configuration commit, normally proposed through `LeaveJoint`.
Authorization, fresh replica identities, peer-policy
updates and fencing after committed removal remain embedding responsibilities.
Recovery replays configuration events above the application applied floor;
persist any additional change-tracking or retirement history the embedding
needs across restarts and compaction.

Additional transfer requests receive `TransferLeadershipError::Backpressure`
while the shared control credit is retained. Canceling a submitted request
retains its credit through immediate output processing.
Transfer success follows routing of its immediate effects and does not promise
that the target has become leader. Backend release before confirmation reports
`Unconfirmed` for submitted requests and `ShuttingDown` for queued requests.

Handle shutdown closes admission, drains the owner, and completes after backend
release. Duplicate shutdown calls also await release before returning
`AlreadyShutDown`; canceling a shutdown future leaves the request in force.
Outstanding snapshot installation must still be completed. Metrics watches
retain the last genuine observation across worker/snapshot handoff and after
closure. A caught transport or subscriber panic fences admission, and discarding
the driver resolves the remaining clients even when several wakeups panic.
A snapshot returned after completed shutdown is refused with `NotReady(Stopped)`
and leaves the driver stopped, its clients closed, and its final metrics intact.

Both the client owner and grouped driver expose `compact_snapshot_with` for
local snapshot maintenance. Once prior outputs and application completions are
consumed, a builder borrows the application and returns an exact descriptor plus
a stable chunk source. Existing queued requests and uncollected results keep
their credits and identity. The grouped driver honors shutdown requested through
any handle before invoking the builder and publishes the resulting metrics.
Ordinary busy or descriptor refusals preserve live clients. Fatal errors and
caught builder, storage, source-disposal, or metrics panics close admission;
outstanding clients remain owned until backend release. Creation, publication,
and worker restart are synchronous. Bound source staging, chunk sizes, and
disposal of returned entries with the existing `LogRetirementWorker` or an
equivalent embedding budget.

The embedding owns inbound peer authentication and group validation, membership
policy, atomic snapshot installation, and transport startup/join. Automatic
membership-plan execution and committed-change tracking remain separate work.
Independent ledger consumers exercise the owner and full handle over real TLS
and file stores, including quorum and application fences, local compaction,
transfer, shutdown, membership role changes and reads after cold reopen.
The [membership consumer](../../reference/ledger/tests/support/owner_tls/client/managed/membership.rs)
checks exact joint/stable commits, recovered roles and replacement of an
uncommitted durable proposal.

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
