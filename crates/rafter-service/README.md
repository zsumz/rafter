# rafter-service

Async managed service layer for Rafter.

`rafter-service` builds an application-facing handle API on top of
`rafter-app`. It provides a managed in-memory driver, read/write receipts,
membership helpers, metrics watches, and transport traits for production
integrations.

Use this crate when you want an async handle over a Rafter group while keeping
transport authentication, peer routing, and durable application storage
explicit.

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
