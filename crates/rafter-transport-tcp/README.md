# rafter-transport-tcp

Blocking demo-only TCP transport for Rafter peer messages.

This crate adds a small std-only TCP helper around `rafter-codec` frames. It
opens one connection per outbound message, uses a length-prefixed frame, and
exists for examples and local tests.

It is intentionally not a production transport reference. It does not keep
persistent per-peer streams, does not provide bounded outbound queues, does not
set write deadlines or read timeouts, and can reorder messages under retries
because every outbound message gets its own connection. A slow or dead peer can
still block the sending thread according to normal blocking socket behavior.

It is also intentionally insecure: it does not authenticate peers, prove sender
identity, prevent replay, or fence removed members.

Production integrations should implement their own transport using the delivery
semantics in [`rafter-service`](../rafter-service/README.md#transport-delivery-semantics),
with explicit authentication, fencing, queueing, and replay-protection policy.
