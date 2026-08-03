# rafter-transport-tls

`rafter-transport-tls` is Rafter's optional bounded, multiplexed, mutually
authenticated TLS-over-TCP peer transport.

It keeps the original Rafter boundary intact: the deterministic kernel remains
sans I/O, the service crate remains transport-agnostic, and this crate owns only
the concrete link mechanics a production embedding may choose to use.

The implemented surface now includes:

- stable bounded `PeerId` and `ClusterId` values;
- explicit SHA-256 leaf-certificate mappings, including certificate rotation
  beneath one stable principal;
- strict PEM identity loading and TLS 1.3 mutual authentication;
- mandatory `rafter/1` ALPN, server-name verification, and no TLS resumption or
  early data;
- bounded caller-managed resolved endpoints;
- one-to-one `PeerId <-> NodeId` bindings per group;
- atomic full `PeerPolicy<PeerId>` installation with a monotonic retirement
  floor;
- a caller-owned canonical `GroupIdCodec<G>`;
- version-1 handshake, peer-frame, and durable-session formats;
- durable connection epochs and exact live-stream sequence progression;
- a blocking runtime with persistent per-`PeerId` senders, authenticated
  receivers, finite worker ownership, endpoint redial, diagnostics, and
  graceful shutdown;
- count-and-byte-bounded outbound and inbound queues, with outbound capacity
  reserved for Raft control traffic;
- `TlsSender<G, C>` implementing `rafter_service::RaftTransport<G>`;
- queued caller-owned snapshot resolution outside the managed driver lock;
- direct composition with `TransportRaftDriver` through the concrete sender and
  `TlsPeerDirectory<G>` validator.

The crate does not own group lifecycle, node allocation, discovery, PKI,
application storage, client RPC, or task orchestration. Endpoint resolution and
certificate issuance remain caller responsibilities. Adding a physical peer
requires building a new owned runtime; replacing or removing that peer's
resolved endpoint set is live and bounded.

## Identity hierarchy

```text
rustls-validated TLS leaf certificate
            |
            | explicit SHA-256 fingerprint lookup
            v
          PeerId                 stable physical transport principal
            |
            | per group
            v
          NodeId                 single-use Raft identity
            |
            v
     PeerPolicy<PeerId>          authorization + retirement floor
```

Connections are keyed by `PeerId`, not `NodeId`, so one physical connection can
multiplex many independently numbered Raft groups.

## Nonblocking service boundary

`TlsSender::send`, `send_snapshot_chunk`, and `update_peers` never dial, resolve
DNS, handshake, sleep, fsync, read snapshot storage, wait for queue capacity, or
call back into the managed driver. `send` encodes one bounded frame.
`send_snapshot_chunk` encodes only canonical routing metadata and reserves the
exact eventual complete-frame bytes before queue admission. `Ok(())` means
accepted into the bounded queue; it does not mean delivered.

Every outbound peer configured when the runtime binds owns one persistent
sender worker, one finite queue, and, when enabled, one bounded snapshot
resolver lane. The queue is bounded by both frame count and retained bytes.
Bulk replication cannot consume the reserved control budget, bounded weighted
selection prevents an unending control stream from permanently starving bulk
work, and a failed bulk write is returned behind later control work. Bulk
write retries are finite and exhaustion is observable.

A runtime configured with `snapshot_resolver` enqueues the kernel's bounded
snapshot directive rather than reading its payload synchronously. A dedicated
per-peer resolver worker invokes `SnapshotChunkResolver<G>` outside the driver
lock, checks that the returned bytes exactly match the directive, builds the
`InstallSnapshotChunk` message, and returns the prepared work to sender
scheduling. A paused runtime does not invoke the resolver before activation.
The sender assigns a live connection sequence only at transmission.
A source refusal or typed resolver error drops that attempt like a lost Raft
message and remains visible in diagnostics. Without a resolver, snapshot
admission fails synchronously with `SnapshotResolverUnavailable`.

## Connection runtime

A sender worker reuses one mutually authenticated connection until it fails,
and compares its endpoint generation before every send. Replacement or removal
closes the stale stream; the worker then reads the latest generation and dials
the bounded endpoint list deterministically. Queued work is
connection-independent. A connection sequence is assigned only after TLS, the
Rafter handshake, and a newer durable connection session have succeeded, so an
ambiguous write can be retried on a fresh stream without reusing the old
stream's sequence space.

Transient failures use capped exponential backoff with duration-precise,
deterministic equal jitter keyed by the local-to-remote peer pair. Jitter stays
inside the capped window, including at the 30-second plateau, so callers of one
recovering peer do not synchronize. Redial bases above the ceiling are
refused instead of silently shortened. Neither a completed handshake nor one
locally successful frame clears retry history; a connection must remain
established for 30 seconds and then complete a write. A peer that accepts one
frame before closing therefore cannot create a durable-session publication
storm. Permanent identity, cluster, version, or frame incompatibility enters
`ConfigurationBlocked` without a rapid durable-redial loop.
`EndpointBook::refresh` provides immediate recovery after same-address remote
repair. A five-minute default sparse-reprobe base, with deterministic per-peer
jitter, expires even while another endpoint remains transient so an unchanged
endpoint generation cannot remain wedged forever. Stale sessions and
noncanonical accepted responses fail the runtime closed. Per-peer diagnostics
expose this state and the last connection error.

The listener is nonblocking. Every accepted socket consumes one finite inbound
connection permit before a receiver worker is spawned. Each receiver completes
mTLS and the Rafter handshake, installs the newest live connection epoch for the
authenticated `PeerId`, and requires exact sequence progression from one. A
newer epoch closes and fences the older live connection. An older delayed
handshake cannot replace a newer epoch.

The configured handshake timeout is one absolute deadline shared by the TLS
exchange and the Rafter hello, not a renewable idle timeout. A client that
trickles syntactically plausible bytes therefore cannot retain a bounded
inbound connection permit indefinitely.

Before delivery, every declared frame reserves a weighted runtime-wide memory
permit before its read buffer is allocated. Cheap outer routing, identity,
authorization, and retirement checks run before inner-message construction.
The public decoder charge cannot be configured below the allocation-counted
32x safe floor. Every `GroupIdCodec` also declares its maximum peak
codec-controlled heap across decoding, error construction, the returned group,
and canonical re-encoding. The runtime adds that bound and the in-place group
size to each frame's charge. Every authenticated receiver separately holds a
connection-lifetime reservation for its preallocated canonical group scratch.
Construction refuses a budget that cannot hold one such scratch reservation and
one configured maximum frame at their complete charges. Required CI executes
the adversarial allocation checker rather than merely compiling it.
The permit covers reading and decoding and moves with an accepted envelope into
the count-and-byte-bounded inbound queue; every refusal releases it. Frame read
buffers are frame-scoped, so idle connections retain no uncharged frame
allocation. Queue or memory pressure drops the frame and remains visible in
diagnostics.

Idle read timeouts are polling points, not forced reconnects. A timeout before
any byte of the next frame preserves the persistent connection. A timeout after
a frame has begun closes the connection because the stream is then ambiguously
framed.

## TLS policy

`TlsIdentity` loads a local certificate chain, exactly one unencrypted private
key, and a nonempty strict trust-root set from PEM bytes or files. It creates
client and server configurations with these fixed properties:

- TLS 1.3 only;
- mutual certificate authentication on every connection;
- normal rustls chain and server-name verification;
- ALPN exactly `rafter/1`;
- an explicit ring crypto provider, with no process-global provider selection;
- no TLS client resumption, server session cache, session tickets, or 0-RTT;
- no dangerous certificate verifier or plaintext mode.

A completed TLS connection is not yet a Rafter principal. The validated leaf
fingerprint must additionally appear in `CertificateDirectory`; a CA-valid but
unconfigured leaf is refused. `AuthenticatedTlsPeer` is produced only by that
post-handshake check. `TlsIdentity::validate_local_peer` applies the same rule
to the local leaf before the runtime begins serving.

Several fingerprints may map to one `PeerId`. Add the next fingerprint, rotate
credentials, confirm new connections, then remove the old fingerprint without
changing Raft membership.

## Security boundary

This transport protects against unauthenticated network peers, CA-valid but
unconfigured leaf certificates, certificate/`PeerId` disagreement,
group-specific sender and recipient forgery, cross-cluster credential reuse,
stale connection sessions, noncanonical or oversized frames, unbounded queue
or connection growth, and traffic from unauthorized or retired group members.

It does not protect against compromise of an authorized replica's private key,
a Byzantine authorized replica, malicious application code, incorrect
membership policy, failures in the caller's certificate issuance or revocation
system, or denial of service within the configured finite limits. Client and
application traffic are outside this peer transport. This is an authenticated
crash-fault transport for Raft, not a Byzantine consensus layer.

## Rafter handshake

`TlsHandshakeConfig` binds the authenticated channel to one exact `ClusterId`,
one local `PeerId`, supported outer and peer-codec version ranges, and a finite
complete-frame contract. Because version 1 has no fragmentation or adaptive
batching, the client offer is its required send bound: the server either accepts
that exact bound or returns `FrameLimitRejected` before session state changes.
An accepted item exceeding that exact bound is treated as an internal invariant
failure; it is never silently discarded while the connection remains healthy.

On the client, `begin_client_hello` durably allocates the next outbound
connection session before returning bytes that may be sent. On the server,
`accept_client_hello` validates the certificate-backed principal claim,
cluster, versions, and frame bound before asking the session store to durably
publish a newer inbound high-water. An accepted server hello is returned only
after that publication succeeds. Stale sessions and ordinary incompatibilities
remain typed protocol refusals; store failures remain typed local errors.

The client then rechecks that the authenticated server is the dial target, the
server hello claims that same principal and cluster, the selected versions were
offered, and the accepted frame bound is valid. TLS authenticates the channel;
the Rafter handshake authenticates its deployment, protocol, and freshness
context.

## Durable connection sessions

For every remote `PeerId`, the session store retains two independent high-water
marks: the highest outbound connection session allocated locally and the
highest inbound session accepted from that peer. Outbound sessions are durably
published before a client hello uses them. A newer inbound session is durably
published before an accepted server hello is sent.

`FileTransportSessionStore::create_new` and `open_existing` are intentionally
separate. There is no open-or-create path: missing or corrupt state under a
stable principal is an operational failure, not permission to reset replay
history. Each mutation is published as synchronized temp bytes, atomic rename,
and parent-directory synchronization. Any ambiguous mutation failure latches
the handle until it is dropped and reopened.

Peer records are replay high-water marks, not an evictable cache. The store has
no removal operation, so `SessionStoreLimits` bounds the distinct physical
principals seen over the state file's lifetime rather than only currently
connected peers. Binding requires component limits to match `TransportLimits`
and preflights aggregate capacity for every certificate-configured remote
principal before the listener or workers become observable. A recovered peer
whose inbound or outbound session high-water is exhausted also fails that
preflight rather than starting a permanently one-way runtime.

Within one accepted connection, `OutboundSequence` and `InboundSequence`
require exact progression from one. Sequence state is not durable because a
restart closes the stream; the next stream must use a newer durable session.

The state-file CRC detects accidental corruption, not hostile rewriting or
rollback. Restoring an older valid copy can reopen old sessions just like losing
the file; unless freshness is guaranteed, recovery requires a new `PeerId`.

## Shutdown and diagnostics

`shutdown` immediately closes admission to new outbound and inbound work. Work
already accepted by an outbound queue receives the configured bounded grace
period to drain. Sender retirement atomically refuses any resolver result that
returns too late, so no prepared snapshot can be stranded after its sender has
exited. Live inbound sockets are shut down to wake receiver workers,
and `join` reaps the acceptor, every sender, and every receiver. Worker panics
are returned as a typed join error rather than disappearing silently.

`diagnostics`, `peer_diagnostics`, `queue_depths`, `health`, and `local_addr`
provide framework-neutral snapshots. Snapshot admission, successful resolution,
source refusal, resolver failure, byte mismatch, revoked queued work, exhausted
bulk retries, receive-memory refusals, and configuration blocks have explicit
counters. The runtime reports starting, ready, degraded, stopping, failed, and
stopped states without requiring a logging or metrics crate.

## Group routing

The caller supplies `GroupIdCodec<G>`. Inbound group bytes are decoded once,
re-encoded, and retained through cheap route admission into full message
decoding; the exact bytes must match. The codec's declared peak covers returned
values, errors, and temporaries used by decode and canonical re-encoding, and is
included in the frame's receive-memory permit. The reusable canonical output
buffer is preallocated and charged to a separate permit held for the inbound
connection's lifetime. The transport therefore imposes no Serde or application
schema while still requiring one canonical, bounded route representation.

## Managed service integration

`TlsSender<G, C>` and `TlsPeerDirectory<G>` are intentionally separate handles.
They compose directly with `TransportRaftDriver`: the sender owns bounded
outbound admission, while the directory implements
`AuthenticatedPeerValidator<G, PeerId>` for inbound validation and complete
peer-policy publication. Policy replacement is atomic, retirement floors remain
monotonic, and accepted outbound work carries revocable proofs for both its
local-source binding and destination authorization. A live multiplexed
connection rechecks authorization for every group frame.

## Reference adoption

The fenced-lock production-composition process now uses this crate directly.
Its remaining link module contains only the fixture's canonical numeric group
codec and a caller-owned filesystem-to-`EndpointBook` adapter. The duplicate
rustls socket runtime and durable replay implementation were removed. Identity
allocation provisions `FileTransportSessionStore` state before publishing a
replica identity, so restart still fails closed when transport freshness state
is missing or corrupt.

The process binds the transport in its paused state before competing for the
replica directory. It activates endpoint discovery and network/session workers
only after exclusive ownership and recovery succeed, and joins those workers
before releasing ownership on every exit path. Recovery outputs and the initial
peer policy can still enter finite in-memory queues while the runtime is paused.

The sharded-counter process uses the same public runtime for all of its groups.
One stable `counter-host-<n>` principal and one persistent connection per remote
host carry canonical `(group, incarnation)` routes. Its reviewed process suite
proves hot/cold scheduling and control progress under bulk/snapshot pressure,
old-incarnation refusal across a live authenticated connection, endpoint
rediscovery, rolling restart, and fail-closed missing or corrupt transport
session state.

The fenced-lock adversarial process client freezes the outer version-1 hello
and peer-frame encoding independently of this crate. It talks to the current
runtime over real mutual TLS, pinning frozen-old-to-current process
compatibility in addition to the committed hello, frame, and state vectors.

## Bounds and wire contracts

Every retained directory, queue, connection set, and wire field has an explicit
finite bound. The default peer-frame receive bound is derived from
`rafter_codec::max_receive_frame_bytes`, not from the append budget alone.
The default weighted receive/decode budget is 256 MiB and charges 32 bytes per
declared wire byte, above the allocation-counted hostile v1 decode peak. The
standalone allocation harness checks minimum-size append entries and maximum
joint-membership frames against that contract.

See [`FORMAT.md`](FORMAT.md). Golden version-1 examples live under `format/`.

## Status

The production implementation and both reference adoptions are complete. The
exact-package runner builds this crate as a consumer-only archive and exercises
it in the Rust 1.88 MSRV and reviewed process lanes. Hello and data-frame fuzz
targets live in the repository fuzz workspace, and frozen version-1 vectors and
process coverage pin the compatibility boundary.

The crate remains `publish = false`. Publication is a separate release-policy
decision; implementation completion does not authorize a version change, tag,
or registry publish.
