# Rafter TLS transport formats, version 1

This file specifies the TLS binding, wire, durable-state, and runtime-binding
contracts for version 1. Socket scheduling is an implementation detail only
where this document does not make an observable bound or ordering promise.

All integers are unsigned big-endian. All length fields count bytes.

## TLS binding

Every connection uses rustls with these non-negotiable properties:

- TLS 1.3 is the only enabled protocol version;
- both peers present a certificate chain rooted in configured trust roots;
- outbound connections perform normal server-name verification;
- ALPN must negotiate the exact bytes `rafter/1`;
- TLS client resumption, server session storage, TLS 1.3 tickets, and early
  data are disabled;
- the validated leaf certificate's SHA-256 fingerprint must appear in the
  explicit `CertificateDirectory`.

Chain validation is necessary but not sufficient. A CA-valid leaf that is not
explicitly mapped is refused. Several leaf fingerprints may map to one stable
`PeerId` during certificate rotation. The directory maps exact DER leaf bytes
through SHA-256; it does not interpret certificate subjects or extensions as a
Rafter principal.

The Rafter hello begins only after the TLS handshake completes and the required
ALPN and explicit leaf mapping are available. TLS records and certificate DER
are governed by TLS and X.509, not by this format document.

## Handshake

Both directions begin with the ten bytes:

```text
RAFTER-TLS
```

Version zero is reserved. The first implementation supports transport version
`1` and peer-codec version `1`, while carrying ranges so later mixed-version
support does not require replacing the hello.

### Client hello

| Field | Encoding |
| --- | --- |
| magic | `[u8; 10]`, exactly `RAFTER-TLS` |
| minimum transport version | `u16`, nonzero |
| maximum transport version | `u16`, nonzero and not below minimum |
| minimum peer-codec version | `u16`, nonzero |
| maximum peer-codec version | `u16`, nonzero and not below minimum |
| cluster ID length | `u8` |
| cluster ID | exact validated UTF-8 bytes |
| claimed peer ID length | `u8` |
| claimed peer ID | exact validated UTF-8 bytes |
| connection session | `u64`, nonzero |
| maximum complete send frame | `u32`, nonzero |

The server interprets the claimed peer ID only after rustls validates the
client chain and the leaf fingerprint maps to an explicit principal. The claim
must equal that authenticated principal.

### Server hello

| Field | Encoding |
| --- | --- |
| magic | `[u8; 10]`, exactly `RAFTER-TLS` |
| selected transport version | `u16` |
| selected peer-codec version | `u16` |
| cluster ID length | `u8` |
| cluster ID | exact validated UTF-8 bytes |
| server peer ID length | `u8` |
| server peer ID | exact validated UTF-8 bytes |
| accepted complete frame bytes | `u32` |
| status | `u8` |

Status `0` means accepted. For an accepted hello both selected versions and the
frame bound are nonzero.

Refusal status tags are:

| Tag | Meaning |
| ---: | --- |
| 1 | unknown leaf certificate |
| 2 | claimed/authenticated identity mismatch |
| 3 | cluster mismatch |
| 4 | transport-version mismatch |
| 5 | peer-codec-version mismatch |
| 6 | frame limit rejected |
| 7 | stale durable connection session |
| 8 | server busy |

For every refusal, both selected versions and the accepted frame bound are
exactly zero. Other combinations are noncanonical and rejected.

The server evaluates a mapped client hello in this order:

1. claimed `PeerId` equals the TLS-authenticated fingerprint mapping;
2. `ClusterId` matches exactly;
3. a common outer transport version exists;
4. a common `rafter-codec` version exists;
5. the declared complete-frame bound can hold a structurally valid frame;
6. the connection session is newer than the durable inbound high-water.

Only the final step mutates session state. A newly accepted high-water is
published durably before an accepted server hello is returned. The accepted
frame bound is the smaller of the client offer and server receive bound.

The client independently requires the TLS-authenticated server principal to be
the dial target, the server hello to claim that same principal and cluster, the
selected versions to fall within its offered ranges, and the accepted frame
bound to fall within its offer.

## Durable session state

The file-backed session store writes one complete checksummed envelope:

| Field | Encoding |
| --- | --- |
| magic | `[u8; 8]`, exactly `RFTSESSN` |
| format version | `u16`, exactly `1` |
| cluster ID length | `u8` |
| cluster ID | exact validated UTF-8 bytes |
| local peer ID length | `u8` |
| local peer ID | exact validated UTF-8 bytes |
| maximum peer records | `u16`, nonzero |
| peer record count | `u16`, not above maximum |
| peer records | count records in strict `PeerId` order |
| checksum | big-endian `u32` CRC-32/IEEE over every preceding byte |

Each peer record is:

| Field | Encoding |
| --- | --- |
| peer ID length | `u8` |
| peer ID | exact validated UTF-8 bytes |
| highest outbound session | `u64`; zero means none |
| highest inbound session | `u64`; zero means none |

A record with both session values zero is noncanonical. Peer IDs are strictly
increasing, so duplicates and alternate ordering are rejected. The encoded
peer bound is itself bounded by 65,535 records, and file reads enforce the
absolute version-1 maximum before trusting any length or count from the file.

The file is published by writing and synchronizing a sibling temp file,
renaming it over the current state, and synchronizing the parent directory. A
failure at any mutating boundary makes the open handle unusable until reopen.
Creation uses create-new semantics and the same temp-file, rename, and directory
synchronization protocol; opening never creates missing session state. The
CRC32 detects accidental corruption only. It is neither tamper evidence nor a
rollback detector.

## Peer frame

After an accepted handshake, one directional TLS stream carries complete
frames:

| Field | Encoding |
| --- | --- |
| body length | `u32` |
| frame kind | `u8`, exactly `1` |
| connection sequence | `u64`, nonzero |
| group ID length | `u16`, nonzero |
| canonical group ID | caller-defined bounded bytes |
| Raft sender | `u64` |
| Raft recipient | `u64` |
| inner message length | `u32` |
| inner message | exactly one `rafter-codec` frame |

The body length excludes its own four-byte prefix and covers every following
field. The inner-message length must consume exactly the rest of the body.
Bytes after the declared body are rejected.

The group ID is decoded with `GroupIdCodec<G>`, then re-encoded and compared
byte-for-byte with the route bytes. The sender encoded inside the
`rafter-codec` message must equal the outer Raft sender.

The outer frame has no checksum. It is carried only inside authenticated TLS;
the inner `rafter-codec` frame retains its own CRC32 accidental-corruption
check.

## Receive bounds

A decoder checks the declared body length against `WireLimits` before invoking
the caller's group decoder. It never allocates from an untrusted declared
length. The default body bound is:

```text
31 fixed bytes
+ 128 maximum group-ID bytes
+ max_receive_frame_bytes(512 KiB append budget)
= 2,163,195 bytes
```

The complete default frame bound is 2,163,199 bytes including the prefix.

## Runtime binding

Runtime behavior does not add bytes to the handshake or peer-frame formats,
but it fixes when those bytes may be produced and how much work may be retained.

### Outbound ownership and redial

The owned runtime creates one sender worker and one finite outbound queue for
every `PeerId` present in `EndpointBook` when `bind` succeeds. Existing endpoint
sets may be atomically replaced or removed while the runtime is live. Adding a
new physical peer requires rebuilding the runtime so worker and queue ownership
remain explicit and bounded.

Ordinary queued work contains canonical group bytes, Raft identities, and one
encoded `rafter-codec` message, but no connection sequence. Snapshot work may
instead contain the kernel's bounded directive and caller-owned group route.
Neither form carries a connection sequence. The worker assigns the next sequence
only after TLS, the Rafter handshake, durable session allocation, and any
snapshot payload resolution succeed. If a write fails ambiguously, the
connection is discarded and prepared work may be retried on a newer session
beginning again at sequence one.

Before every dial the worker reads the latest endpoint generation and cycles
through its finite resolved socket-address list. `RaftTransport::send` never
performs endpoint resolution, dial, handshake, sleep, disk I/O, or a blocking
queue wait.

### Outbound queue admission and selection

Each peer queue has independent maximum retained frame and byte counts. The
configured control reservation is unavailable to replication and snapshot
traffic, so bulk traffic cannot consume every slot ahead of elections,
heartbeats, and protocol responses.

Selection is bounded weighted priority. A finite control burst is followed by
an opportunity for non-control traffic, and replication and snapshot work
alternate when both remain available. Queue capacity is retained while an item
is in flight and released only after it is sent or explicitly dropped; moving a
frame from queued to current work therefore cannot evade the configured bound.

### Snapshot directives

`RaftTransport::send_snapshot_chunk` never reads snapshot storage. It validates
the route and sender, canonically encodes the group ID, computes the exact
complete-frame bytes implied by the directive, and attempts one nonblocking
snapshot-queue admission.

The sender worker invokes the configured `SnapshotChunkResolver<G>` outside the
managed driver lock. The resolver receives the caller-owned group route, outer
Raft sender and recipient, and the kernel's `SnapshotChunkSend`. It returns only
the opaque payload bytes. The worker requires exactly `chunk.len` bytes, builds
the `InstallSnapshotChunk` message itself, and requires the materialized frame
length to equal the bytes reserved at admission before assigning a connection
sequence.

`Ok(None)` means the named snapshot is no longer available and drops this
attempt like a lost Raft message. A typed resolver error likewise drops only
that attempt. Both outcomes and any byte-accounting mismatch are counted.
Without a resolver, synchronous admission returns
`SnapshotResolverUnavailable`; no directive is silently accepted and lost.

### Inbound connection and queue bounds

The nonblocking listener grants a finite connection permit before spawning a
receiver. The permit covers TLS and Rafter handshakes as well as the established
connection. Receiver `JoinHandle`s are retained and continuously reaped.

After the durable inbound session decision, the runtime installs that session
as the live epoch for the authenticated `PeerId`. A higher session closes and
fences the previous socket. An equal or lower delayed session cannot replace a
newer live epoch. Frame admission and live-epoch validation linearize under the
same epoch lock.

Inbound retention is bounded by count and bytes both per authenticated peer and
globally. A frame admitted to neither bound is dropped and counted. Releasing a
drained envelope releases both bounds exactly once.

A read timeout before any byte of the next frame is an idle polling event and
preserves the connection. A timeout after a length prefix or body has begun is
a framing failure and closes the connection.

### Shutdown

Shutdown closes new outbound and inbound admission immediately. Accepted
outbound items may drain until every queue is empty or the configured grace
period expires. The runtime closes live inbound sockets, stops accepting, and
retains ownership of every worker until `join` reaps it. Worker panics remain
observable through typed join errors. Terminal session-store failures latch
`Failed` health and remain visible through diagnostics and subsequent typed send
refusals.

## Golden vectors

The `.hex` files contain lowercase hexadecimal with one trailing newline:

- `client-hello-v1.hex`
- `server-hello-v1.hex`
- `peer-frame-v1.hex`
- `session-state-v1.hex`

Tests decode the checked-in bytes, compare their semantic fields, re-encode,
and require exact equality.
