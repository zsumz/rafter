# Fenced Lock Service Contract

Status: second reference-consumer contract for Rafter 1.0 API discovery.

This crate begins as a dependency-free deterministic lock service. It does not
use Rafter yet. A later slice will integrate this exact application contract
through Rafter's public crates and record every seam that is missing, awkward,
or product-specific.

The lock service is deliberately small. It exists to prove:

- linearizable authority over a named resource;
- fencing tokens that survive every form of ownership loss;
- replicated logical expiration with no real-time claim;
- bounded request deduplication across retries and unknown outcomes;
- snapshot-safe client sessions and token high-water marks; and
- agreement between an implementation and a structurally independent oracle.

## Resource Model

`LockConfig` fixes the maximum client slots and the maximum number of tracked
resources when a service is created. A client ID addresses one configured slot,
so session state is bounded even when epochs are replaced repeatedly.

A resource is *tracked* once an acquisition on its name has succeeded. A
tracked resource keeps its fencing-token high-water mark forever. Reclaiming
that mark would reissue a token that a guarded resource has already accepted,
so the tracked-resource table never shrinks and `max_resources` is the honest
bound on it. Acquiring an untracked resource when the table is full is
rejected; it does not evict another resource's mark.

Resource names are bounded inline ASCII: at least one and at most 64 bytes,
each byte an ASCII alphanumeric or one of `-`, `_`, `.`, `/`. Names are
compared byte-exactly. No normalization, case folding, or Unicode
canonicalization is applied, because every replica must reach the same naming
decision from the same bytes without consulting a table that can differ
between builds.

## Commands

```text
OpenSession(client_id, session_epoch)

Submit(
  client_id,
  session_epoch,
  sequence,
  request_fingerprint,
  operation
)
```

Operations are:

```text
Acquire(resource, lease)
Renew(resource, token, lease)
Release(resource, token)
ExpireThrough(horizon)
```

Queries are not replicated mutations:

```text
GetLock(resource)
```

Leases are nonzero logical durations. Fencing tokens and session epochs and
sequences are nonzero. Logical time and expiries are unsigned and start at
zero.

## Queries

`GetLock` reports the current holder, expiry, the resource's fencing-token
high-water mark, and the current logical time. An untracked resource reports no
holder and no high-water mark.

Queries never mutate. `GetLock` on an unknown name does not create a tracked
resource.

The Rafter adapter will serve every query behind an ordinary linearizable read
barrier. This application makes **no** lease-read claim: the app-layer
leader-lease read path is unsupported today, and this contract must not imply
otherwise. Query behavior is modeled here; transport and read barriers are not.

## Session Protocol

Session epochs and request sequences are nonzero integers.

`OpenSession` behaves as follows:

- an unused client slot accepts its first epoch;
- the current epoch is idempotent and preserves its cached completion;
- a greater epoch replaces the session and clears its sequence and cache;
- a lower epoch is rejected as stale; and
- a client ID outside the configured slot range is rejected.

Each active session stores at most one completed request:

```text
current session epoch
highest completed sequence
exact bounded operation
request fingerprint
cached result
```

Clients may have at most one operation outstanding. For `Submit`:

1. The client slot and exact session epoch must be active.
2. The supplied fingerprint must equal the fingerprint of the supplied
   operation.
3. `highest + 1` is the only sequence that may execute.
4. Retrying `highest` with the exact same operation returns the cached result
   without changing state.
5. Reusing `highest` with another operation is a conflicting retry.
6. A lower sequence is stale.
7. A sequence above `highest + 1` is a gap.

Checks run in that order. Envelope self-consistency is decided before sequence
admission, because a request whose fingerprint does not describe its own
operation is malformed regardless of where its sequence falls.

When the highest sequence reaches its numeric maximum, the client must open a
greater session epoch before issuing another operation.

Every next-sequence operation consumes its sequence and caches its result,
including deterministic lock rejections. This prevents a rejected request from
succeeding later when unrelated state changes.

Session, sequence, fingerprint, and conflicting-retry rejections do not consume
a sequence.

### Request fingerprints

The fingerprint is a deterministic 64-bit digest of the operation's canonical
encoding. It binds a request identity to the operation the client believes it
sent, which is what an adapter needs to route a retry after an unknown outcome.

The fingerprint is **not** the admission key. Retry and conflict decisions
compare the exact bounded operation, so a digest collision can never admit a
conflicting retry as an exact one. The fingerprint is checked for
self-consistency and cached; it never substitutes for exact comparison.

### Session retirement

This slice retires nothing. Client slots are fixed at configuration time, a
greater epoch replaces a slot's session in place, and sessions are never
collected. Session retirement and eviction policy must become explicit before
durable process admission.

Locks do not belong to sessions. Replacing a client's session epoch clears that
client's deduplication state and nothing else; it never releases a lock. A
session-scoped release would be an expiration path that logical time does not
govern, and it would let a client restart silently revoke authority that a
guarded resource is still honoring. Ownership is released only by `Release` or
by logical expiration.

## Ownership

A held lock records its owner as a client slot, its fencing token, and its
expiry. `Renew` and `Release` require both:

1. the requesting client slot is the recorded owner; and
2. the presented fencing token equals the lock's current token.

Ownership is checked before the token, so a caller that lost the lock to
another client learns that first. A caller holding a token for a tenure that
has already ended is not the owner of any later tenure.

## Replicated Logical Time

Logical time starts at zero and advances **only** through `ExpireThrough`. It
is a replicated counter, not a clock. Nothing in this service expires after a
real-world duration, and no part of this contract may be read as a wall-clock
or timeout guarantee.

`ExpireThrough(horizon)` requires `horizon > logical_time`. An equal horizon is
**rejected**, not treated as idempotent: retry safety is already supplied by
the session cache, which returns the recorded result for an exact retry of the
highest completed sequence. A second, weaker idempotence path would hide a
driver that replays a stale horizon under a fresh sequence, so the strictly
monotonic rule is enforced and reported as `LogicalTimeNotAdvanced`.

Only the service's authorized expiration driver should submit `ExpireThrough`.
That authorization lives **outside** the replicated state machine. The state
machine deliberately does not distinguish the driver from any other client: the
driver occupies an ordinary client slot and obeys the same session protocol.
Restricting who may submit the operation is the adapter's and the transport's
responsibility, and it must not be inferred from anything in this module.

## Expiry Boundary

A held lock carries an expiry `E`. The lock is held at every logical time `t`
satisfying `t < E`.

`ExpireThrough(h)` releases every held lock whose expiry satisfies `E <= h`.
A lock with expiry `E` therefore survives `ExpireThrough(E - 1)` and is
released by `ExpireThrough(E)`. `E` is the first logical time at which the
lease no longer holds.

`Acquire` at logical time `T` with lease `L` sets `E = T + L`. `L` is nonzero,
so `E > T` and a lock is never born expired. An expiry that would overflow is
rejected as `LeaseOverflow` rather than saturating.

### Derived state invariant

Logical time advances only through `ExpireThrough`; `ExpireThrough` releases
every lock it passes; `Acquire` and `Renew` both set an expiry strictly greater
than the current logical time. By induction:

> Every lock present in the lock table satisfies `expiry > logical_time`.

A present lock is therefore a held lock. There is no lapsed-but-unswept state,
no operation needs to test liveness separately from presence, and snapshot
restore rejects any state that violates this.

## Renewal

`Renew` extends a tenure. It does not start one.

- It never issues a new fencing token. The token names an uninterrupted
  ownership tenure, and a guarded resource that already accepted token `N` must
  not be forced to accept a new token for ownership that never lapsed.
- The new expiry is `max(current_expiry, logical_time + lease)`. Expiry is
  monotone non-decreasing for the life of a tenure.
- A renewal that would not extend the expiry succeeds and leaves it unchanged.
  Shortening is not offered: an owner that could pull its own expiry backwards
  could let a second owner acquire earlier than the first owner believes, which
  is the exact failure fencing exists to prevent.
- Renewing an unheld resource is rejected. Expiration ends a tenure; it cannot
  be undone.

## Fencing Tokens

Every successful acquisition receives a fencing token from the resource it
names. Tokens are scoped to one resource name. The first acquisition of a
resource receives token 1, and each later acquisition of that resource receives
the resource's high-water mark plus one. Tokens issued for different resource
names are unrelated and must never be compared.

The per-resource high-water mark survives:

- release;
- logical expiration;
- deletion and recreation of the lock, meaning any sequence of release or
  expiration followed by a new acquisition of the same name;
- snapshot and compaction; and
- restart.

A held lock's token always equals its resource's high-water mark, because
acquisition is the only issuer and a resource cannot be acquired while held.
Snapshot restore rejects any state where the two differ.

A resource whose token space is exhausted fails closed with `TokenExhausted`
and becomes permanently unacquirable. Wrapping would reissue a token that a
guarded resource has already accepted.

## Guarded Resource

The test system includes an independent downstream resource. It is not part of
the replicated state machine, it has no knowledge of the lock table, and it
shares only the fencing-token and resource-name vocabulary.

The guarded resource records the highest fencing token it has accepted. It:

- rejects a write naming a different resource;
- rejects a write carrying a token strictly older than the highest accepted
  token; and
- accepts a write carrying the highest accepted token again, because one
  uninterrupted tenure performs many writes under one token.

The required safety property is not that tokens increase. It is:

> Once a later owner's write is accepted, a stale former owner cannot modify
> the guarded resource.

That property is asserted directly: a former owner whose lock was released or
expired, whose successor has written once, is refused.

## Result Taxonomy

Every replicated command produces exactly one stable response.

```text
SessionOpened(session_epoch)
Operation(operation_result)
Rejected(request_rejection)
```

Operation results are:

```text
Acquired(token, expiry)
Renewed(token, expiry)
Released
Expired(released_locks, logical_time)
Rejected(lock_rejection)
```

Lock rejections consume and cache their sequence:

```text
LockHeld(owner, token, expiry)
LockNotHeld
NotLockHolder(owner)
FencingTokenMismatch(current)
LeaseOverflow
TokenExhausted
ResourceCapacityExceeded
LogicalTimeNotAdvanced(current)
```

Request rejections do not consume a sequence:

```text
ClientOutOfRange
SessionNotOpen
StaleSession(current)
FutureSession(current)
StaleSequence(highest)
SequenceGap(expected)
ConflictingRetry
FingerprintMismatch(expected)
```

Acquiring a resource that is already held is rejected even when the requester
is the current owner. An owner extends its tenure with `Renew`; re-entrant
acquisition would mint a new token for unchanged ownership and needlessly
invalidate the previous token at the guarded resource. Retry safety is
unaffected, because an exact retry returns the cached acquisition.

`Release` and `Renew` report `LockNotHeld` for both a tracked-but-free resource
and an unknown name, so a caller cannot probe whether a name was ever used.

## Invariants

The implementation and oracle must establish:

1. Fencing tokens issued for one resource strictly increase across
   acquisitions.
2. A resource's high-water mark never decreases, through release, expiration,
   recreation, snapshot, and restore.
3. A held lock's token equals its resource's high-water mark.
4. Every present lock satisfies `expiry > logical_time`.
5. Logical time is strictly monotone and advances only through
   `ExpireThrough`.
6. A lock with expiry `E` survives `ExpireThrough(E - 1)` and not
   `ExpireThrough(E)`.
7. Only the recorded owner presenting the current token may renew or release.
8. Renewal preserves the token and never lowers the expiry.
9. An accepted request identity changes state at most once.
10. An exact retry returns the original result.
11. Conflicting request reuse never changes state.
12. An older session cannot act after a newer epoch opens, and a newer epoch
    never releases a lock.
13. Snapshot and restore preserve the lock table, sessions, cached operations,
    fingerprints, cached results, logical time, and every per-resource
    high-water mark.
14. Resource bounds fail closed without evicting live correctness state.
15. A stale former owner is refused by the guarded resource once a later owner
    has written.

Aggregate invariants do not imply linearizability. The later process adapter
will record invocation, completion, rejection, unknown outcome, and real-time
ordering for an independent history checker, and will cover leadership loss,
read cancellation, isolated former leaders, and stale-owner attempts against
the guarded resource.

## Independent Oracle Rule

The implementation and reference oracle share command, result, and inspection
types only. The fingerprint digest is part of that shared schema, in the same
way that operation equality is; it is an encoding rule, not a transition.

They do not share:

- transition functions;
- validation helpers;
- session or sequence decision code;
- lock table mutation helpers;
- token issuance or high-water bookkeeping;
- deduplication logic; or
- snapshot reconstruction.

No code is shared with any other reference consumer either. No common harness
exists yet, and none will be extracted before a second consumer proves the same
shape is needed.

The implementation keeps one ordered record per tracked resource, with the
holder embedded in that record and the high-water mark stored beside it. The
oracle keeps an append-only journal of every tenure ever opened and stores no
high-water mark at all; it derives the current holder, the mark, and the
tracked-resource count by folding that journal. A bookkeeping bug in either
direction cannot be mirrored by a structure that does not keep the same books.

## Snapshot Contract

The pure model snapshot is transport-neutral and opaque outside the
implementation. It carries the lock table, every tracked resource's high-water
mark, all sessions with their cached operation, fingerprint, and result, and
the current logical time.

Restoring it validates configured bounds, client-slot ownership and range,
uniqueness, cached-fingerprint agreement, the expiry invariant, and the
held-token/high-water-mark equality.

The durable adapter will later define a versioned byte representation. Its
application transaction must atomically persist:

```text
lock table mutations
fencing high-water marks
session and deduplication mutation
cached command result
replicated logical time
applied Raft index
```

Compaction must never make an acknowledged command executable again, and must
never lower a high-water mark.

## History Vocabulary

A client operation history contains:

```text
Invoked(operation_id, command)
Completed(operation_id, response)
Unknown(operation_id)
```

Deterministic rejections are normal completed responses. `Unknown` means the
caller cannot tell whether the replicated command committed and must retry the
same request identity. Every invocation carries its full command, so retries
under one request identity are recoverable from the history alone.

## First Milestone Boundary

The first milestone contains:

- this contract;
- bounded command and result types;
- a pure deterministic lock service;
- a structurally independent oracle;
- an independent guarded resource;
- snapshot round-trip and replay tests;
- differential exploration and seeded differential workloads; and
- the history vocabulary.

It intentionally contains no Rafter dependency, transport, filesystem backend,
shared reference framework, or new Rafter public API.
