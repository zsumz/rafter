# Fenced Lock Service Contract

Status: second reference-consumer contract for Rafter 1.0 API discovery.

This crate began as a dependency-free deterministic lock service and now carries
that same application contract onto Rafter's public crates, over a durable
transactional backend of its own. Every seam that proves missing, awkward, or
product-specific is recorded as it is found.

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
will record invocation, completion, rejection, unknown outcome, provable
refusal, and real-time ordering for an independent history checker, and will
cover leadership loss, read cancellation, isolated former leaders, and
stale-owner attempts against the guarded resource.

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

The durable adapter defines the versioned byte representation. There is exactly
one: the frame the application snapshot carries is the same frame the durable
store commits. The contract enumerates one set of facts for both, so a second
encoding would be a second chance to forget a high-water mark.

Compaction must never make an acknowledged command executable again, and must
never lower a high-water mark.

## Durable Backend

The durable composition keeps the lock service in a consumer-owned transactional
store. The store is an implementation choice; everything in this section is not.

### Transaction boundary

One transaction atomically commits:

```text
lock table mutations
fencing high-water marks
session and deduplication mutation
cached command result
replicated logical time
applied Raft index
```

All of it or none of it. There is no recoverable state in which a lock moved
without the cached result that explains it, or in which the applied index moved
without the data it names.

Applying a batch and installing a snapshot are the same transaction with
different applied-index rules. An apply must strictly advance the applied index;
an install may republish the current one, because adopting the state a replica
already holds must not require inventing an index. Neither may lower it.

The transaction commits before `apply_batch` returns, so a client's reply — and
any fencing token inside it — can only follow the commit point.

### Format discipline

The on-disk representation is versioned, self-describing, and checksummed. Every
record declares a magic and a format version, and a build that does not
recognize either refuses the artifact rather than reinterpreting it. Checksums
are accidental-corruption checks, not authentication tags.

A durable artifact records the `LockConfig` it was written under. Opening it
under different bounds is refused, because the bounds decide which states are
valid and a smaller resource bound would describe a service that could evict a
mark.

The applied Raft index appears both in the framing a recovery reads without
decoding and in the payload the snapshot install path checks. An artifact whose
two copies disagree is refused rather than reconciled.

A committed payload is restored through the pure model's own validating restore.
Verified bytes are not a licence to skip the invariant checks: a state that
breaks the expiry invariant or the held-token/high-water-mark equality is
refused however well sealed it is.

### Crash windows

Crash points cover every byte boundary before, during, and after the
transaction, including the interval after application persistence but before a
client reply. That last window is real and is answered by the deduplication
cache: the client retries the same request identity and receives the token the
crashed run minted, rather than a second token for one tenure.

A crash at any byte boundary leaves the store recoverable to exactly the
pre-transaction or the post-transaction state, never between. A failure reported
by the store means the outcome is unknown; reopening is the only thing that
decides it, and no caller may infer from an error that no bytes changed.

After a failed publication the handle is poisoned and refuses every later
transaction until it is reopened. A store that failed part way through
publishing cannot describe its own artifact.

### Recovery guarantees

Recovery is the oracle for every interrupted transaction. It reports what it
found, and its report is evidence a test asserts on rather than a diagnostic:
a crash test that could not show which window it reproduced would prove only
that an uninterrupted store works.

Recovery never repairs and never guesses. An unreadable artifact is not adopted,
and the next transaction supersedes it.

Recovery fails closed rather than starting empty when it can tell that a
committed state existed and has become unreadable. A lock service that opened
empty would hand out token 1 for a resource whose guarded downstream has already
accepted a far higher token, which is worse than not starting at all. A store
that has never committed is a different case and opens normally.

### Mark durability

This is the statement the whole durable design exists to keep:

> A recovered store must never issue a fencing token at or below any per-resource
> high-water mark it has ever durably acknowledged.

It holds across every path: apply, crash, recovery, replay, snapshot build,
snapshot install, and restart. A resource that disappears from the state is the
same violation as one whose mark decreases, and both are refused.

The durability boundary enforces it directly rather than trusting the model to
have been right. No transaction that would lower or drop an acknowledged mark is
published, and no recovered artifact that lowers one is adopted. These checks do
not replace the model's bookkeeping — the pure lock service remains the semantic
authority on which token a resource issues next — they refuse to make a
contradiction durable.

The property is asserted against the independent guarded resource, which knows
nothing about locks, sessions, or storage. If any recovery path lost a mark, the
next acquisition would mint a token the guard had already accepted and the guard
would refuse the current owner. That refusal is the observable form of the
failure fencing exists to prevent, and no aggregate check substitutes for it.

### What the durable slice does not close

Process-per-node composition is deferred. Every replica in these tests runs in
one process over its own store directory, and Raft's own durable state is still
modeled by in-memory stores handed between incarnations. Restart is therefore
real for the application and modeled for Raft.

Two consequences follow and are stated rather than left to be discovered.
Exclusive ownership of a store directory is assumed, not enforced; a real
deployment needs a lock that only a process composition can take. And a crash
test that never leaves its process reads its own writes back through the page
cache, so it proves which bytes reached the file and what a fresh opener makes
of them, but it does not prove that a durability barrier reached the medium.
Those barriers are justified by the ordering argument in the store's own
documentation and by review. A power-loss claim on a particular filesystem needs
evidence this suite does not supply.

Raft log compaction driven through the managed service is deferred with the
process half for the same reason. The application's participation in compaction
— building a snapshot at its own applied index, and installing one without
making an acknowledged command executable again — is covered here.

## History Vocabulary

A client operation history contains:

```text
Invoked(operation_id, command)
Completed(operation_id, response)
Unknown(operation_id)
NotCommitted(operation_id)
```

Deterministic rejections are normal completed responses. Every invocation
carries its full command, so retries under one request identity are recoverable
from the history alone.

### Mutation outcomes

The three terminal outcomes differ only in what the caller can prove:

- `Completed` carries the replicated response, lock and request rejections
  included.
- `NotCommitted` means the command provably never entered the replicated log.
  No copy of that attempt can commit later, so it minted no fencing token and
  consumed no sequence, and the caller may issue a fresh attempt under the same
  request identity.
- `Unknown` means the caller cannot tell. The command may have committed, so
  the caller must retry the *same* request identity and let the session cache
  decide.

A refusal is provable only when the managed write surface this application
consumes reports a `WriteError` that the service cannot reach once an entry has
been appended. That is the adapter's `SubmitOutcome::Refused`, and it is
exactly this set:

- `NotLeader`, `Rejected`, and `PayloadTooLarge`, which are the service's
  rendering of a proposal rejection. `rafter-app` documents that rejection as
  the local node refusing the proposal before replication, and Rafter raises it
  only from the pre-append admission check, so the command entered no log and
  no peer ever received the bytes.
- `ShuttingDown`, refused at admission before a proposal is started.
- `LocalProposalIdExhausted`, refused before the command reaches the group at
  all.

Every other lost outcome stays `Unknown`, because none of them is
distinguishable from a commit:

- `UnknownOutcome`, whatever diagnostic reason it carries. A dropped local
  proposal may already have replicated to a quorum.
- `ApplyFailed`, `Storage`, `Transport`, `Poisoned`, and
  `ManagedInvariantViolation`, none of which the service confines to the
  pre-append window, and each of which carries only a rendered message that a
  caller cannot inspect further.
- A proposal that was appended and then abandoned by a caller that stopped
  waiting. The entry exists and may yet commit.
- Any outcome lost to process or connection failure.
- Any `WriteError` variant this document does not name. That type is
  `#[non_exhaustive]`, so an unrecognized refusal defaults to the weaker claim.

What a driver knows about its own network never earns `NotCommitted`. A test may
have cut every link itself, but a history records only what the service surface
reported, because that is all a deployed client can read.

The distinction is worth drawing because `Unknown` is the weaker claim: a
checker must allow an `Unknown` operation to have taken effect, so an
implementation that minted a token for a refused acquisition would be explained
away. `NotCommitted` removes that excuse.

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
