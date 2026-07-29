# Fenced Lock Service Contract

Status: completed authority acceptance consumer. Its deterministic histories,
independent bounded linearizability checker, and independent guarded-resource
checker run in source and exact-package modes. Its integration process suite
and bounded authenticated production-composition fixture run in source and
exact-package process modes.

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

Aggregate invariants do not imply linearizability. The deterministic and
process adapters record invocation, completion, rejection, unknown outcome,
provable refusal, linearizable-query cancellation, and real-time order. A
bounded black-box checker decides those histories against the independent
oracle. A separate guarded-resource history proves stale-owner exclusion
without folding the downstream resource into the lock state machine.

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

No transition, validation, session, token, snapshot, or oracle code is shared
with another reference consumer. The unpublished reference harness now owns
only the `OperationId`, complete-interval representation, bounds, real-time
predecessor construction, bounded backtracking, memoization, and generic
frontier counts proven common by the ledger and lock checkers. This crate still
owns the event parser, state key, every sequential action, typed mismatch
reason, guarded-resource check, and replay rendering.

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

That exception carries an obligation. **The applied Raft index is not the whole
ordering key for the session cache.** Two images can name one index and still
disagree about which requests have completed, so an install that republishes an
unchanged index must also dominate the durable session cache, client slot by
client slot: the session epoch first, then the highest completed sequence under
it. Opening a newer epoch is what legitimately clears an older epoch's cache,
which is why the epoch outranks the sequence rather than sitting beside it.

Dropping a cached completion makes an acknowledged operation executable again,
and for an acquisition that mints a second fencing token for one tenure — the
same failure as a lost mark, reached by another road. The check is scoped to an
unchanged index deliberately: above it the model has legitimately advanced and
is the authority on the sessions it retired along the way, and the durability
boundary does not second-guess it.

The transaction commits before `apply_batch` returns, so a client's reply — and
any fencing token inside it — can only follow the commit point.

### Format discipline

The on-disk representation is versioned, self-describing, and checksummed. Every
record declares a magic and a format version, and a build that does not
recognize either refuses **the store**, rather than reinterpreting the artifact
or quietly falling back to another copy of the state. That refusal applies
wherever the field is present and whatever else is missing; a check that runs
only once enough other bytes have arrived is a check about length rather than
about the field. Checksums are accidental-corruption checks, not authentication
tags.

A magic's first byte doubles as the publication mark described under recovery,
so the mark costs no extra field: a sealed copy is byte-for-byte what it would
be without it, and both checksums are computed over the sealed form. That last
detail is what makes the mark checkable rather than merely present. Restoring the
mark to its sealed value and reading the copy again is a well-defined question
with a checksum behind it, which is how recovery tells a whole image with a
rotted mark from an image a publication never finished, and it is why the mark
needed no format change to stop being the one unprotected byte.

A format version this build does not write deserves naming on its own, because
it needs no corruption to occur: a binary downgrade produces it from entirely
healthy files. It is always a refusal, and the one refusal the repair entry point
will not clear either. That order has a cost, and the cost is named rather than
left to be discovered: the version byte is read before the checksum that covers
it, so a single altered version byte makes a copy unreadable by both entry
points. That is a refusal and not a loss — every byte is still on the medium —
and the alternative trades it for a repair that can discard a newer build's
committed work, which is the worse of the two.

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

One boundary is resolved by the named repair entry point rather than by opening:
the point at which the whole image is durable and its publication mark has not
yet been promoted. Those bytes are also exactly what a live copy whose mark byte
later rotted leaves, so opening refuses rather than choosing between two
histories it cannot see, and a caller who has decided which happened resolves it
to the pre-transaction state — but only where the two histories agree about every
fencing mark a client could hold. Where they do not, both entry points refuse.
The argument is under "Recovery guarantees".

After a failed publication the handle is poisoned and refuses every later
transaction until it is reopened. A store that failed part way through
publishing cannot describe its own artifact.

### Recovery guarantees

Recovery is the oracle for every interrupted transaction. It reports what it
found, and its report is evidence a test asserts on rather than a diagnostic:
a crash test that could not show which window it reproduced would prove only
that an uninterrupted store works.

Opening never rewrites an artifact and never guesses. An unreadable artifact is
not adopted. Discarding one is a separate, named entry point, described at the
end of this section.

**An artifact this build cannot read is never treated as absent.** That is the
principle the rest of this section is an application of. Unreadable committed
state is not residue to step over, not a copy to skip in favour of another one,
and not a reason to open at an older state and carry on: it is a refusal to
open, reported to the caller, who is the only party that can decide what to do
about it.

Recovery may skip a copy it cannot read only when it can *prove* that copy was
not the live one. **That proof rests on a mark the publication wrote, and it is
not that mark alone**, and the difference is the whole of this section. The
publication writes the half no reader could infer — every interrupted
publication leaves the slot unsealed — and the reader supplies the other half by
re-reading the slot with the mark restored and asking whether it holds a whole
image.

An enumeration of what an interrupted publication leaves is not that proof. Such
a list can be complete — a short header, a short payload, a payload with no
seal, a torn seal really are all an interrupted publication can leave — and
still be useless in the direction a reader needs it, because the reader is
asking the converse: does *this* shape mean a publication was interrupted? It
does not. A sealed image that has lost its last byte is a torn seal. It is a
strict prefix of an image this build wrote, it carries this store's magic and
version, and no checksum over the bytes present fails. It matches the
enumeration exactly and it is the live image; skipping it drops an acknowledged
high-water mark and reissues a token a guarded resource has already accepted.
Nor can any test on the bytes separate the two, because the two are the same
bytes.

So a publication marks its work as unfinished while it is unfinished. **The
first byte of a copy is its publication mark**: held at a value no sealed image
carries while the image is being written, and promoted to the sealed value by a
single byte written only after every other byte of that image is durable. A
crash leaves a prefix of what was written and that byte goes out first, so every
interrupted publication leaves the mark unsealed.

**The mark is half of the rule and not the rule.** Reading it as the whole rule
is how this section was wrong the second time. `interrupted ⇒ unsealed` is what
the paragraph above proves; its contrapositive is `sealed ⇒ not interrupted`;
and *skipping* on an unsealed mark needs neither of those but `unsealed ⇒ was
being written`, which is a third statement, and false. One byte shows it: the
sealed mark is `0x52`, the unsealed value is `0x00`, and a live copy whose first
byte rots between them reads as residue. Recovery adopted the stale partner, an
acknowledged fencing high-water mark regressed by a generation, the token it had
reached was reissued to a fresh tenure, and a guarded resource accepted two
independent tenures under one token — reached through the rule that exists to
prevent exactly that. Every *other* byte of the same header is under a checksum
and refuses the store. The mark byte was the only header byte no checksum was
ever consulted for, because the mark test returned first.

So skipping now requires the unsealed mark **and** positive evidence that the
bytes are not a whole image. Recovery reads the copy a second time with the mark
restored to the value both checksums were computed over — which is what makes
the question well defined — and skips only what still fails to verify at a step
this build can read. Three outcomes, three different facts:

- **Not a whole image**: a header cut short, a header checksum over bytes that
  are all present, a payload that is not all there, no trailer, a torn trailer,
  a trailer that seals nothing, bytes past the seal. With the unsealed mark that
  is ordinary residue, and it is skipped.
- **A whole image that verifies**, with only the mark reading unsealed. Two
  histories leave exactly these bytes — the written-but-not-committed window,
  and a live copy whose mark rotted — and nothing separates them, the
  generations included: the copy being written carries the live copy's
  generation plus one under both readings. Recovery refuses. Refusing is
  recoverable under both readings and skipping is recoverable under only one,
  and the choice is made on that asymmetry rather than on a guess about which
  history is likelier. Where the generations *do* settle it — a whole unsealed
  image the other copy's sealed image outranks, which is what a publication
  interrupted in its first bytes leaves — recovery resolves it with no operator
  at all.
- **A version this build cannot read**, which stops the second reading before it
  can say anything at all.

Every damage with a *sealed* mark refuses the store. A foreign magic, an
unreadable version, a checksum failing over bytes that are all present, bytes
past the seal, a sealed image cut short, a file emptied, a file missing: none of
them can be shown to be the copy that was being written, so recovery has no
argument about which copy the damage landed in. Skipping such a copy is a silent
one-generation rollback.

That the mark byte is now no weaker than its neighbours is a claim about every
byte of an image, so it is checked as one: a unit test alters every byte of a
sealed image to every other value it could take and requires that none of the
results is residue. A rule this narrow is exactly the kind that decays quietly,
and a paragraph would not have noticed.

Two rules follow from putting the mark first, and both were wrong while the
shapes were doing the work:

- **The magic and the version are read at every length that carries them**,
  before anything classifies a copy by how many bytes it has. The argument for
  refusing a foreign version is about the field, so it holds wherever the field
  is present; gating it on a full header made the same bytes refused at one
  length and adopted as this build's own residue at another. It holds on both
  sides of the seal test, because the second reading of an unsealed copy goes
  through the same version gate.
- **A durable file of zero bytes, or a missing one, is damage.** Creation writes
  the mark into both copies and no publication ever shortens one to nothing, so
  neither state is one this store leaves behind. A pair of emptied files is not
  a store that has never committed; it is a store whose files were emptied, and
  opening a fresh service over them discards every high-water mark with nothing
  reported.

Recovery also fails closed, rather than starting empty, when no copy is readable
at all. A lock service that opened empty would hand out token 1 for a resource
whose guarded downstream has already accepted a far higher token, which is worse
than not starting at all. A store that has never committed is a different case
and opens normally: its copies carry their creation marks, which is not damage.

The recovery report names **which** copy was damaged as well as how. That index
is what distinguishes benign crash residue from anything else, and a report that
could not draw the distinction would leave a caller unable to tell a clean
restart from one worth investigating.

Destructive recovery is available and is a separate, named entry point. It
adopts the readable partner of a copy this build cannot read, and reports which
copy it gave up, what that copy held, and the generation it adopted instead. The
store did without it while its refusals were rarer; what changed is that an
ordinary crash between a publication's barrier and its seal is now a refusal, and
a store whose ordinary crash residue needs an operator with no documented way
forward is worse than one that names the way forward and reports what it costs.

That entry point covers an interrupted publication that raised no fencing
high-water mark — a release, an expiry, a renewal, a session open. It does not
cover an interrupted **acquisition**, and that half is named here rather than
left to be discovered: an acquisition raises a mark, the interrupted copy is the
newer one, and no older partner carries a mark that copy was the first to hold,
so the discard rule below refuses it in both entry points. Acquisition is the
operation a fencing lock exists to perform, so a store crashed mid-acquisition
is a store no reading entry point opens.

**There is a third entry point for exactly that store, and it is a third
decision rather than a flag on the second.** It deletes both copies and opens an
empty store for the replicated log to refill, reporting what it deleted and how
far that store had applied. It is sound because this store publishes only what
the log has already committed — a state machine applies an entry after the entry
commits, and the publication happens during the apply — so no mark it deletes is
one the log cannot return. Two things it costs are stated rather than softened.
Until the replay has run the store holds no acknowledged marks, so the two
checks that defend them have nothing to compare against and will accept any
state offered. And the premise it rests on is about the group, not this
directory: re-seeding one replica of three is recoverable, re-seeding a quorum
destroys the marks outright, and nothing in the call can tell those apart.

**What refills the emptied store is the log this replica has *retained*, and
nothing else.** That is narrower than "the replicated log", and the difference
is a replica that has compacted. Earlier revisions of this section said the
group supplies whatever local compaction dropped, as a snapshot; it does not,
because a follower whose log matches the leader's is never sent one. So a
re-seed on a compacted replica used to reach exactly the state the paragraphs
below give as the reason to refuse a repair — a store handing out a token a
guarded downstream had already accepted — by a route that reported nothing.
It no longer runs: the emptied store's honest `LogIndex::ZERO` is below the
Raft snapshot boundary beside it, and the group over the two refuses by name
with `GroupError::AppliedIndexBelowSnapshotBoundary`. The remedy there is to
delete the Raft state alongside the store so the replica rejoins empty and is
sent a snapshot the ordinary way, not to re-seed again.

The three read progressively less: opening reads, repairing chooses between two
readings, re-seeding keeps neither. Each is a separate call, so no caller
reaches a later one by retrying an earlier one.

**Wherever a copy is discarded or set aside, its fencing high-water marks are
compared against the copy adopted in its place, and a discard the adopted copy
cannot dominate is refused by both entry points.** This section used to say
instead that a repair cannot report how much was given up, because reading the
discarded copy is exactly what failed. That was true of every damage except the
one an ordinary crash produces: a whole image whose only fault is its mark
*verified* under the mark restored, so this build can read it. The repair adopted
the stale partner anyway and reported a generation delta while an acknowledged
fencing mark regressed and a guarded resource accepted two independent tenures
under one token.

A repair that must discard a higher-marked copy is never legitimate here, and the
argument is the one the rest of this section rests on. The two histories behind
those bytes differ in exactly one observable: whether the discarded copy's marks
were ever acknowledged to a client. When the adopted copy carries them all, the
two readings agree on every mark a client could hold, adopting is correct under
both rather than a choice between them, and the repair costs nothing observable.
When it does not, the readings disagree about the one fact a fencing lock exists
to protect; nothing in the bytes decides which holds, and neither can the caller,
because the deciding evidence is in the guarded downstream this store cannot
read. Proceeding on the strength of having been called by name would be consent
in place of information. So it refuses and names the resource and both marks
rather than the two generations, because the marks are the loss and the
generations are not. The way forward is the re-seed entry point above: this
replica's copies are a projection of a committed log and the log rebuilds them,
while a fencing token that has left the cluster is in no log and nothing rebuilds
the guarded resource that accepted it.

Two boundaries of that rule are stated rather than left to be found, and each is
tested on both sides. The **session cache** is deliberately not required to
survive a repair: session progress advances on every applied entry, so requiring
it would refuse every repair, and unlike a token it is bounded by the applied
index the store reports and is restored by replaying from there. An image this
build **cannot decode** cannot be compared at all; there the older sentence is
the true one, the repair proceeds, and the report says the comparison did not run
rather than implying it did.

Three refusals stay outside the entry point's reach entirely, and each is a case
where there is no second reading to choose between. A version this build cannot
read is a newer build's committed work rather than damage, so the remedy for
damage must not discard it — the sibling ledger refuses the same shape from both
entry points. No readable copy at all leaves nothing to adopt, and opening empty
would hand out token 1 for a resource whose guarded downstream has already
accepted far more; a damaged copy whose partner has never held an image is the
same fact reached from the other side. A missing file is not damage found in an
artifact that was read, and re-creating it is a different act from choosing
between two files that are both present.

Creating a store's files is reported too, and counts against a clean opening.
Nothing inside a durable store can tell a replica that has never run from one
whose directory was emptied — both arrive as absent files — so the store states
the fact and the caller, which knows whether this replica has run before, judges
it. A creation report that no caller reads is a creation report that costs
nothing to be wrong.

Reports are consumed rather than produced. The driver that reopens a replica's
store across a restart asserts that a replica no scenario interrupted came back
with nothing to report, and that a replica creates its files on its first
opening and no other, so a rollback, a stray damaged copy, or a directory that
lost its store fails a test rather than passing quietly. A report nothing reads
is a report that costs nothing to be wrong.

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

The deterministic suites run every replica in one process over its own store
directory, and Raft's own durable state there is modeled by in-memory stores
handed between incarnations. Restart in those suites is therefore real for the
application and modeled for Raft. [Process Composition](#process-composition)
below closes that half — a replica is an operating-system process over
file-backed Raft stores — and the deterministic suites keep their shape because
what they buy is control over delivery, not durability.

One consequence of the deterministic suites' shape is stated rather than left to
be discovered, and it is not closed anywhere in this crate: a crash test that
never leaves its process reads its own writes back through the page cache, so it
proves which bytes reached the file and what a fresh opener makes of them, but
it does not prove that a durability barrier reached the medium. Killing a
process does not close it either — the kernel still holds the page cache, so a
`SIGKILL` loses nothing that reached a `write`. Those barriers are justified by
the ordering argument in the store's own documentation and by review. A
power-loss claim on a particular filesystem needs evidence no suite here
supplies.

Raft log compaction driven through the managed service is still deferred. The
application's participation in compaction — building a snapshot at its own
applied index, and installing one without making an acknowledged command
executable again — is covered here.

## Process Composition

The lock service runs as one operating-system process per replica. This is the
**integration** composition level defined in
[`docs/reference-consumers.md`](../../docs/reference-consumers.md), and this
section says exactly what that level establishes and exactly what it does not.
No claim below closes the 1.0 production-composition criterion.

### The replica process

One process owns one replica directory:

```text
<cluster-dir>/node-<id>/
  raft/          Rafter's file-backed hard state, log, and snapshots
  app/           the two lock-state slot files
  peer.addr      this replica's advertised peer address
```

Startup order is part of the contract, not an implementation detail:

1. bind the client port and announce it, refusing service;
2. acquire exclusive ownership of the replica directory;
3. open the durable lock store and read its applied floor;
4. recover the Raft runtime *through that floor* and hand the recovery outputs
   to the managed driver rather than applying them outside it; then
5. announce readiness and begin serving.

Step 1 precedes step 2 deliberately. A replica that cannot yet recover is
reachable and says so, rather than being indistinguishable from one that is not
running.

Step 4 hands the driver the recovery *outputs*, not an already-stepped group.
The outputs contain peer messages and snapshot directives, and a group stepped
outside the driver would have dropped them where nothing could route them.

### Ownership

Two live processes over one replica directory would interleave publications and
destroy each other's slots. The invariant is that one process owns one
directory, and it is enforced by the operating-system lock `rafter-storage`
takes over the Raft store directory, acquired before the lock store is opened
and held for the life of the process.

The lock store's slot files have no lock of their own, so this invariant rests
on that ordering rather than on the store defending itself. A second process is
refused at the Raft store and therefore never reaches the slots it would have
corrupted. A replica that finds its directory owned waits for a bounded period
rather than failing immediately, because a restarting replica legitimately races
the exit of the incarnation it replaces.

### Readiness

A replica refuses every client operation until it has applied every command it
knows to be committed — the group's committed *application* index, never the
commit index, because elections and membership changes commit entries the
application is never told about. Status is reported whether or not the gate is
open, so readiness is observable while it is closed.

Readiness means recovery finished. It does **not** mean the replica is current
with its cluster: a replica that recovers a durable floor below the cluster's
committed index is ready, and its local reads may be behind until it catches
up. Anything routing on readiness must understand it as a recovery signal, not
a freshness one.

The readiness gate is also where a store that will not open stops the replica,
and for this store that is not a rare event. `LockStore::open` refuses any
damaged slot it cannot prove was the one being written, whether or not the
partner is intact, because adopting the partner rolls the store back one
generation and a generation can contain a fencing high-water mark a guarded
resource has already accepted. A `SIGKILL` between a durable image and its seal
produces exactly such a slot, so an ordinary crash can leave a replica that a
plain restart will not open.

### Recovery is a decision with three settings

The process therefore takes `--recover`, and the three settings are strictly
increasing in what they discard. None is reachable by restarting into the
previous one:

- `open` adopts what recovery can prove and refuses the rest;
- `repair` additionally gives up a slot this build cannot read, and is itself
  refused when the slot it would give up carries a mark the adopted slot cannot
  dominate; and
- `reseed` deletes this replica's durable lock state outright.

Each announces what it cost, and a refusal names the setting that follows it.
`reseed` discards acknowledged fencing marks, and it is safe only because those
marks are also in the replicated log the replica is about to re-apply — which is
a fact about the cluster, not about this process. A replica reseeded while its
own log had been compacted past the discarded floor does not recover, and
nothing claims otherwise.

Recovery reports are consumed rather than produced. A replica announces the
residue it recovered from, announces separately that it had to create its slot
files, announces what a repair or a reseed discarded, and refuses on the report
that needs a human. A report nothing reads is a report that costs nothing to be
wrong.

The creation announcement is the one that needs the supervisor's own knowledge
beside it. On a replica's first boot it is expected; on a restart it means the
slots are gone and the replica is about to serve an empty lock table from
applied index zero with no fencing high-water marks at all. The store cannot
distinguish those, so it reports and the supervisor judges.

### The link between replicas

Peer messages travel over TCP in `rafter-transport-tcp-insecure`'s published
frame encoding, over connections this deployment owns. The split is deliberate:
the frame format is a wire contract Rafter publishes, while connection
lifetime, address discovery, queueing, and peer identity are deployment policy.
The demo transport's own connection-per-message shape is not used, because a
three-replica cluster ticking every 20 ms would leave hundreds of sockets a
second in `TIME_WAIT` — a port-exhaustion failure that arrives only under load.

The link bounds what it can: a maximum frame length refused before its bytes are
read, a bounded outbound queue per peer whose overflow drops frames, and
deadlines on connect and write. Dropping is correct rather than lossy — Raft
tolerates loss, reordering, and duplication, `rafter-service` counts a refused
send instead of failing the write behind it, and a blocking send inside the
driver's own lock would convert one slow peer into a stalled replica.

It authenticates nothing. `rafter-service` asks a deployment for an
authenticated principal and a validator that maps principals to replicas; the
principal supplied here is built from the sender field inside the frame that was
just decoded, so the identity check is a tautology. The validator's other two
questions are answered for real — an unrouted group is refused, and a fenced
member is refused — and identity is not answered at all. A dialer additionally
names the replica it believes it reached, and an acceptor that is not that
replica closes the connection; that stops a recycled ephemeral port from
misrouting a restarted replica's traffic, and it stops nothing an adversary
would do.

Peers find each other through a file in a shared directory, published only after
a replica owns its directory and re-read on every dial, so a replica that
restarted on a fresh port is found without reconfiguration.

### The client protocol

Clients speak a line-oriented protocol over a second port. It carries the three
terminal write outcomes intact across the process boundary, and the distinction
between them is the same one this contract draws everywhere else:

- a replicated response, with its disposition;
- a provable refusal, emitted only when the write error's own
  `WriteFate` is `NotAppended` — which `rafter-service` documents as the driver
  having observed the refusal itself — or when the readiness gate refused before
  the command reached `rafter-service` at all; and
- an unknown outcome, which is everything else, including the answer a killed
  replica never sent.

Two things the protocol does not carry are named here rather than discovered:

**The request fingerprint.** A request identity carries a digest of the
operation it claims, and the state machine rejects an envelope whose digest does
not describe its operation. Over this protocol the operation travels in the
clear on the same line, so the replica derives the digest from it and
`FingerprintMismatch` is unreachable across the process boundary. It is
exercised by the deterministic suites, where a caller can build an envelope that
disagrees with its own operation. `ConflictingRetry` is unaffected, because the
operation itself is compared.

**Any notion of who is asking.** A client id here is a bounded slot number in
the replicated state machine, which is deduplication vocabulary and not a
principal. Nothing authenticates a connection, so nothing stops one connection
from acting under another client's identity — including `ExpireThrough`, which
[Replicated Logical Time](#replicated-logical-time) says only the service's
authorized expiration driver should submit. That authorization lives outside the
replicated state machine by design, and at this composition level it lives
nowhere at all.

### Reads across the process boundary

`QUERY` is a linearizable read behind an ordinary barrier, which is the only
consistency this application offers a client.

`LOCAL` is a weaker read on the same path. `rafter-service`'s transport driver
serves `ReadConsistency::Local` as well as `ReadConsistency::Linearizable`, so
this verb reaches the replica's state machine through the read path a query
uses, with the same options type, the same receipt, and the same refusals — a
poisoned group and a state machine below its runtime's snapshot boundary are
refused here exactly as they are for a query. What it gives up is real, and is
why it stays a separate verb rather than an argument to `QUERY`: there is no
barrier, no quorum round, and no read proof, so it answers a plain status rather
than the application's query outcome. It exists so an operator — or a test
watching a rejoining replica — can ask what *this* replica holds. Nothing
routing on correctness may use it.

An earlier revision of this document said there was no such path, because the
driver refused every level but linearizable and this verb borrowed the group
instead. That borrow ran none of the refusals above, which is what the refusal
cost rather than saved.

### What the process suite establishes

Real processes are killed with `SIGKILL` and restarted from their own durable
stores. The suite establishes that:

- three processes elect a leader and serve the lock over real sockets, with
  acquisition, renewal, release, and reacquisition behaving exactly as the
  sections above specify;
- a session retry after the leader is killed replays its cached result and
  issues no second token;
- a replica killed mid-write recovers a durable applied floor from its own
  store, by whichever recovery setting that store's own report named, and
  catches up past it — high-water mark included;
- a store holding a slot this build cannot verify refuses to serve, says so in
  a line a supervisor can match on, is not talked round by a restart, and names
  the setting that opens it, which then reports what it discarded;
- readiness gates: a replica that cannot complete recovery refuses to replicate
  and refuses to read, and answers the same request once recovery completes;
- every resource's fencing high-water mark is monotone across a cluster-wide
  kill and restart, and the next tenure of each resource is strictly above what
  was acknowledged before the cluster died;
- an unauthenticated caller can act as any client and can drive replicated
  logical time, and is still held to the session protocol; and
- **fencing holds across process boundaries and across a restart.** A client
  acquires a lock and writes to the guarded resource. Its replica is killed
  while it still holds the lock — nothing has expired, and the surviving
  majority confirms the tenure is intact. The majority then expires the lease
  through consensus, a later client acquires a strictly higher token and writes,
  and the original client, which has learned nothing, is refused by the guarded
  resource. Every replica is then killed and restarted from its durable state,
  and the high-water mark comes back, the next tenure is above it, and both
  retired tokens stay refused.

Nothing in that last item waits for a lease to lapse. Expiry here is a
replicated command with a deterministic effect, which is why the lock's process
suite has no timing in it beyond waiting for elections — an advantage over a
clock-based lease that is worth stating, because it is why these tests bound
real time to elections and socket delivery alone.

Every restart above comes back under the *same* node ID, and that is
deliberate. Rafter's `NodeId` is single-use within its group — a committed
removal retires it, and a replacement replica joins under a fresh one — but a
kill and a restart are neither a removal nor a replacement. This cluster's
membership never changes, no replica is ever removed, and each process reopens
its own durable store as the replica it already was. Reusing the ID across a
restart is the ordinary path and stays legitimate; what would not be is
reopening under an ID some earlier committed removal had retired, which nothing
here does because nothing here removes anything.

### What it deliberately does not establish

- **Transport security.** Nothing is authenticated, encrypted, replay
  protected, or fenced by the link.
- **Authenticated identity.** Neither a peer nor a client proves who it is. The
  peer principal is the frame's own claim; the client identity is a slot number.
- **The expiration driver as a role.** The contract says authorization for
  `ExpireThrough` lives outside the state machine. At this level it exists
  nowhere, and a test demonstrates that rather than leaving it to this
  paragraph.
- **Persisted replica identity.** A replica's identity is an argument and a
  directory name, not a durable, verifiable fact.
- **Discovery.** Peers find each other through a file in a shared directory.
- **Structured metrics and diagnostics.** Lifecycle is a handful of stdout
  lines.
- **Signal handling.** Clean shutdown is a client command. There is no signal
  handler, because installing one from `std` alone is not possible and this
  workspace takes no external dependency; `SIGTERM` and `SIGKILL` both terminate
  abruptly, which the store's crash contract already covers.
- **Durability barriers reaching the medium.** Killing a process proves what a
  fresh opener makes of the bytes that reached the file. It does not prove that
  a barrier reached the disk.
- **A bound on inbound peer frames.** The replica's client drain is budgeted per
  pass, so no client population can hold the process off its clock or its own
  terminal exit. The peer drain is not: it takes an unbounded channel until it
  goes quiet. This is a named residual and not an oversight. Peers are the
  cluster's own replicas rather than arbitrary clients, so the drain is bounded
  by cluster size times Raft's per-peer in-flight window and terminates on its
  own; and a per-pass budget would cap the work while leaving the channel's
  memory unbounded, which is the appearance of a bound rather than one. The
  bound that would be real is refusing a peer connection, and it is out of scope
  on the same terms as the client connection limit — the link authenticates
  nothing, so any peer may already claim any identity.
- **Snapshot transfer over the link.** These tests keep short logs and never
  compact, so no replica ever installs a snapshot from a peer. The link refuses
  a leader chunk directive on the ground that `DurableRaftNode` resolves every
  one of them into an ordinary message before the driver sees it — which its
  documentation states and its code does — and counts the refusal on the `LINK`
  line so a violation is visible. No test here has made it fire, and none
  claims to have.
- **Bounded linearizability evidence.** The checker places at most 24
  operations and visits at most 200,000 configurations per history. A history
  above either bound is undecided and fails closed; this
  suite does not claim an unbounded proof. Within those limits, the process
  histories are decided solely from the client-visible vocabulary below,
  without logs, replica state, applied indexes, or membership.

## History Vocabulary

A client operation history contains:

```text
Invoked(operation_id, command)
Completed(operation_id, disposition, response)
Unknown(operation_id)
NotCommitted(operation_id)
QueryInvoked(operation_id, GetLock(resource))
QueryCompleted(operation_id, typed_resource_status)
QueryAbandoned(operation_id)
```

Deterministic rejections are normal completed responses. Every invocation
carries its full command, so retries under one request identity are recoverable
from the history alone. Every operation has exactly one invocation and one
terminal event. An operation whose terminal precedes another invocation must
linearize first; overlapping intervals may be ordered either way.

### Mutation outcomes

The three terminal outcomes differ only in what the caller can prove:

- `Completed` carries both the replicated response and `ApplyDisposition`,
  including lock and request rejections. A replay therefore cannot be explained
  as a fresh application.
- `NotCommitted` means the command provably never entered the replicated log.
  No copy of that attempt can commit later, so it minted no fencing token and
  consumed no sequence, and the caller may issue a fresh attempt under the same
  request identity.
- `Unknown` means the caller cannot tell. The command may have committed, so
  the caller must retry the *same* request identity and let the session cache
  decide.

A refusal is provable only when the managed write surface this application
consumes reports `WriteFate::NotAppended` for the failed write. That fate is the
driver's own report that it observed the refusal before the command reached the
local Raft log, and it is the adapter's `SubmitOutcome::Refused`. This
application does not maintain its own list of refusing variants: a second
classification here could disagree with the one the cluster actually proved, so
the criterion is the reported fate and nothing else.

The service reports that fate for two groups of failure:

- `NotLeader`, `Rejected`, and `PayloadTooLarge`, which are the service's
  rendering of a proposal rejection. `rafter-app` documents that rejection as
  the local node refusing the proposal before replication, and Rafter raises it
  only from the pre-append admission check, so the command entered no log and
  no peer ever received the bytes. `WrongGroup`, `ShuttingDown`, and
  `LocalProposalIdExhausted` are refused at admission, before the command
  reaches the group at all.
- `StateMachine`, `Storage`, `Transport`, `Poisoned`, and
  `ManagedInvariantViolation`, each of which carries an explicit fate rather
  than a fixed one, and so proves non-replication only when the driver observed
  it before the append.

Every other lost outcome stays `Unknown`, because none of them is
distinguishable from a commit:

- `UnknownOutcome`, whatever diagnostic reason it carries. A dropped local
  proposal may already have replicated to a quorum.
- Any of the fate-carrying variants above whose fate is `Unresolved`.
- A proposal that was appended and then abandoned by a caller that stopped
  waiting. The entry exists and may yet commit.
- Any outcome lost to process or connection failure.
- Any `WriteError` variant this build does not recognize. That type is
  `#[non_exhaustive]` and `WriteFate::may_commit` is written as the negation of
  the refusal, so an unrecognized variant defaults to the weaker claim.

What a driver knows about its own network never earns `NotCommitted`. A test may
have cut every link itself, but a history records only what the service surface
reported, because that is all a deployed client can read.

The distinction is worth drawing because `Unknown` is the weaker claim: a
checker must allow an `Unknown` operation to have taken effect, so an
implementation that minted a token for a refused acquisition would be explained
away. `NotCommitted` removes that excuse.

### Query outcomes

Only ordinary linearizable `QUERY LOCK` operations enter this history. A
successful query records its exact typed `ResourceStatus`; a barrier refusal,
cancellation, connection loss, or caller abandonment records
`QueryAbandoned`, which supplies no invented value and is discharged by the
checker. `LOCAL LOCK` is explicitly excluded because it is a local, potentially
stale observation rather than a linearizable operation.

### Black-box decision

The checker validates operation intervals before searching. Duplicate
invocations, duplicate terminals, terminals without invocations, unterminated
operations, and crossed mutation/query terminals are malformed histories and
are never weakened into something searchable.

For a well-formed history it performs a bounded Wing–Gong-style backtracking
search over operations minimal in real-time order. `ReferenceLockService` is
the sequential specification. Completed mutations must match both disposition
and response, an `Unknown` mutation branches between applied and absent,
`NotCommitted` is absent, completed queries must equal the oracle at their
chosen linearization point, and abandoned queries are absent. Failed
`(unplaced operations, oracle state)` configurations are memoized. A failure
retains the exact history, deepest placed prefix, and every blocked candidate
so it can be replayed.

### Guarded-resource history

The downstream guard has a separate history:

```text
GuardedInvoked(operation_id, guarded_resource, claimed_resource, token, value)
GuardedCompleted(operation_id, accepted_value_or_exact_rejection)
```

A recording wrapper places these events immediately around the external
guard's `apply`. Its checker requires one completion per invocation, keeps an
independent accepted-token floor per protected resource name, accepts retries
at the current token, and rejects any recorded acceptance below a later token.
Wrong-resource and stale-token refusals must match exactly. Deterministic and
process scenarios run this checker together with the lock checker, but the two
specifications and histories remain separate.

## First Milestone Boundary

This section records where the crate stopped at its first milestone. The
sections above supersede it: the adapter carries this contract onto Rafter's
published crates, and [Durable Backend](#durable-backend) puts the service's
state in a real transactional store. It is kept because the boundary it names
is what gave the adapter an application contract to meet, rather than letting
integration convenience define the application.

The first milestone contained:

- this contract;
- bounded command and result types;
- a pure deterministic lock service;
- a structurally independent oracle;
- an independent guarded resource;
- snapshot round-trip and replay tests;
- differential exploration and seeded differential workloads; and
- the history vocabulary.

It intentionally contained no Rafter dependency, transport, filesystem backend,
shared reference framework, or new Rafter public API. There is still no shared
application framework and no new Rafter public API. A later unpublished harness
shares only neutral bounded-search mechanics. The transport exclusion no longer
does —
[Process Composition](#process-composition) puts replicas on real sockets — and
the deterministic suites keep their in-process network because controlling
delivery is what they are for.

## Process Composition Boundary

The process slice makes a replica a process. It adds:

- a `lock-node` binary configured entirely by arguments and environment
  variables, following the startup order under
  [Process Composition](#process-composition);
- a consumer-owned TCP peer link over `rafter-transport-tcp-insecure`'s
  published frame encoding, with a `RaftTransport` and an
  `AuthenticatedPeerValidator` the managed driver owns;
- a line-oriented client protocol that preserves this contract's three terminal
  write outcomes across the process boundary;
- process orchestration in test support that spawns, kills, restarts, and
  escalates recovery for real processes, with per-test scratch directories and
  bounded predicate waits; and
- a process suite that kills replicas with `SIGKILL` and asserts the fencing
  property against a guarded resource outside the cluster.

The suite is labelled integration evidence, and it is `#[ignore]`d by default:
`docs/reference-consumers.md` puts durable process tests in the main and
nightly lanes, while the every-PR lane wants a package build and the
deterministic suites. Both dependency modes still compile the binary and the
suite, so a consumer that stopped building is caught everywhere; only running it
is deferred. `scripts/reference-process-check` gates the selection against
`verification/reference-process-test-inventory.fenced-lock.txt`, alongside the
ledger's own inventory.

Four limits stay, and none of them is an oversight:

1. This `lock-node` composition is the integration level. The separate
   [`lock-production-node`](#production-composition-boundary) fixture closes the
   bounded production-composability acceptance criterion without turning the
   fixture into a generic server, transport, or deployment product.
2. Real time is load bearing in a way the deterministic driver never allowed.
   The suite bounds it rather than eliminating it: every wait is a polled
   predicate against a deadline, no test sleeps and assumes, and every test that
   kills a process asserts the property that holds wherever the kill landed.
   Lease expiry is *not* one of those places — it is a replicated command, so no
   test here waits for a lease to lapse.
3. An ordinary crash of this store is not always recoverable by reopening. That
   is the design working, not a defect: refusing a slot that cannot be proved is
   recoverable under both readings of it, and adopting its partner is
   recoverable under only one. The cost is that a restart may need an operator's
   decision, so the process names it, the harness escalates only to the setting
   the process named, and the escalation is recorded rather than absorbed.
4. The lock store's slot files still have no lock of their own. One process per
   replica directory is enforced by the Raft store's lock and by opening it
   first.

## Production Composition Boundary

`lock-production-node` is one unpublished, production-shaped acceptance
composition beside the insecure integration process. It is deliberately not a
generic Rafter server.

The fixture adds caller-owned durable replica identity and a monotonic
per-group allocation mark; Rustls mutual peer authentication against a
dedicated principal map; envelope/channel identity agreement; durable
connection sessions and a 64-frame replay window; bounded peer frames, queues,
connections, client lines, and pending work; and JSON operations diagnostics.
It uses `FileRaftNodeStores` for publication/recovery correctness and the
transactional lock backend for application state. This does not relabel the
file store as a segmented high-throughput WAL.

Membership orchestration is explicit. A replacement receives a fresh identity,
joins as a learner, catches up through the leader's observed match index, and is
promoted through joint consensus. `LEAVE_JOINT` is a separate required step;
acceptance of the first step is never described as completion. Only after a
removal is observed committed does the caller retire the old identity.

Readiness opens only after identity and replay metadata verify, TLS is
configured, both durable stores recover, the control-plane checkpoint is
restored and reconciled, the application reaches its committed application
floor, and bounded workers are live. A listener that exists earlier returns a
typed not-ready refusal. Missing, corrupt, foreign, or contradictory metadata
fails closed.

The reviewed production process suite independently proves:

- authenticated three-process election and lock service;
- unauthenticated, unknown-certificate, and certificate/envelope mismatch
  refusal before Raft;
- duplicate, old-session, outside-window, restart, and removed-peer replay
  refusal with bounded durable state;
- clean checkpoint restart and refusal of missing or corrupt checkpoints;
- monotonic allocation, learner catch-up, committed removal, permanent
  retirement, and fresh-ID promotion;
- bounded client connection overflow with every admitted request receiving a
  terminal response; and
- stale-owner rejection by the guarded resource alongside the independent lock
  history checker.

The committed certificates are test-only and protect no external secret; the
CA signing key is not present. Client authentication, certificate issuance and
rotation, deployment control, general discovery, and signal management remain
embedding concerns.
