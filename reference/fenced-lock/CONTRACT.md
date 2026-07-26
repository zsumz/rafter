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
shared reference framework, or new Rafter public API. Of those, three still
hold: there is no shared reference framework, no new Rafter public API, and no
real transport — the deterministic network the tests drive routes messages in
process, and the process composition remains deferred.
