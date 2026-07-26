# Replicated Ledger Contract

Status: first reference-consumer contract for Rafter 1.0 API discovery.

This crate began as a dependency-free deterministic ledger. It now also carries
the `rafter-app` adapter that runs that exact application contract through
Rafter's public crates, plus a consumer-owned deterministic three-node driver
used by its integration tests. Every seam that is missing, awkward, or
product-specific is recorded as it is found.

The ledger is deliberately small. It exists to prove:

- atomic application effects;
- bounded request deduplication;
- deterministic retries after unknown outcomes;
- snapshot-safe client sessions;
- linearizable reads and mutations, checked as an ordering property over
  recorded histories rather than inferred from preserved aggregates; and
- agreement between an implementation and a structurally independent oracle.

## Resource Model

`LedgerConfig` fixes the maximum client slots and open accounts when a ledger is
created. A client ID addresses one configured slot, so session state is bounded
even when epochs are replaced repeatedly.

The first model uses numeric account and client identifiers and fixed-width
amounts. It has no user-controlled strings or byte payloads.

## Commands

```text
OpenSession(client_id, session_epoch)

Execute(
  client_id,
  session_epoch,
  sequence,
  mutation
)
```

Mutations are:

```text
OpenAccount(account_id)
Deposit(account_id, nonzero_amount)
Transfer(from, to, nonzero_amount)
CloseAccount(account_id)
```

Queries are not replicated mutations:

```text
GetAccount(account_id)
GetLedgerSummary
```

A query is application vocabulary rather than Rafter integration. Both the
implementation model and the oracle answer queries from their own state through
their own code, so a query has a specified answer independent of any adapter.
The Rafter adapter serves a query only after an ordinary linearizable read
barrier; the barrier is the adapter's, the answer is the model's.

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
exact bounded mutation
cached result
```

Clients may have at most one mutation outstanding. For `Execute`:

1. The client slot and exact session epoch must be active.
2. `highest + 1` is the only sequence that may execute.
3. Retrying `highest` with the exact same mutation returns the cached result
   without changing state.
4. Reusing `highest` with another mutation is a conflicting retry.
5. A lower sequence is stale.
6. A sequence above `highest + 1` is a gap.

When the highest sequence reaches its numeric maximum, the client must open a
greater session epoch before issuing another mutation.

Every next-sequence mutation consumes its sequence and caches its result,
including deterministic business rejections. This prevents a rejected request
from succeeding later when unrelated state changes.

Session, sequence, and conflicting-retry rejections do not consume a sequence.

## Ledger Semantics

- Accounts open with a zero balance.
- An account ID cannot be opened twice while it is present.
- Opening an account fails at the configured account bound.
- Deposits are the only external source of funds.
- Deposits fail if the account is absent or its balance would overflow.
- Transfers require two distinct open accounts and sufficient source funds.
- Transfers fail if the destination balance would overflow.
- Accounts close only at zero balance.
- A closed account ID may be opened again as a new zero-balance account.

Business rejections are client-visible deterministic results and are cached
under their accepted request identity.

## Invariants

The implementation and oracle must establish:

1. A transfer preserves total balance.
2. Total balance equals the sum of successful external deposits.
3. No account balance becomes negative.
4. An account closes only at zero balance.
5. An accepted request identity changes state at most once.
6. An exact retry returns the original result.
7. Conflicting request reuse never changes state.
8. An older session cannot act after a newer epoch opens.
9. Snapshot and restore preserve balances, sessions, cached mutations, cached
   results, deposit totals, and retry behavior.
10. Resource bounds fail closed without evicting live correctness state.

Aggregate invariants do not imply linearizability, and the two are checked
separately. The driver records invocation, completion, rejection, unknown
outcome, provable refusal, query result, and real-time ordering; the
[history checker](#linearizability) decides the ordering property over that
record.

## Independent Oracle Rule

The implementation and reference oracle share command, result, and inspection
types only.

They do not share:

- transition functions;
- validation helpers;
- session or sequence decision code;
- account mutation helpers;
- deduplication logic; or
- snapshot reconstruction.

The implementation uses ordered maps. The first oracle uses separate linear
collections and its own transition code so a shared implementation bug cannot
make both sides agree.

## Snapshot Contract

The pure model snapshot is transport-neutral and opaque outside the
implementation. Restoring it validates configured bounds, client-slot
ownership, uniqueness, and the ledger supply invariant.

The adapter defines the versioned byte representation. Its snapshot frame
carries the applied Raft index alongside account balances, active sessions,
cached mutations, and cached results, and installing one restores through the
model's validating restore path. An install whose payload index disagrees with
the declared index is refused, as is an install that would move the applied
index backwards.

The durable adapter's application transaction must atomically persist:

```text
account mutations
session and deduplication mutation
cached command result
applied Raft index
```

Compaction must never make an acknowledged command executable again. The
adapter enforces the same rule against replay: a committed entry at or below
the applied index is refused rather than applied a second time.

The durable store enforces it once more, at the one publication the applied
index cannot judge. A rewrite — which is how both an install and a compaction
publish — may republish the applied index the store already holds, because
compacting in place must not require inventing a new one. **The applied Raft
index is not the whole ordering key for the deduplication cache**: two images
can name one index and still disagree about which requests have completed, so a
rewrite at an unchanged index must also dominate the durable cache, client slot
by client slot — the session epoch first, then the highest completed sequence
under it. Replacing a session epoch is what legitimately clears an older epoch's
cache, which is why the epoch outranks the sequence rather than sitting beside
it.

The check is scoped to an unchanged index deliberately. Above it the model has
legitimately advanced and is the authority on the sessions it replaced along the
way; at an unchanged index nothing legitimately changed, so a state that lost a
completion is a poorer image of the same commit point and is refused before a
byte is written.

## Durable Backend

The durable adapter's backend is a consumer-written store over one journal
file. It is not a Rafter API and never will be: where an application's state
lives is application policy, and the only thing Rafter's contract asks of it is
that a returned apply be recoverable.

Two state machines serve this application. They differ in where state lives and
in nothing else. Every ledger, session, and deduplication decision is the pure
model's, and the applied-index, read-barrier, and snapshot-admission rules are
shared code rather than two implementations that agree today.

### Transaction boundary

One transaction carries the whole application state at one applied index. The
four facts the contract lists are not assembled from separate writes: they are
encoded into one image, and that image is the unit the journal commits.

The transaction's commit point is the durability barrier that follows the
promotion of its append mark — the one byte written after the whole frame,
including its commit record, is already durable. Applying a batch commits once,
then returns its results, so the
results a caller replies with have already survived the crash they describe.
The interval between that commit point and the reply is a real window, and the
contract's answer to it is the deduplication cache: the caller retries the same
request identity and the cached result answers it.

`Ok` means the new state is visible to a fresh opener. An error means the
outcome is unknown; reopening decides it, and no caller may infer from an error
that no bytes changed. A store whose publication failed refuses every later
transaction until it is reopened.

### Crash windows

A crash at any byte boundary leaves the store recoverable to exactly the
pre-transaction or the post-transaction state, never between:

- before the transaction emits a byte;
- part-way through its begin record, its image, or its commit record;
- with the whole frame durable and its append mark not yet promoted — the
  write-ahead window, which is the pre-transaction state because a transaction
  is committed by the promotion of that mark and by nothing else, and which is
  the one boundary opening will not resolve on its own: those bytes are also
  exactly what a committed frame whose mark byte later rotted leaves, so opening
  refuses and the named repair entry point resolves it to the pre-transaction
  state for a caller who has decided which happened;
- after the commit point but before the reply is released; and
- during a snapshot install, which publishes by rewriting the journal and
  therefore commits at its rename.

Recovery discards an uncommitted tail before accepting another transaction, so
an append never follows abandoned bytes.

### What opening may discard

**An image of committed state this build cannot read is never treated as
absent.** Unreadable bytes are not residue to step over and not a reason to open
at a shorter history and carry on: they are a refusal to open, reported to the
caller, who is the only party that can decide what to do about them.

That principle has teeth here because recovery walks frames forward and stops at
the first one it cannot read. Where it stopped is *not* evidence about the bytes
beyond it. A frame in the middle of a long journal can become unreadable, and
every frame after it is then unreachable too — whole, correctly sealed,
acknowledged transactions included — because a frame's offset is only knowable
through the frame before it.

So opening may discard only residue it can *prove* no commit point covered.
**That proof rests on a mark the append wrote, and it is not that mark alone**,
and the difference is the whole of this section. The append writes the half no
reader could infer — every interrupted append leaves the frame unsealed — and
the reader supplies the other half by re-reading the tail with the mark restored
and asking whether these bytes are a whole frame.

An enumeration of what an interrupted append leaves is not that proof. Such a
list can be complete — fewer bytes than a begin record, a partial image, a whole
image with no commit record, a partial commit record really are all an
interrupted append can leave — and still be useless in the direction a reader
needs it, because the reader is asking the converse: does *this* shape mean an
append was interrupted? It does not. A journal that has lost its last byte ends
in a partial commit record. It is a strict prefix of a frame this build wrote,
it carries this store's magic and version, and no checksum over the bytes
present fails. It matches the enumeration exactly, and the frame it ends in was
committed and acknowledged; truncating it deletes a whole transaction during an
operation the caller asked for as a read. Nor can any test on the bytes separate
the two, because the two are the same bytes.

The same argument ran backwards at the other end. A tail of zero bytes — what a
crash leaves when a file's size reached the medium and its data did not — was
benign below a begin record's length and corruption at or above it, because that
is where the zeros began failing a magic test. One kind of residue, two opposite
verdicts, decided by how many bytes happened to land, and neither verdict a
statement about whether the bytes were committed.

So an append marks its frame as unfinished while it is unfinished. **The first
byte of a frame is its append mark**: held at a value no sealed frame carries
while the frame is being written, and promoted to the sealed value by a single
byte written only after every other byte of that frame is durable. A crash
leaves a prefix of what was written and that byte goes out first, so every
interrupted append leaves the mark unsealed.

**The identity is asked before the mark.** The sentence that used to stand here
— the unsealed value is zero, so the ordinary residue of a delayed allocation
reads as exactly what it is, at every length — was true of the residue it was
written about, and claimed a scope one step wider than the mechanism reached: it
made zeros landing over a *committed* frame read as residue too. One zeroed byte
was the single fault the next rule covers and was refused. **Two** adjacent zero
bytes — one 16-bit word, far under a sector, one physical event and not two —
destroyed the begin magic, which was consulted only *below* the mark test, and
the frame plus every committed frame after it was deleted during a read.

So bytes one through three of the begin magic are read first, at every length,
above the mark. They are the frame's identity, they are not the mark, and no
append leaves them wrong: the append writes the whole begin record with byte
zero held unsealed, so the first write that reaches byte one carries the magic.
A tail failing that test is refused and never truncated. The sibling fenced-lock
consumer has read its magic above its mark since the generation that put it
there; the two stores now ask the same questions in the same order, and a unit
test in each pins the order as a table naming the other, because arguing it in
prose about one byte is how they drifted apart on the next one.

One residue has to survive that test, and it is named rather than smuggled
through: a tail that is **zeros all the way to the end of the file** is
truncated, because that is what a crash leaves when a file's size reached the
medium and its data did not, and refusing it would need an operator after the
most ordinary crash there is. That rule rests on a claim about the physical
world rather than about this program, so its limit is stated with it: a
committed frame that is both the *last* frame and entirely zeroed is discarded
and its transactions are lost. What the rule guarantees instead is the bound —
the loss can never reach a byte that is not itself zero, and never a frame
beyond the damage. Every zero run with a single non-zero byte anywhere after it,
which is every zero run with a committed frame behind it, fails the identity
test and refuses.

**The mark is half of the rule and not the rule.** Reading it as the whole rule
is how this section was wrong the second time. `interrupted ⇒ unsealed` is what
the paragraph above proves; its contrapositive is `sealed ⇒ not interrupted`;
and truncating on an unsealed mark needs neither of those but `unsealed ⇒ was
being written`, which is a third statement, and false. One byte shows it: the
sealed mark is `0x52`, the unsealed value is `0x00`, and a committed frame whose
first byte rots between them reads as an interrupted append. Opening truncated
from there to the end of the file, returned success, and reported the deleted
transactions as bytes no commit point ever covered — while every *other* byte of
the same begin record, being under a checksum, refused the store. The mark byte
was the only byte no checksum was ever consulted for, because the mark test
returned first.

So truncating now requires the unsealed mark **and** positive evidence that the
bytes are not a whole frame. Opening reads the tail a second time with the mark
restored to its sealed value — the value every checksum in a frame is computed
over — and truncates only what still fails to be a whole frame at a step this
build can read. Three outcomes, three different facts:

- **not a whole frame**: too short for a begin record, a begin record that does
  not verify, a partial or mismatched image, a missing or partial commit record.
  With the unsealed mark, that is residue, and it is truncated. This is the
  ordinary crash residue. A zero-filled tail is truncated too, but by the rule
  above rather than by this one: the two are separate report variants because
  they rest on separate premises and fail in separate places, and a single
  predicate carrying both proofs is how a rule's scope drifts past its
  mechanism.
- **a whole frame that verifies**, with only the mark reading unsealed. Two
  histories leave exactly these bytes — the write-ahead window, and a committed
  frame whose mark rotted — and nothing in the bytes separates them. Opening
  refuses. Refusing is recoverable under both readings and truncating is
  recoverable under only one, and the choice is made on that asymmetry rather
  than on a guess about which history is likelier.
- **a version this build cannot read**, which stops the second reading before it
  can say anything at all.

Every shape with a *sealed* mark is what some completed append sealed, and any
damage to it happened afterwards — a sealed frame cut short, a begin record that
does not verify, an image that does not match its checksum, a commit record that
seals nothing. Each may sit at or below the last commit point and refuses the
store. So does a tail carrying neither frame mark or a foreign begin magic,
which is refused above all of this rather than within it.

That the mark byte is now no weaker than its neighbours is a claim about every
byte of a frame, so it is checked as one: a unit test alters every byte of a
sealed frame to every other value it could take and requires that none of the
results is residue. A rule this narrow is exactly the kind that decays quietly,
and a paragraph would not have noticed.

Destructive recovery remains available and is a separate, named entry point. It
discards from the unreadable frame to the end of the file and reports the
offset, the corruption, and the byte count. It cannot report how many
transactions were lost — frames past a corrupt one cannot be located, let alone
counted — and that unknowable number is exactly why discarding them has to be
something a caller asks for rather than something a read does quietly.

One refusal is deliberately outside its reach: a frame declaring a format
version this build cannot read, whether its mark is sealed or not. That needs no
corruption at all — a newer build appending over a header this one still reads
produces it from healthy bytes — so it is a newer build's committed work rather
than damage, and it is refused by both entry points under its own name. Letting
the remedy for damage also clear it would make the documented answer to "this
will not open" a way to delete committed work.

That order has a cost, and the cost is named rather than left to be discovered.
The version byte is read before the checksum that covers it, so a single altered
version byte makes a journal unopenable by either entry point. That is a refusal
and not a loss — every byte is still on the medium — and the alternative trades
it for a repair that can delete a newer build's committed work, which is the
worse of the two. The same is true of the journal header's own version byte.

The same principle covers a journal too short to hold its header. Creation is a
rename, so an interrupted creation leaves a staging file and never a headerless
journal; a headerless journal is therefore something else's doing, it is
unreadable rather than absent, and it is refused rather than re-created.

Creating the journal is reported too, and counts against a clean opening.
Nothing inside a durable store can tell a replica that has never run from one
whose journal was deleted — both arrive as an absent file — so the store states
the fact and the caller, which knows whether this replica has run before, judges
it. A creation report that no caller reads is a creation report that costs
nothing to be wrong.

### Directory residue

Opening removes the one staging name this store writes, and nothing else beside
the journal.

The rule used to be wider: anything whose name began with the journal's name and
a dot, on the reasoning that a staging file is always some dead process's work
and the widest rule leaks the least. That had the direction of its own proof
backwards in the same way the tail classifier did. Every staging file this store
writes matches the prefix; matching the prefix does not make a file one. When
the journal will not open, the process tells an operator to run a repair, the
obvious first move is to copy the journal aside, and the obvious name for the
copy begins with the journal's name and a dot — so opening the store deleted the
backup its own instructions invited, and reported it as one boolean. Leaking is
the smaller failure: a file this store cannot have written is somebody's
evidence. What was removed is now reported by size as well as by fact.

Removing this store's own staging file is safe because there is no other writer.
That comes from the ownership discipline below — the Raft store's lock, taken
before the journal is opened and held for the life of the process — and not from
the staging name, which defends nothing on its own.

### Versioning and integrity

Every record carries a four-byte magic, a version byte, and a trailing
CRC-32/IEEE over its own preceding bytes; integers are unsigned and big-endian
and nothing is padded. A version this build cannot read is refused rather than
reinterpreted, wherever the field is present and whatever else is missing, and
the journal header records the resource bounds it was created under, so a
journal cannot be reopened under bounds that would change which images are
valid.

A frame's magic carries the append mark in its first byte, so the mark costs no
extra field: a sealed frame is byte-for-byte what it would be without it, and
every checksum is computed over the sealed form. That last detail is what makes
the mark checkable rather than merely present. Restoring the mark to its sealed
value and re-reading is a well-defined question with a checksum behind it, which
is how recovery tells a whole frame with a rotted mark from a frame an append
never finished, and it is why the mark needed no format change to stop being the
one unprotected byte.

A frame carries three checksums, and each answers a different question: the
begin record's own checksum makes the image length safe to trust, the image
checksum detects a torn or corrupt image, and the commit record's frame
checksum binds that commit record to that begin record and that image, so a
record surviving from an abandoned tail cannot seal a frame it never covered.
The checksums detect accidental corruption and torn writes; they are not
authentication tags.

Recovery decodes a committed image through the model's validating restore path,
so an image whose checksums verify still cannot produce a ledger that violates
a resource or supply invariant.

A recovery report is evidence a caller asserts on rather than a diagnostic. It
records what opening found and did — the frames it replayed, the residue it
truncated, the journal it created, the staging file it swept and how large that
was, and, for a repair, exactly what was discarded — and a reopen that reports
any of those is a fact to be looked at rather than stepped over.

### What the crash tests establish

The crash tests interrupt real publications at named byte boundaries, reopen
the store, and compare the whole recovered state — balances, sessions, cached
mutations, cached results, deposit total, and applied index together — against
the two states the transaction sits between. Comparing them as one value is how
"atomically" is asserted: there is no way to be equal on the effects and not on
the cached result, or on the data and not on the applied index.

They also establish that recovery followed by replay from the recovered applied
index reconstructs the uninterrupted run, checked against the independent
oracle rather than against the implementation.

Their limit is stated rather than implied: interrupting a publication inside a
live process proves which bytes reached the file and what a fresh opener makes
of them. It does not prove that a durability barrier reached the medium, and
removing one leaves the suite green.

## Process Composition

The ledger runs as one operating-system process per replica. This is the
**integration** composition level defined in
[`docs/reference-consumers.md`](../../docs/reference-consumers.md), and this
section says exactly what that level establishes and exactly what it does not.
No claim below closes the 1.0 production-composition criterion.

### The replica process

One process owns one replica directory:

```text
<cluster-dir>/node-<id>/
  raft/          Rafter's file-backed hard state, log, and snapshots
  app/           the ledger journal
  peer.addr      this replica's advertised peer address
```

Startup order is part of the contract, not an implementation detail:

1. bind the client port and announce it, refusing service;
2. acquire exclusive ownership of the replica directory;
3. open the durable application store and read its applied floor;
4. recover the Raft runtime *through that floor* and consume the recovery
   outputs before anything else touches the group; then
5. announce readiness and begin serving.

Step 1 precedes step 2 deliberately. A replica that cannot yet recover is
reachable and says so, rather than being indistinguishable from one that is not
running.

### Ownership

Two live processes over one replica directory would interleave publications and
corrupt each other. The invariant is that one process owns one directory, and
it is enforced by the operating-system lock `rafter-storage` takes over the
Raft store directory, acquired before the application journal is opened and
held for the life of the process.

The ledger journal has no lock of its own, so this invariant rests on that
ordering rather than on the journal defending itself. A second process is
refused at the Raft store and therefore never reaches the journal it would have
corrupted. A replica that finds its directory owned waits for a bounded period
rather than failing immediately, because a restarting replica legitimately
races the exit of the incarnation it replaces.

### Readiness

A replica refuses every client operation until it has applied every command it
knows to be committed — the promoted committed-application-index floor, never
the commit index, because elections and membership changes commit entries the
application is never told about. Status is reported whether or not the gate is
open, so readiness is observable while it is closed.

Readiness means recovery finished. It does **not** mean the replica is current
with its cluster: a replica that recovers a durable floor below the cluster's
committed index is ready, and its local reads may be behind until it catches
up. Anything routing on readiness must understand it as a recovery signal, not
a freshness one.

The readiness gate is also where a journal that will not open stops the
replica. An application store holding a region this build cannot read reports a
distinct startup error, and the replica announces it and refuses to serve rather
than opening at whatever shorter history it can reach — that history may be
missing transactions this replica already answered a client with. Restarting
never clears it. Discarding the region takes an explicit option, and the replica
announces what the discard cost.

Recovery reports are consumed rather than produced. A replica announces residue
it recovered from, announces separately that it had to create the journal, and
refuses on the report that needs a human; the drivers that reopen a store in the
test suites assert that a replica no scenario interrupted came back with nothing
to report, and that a replica creates its journal on its first opening and no
other. A report nothing reads is a report that costs nothing to be wrong.

The creation announcement is the one that needs the supervisor's own knowledge
beside it. On a replica's first boot it is expected; on a restart it means the
journal is gone and the replica is about to serve an empty ledger from applied
index zero. The store cannot distinguish those, so it reports and the supervisor
judges.

### The link between replicas

Peer messages travel over TCP in a consumer-owned frame format. Both the
transport and its encoding are deployment policy: Rafter's contract asks a
transport to deliver a message value to a peer and says nothing about the bytes
in between.

The encoding is consumer-owned rather than borrowed from Rafter's demo TCP
transport for a boundary reason. That crate and `rafter-codec` are outside the
set of Rafter crates the source-mode dependency override patches, so a consumer
naming them would resolve two Rafter crates from the registry while resolving
the rest from the checkout — the partially patched graph the program document
warns about. The two structurally awkward payloads are not hand-written: log
entries and snapshot metadata are encoded through `rafter-storage`'s published
codecs, so no membership encoding is ever re-derived here.

The link bounds what it can: a maximum frame length refused before its bytes
are read, a bounded outbound queue per peer whose overflow drops frames, and
deadlines on connect and write. Dropping is correct rather than lossy — Raft
tolerates loss, reordering, and duplication, while a blocked send would convert
one slow peer into a stalled replica.

It authenticates nothing. It does not prove sender identity, prevent replay, or
fence a removed member, and a message's claimed sender is the field inside the
message rather than anything the connection established.

### The client protocol

Clients speak a line-oriented protocol over a second port. It carries the three
terminal mutation outcomes intact across the process boundary, and the
distinction between them is the same one this contract draws everywhere else:

- a replicated response, with its disposition;
- a provable refusal, emitted only when the application layer reported the
  local node refusing the proposal before replication, or when the readiness
  gate refused before the command was handed to `rafter-app` at all; and
- an unknown outcome, which is everything else, including the answer a killed
  replica never sent.

The protocol authenticates nothing and identifies no client. A client identity
in this ledger is a bounded slot number in the replicated state machine, which
is deduplication vocabulary and not a principal.

### What the process suite establishes

Real processes are killed with `SIGKILL` and restarted from their own durable
stores. The suite establishes that:

- three processes elect a leader and serve the ledger over real sockets;
- a session retry after the leader is killed returns the cached result rather
  than executing again;
- a write lost to a dying leader takes effect exactly once once its identity is
  retried, under every reading of where the kill landed;
- a replica whose journal holds an unreadable region refuses to serve, says so
  in a line a supervisor can match on, and is not talked round by a restart;
  repairing it is a separate, explicit run that reports what it discarded;
- a replica killed mid-write recovers a durable applied floor from its own
  journal and catches up past it;
- readiness gates: a replica that cannot complete recovery refuses to
  replicate and refuses to read, and answers the same request once recovery
  completes; and
- nothing acknowledged before a cluster-wide kill executes a second time after
  it.

Every history is recorded in this contract's vocabulary and checked by the same
black-box linearizability checker the deterministic suites use.

### What it deliberately does not establish

- **Transport security.** Nothing is authenticated, encrypted, replay
  protected, or fenced.
- **Authenticated identity.** Neither a peer nor a client proves who it is.
- **Persisted replica identity.** A replica's identity is an argument and a
  directory name, not a durable, verifiable fact.
- **Discovery.** Peers find each other through a file in a shared directory.
- **Structured metrics and diagnostics.** Lifecycle is a handful of stdout
  lines.
- **Signal handling.** Clean shutdown is a client command. There is no signal
  handler, because installing one from `std` alone is not possible and this
  workspace takes no external dependency; `SIGTERM` and `SIGKILL` both
  terminate abruptly, which the store's crash contract already covers.
- **Durability barriers reaching the medium.** Killing a process proves what a
  fresh opener makes of the bytes that reached the file. It does not prove that
  a barrier reached the disk.

## History Vocabulary

A client operation history contains:

```text
Invoked(operation_id, command)
Completed(operation_id, response)
Unknown(operation_id)
NotCommitted(operation_id)

QueryInvoked(operation_id, query)
QueryCompleted(operation_id, result)
QueryAbandoned(operation_id)
```

Position in the recorded sequence is the real-time order. An operation whose
terminal event precedes another operation's invocation happened before it; two
operations whose intervals overlap are concurrent and may be ordered either way.
Every operation contributes exactly one invocation event and exactly one
terminal event, correlated by `operation_id`. An operation that is invoked and
never reaches a terminal event is a recorder defect, not a representable
outcome: a caller that is still waiting is recorded as `Unknown`, and a query
that never answered is recorded as `QueryAbandoned`.

The vocabulary is closed and in-memory. It is deliberately not a wire format —
the versioned frames this contract defines are the replicated command and
snapshot frames — so adding a terminal outcome is a change to this document
rather than a compatibility negotiation.

Deterministic rejections are normal completed responses.

### Mutation outcomes

The three mutation outcomes differ only in what the caller can prove:

- `Completed` carries the replicated response.
- `NotCommitted` means the command provably never entered the replicated log.
  No copy of that attempt can commit later, so its request identity is still
  unused and the caller may issue a fresh attempt.
- `Unknown` means the caller cannot tell. The command may have committed, so
  the caller must retry the *same* request identity and let the session cache
  decide.

A refusal is provable only when the application layer reports it as a proposal
rejection — `ProposalBegin::Rejected` or the equivalent `ProposalEvent::Rejected`
for the same local proposal. `rafter-app` documents that event as the local node
refusing the proposal before replication, and Rafter emits it only from the
pre-append admission check: leadership, pending leadership transfer, and payload
size are all decided before the entry is appended. The command therefore never
entered this node's log and was never sent to a peer, and no other node holds a
copy of that attempt because only this node ever had the bytes.

Every other lost outcome stays `Unknown`, including all of the following, none
of which the caller can distinguish from a commit:

- `ProposalEvent::UnknownOutcome`, whatever its diagnostic reason. A dropped
  local proposal may already have replicated to a quorum.
- A proposal that was accepted and appended, after which the caller stopped
  waiting. The entry exists and may yet commit.
- Any outcome lost to process or connection failure.

The distinction is worth drawing because `Unknown` is the weaker claim: a
checker must allow an `Unknown` operation to have taken effect, so an
implementation that wrongly applied a refused command would be explained away.
`NotCommitted` removes that excuse.

### Query outcomes

Queries are linearizable operations in the history, not observations outside it.
A `QueryCompleted` result must be explained by the same ordering that explains
every mutation around it.

`QueryAbandoned` records a query that returned no value to the caller. A refused
barrier, a barrier canceled by leadership loss, and a caller that stopped
waiting are deliberately not distinguished: none of them delivered a result, and
a query that returned nothing constrains no ordering. The event is retained
because an issued query that answered nothing is evidence about availability
even when it is not evidence about correctness.

## Linearizability

Every recorded history must admit a legal real-time ordering: a total order over
its operations such that

1. the order respects real time — if one operation's terminal event precedes
   another's invocation, it comes first;
2. running that order through the ledger's sequential specification produces
   exactly the response each `Completed` operation observed and exactly the
   result each `QueryCompleted` operation observed;
3. each `Unknown` mutation appears in the order, or does not, whichever admits
   an explanation — both readings are permitted because both are consistent
   with what the caller saw; and
4. each `NotCommitted` mutation does not appear in the order at all, and each
   `QueryAbandoned` query is likewise absent.

The sequential specification is the independent oracle, never the implementation
model. A checker that replayed the implementation would agree with it by
construction.

Application invariants and linearizability remain separate checks. Total balance
equalling total deposits holds for orderings that never happened; it says
nothing about whether the observed operations can be arranged in real time at
all. Both are asserted.

The checker is a bounded decision procedure, so it also has to say when it will
not decide. It refuses a history that needs more operations ordered than its
declared bound, and it refuses a search that exceeds its configuration budget.
Both are refusals, reported as undecided; neither silently checks a truncated
history. Operations settled without searching — a `NotCommitted` mutation, a
`QueryAbandoned` query — do not count against the operation bound, because
removing them is exact rather than approximate: no ordering constraint between
two remaining operations passes through a removed one.

## First Milestone Boundary

The first milestone contains:

- this contract;
- bounded command and result types;
- a pure deterministic ledger;
- a structurally independent oracle;
- snapshot round-trip and replay tests;
- differential exploration over small command histories; and
- the history vocabulary.

It intentionally contains no transport, filesystem backend, shared reference
framework, or new Rafter public API.

## Adapter Boundary

The adapter slice adds:

- the `rafter-app` replicated state machine over the pure model, with versioned
  command and snapshot frames;
- the linearizable `GetAccount` and `GetLedgerSummary` read path;
- a consumer-owned deterministic three-node driver in test support; and
- integration tests that drive the session protocol, a leader change with an
  unknown-outcome window, linearizable reads, restart, and a snapshot round
  trip through real replication.

The adapter adapts the implementation model only. It never touches the oracle,
never re-derives a ledger, session, or deduplication decision, and depends only
on published Rafter crates. It still contains no transport, no filesystem
backend, no shared reference framework, and no new Rafter public API.

The driver still models one thing it does not make real: durable Raft state
lives in shared in-memory media that outlive one node incarnation. Durable
process composition arrives with the last slice.

## History Checker Boundary

The checker slice closes the two gaps the adapter slice deferred: queries were
absent from the recorded history, and a provable refusal was recorded as an
unknown outcome. It adds:

- query invocation and completion events, plus the `NotCommitted` terminal
  outcome and its observable criterion;
- a black-box linearizability checker over recorded histories, using the oracle
  as its sequential specification and sharing no transition code with the
  implementation model;
- driver support for starting an operation without waiting for it, so recorded
  histories contain genuinely overlapping operations rather than a serial
  transcript; and
- seeded workloads that record queries, unknown outcomes, and refusals, checked
  both for model agreement and for linearizability.

The checker reads only the history. It never inspects replicas, logs, applied
indexes, or the implementation model, so an external user recording the same
client-visible events could run the same check.

## Durable Backend Boundary

The durable slice makes the application's state real. It adds:

- the transactional store described under [Durable Backend](#durable-backend),
  with its versioned journal, its checksums, its typed failures, and its
  deterministic per-store fault seam;
- a second `rafter-app` state machine over that store, sharing the pure model
  and every applied-index and snapshot rule with the in-memory one;
- crash-point tests over every boundary the transaction has, including a sweep
  of every byte of one transaction's frame; and
- a driver that opens each replica's application through a factory, so a
  restart reopens a journal rather than handing back a value, and a replica
  whose durable apply failed is treated as the dead process it is.

Two limits stay. Raft's own durable state is still in-memory media the driver
hands between incarnations, so this is application durability rather than
process durability. And the crash tests interrupt publications inside one live
process, which cannot establish that a durability barrier reached the medium.

The first of those closes with [Process Composition](#process-composition): a
replica is an operating-system process over file-backed Raft stores, and a
restart is a new process reading both durable stores back from disk. The second
does not close there and does not close anywhere in this crate: killing a
process proves that the bytes which reached the file are recovered correctly,
which is a stronger statement than the in-process crash tests make but still
not a statement about the medium.

Two limits are deliberate. The checker decides bounded histories only, and says
so rather than approximating. And the `NotCommitted` criterion is stated in
terms of what `rafter-app` reports, not in terms of what the driver happens to
know about its own network: a criterion the driver could only meet by privileged
observation would not survive the move to real processes.

## Process Composition Boundary

The process slice makes a replica a process. It adds:

- a `ledger-node` binary configured entirely by arguments and environment
  variables, following the startup order under
  [Process Composition](#process-composition);
- a consumer-owned TCP peer link and peer frame format, reusing
  `rafter-storage`'s published codecs for log entries and snapshot metadata;
- a line-oriented client protocol that preserves this contract's three terminal
  mutation outcomes across the process boundary;
- process orchestration in test support that spawns, kills, and restarts real
  processes with per-test scratch directories and bounded predicate waits; and
- a process suite that kills replicas with `SIGKILL` and checks the resulting
  histories with the same black-box checker.

That criterion the previous slice worried about held. `NotCommitted` is still
decided by what the application layer reports, and it crossed the process
boundary without needing anything the client could not observe.

The suite is labelled integration evidence, and it is `#[ignore]`d by default:
`docs/reference-consumers.md` puts durable process tests in the main and
nightly lanes, while the every-PR lane wants a package build and the
deterministic suites. Both dependency modes still compile the binary and the
suite, so a consumer that stopped building is caught everywhere; only running
it is deferred.

Three limits stay, and none of them is an oversight:

1. The composition is the integration level. Everything under [what it
   deliberately does not establish](#what-it-deliberately-does-not-establish)
   remains open, and the production-composition criterion is untouched.
2. Real time is load bearing in a way the deterministic driver never allowed.
   The suite bounds it rather than eliminating it: every wait is a polled
   predicate against a deadline, no test sleeps and assumes, and every test
   that kills a process asserts the property that holds wherever the kill
   landed. What real time did surface is worth recording — leadership genuinely
   changes under load even with a surviving quorum, and pre-vote leader
   stickiness means the survivor with the *longest* election timeout wins a
   failover rather than the shortest.
3. The ledger journal still has no lock of its own. One process per replica
   directory is enforced by the Raft store's lock and by opening it first.
