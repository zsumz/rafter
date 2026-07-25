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

The transaction's commit point is the durability barrier that follows its
commit record. Applying a batch commits once, then returns its results, so the
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
- with the image whole and the commit record absent — the write-ahead window,
  which is the pre-transaction state because a transaction is committed by its
  commit record and by nothing else;
- after the commit point but before the reply is released; and
- during a snapshot install, which publishes by rewriting the journal and
  therefore commits at its rename.

Recovery discards an uncommitted tail before accepting another transaction, so
an append never follows abandoned bytes.

### Versioning and integrity

Every record carries a four-byte magic, a version byte, and a trailing
CRC-32/IEEE over its own preceding bytes; integers are unsigned and big-endian
and nothing is padded. A version this build cannot read is refused rather than
reinterpreted, and the journal header records the resource bounds it was
created under, so a journal cannot be reopened under bounds that would change
which images are valid.

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
Both close with the durable process-composition slice, which is also where the
production composition the program requires — authenticated transport, bounded
frames, readiness gating after complete recovery — is assembled.

Two limits are deliberate. The checker decides bounded histories only, and says
so rather than approximating. And the `NotCommitted` criterion is stated in
terms of what `rafter-app` reports, not in terms of what the driver happens to
know about its own network: a criterion the driver could only meet by privileged
observation would not survive the move to real processes.
