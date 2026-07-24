# Replicated Ledger Contract

Status: first reference-consumer contract for Rafter 1.0 API discovery.

This crate begins as a dependency-free deterministic ledger. It does not use
Rafter yet. The next slice will integrate this exact application contract
through Rafter's public crates and record every seam that is missing, awkward,
or product-specific.

The ledger is deliberately small. It exists to prove:

- atomic application effects;
- bounded request deduplication;
- deterministic retries after unknown outcomes;
- snapshot-safe client sessions;
- linearizable queries once attached to Rafter; and
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

The Rafter adapter will serve queries only after an ordinary linearizable read
barrier. Query behavior is modeled here, but transport and read barriers are
not.

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

Aggregate invariants do not imply linearizability. The later process adapter
will record invocation, completion, rejection, unknown outcome, and real-time
ordering for an independent history checker.

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

The durable adapter will later define a versioned byte representation. Its
application transaction must atomically persist:

```text
account mutations
session and deduplication mutation
cached command result
applied Raft index
```

Compaction must never make an acknowledged command executable again.

## History Vocabulary

A client operation history contains:

```text
Invoked(operation_id, command)
Completed(operation_id, response)
Unknown(operation_id)
```

Deterministic rejections are normal completed responses. `Unknown` means the
caller cannot tell whether the replicated command committed and must retry the
same request identity.

## First Milestone Boundary

The first milestone contains:

- this contract;
- bounded command and result types;
- a pure deterministic ledger;
- a structurally independent oracle;
- snapshot round-trip and replay tests;
- differential exploration over small command histories; and
- the history vocabulary.

It intentionally contains no Rafter dependency, transport, filesystem backend,
shared reference framework, or new Rafter public API.
