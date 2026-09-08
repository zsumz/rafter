# Architecture

This is the embedder's tour: what each layer owes the one above it, where
your code sits, and which rules are load-bearing. The README's
[Crates](../README.md#crates) table lists the packages; this document explains
the machine they form. The verification story has its own documents —
[raft-invariants.md](./raft-invariants.md) and
[model-checking.md](./model-checking.md) — and the architecture described
here is executable policy in [`zrail.toml`](../zrail.toml), not prose
convention.

## The shape

```txt
            your transport            your state machine
                 ▲  │                        ▲
                 │  ▼                        │
  ┌──────────────────────────────────────────────────────┐
  │  rafter-service / rafter-multiraft   (embeddings)    │
  │  ┌────────────────────────────────────────────────┐  │
  │  │  rafter-app          RaftGroup                 │  │
  │  │  ┌──────────────────────────────────────────┐  │  │
  │  │  │  rafter-runtime   DurableRaftNode        │  │  │
  │  │  │  ┌───────────────────┐  ┌──────────────┐ │  │  │
  │  │  │  │  rafter (kernel)  │  │rafter-storage│ │  │  │
  │  │  │  └───────────────────┘  └──────────────┘ │  │  │
  │  │  └──────── persist-before-output ───────────┘  │  │
  │  └────────────────────────────────────────────────┘  │
  └──────────────────────────────────────────────────────┘
```

Every layer is optional except the kernel. Embed at the lowest layer that
still leaves you owning what you need to own; each layer above adds
structure and takes nothing away — storage, transport, scheduling,
identity, and recovery policy remain the caller's at every altitude.

## The kernel: one step, explicit outputs

`rafter::Node` is a deterministic state machine and nothing else. It holds
Raft protocol state — term, vote, log, membership, replication and read
progress — and exposes one operation that matters:

```rust
let outputs: Vec<Output> = node.step(input);
```

An [`Input`] is logical time (`Tick`), a peer message, a client proposal, a
read request, or a membership change. The returned [`Output`] values are
everything that step produced, in order: messages to send, entries to
apply, state to persist, events to report. The kernel never performs IO,
never reads a clock, never spawns, and never retries — the
`kernel-sans-io` scope in `zrail.toml` and the source-boundary guard tests
make that a build failure rather than a review comment. Drive it from a
simulation and every run is bit-identical; drive it from production and
the same transitions hold. That single property is what the model checker
and the simulator lean on, and it is why the kernel can be checked at all.

Two consequences worth internalizing before embedding:

- **Logical time is an input.** The kernel counts ticks; you decide what a
  tick is. Election timeouts and heartbeats are functions of your tick
  cadence, not of wall clocks the kernel consults.
- **Outputs are obligations, not suggestions.** Each output names something
  the embedding must do — send this, persist this, apply this. Dropping an
  output is a correctness bug in the embedding, not a tuning choice.

## Persist-before-output: the one ordering rule

Between the kernel and everything durable sits a single trait,
`rafter_runtime_api::PersistedRaftRuntime`, and a single rule: **outputs
are released only after their durability obligations have completed.** A
vote is not sent until the vote is durable; an append acknowledgment does
not leave until the entries it acknowledges are on disk; an apply is not
handed to the state machine until the log that justifies it is persisted.
Callers above the boundary may therefore act on returned outputs without
adding their own durability fence — the fence already happened.

`rafter-runtime-api` names that obligation and deliberately nothing else:
no medium, no layout, no execution model. `rafter-runtime`'s
`DurableRaftNode` is the shipped implementation — it composes the kernel
with `rafter-storage`'s hard-state, log-segment, and snapshot stores, and
its recovery paths rebuild a node from those stores while refusing images
that bootstrap validation cannot vouch for. Bring your own runtime by
implementing the trait; everything above it neither knows nor cares.

`rafter-storage` is deliberately beneath the wire: its disk formats and
`rafter-codec`'s peer-message format evolve independently, and a
`[[dependency]]` rule in the architecture contract keeps either from
borrowing the other.

## The application layer: a group that owns nothing but order

`rafter_app::RaftGroup` binds one durable node to one
`ReplicatedStateMachine` — yours — and takes over the bookkeeping every
embedding otherwise reinvents: correlating proposals to their eventual
fates, running read barriers, surfacing membership events losslessly, and
poisoning the group on the failures that must not be stepped past. It is
synchronous and runtime-agnostic (the `application-io-free` scope holds it
to that): `step` drives the durable runtime, applies committed commands to
your state machine, validates the apply results, and returns a report with
outbound messages, completed application results, and lifecycle events. Route
the messages and consume the results; `report.applied` describes work already
done and must not be applied again.

The group layer's obligations are stated where they live: proposal fate
reporting in `group/proposal.rs`, read consistency in `group/read.rs`,
poison policy in `group/poison.rs`. If you read one module contract before
embedding, read `group/mod.rs`.

## Service and multiraft: structure for the async edge

`rafter-service` wraps groups in an async handle and defines the transport
*traits* — who may deliver a peer envelope, what an authenticated peer
identity is, how checkpoints merge across a transport handover. Its driver
is one state machine deliberately split across focused files; every file
says which slice it owns. `rafter-multiraft` hosts many caller-defined
groups behind one managed scheduler whose policy boundary is enforced —
the scheduler cannot see consumer schemas, retention policy, or
authentication policy, and the sharded-counter reference consumer drives
it through deterministic 64-to-4,096-group profiles as proof.

`rafter-transport-tcp-insecure` exists for examples, tests, and local
clusters, and says so in its name. `rafter-transport-tls` is the
authenticated production transport: rustls-based sessions, durable
connection identity, bounded receive memory, fail-closed session state.
Neither decides retry policy or peer membership — transports move
envelopes for identities the embedding has already authorized.

## Recovery and snapshots

Recovery is validation, not optimism. Bootstrap refuses images whose
snapshot boundary, membership, or writer cannot be justified
(`node/bootstrap/error.rs` enumerates every refusal), and the local
snapshot install path checks every claim a descriptor makes before
compacting anything — the full contract is on
`Node::install_local_snapshot`, and it is the best single page on how the
kernel treats authority. Streaming snapshot transfer is bounded and
chunked today; a public streaming interface remains additive future work
(see [work-completion.md](./work-completion.md)).

## Where behavior is proven

Nothing above is taken on faith. The 44-invariant catalog
([raft-invariants.md](./raft-invariants.md)) binds each claim to detector
tests replayed under an inventory whose identity is reviewed; the
simulator explores bounded failure schedules deterministically; Maelstrom
supplies sampled end-to-end evidence; and three reference consumers — a
ledger, a fenced lock, a sharded counter — prove the embedding story
against published crate archives, not the source tree. The layering,
source boundaries, size discipline, and capability ownership described
here are checked on every pull request by the zrail architecture contract
and the repository's guard tests. When this document and the contract
disagree, the contract is right and this document has a bug.
