# rafter-storage

Durable storage traits and a small, explicit file-backed reference
implementation for Rafter.

The crate owns three persistence domains:

- **hard state** — current term, vote, commit floor, and committed configuration
  identity;
- **log** — a contiguous retained suffix with durable suffix truncation and
  prefix compaction;
- **snapshots** — immutable snapshot envelopes, a manifest-selected current
  snapshot, and resumable inbound snapshot staging.

Each domain has an in-memory implementation for tests and volatile runtimes and
a synchronous file-backed implementation for durable nodes. The standard file
layout can be opened as one bundle with `FileRaftNodeStores`.

## Reference contracts

Two documents are normative for the current implementation:

- [`STORAGE_FORMAT_V1.md`](STORAGE_FORMAT_V1.md) specifies every versioned
  on-disk artifact byte for byte.
- [`DURABILITY_PROTOCOL.md`](DURABILITY_PROTOCOL.md) specifies publication
  order, commit points, crash residue, recovery behavior, and filesystem
  assumptions.

Exact v1 bytes are also pinned by the golden vectors under
[`tests/vectors/v1`](tests/vectors/v1).

## Reading the implementation

The source tree follows the durability model rather than collecting every store
in one orchestration file:

- [`src/format/v1`](src/format/v1) owns the exact version-1 byte grammars;
- [`src/raft_hard_state_store`](src/raft_hard_state_store) separates the public
  contract, errors, file publication, and in-memory behavior;
- [`src/raft_log_segment`](src/raft_log_segment) separates continuity, framing,
  replay and repair, logical mutation, and durable replacement;
- [`src/raft_snapshot_store`](src/raft_snapshot_store) separates validation,
  publication, payload sourcing, open-time verification, and pending-transfer
  recovery.

The top-level domain modules are declarative facades. Repository architecture
tests require module ownership contracts, scenario contracts, separate test
bodies, declarative facades, and tight facade-size budgets so these boundaries
cannot silently collapse back into mixed-responsibility files.

## Standard file layout

```text
<replica>/
├── .rafter-storage.lock
├── hard-state
├── log
├── log.compact
└── snapshots/
    ├── current.snapshot
    ├── snapshot-<sequence>-<index>-<term>-<writer>.rfsn
    ├── pending.snapshot-transfer
    └── pending.snapshot-transfer.body
```

`FileRaftNodeStores` acquires an exclusive operating-system lock through
`.rafter-storage.lock` before opening or repairing any store. The lock file is
persistent coordination metadata; the lock itself is released when the bundle,
or every store returned by `into_parts`, is dropped. Standalone file-store
constructors deliberately do not acquire this bundle lock, so custom embeddings
must provide equivalent exclusive ownership.

Temporary files may appear beside these artifacts after an interrupted write.
They are never selected as current state. See the durability protocol for the
precise recovery rule for each artifact.

## Observable snapshot opening

`FileRaftSnapshotStore::open_with_report` returns the opened store together with
nonfatal actions taken during restart. The report records directory creation,
durable discard of missing, short, or checksum-inconsistent optional staging,
and ignored unpublished body suffixes. Authoritative manifest or snapshot
corruption still fails the open.

Snapshot publication sequences use the complete `u64` domain without
saturation. Sequence `u64::MAX` may be published once; afterward writes return
`RaftSnapshotStoreWriteError::SnapshotSequenceExhausted` before creating a file.

## Integrity model

Every metadata envelope is versioned and protected by CRC-32/IEEE. Snapshot
files carry both a payload checksum and a whole-envelope checksum. These checks
catch torn writes, partial files, stale manifests, and accidental media
corruption in a non-Byzantine deployment. They are **not** authentication tags
or tamper evidence. Deployments with an adversarial-storage threat model must
authenticate storage below this crate or validate a stronger digest in the
application snapshot format.

## Integration boundary

Use this crate with `rafter-runtime` when opening durable Raft nodes. This crate
does not run Raft, apply committed entries, persist application state, choose
transport behavior, or decide when a recovered datastore may serve traffic.

Application state must maintain its own durable applied floor. Supply that
floor during runtime recovery so committed commands already reflected in the
application database are not emitted a second time.

The file-backed stores are a deliberately clear reference implementation, not
a segmented high-throughput WAL or a database engine. A production datastore
may implement the storage traits on top of its own transactional engine while
preserving the same success, ordering, and recovery contracts.
