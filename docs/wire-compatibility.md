# Peer-wire compatibility qualification

`scripts/wire-compatibility-check` compiles the same independent public-API
consumer against published `0.0.2-alpha.1` archives and the candidate archives.
Each binary writes frames that both binaries must decode to identical expected
message values and re-encode to identical bytes. The two writers must also
produce the same canonical RFPM v1 bytes. Checksums identify the actual
archives even while both sets carry the same alpha version.

## Covered messages

The fixture covers all nine encodable top-level message kinds with 22 cases:

| Shape | Cases |
| --- | --- |
| RequestVote | Zero log position and wide `u64` term, node and index values. |
| RequestVoteResponse | Grant and refusal. |
| PreVote / PreVoteResponse | Request, grant and refusal. |
| TimeoutNow | Leader identity and term. |
| AppendEntries | Empty heartbeat; mixed batch containing empty, binary and 64 KiB application payloads, a no-op, stable membership and joint membership. Includes a wide replication sequence number. |
| AppendEntriesResponse | Success and refusal, preserving sequence and match index. |
| InstallSnapshotChunk | Partial and final 64 KiB chunks, each with absent, stable or joint committed membership. Preserves descriptor fields, transfer identity, checksum and offset. |
| InstallSnapshotResponse | Success and refusal, with and without transfer identity. |

The consumer matches the public message enum exhaustively, so a new variant
requires an explicit qualification decision. It also checks every existing
wire tag and the exact case inventory. It uses no codec internals or test hooks.

Both binaries must return the corresponding typed error for a future version,
reserved tag 5, invalid magic, checksum corruption, truncation, trailing bytes
and an invalid boolean. Structural mutations carry recomputed checksums where
appropriate so a checksum failure cannot stand in for the requested refusal.
Both also refuse encoding the unsupported whole `InstallSnapshot` message.
The complete malformed-input suite and published byte grammar remain in
[`rafter-codec`](../crates/rafter-codec/WIRE_FORMAT_V1.md).

## Running the matrix

With Python 3.11+ and rustup installed:

```sh
scripts/wire-compatibility-check
scripts/wire-compatibility-check --offline
scripts/wire-compatibility-check --candidate-archives /absolute/path/to/package
```

The baseline lock is
[`verification/wire-compatibility-baseline.json`](../verification/wire-compatibility-baseline.json).
It pins crates.io checksums for `rafter`, `rafter-codec` and `rafter-crc32`.
The runner shares archive validation, isolated consumer builds and dependency
boundary checks with the [storage matrix](storage-upgrades.md). Each matrix
declares its own exact public dependency closure; substituting one baseline
lock for the other fails.

Each run retains a fresh receipt, both dependency graphs and Cargo lockfiles,
compiler identity, archive and executable hashes, source-fixture hashes,
command logs, every emitted frame and malformed-frame inputs. Defaults are
`target/wire-compatibility/run-*` for results and
`target/wire-compatibility/archives` for the verified baseline cache.
`--artifacts`, `--baseline-cache`, `--toolchain` and `--keep` follow the storage
runner's conventions. Offline mode requires warmed archive and Cargo caches.

The deterministic and MSRV exact-package lanes run both matrices as their
final phase, using the same archives already tested by the reference consumers.
Hosted package jobs retain the compatibility receipts with their other evidence.

## Scope still to qualify

This proves codec interoperability for the listed messages. It does not prove
mixed-version cluster correctness, transport authentication interoperability,
rolling storage upgrades or safe downgrades. The next check must run old and
candidate replicas together through writes, linearizable reads, membership
changes and snapshot catch-up. Older `0.0.1` combinations, full public API
compatibility, and unsupported-version negotiation need their own policy and
executable cases before any stable-release promise.
