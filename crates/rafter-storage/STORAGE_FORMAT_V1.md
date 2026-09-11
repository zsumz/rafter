# Rafter storage format, version 1

This document is the byte-level contract for the current Rafter durable-storage
artifacts. It describes the canonical bytes emitted by version 1 writers in
`rafter-storage`.

Version 1 is the first public storage format. Earlier internal draft layouts are
not supported. A future incompatible meaning requires a new envelope version
and an explicit migration or compatibility plan; existing version-1 fields and
tags must not be reassigned.

Golden examples live under `tests/vectors/v1` and are executable contracts in
`tests/format_v1_vectors.rs`.

## Common rules

Unless an artifact says otherwise:

- integers are unsigned and big-endian;
- byte sequences are packed with no alignment or padding;
- all trailing bytes are rejected;
- a version byte other than `1` is rejected;
- the final checksum is CRC-32/IEEE over every preceding byte in that artifact;
- CRC-32 is an accidental-corruption check, not an authentication tag;
- strings are UTF-8 and their length is measured in bytes;
- node IDs, terms, configuration IDs, transfer IDs, and log indexes are stored
  as their underlying `u64` values.

Version-1 writers emit member IDs in the deterministic order provided by
`MembershipSet`, and readers require each voter and learner list to be in
strictly ascending node-id order. Duplicate IDs remain membership-validation
errors. A successfully decoded version-1 artifact therefore re-encodes to the
same bytes: `encode(decode(bytes)) == bytes`.

## Artifact registry

| Artifact | Magic | Version | Stable path or container |
|---|---:|---:|---|
| Shared Raft WAL (opt-in) | `RFWB` | `1` | `hard-state` |
| Shared WAL checkpoint | `RFWC` | `1` | `raft-wal-checkpoint-*.rfwc` |
| Shared WAL generation segment | `RFWS` | `1` | `raft-wal-segment-*.rfwb` |
| Shared WAL current manifest | `RFWM` | `1` | `raft-wal-current` |
| Hard-state journal (opt-in) | `RFHJ` | `1` | `hard-state` |
| Hard-state envelope | `RFHS` | `1` | `hard-state` |
| Log-entry envelope | `RFLE` | `1` | Inside one log frame |
| Log frame | — | — | Repeated in `log` |
| Compaction marker | `RFLC` | `1` | `log.compact` |
| Snapshot envelope | `RFSN` | `1` | `snapshot-*.rfsn` |
| Current-snapshot manifest | `RFSM` | `1` | `snapshots/current.snapshot` |
| Pending-transfer manifest | `RFPT` | `1` | `snapshots/pending.snapshot-transfer` |
| Pending-transfer body | — | — | `snapshots/pending.snapshot-transfer.body` |

## Hard-state envelope (`RFHS`)

The envelope has a fixed size of 51 bytes.

```text
magic                              [4]   "RFHS"
version                            u8    1
current_term                       u64
voted_for_present                  u8    0 or 1
voted_for_node                     u64
commit_index                       u64
committed_configuration_present    u8    0 or 1
committed_configuration_index      u64
committed_configuration_id         u64
crc32                              u32
```

When `voted_for_present` is zero, `voted_for_node` must be zero. When
`committed_configuration_present` is zero, both committed-configuration fields
must be zero. Readers reject non-zero absent fields as noncanonical version-1
bytes.

Checksum coverage ends immediately before `crc32`.

## Hard-state journal (opt-in)

`JournalRaftHardStateStore` uses a distinct format at the same `hard-state`
path. Its header is `RFHJ` (4 bytes), version `1` (1 byte), then the big-endian
CRC-32/IEEE of those 5 bytes (4 bytes). The 9-byte header is followed by zero
or more complete, fixed-width 51-byte RFHS v1 envelopes, in append order.
The last complete envelope is current; a header alone represents default state.

Recovery validates the header and every complete envelope, then truncates only
an incomplete final envelope (1–50 bytes). A complete invalid envelope anywhere
is an error, even when followed by an incomplete suffix. Unknown versions and
partial headers are errors. Reads use constant memory and linear replay time.

RFHS and RFHJ are mutually incompatible; there is no automatic migration.
After 4,096 complete records, the next publication replaces the journal with
its existing header and one incoming state record through temp-file sync,
rename, and parent-directory sync. The reserved `<path>.checkpoint.tmp` is
never a recovery source. A successful journal therefore contains at most
4,096 records (208,905 bytes); older oversized journals are validated in full
and checkpointed on their next write. See `DURABILITY_PROTOCOL.md`.

## Log-entry envelope (`RFLE`)

```text
magic            [4]   "RFLE"
version          u8    1
index            u64
term             u64
entry_kind       u8
entry_payload    ...
crc32            u32
```

### Entry kinds

| Tag | Meaning | Payload |
|---:|---|---|
| `0` | Application | `payload_len[u32]`, then `payload[payload_len]` |
| `1` | Stable configuration | `configuration_id[u64]`, then one membership set |
| `2` | Joint configuration | `configuration_id[u64]`, old set, then new set |
| `3` | Leadership no-op | no payload |

### Membership set

```text
voter_count      u32
voters           voter_count * u64
learner_count    u32
learners         learner_count * u64
```

The encoder rejects a payload or member count that cannot fit its `u32` length
field. Voter and learner lists must each be stored in strictly ascending node-id
order. The decoder also validates nonempty voters, uniqueness, and voter/learner
disjointness through `MembershipSet`.

## Log file framing

The `log` file is a concatenation of frames:

```text
entry_envelope_len    u32
entry_envelope        entry_envelope_len bytes, exactly one RFLE envelope
```

The length excludes the four-byte frame header. No file header, footer, or
whole-file checksum exists. Recovery scans frames in order and reports the byte
offset of the first partial or corrupt frame.

Strict open rejects any partial, corrupt, or noncontiguous frame. The explicit
uncommitted-tail repair mode may truncate only at the first bad frame or gap
strictly above the durable hard-state commit floor.

## Log-compaction marker (`RFLC`)

The marker has a fixed size of 17 bytes.

```text
magic                [4]   "RFLC"
version              u8    1
compacted_through    u64
crc32                u32
```

`compacted_through` is the highest log index covered by a durable snapshot.
During replay, entries at or below this boundary are ignored even if their old
frames remain in the log file after an interrupted reclamation rewrite.

## Snapshot envelope (`RFSN`)

```text
magic                                  [4]   "RFSN"
version                                u8    1
group_id_len                           u16
group_id                               group_id_len bytes, UTF-8
writer_node_id                         u64
last_included_index                    u64
last_included_term                     u64
hard_state_term                        u64
application_kind_len                   u16
application_kind                       application_kind_len bytes, UTF-8
application_version                    u16
committed_configuration_present        u8    0 or 1
committed_configuration                ...   when present
application_payload_len                u64
application_payload                    application_payload_len bytes
application_payload_crc32              u32
envelope_crc32                         u32
```

When committed configuration is present:

```text
configuration_identity_present         u8    0 or 1
configuration_index                    u64   when identity is present
configuration_id                       u64   when identity is present
membership_kind                        u8    0 stable, 1 joint
membership                             one stable set or old+new joint sets
```

Snapshot membership sets use `u16` voter and learner counts:

```text
voter_count      u16
voters           voter_count * u64
learner_count    u16
learners         learner_count * u64
```

Voter and learner lists must each be stored in strictly ascending node-id
order. Valid membership encoded in another order is rejected rather than
normalized during decode.

The payload checksum covers only `application_payload`. The envelope checksum
covers every byte through and including `application_payload_crc32`, but not
`envelope_crc32` itself.

The file-backed store parses the metadata header and verifies both checksums in
a streaming pass. The payload need not be materialized in memory.

## Current-snapshot manifest (`RFSM`)

```text
magic            [4]   "RFSM"
version          u8    1
sequence         u64
file_name_len    u16
file_name        file_name_len bytes, UTF-8
crc32            u32
```

The file name must be a nonempty plain file name: it may not be `.`, `..`, or
contain `/` or `\`. The standard writer uses:

```text
snapshot-<sequence>-<last_included_index>-<last_included_term>-<writer_id>.rfsn
```

The manifest is the sole selector of the current snapshot. A complete snapshot
file not named by this manifest is retained but ignored by open.

## Pending-transfer manifest (`RFPT`)

```text
magic                         [4]   "RFPT"
version                       u8    1
leader_id                     u64
transfer_id                   u64
total_payload_len             u64
application_payload_crc32     u32
received_payload_len          u64
staged_body_crc32             u32
metadata_envelope_len         u64
metadata_envelope             metadata_envelope_len bytes
crc32                         u32
```

In version 1, `metadata_envelope` is a complete `RFSN` envelope with the staged
snapshot metadata and an empty application payload. Its own payload and
envelope checksums remain present. This nesting is part of the current v1 bytes
and cannot be changed without a new `RFPT` version. Readers reject nested
metadata envelopes larger than 4 MiB or carrying application payload bytes.

`transfer_id` must equal the deterministic `RaftSnapshot::transfer_id()` derived
from `metadata_envelope`, `total_payload_len`, and
`application_payload_crc32`. `received_payload_len` must not exceed
`total_payload_len`. Violations are corrupt manifest state and fail open rather
than being treated as an interrupted optional body update.

`staged_body_crc32` covers exactly the first `received_payload_len` bytes of the
pending-transfer body. The outer checksum covers the entire manifest before
its final checksum field.

## Pending-transfer body

The body file is raw snapshot payload bytes with no independent header or
trailer. The pending-transfer manifest gives its logical length and checksum.
A body may be longer than `received_payload_len` after a crash between body
append and manifest replacement; only the manifest-described prefix is staged.
A shorter or checksum-mismatched body is inconsistent optional progress and is
discarded during recovery.

## Compatibility rules

For every versioned artifact:

1. Existing magic values, tags, field widths, and field order are immutable.
2. A reader must reject unknown versions rather than guessing an older meaning.
3. A writer must emit canonical flags, ordering, and zero fillers described
   above.
4. Adding bytes to a closed envelope requires a new version because trailing
   bytes are rejected.
5. A migration must preserve the durable ordering and crash recovery contracts
   in `DURABILITY_PROTOCOL.md` in addition to translating bytes.

## Shared Raft WAL (opt-in)

`WalRaftNodeStores` uses an independent format, with no implicit migration from
RFHS/RFHJ plus `log`. Its eight-byte header is `RFWB 00 00 00 01`. The `log`
path must be absent. Existing snapshot envelopes and manifests are unchanged.

Each atomic record contains:

```text
magic                 [4]   "BCH1"
body_length           u32   29..67108864
body_length_inverse   u32   bitwise complement of body_length
header_crc32          u32   CRC32 of the preceding 12 bytes
body                  [body_length]
body_crc32            u32   CRC32 of body
end_magic             [4]   "END1"
```

The body holds an operation number (`u64`, starting at 1 and increasing by 1),
compaction and truncation indexes (two `u64`; maximum means absent), a hard-state
presence byte (0 or 1), an optional complete 51-byte RFHS envelope, an entry count
(`u32`), then that many `u32` lengths and complete RFLE envelopes. No trailing
bytes are accepted. Truncation precedes append. Compaction is a separate record.
Entries remain contiguous above the compacted floor; truncation cannot erase a
durable committed entry, and hard-state term, vote promises, and commit cannot
regress. Commit cannot exceed the resulting log or compacted boundary.

An incomplete final record is discarded as a unit. A complete invalid header,
body, checksum, operation sequence, or logical mutation fails recovery. The
header checksum prevents a damaged length from silently hiding complete records.
The initial header must be complete. Before the first prefix compaction, records
are appended to `hard-state` exactly as in the original RFWB v1 layout.

Prefix compaction first appends and synchronizes its ordinary RFWB record. It
then materializes the resulting live state into a new generation:

```text
raft-wal-checkpoint-{generation:020}.rfwc
raft-wal-segment-{generation:020}.rfwb
raft-wal-current
```

The checksummed `RFWC 00 00 00 01` checkpoint records the generation, last
operation number, compacted boundary, complete RFHS envelope, current snapshot
boundary/term/transfer identity, and every retained RFLE envelope. Retained
entries must be contiguous immediately above the compacted boundary. The
checkpoint is immutable and must be complete; truncation or corruption of a
manifest-selected checkpoint fails recovery.

```text
magic                    [8]   "RFWC 00 00 00 01"
generation               u64   starts at 1
operation                u64   latest included publication
compacted_through        u64
hard_state               [51]  complete RFHS envelope
snapshot_present         u8    canonical value 1
snapshot_index           u64
snapshot_term            u64
snapshot_transfer_id     u64
retained_entry_count     u64
retained_entries         repeated { envelope_length u32, RFLE envelope }
checkpoint_crc32         u32   CRC32 of every preceding checkpoint byte
end_magic                [4]   "ENDC"
```

The fresh segment begins with a checksummed `RFWS 00 00 00 01` header binding
its generation and preceding operation number, followed by ordinary RFWB
records. Operation numbering therefore continues across checkpoints. Only an
incomplete final record of the selected segment may be discarded.

```text
magic                    [8]   "RFWS 00 00 00 01"
generation               u64
preceding_operation      u64
header_crc32             u32   CRC32 of the preceding 24 bytes
records                   repeated RFWB records
```

The fixed-size, checksummed `RFWM 00 00 00 01` manifest names the selected
generation, operation, and compacted boundary. A selected checkpoint and segment
must agree with all three fields. Recovery never falls back from a corrupt or
missing manifest-selected file to an older generation or to `hard-state`.

```text
magic                    [8]   "RFWM 00 00 00 01"
generation               u64
operation                u64
compacted_through        u64
manifest_crc32           u32   CRC32 of the preceding 32 bytes
```

Checkpoint and segment files are synchronized before the manifest is replaced
and its parent directory synchronized. Only then are `hard-state`, the previous
generation, and abandoned preparation files removed. Cleanup is directory-
synchronized. A crash can therefore leave bounded obsolete residue, but reopen
selects exactly one published generation and removes the residue only after
validating that authority. Each later compaction repeats this protocol, so Raft
WAL bytes and replay work are proportional to the retained suffix plus writes
since the latest compaction, rather than the replica's lifetime write count.

The snapshot reference binds reclamation to a durably published snapshot that
covers the compacted prefix. Snapshot files and application-state durability
remain separate artifacts with their own retention and reclamation policies.
