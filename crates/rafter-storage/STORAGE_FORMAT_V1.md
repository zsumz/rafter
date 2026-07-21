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
