# Rafter peer wire format, version 1

This document is the byte-level interoperability contract for
`rafter-codec` version 1. The format encodes exactly one `rafter::Message` per
codec frame. Existing tags and field order are stable and must not be
reassigned.

## Frame envelope

| Field | Encoding |
| --- | --- |
| Magic | ASCII `RFPM`, 4 bytes |
| Version | `u8`, exactly `1` |
| Message tag | `u8` from the registry below |
| Payload | Tag-dependent fields in the listed order |
| Checksum | Big-endian `u32`, CRC-32/IEEE over every preceding frame byte |

The checksum is the IEEE 802.3 CRC-32 used by `rafter-crc32` (also known as
CRC-32/ISO-HDLC). It detects accidental corruption and misframing. It is not an
authentication tag and provides no adversarial integrity.

`decode_message` consumes exactly one codec frame. Any byte after the checksum
is a `TrailingBytes` error. A stream transport must provide an outer framing
mechanism, such as a length prefix; outer framing is not covered by this
specification.

The decoder must parse enough structure to locate the checksum before it can
verify that checksum. It therefore validates the envelope and payload in wire
order, verifies the checksum, and finally rejects trailing bytes. Which error
wins when a frame contains multiple independent defects is diagnostic behavior,
not part of v1 wire compatibility; callers must not rely on that precedence.

## Scalar and collection rules

- Integers are unsigned and big-endian.
- Booleans are exactly `0` or `1`.
- Strings are `u16 byte length` followed by valid UTF-8 bytes.
- Blobs are `u32 byte length` followed by opaque bytes.
- Counts describe elements, not bytes.
- Membership counts are `u16`; each member is a `u64` node ID.
- No padding, alignment bytes, or optional-field padding exists.
- An optional value begins with a boolean presence byte and is followed by its
  payload only when present.

## Message tag registry

| Tag | Message | Status |
| ---: | --- | --- |
| 1 | `RequestVote` | Allocated |
| 2 | `RequestVoteResponse` | Allocated |
| 3 | `AppendEntries` | Allocated |
| 4 | `AppendEntriesResponse` | Allocated |
| 5 | Draft whole `InstallSnapshot` | Permanently reserved and rejected |
| 6 | `InstallSnapshotResponse` | Allocated |
| 7 | `InstallSnapshotChunk` | Allocated |
| 8 | `PreVote` | Allocated |
| 9 | `PreVoteResponse` | Allocated |
| 10 | `TimeoutNow` | Allocated |

Whole `InstallSnapshot` remains a core message for direct or internal use. It
has no v1 peer encoding; peer transports use `InstallSnapshotChunk`.

## Top-level message payloads

The tables below begin immediately after the message tag.

### `RequestVote` and `PreVote`

| Field | Encoding |
| --- | --- |
| `term` | `u64` |
| `candidate_id` | `u64` |
| `last_log_index` | `u64` |
| `last_log_term` | `u64` |

### `RequestVoteResponse` and `PreVoteResponse`

| Field | Encoding |
| --- | --- |
| `term` | `u64` |
| `voter_id` | `u64` |
| `vote_granted` | Boolean |

### `TimeoutNow`

| Field | Encoding |
| --- | --- |
| `term` | `u64` |
| `leader_id` | `u64` |

### `AppendEntries`

| Field | Encoding |
| --- | --- |
| `term` | `u64` |
| `leader_id` | `u64` |
| `prev_log_index` | `u64` |
| `prev_log_term` | `u64` |
| `entry_count` | `u32` |
| `entries` | `entry_count` log-entry payloads |
| `leader_commit` | `u64` |
| `sequence` | `u64` |

### `AppendEntriesResponse`

| Field | Encoding |
| --- | --- |
| `term` | `u64` |
| `follower_id` | `u64` |
| `success` | Boolean |
| `match_index` | `u64` |
| `sequence` | `u64` |

### `InstallSnapshotResponse`

| Field | Encoding |
| --- | --- |
| `term` | `u64` |
| `follower_id` | `u64` |
| `success` | Boolean |
| `last_included_index` | `u64` |
| transfer ID present | Boolean |
| `transfer_id` | `u64` when present |
| `next_offset` | `u64` |

### `InstallSnapshotChunk`

| Field | Encoding |
| --- | --- |
| `term` | `u64` |
| `leader_id` | `u64` |
| `transfer_id` | `u64` |
| `metadata` | Snapshot metadata payload below |
| `total_payload_len` | `u64` |
| `application_payload_crc32` | `u32` |
| `offset` | `u64` |
| `chunk` | Blob |
| `done` | Boolean |

## Log-entry payloads

Every entry begins with `term: u64` and an entry tag.

| Tag | Kind | Fields after the tag |
| ---: | --- | --- |
| 0 | Application | `payload: Blob` |
| 1 | Stable configuration | `config_id: u64`, membership set |
| 2 | Joint configuration | `config_id: u64`, old membership set, new membership set |
| 3 | No-op | None |

## Snapshot metadata payload

| Field | Encoding |
| --- | --- |
| `group_id` | String |
| `writer_id` | `u64` |
| `last_included_index` | `u64` |
| `last_included_term` | `u64` |
| `hard_state_term` | `u64` |
| application `kind` | String |
| application `version` | `u16`, nonzero |
| committed configuration present | Boolean |
| committed configuration | Payload below when present |

A committed-configuration payload is:

| Field | Encoding |
| --- | --- |
| configuration position present | Boolean |
| `index` | `u64` when the position is present |
| `config_id` | `u64` when the position is present |
| `membership` | Membership configuration payload |

## Membership payloads and canonicality

A membership configuration begins with this tag:

| Tag | Kind | Fields after the tag |
| ---: | --- | --- |
| 0 | Stable | One membership set |
| 1 | Joint | Old membership set, then new membership set |

Each membership set is:

| Field | Encoding |
| --- | --- |
| voter count | `u16` |
| voters | voter-count `u64` node IDs |
| learner count | `u16` |
| learners | learner-count `u64` node IDs |

Voters must be strictly increasing, and learners must be strictly increasing.
A descending list is noncanonical and rejected. Duplicates, an empty voter
set, and voter/learner overlap are invalid memberships and are rejected by
domain validation. These rules give each logical membership exactly one v1
byte representation.

## Semantic validation

A frame is valid only when both its byte grammar and the reconstructed domain
values are valid. A conforming v1 decoder must reject snapshot metadata that
violates any of these rules:

| Value | Valid domain |
| --- | --- |
| `group_id` | 1 to 128 bytes, matching ASCII `[A-Za-z0-9._:-]+` |
| application `kind` | 1 to 128 bytes, matching ASCII `[A-Za-z0-9._:-]+` |
| application `version` | `1..=65535` |
| `last_included_index` | `1..=u64::MAX - 1` |
| `last_included_term` | `1..=hard_state_term` |

Membership payloads must also satisfy all canonicality and validity rules in
the preceding section. These checks apply wherever the corresponding nested
value appears. Scalar fields without a rule here or in their payload section
accept their full encoded range.

These semantic domains are part of the version 1 wire contract even though the
decoder reconstructs them through `rafter` model constructors. Tightening or
loosening one of those constructors is a v1 compatibility change and requires
codec review. Existing v1 acceptance must remain stable unless a new wire
version deliberately replaces it.

## Receive limits and transport responsibility

The codec imposes no receive limit. A transport must enforce its limit before
allocating a frame. Call `max_receive_frame_bytes(max_append_entries_bytes)`
rather than deriving a limit from this prose; it returns the largest frame a
conforming v1 leader can emit, and `tests::receive_limits` enumerates the tag
registry to check that claim frame kind by frame kind.

At the default 512 KiB append budget that limit is **2,163,036 bytes**. Three
frame shapes set it, and only the first scales with the append budget:

| Frame | Maximum bytes | Set by |
| --- | --- | --- |
| `AppendEntries`, one maximum application entry | 524,299 | `max_append_entries_bytes` |
| `AppendEntries`, one joint configuration entry | 2,097,207 | the `u16` membership counts |
| `InstallSnapshotChunk`, maximum joint committed configuration | 2,163,036 | the `u16` membership counts |

`NodeConfig::max_append_entries_bytes` is a batching target, not a maximum
frame size. Sizing a receive limit against the append term alone under-sizes it
by 4.1x, because two things escape that budget:

- **Configuration entries are exempt from the batch budget in both
  directions.** A leader checks only application payloads against it
  (`node/replication/proposal.rs`), and batch assembly always includes the
  first entry whatever its size (`node/log.rs`) so an oversized entry cannot
  stall replication. A leader will therefore emit a 2 MB configuration frame
  under a 512 KiB budget.
- **Membership size is bounded only by this format.** `MembershipSet::new`
  imposes no size limit, so the ceiling is the `u16` voter and learner counts
  in this format: 65,535 each, in each of the four halves of a joint
  configuration. Snapshot-chunk metadata embeds a committed configuration and
  so inherits the same term — "metadata plus 64 KiB of chunk data" is
  dominated by the metadata, not the chunk.

### What the append term depends on

The append term is exact — 524,299 bytes at the default budget, to the byte —
but it is exact because of an over-charge in another crate, so record the
dependency rather than rediscover it. `LogEntry::replication_bytes` charges
every entry kind more than it costs on this wire: 64 bytes charged against 13
encoded for an application entry, 16 against 9 for a noop. A leader admits a
proposal on the charged figure, so a bound derived from the charged budget is
never exceeded by the encoding. If `replication_bytes` were ever brought down
to the true wire cost, this bound would become an under-estimate.
`tests::receive_limits::the_append_bound_rests_on_replication_bytes_over_charging_the_wire`
fails if that direction reverses.

The snapshot term likewise assumes a sender that caps one chunk at 64 KiB
(`rafter`'s private `INSTALL_SNAPSHOT_CHUNK_BYTES`). The codec accepts any
chunk length and cannot check that cap from here, so a transport that must
tolerate peers built from different source should size against its own maximum.

Transports crossing an untrusted boundary must authenticate the channel or the
outer frame. The v1 checksum is only an accidental-corruption check.

## Interoperability vectors

Human-readable exact-byte fixtures live under `tests/vectors/v1/`. The
`wire_v1_vectors` integration test checks both directions: every fixture is the
exact encoder output for its message and decodes back to that message. These
vectors pin the format independently of the internal module layout. Tokens use
two-digit hexadecimal bytes, whitespace is insignificant, and `#` begins a
comment through the end of the line. `5a*65536` is the compact notation for
65,536 consecutive `5a` bytes in the maximum snapshot-chunk vector.
