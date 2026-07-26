# rafter-codec

Versioned wire codec for Rafter peer messages.

This crate encodes and decodes `rafter::Message` values at the process runtime
boundary. It stays outside the core crate so the protocol kernel remains
deterministic and transport-free.

The frame checksum catches accidental corruption and misframing. It is not an
authentication tag. Production transports should authenticate peers and protect
the channel outside this codec.

Use `encode_message` and `decode_message` when building a custom peer
transport.

The codec imposes no receive limit. A transport must enforce one before
allocating a frame, and should size it with
`max_receive_frame_bytes(config.max_append_entries_bytes())` rather than by
hand. At the default 512 KiB append budget that is 2,163,036 bytes.

`NodeConfig::max_append_entries_bytes` is a batching target, not a maximum
frame size, and sizing against it alone under-sizes a receive limit by 4.1x. A
leader can emit a 2 MB `AppendEntries` frame carrying a single joint
configuration entry, and a similarly large `InstallSnapshotChunk`, because
configuration entries are exempt from the batch budget and membership size is
bounded only by the wire format's `u16` member counts. See
[`WIRE_FORMAT_V1.md`](WIRE_FORMAT_V1.md) for the per-frame table and for the
cross-crate dependencies the bound rests on.

Stream transports must also provide outer framing, such as a length prefix;
that prefix is not part of the codec frame.

This pre-release crate supports exactly one peer wire format. Frames still
carry a version byte, and `decode_message` rejects every version other than the
current one with a typed unsupported-version error. The current peer wire
format serializes chunked snapshot transfer, not unbounded whole-snapshot
payloads. Rolling compatibility begins only after Rafter has a public wire
compatibility promise.

See [`WIRE_FORMAT_V1.md`](WIRE_FORMAT_V1.md) for the exact byte order, tag
registry, canonicality rules, checksum coverage, and nested payload grammar.

## Coverage

Run `scripts/codec-coverage` from the workspace root for a source-based line
and branch report. The script runs every `rafter-codec` test under nightly LLVM
instrumentation, writes `target/rafter-codec-coverage.json`, and enforces the
local 92% line and 95% branch ratchets. It is not part of CI.

The one-time tool setup is:

```console
cargo install cargo-llvm-cov --locked
rustup toolchain install nightly --component llvm-tools-preview
```
