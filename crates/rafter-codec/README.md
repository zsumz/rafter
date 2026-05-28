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

This pre-release crate supports exactly one peer wire format. Frames still
carry a version byte, and `decode_message` rejects every version other than the
current one with a typed unsupported-version error. The current peer wire
format serializes chunked snapshot transfer, not unbounded whole-snapshot
payloads. Rolling compatibility begins only after Rafter has a public wire
compatibility promise.
