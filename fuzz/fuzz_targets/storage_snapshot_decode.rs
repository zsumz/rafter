//! Fuzzes `rafter_storage::decode_raft_snapshot` on raw bytes.
//!
//! Invariants:
//! - decode never panics on any input: it returns `Ok` or a typed `Err`.
//! - Canonical-byte law on success: re-encoding a decoded version-1 snapshot
//!   must reproduce the exact input bytes: `encode(decode(bytes)) == bytes`.
//! - Semantic round-trip law: decoding those canonical bytes returns the same
//!   snapshot value.
//!
//! Note the envelope ends in a whole-envelope CRC32, so blind mutations
//! mostly stop at `EnvelopeChecksumMismatch`; the committed seeds carry
//! valid current-format envelopes past that gate.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(snapshot) = rafter_storage::decode_raft_snapshot(data) else {
        return;
    };

    let encoded = rafter_storage::encode_raft_snapshot(&snapshot)
        .expect("canonical-byte law: a decoded snapshot must re-encode");
    assert_eq!(
        encoded.as_slice(),
        data,
        "canonical-byte law: encode(decode(bytes)) != bytes"
    );
    let redecoded = rafter_storage::decode_raft_snapshot(&encoded)
        .expect("round-trip law: re-encoded snapshot envelope must decode");
    assert_eq!(
        snapshot, redecoded,
        "round-trip law: decode(encode(s)) != s"
    );
});
