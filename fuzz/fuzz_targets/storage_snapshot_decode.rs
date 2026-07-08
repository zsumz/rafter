//! Fuzzes `rafter_storage::decode_raft_snapshot` on raw bytes.
//!
//! Invariants:
//! - decode never panics on any input: it returns `Ok` or a typed `Err`.
//! - Round-trip law on success: `encode_raft_snapshot` of the decoded
//!   snapshot must decode back to an equal current-format snapshot:
//!   `decode(encode(s)) == s` must hold.
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
        .expect("round-trip law: a decoded snapshot must re-encode");
    let redecoded = rafter_storage::decode_raft_snapshot(&encoded)
        .expect("round-trip law: re-encoded snapshot envelope must decode");
    assert_eq!(
        snapshot, redecoded,
        "round-trip law: decode(encode(s)) != s"
    );
});
