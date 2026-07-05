//! Fuzzes `rafter_codec::decode_message` on raw bytes.
//!
//! Invariants:
//! - decode never panics on any input: it returns `Ok` or a typed `Err`.
//! - Round-trip law on success: re-encoding the decoded message must both
//!   succeed and decode back to an equal message. The re-encoded bytes may
//!   legitimately differ from the input (e.g. a v2/v3 frame re-encodes at the
//!   current version), but `decode(encode(m)) == m` must hold.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(message) = rafter_codec::decode_message(data) else {
        return;
    };

    let encoded = rafter_codec::encode_message(&message)
        .expect("round-trip law: a decoded peer message must re-encode");
    let redecoded = rafter_codec::decode_message(&encoded)
        .expect("round-trip law: re-encoded peer message bytes must decode");
    assert_eq!(message, redecoded, "round-trip law: decode(encode(m)) != m");
});
