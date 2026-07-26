//! Fuzzes `rafter_storage::decode_raft_snapshot` on raw bytes.
//!
//! Invariants:
//! - decode never panics on any input: it returns `Ok` or a typed `Err`.
//! - Canonical-byte law on success: re-encoding a decoded version-1 snapshot
//!   must reproduce the exact input bytes: `encode(decode(bytes)) == bytes`.
//! - Semantic round-trip law: decoding those canonical bytes returns the same
//!   snapshot value.
//!
//! # Why this target reseals the envelope
//!
//! `decode_raft_snapshot` verifies the whole-envelope CRC32 as its *first*
//! statement, before parsing anything. That order is a deliberate durability
//! choice and production must keep it: a corrupt envelope should be rejected
//! without interpreting a single byte of it.
//!
//! It also means a fuzzer handed raw bytes never gets past that gate. Measured
//! over 200,000 plain byte-mutations of the committed seeds, the raw-bytes-only
//! harness reached the parser 80 times (0.04%) — and all 80 were byte-identical
//! to an unmutated seed, so *no* mutation ever reached it. Real libFuzzer does
//! no better: 2,000,000 executions with CMP tracing and its auto-dictionary
//! plateaued at 317 edges after run 8,092 and never improved. The header
//! parser, the typed-metadata decoder, the membership decoder and the inner
//! payload-CRC comparison received no coverage at all.
//!
//! So the harness — not production — repairs the checksum, in three paths:
//!
//! - **A. raw.** `data` as a complete envelope. Keeps the gate itself, and its
//!   rejection path, under test.
//! - **B. resealed.** `data` as an envelope *body*, with the CRC32 the format
//!   requires appended. Always passes the gate, so every mutation lands in the
//!   parser. This is what makes coverage feedback possible at all.
//! - **C. fully repaired.** A body whose framing is self-consistent but whose
//!   *inner* payload CRC is wrong stops one gate short of the oracle. Rewriting
//!   that field with the payload's real CRC makes the canonical-byte law
//!   reachable from a mutated body, not only from an unmutated seed.
//!
//! Path B hardcodes one fact about a lower layer: the envelope ends in a
//! big-endian CRC32 over everything before it. If that ever stops being true
//! the reseal would silently stop working and this target would quietly go back
//! to fuzzing CRC space. The assertion in path B turns that silent degradation
//! into a loud failure.

#![no_main]

use libfuzzer_sys::fuzz_target;
use rafter_crc32::crc32;
use rafter_storage::DecodeRaftSnapshotError;

/// Appends the trailing big-endian CRC32 that a version-1 snapshot envelope
/// carries over its body, mirroring `format::envelope::finish_checksummed`.
fn seal(body: &[u8]) -> Vec<u8> {
    let mut envelope = Vec::with_capacity(body.len() + 4);
    envelope.extend_from_slice(body);
    envelope.extend_from_slice(&crc32(body).to_be_bytes());
    envelope
}

/// Decodes one envelope and, on success, enforces the canonical-byte and
/// round-trip laws. Returns the decode error so the caller can drive the next
/// repair stage.
fn decode_checked(envelope: &[u8]) -> Result<(), DecodeRaftSnapshotError> {
    let snapshot = rafter_storage::decode_raft_snapshot(envelope)?;

    let encoded = rafter_storage::encode_raft_snapshot(&snapshot)
        .expect("canonical-byte law: a decoded snapshot must re-encode");
    assert_eq!(
        encoded.as_slice(),
        envelope,
        "canonical-byte law: encode(decode(bytes)) != bytes"
    );
    let redecoded = rafter_storage::decode_raft_snapshot(&encoded)
        .expect("round-trip law: re-encoded snapshot envelope must decode");
    assert_eq!(
        snapshot, redecoded,
        "round-trip law: decode(encode(s)) != s"
    );
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    // Path A: the envelope checksum gate, including its rejection path.
    let _ = decode_checked(data);

    // Path B: reseal so the mutation reaches the parser.
    let resealed = seal(data);
    let outcome = decode_checked(&resealed);
    assert!(
        !matches!(
            outcome,
            Err(DecodeRaftSnapshotError::EnvelopeChecksumMismatch { .. })
        ),
        "resealed envelope was still rejected by the envelope checksum: this \
         harness's seal no longer matches the storage format's checksum \
         discipline, so every mutation would die at the gate again"
    );

    // Path C: repair the inner payload CRC so the oracle is reachable from a
    // mutated body. The payload-CRC field is the last four bytes of the body
    // whenever the framing is self-consistent, which is the only case that can
    // reach `Ok` anyway.
    if let Err(DecodeRaftSnapshotError::PayloadChecksumMismatch { actual, .. }) = outcome {
        if let Some(field) = data.len().checked_sub(4) {
            let mut body = data.to_vec();
            body[field..].copy_from_slice(&actual.to_be_bytes());
            let _ = decode_checked(&seal(&body));
        }
    }
});
