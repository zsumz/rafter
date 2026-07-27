//! The checksum the whole format is sealed with.

use crate::store::format::{crc32, CRC32_POLYNOMIAL};

/// A second implementation, deliberately written a different way.
///
/// The store's own `crc32` folds four bits at a time through a table built
/// from the polynomial; this one walks the message a single bit at a time
/// and never builds a table. Agreement between two shapes is worth more
/// than agreement between one shape and itself.
fn reference_crc32(bytes: &[u8]) -> u32 {
    let mut state = u32::MAX;
    for byte in bytes {
        state ^= u32::from(*byte);
        for _ in 0..8 {
            state = if state & 1 == 1 {
                (state >> 1) ^ CRC32_POLYNOMIAL
            } else {
                state >> 1
            };
        }
    }
    !state
}

#[test]
fn checksums_match_the_published_vector() {
    assert_eq!(
        crc32(b"123456789"),
        0xCBF4_3926,
        "the store's CRC-32 must be the standard IEEE one"
    );
    assert_eq!(crc32(&[]), 0, "the empty message checksums to zero");
}

#[test]
fn checksums_agree_with_an_independently_written_implementation() {
    let mut sample = Vec::new();
    for length in 0..96_usize {
        sample.push(u8::try_from(length * 11 % 251).expect("the modulus fits a byte"));
        assert_eq!(
            crc32(&sample),
            reference_crc32(&sample),
            "the two implementations disagree at length {length}"
        );
    }
}
