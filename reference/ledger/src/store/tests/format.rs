//! The checksum every record in the format is sealed with.

use crate::store::format::{crc32, CRC32_POLYNOMIAL};

/// A second implementation, deliberately written a different way.
///
/// The store's own `crc32` folds the polynomial into a running state; this
/// one shifts the message through a register. Agreement between two shapes
/// is worth more than agreement between one shape and itself.
fn reference_crc32(bytes: &[u8]) -> u32 {
    let mut state = u32::MAX;
    for byte in bytes {
        let mut mask = 1_u8;
        while mask != 0 {
            let bit = u32::from(*byte & mask != 0);
            let top = state & 1;
            state >>= 1;
            if top ^ bit == 1 {
                state ^= CRC32_POLYNOMIAL;
            }
            mask <<= 1;
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
    for length in 0..64_usize {
        sample.push(u8::try_from(length * 7 % 251).expect("the modulus fits a byte"));
        assert_eq!(
            crc32(&sample),
            reference_crc32(&sample),
            "the two implementations disagree at length {length}"
        );
    }
}
