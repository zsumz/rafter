//! Table-driven and streaming CRC-32 agreement scenarios.
//!
//! The table-driven implementation must match the known IEEE vector and a
//! bitwise reference across byte shapes, and a running checksum must be
//! readable mid-stream and agree with the one-shot value on any split.

use super::*;

fn bitwise_crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFF_u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = 0_u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

#[test]
fn crc32_matches_known_vector() {
    assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
}

#[test]
fn frame_checksum_fast_path_matches_bitwise_reference() {
    for bytes in [
        b"".as_slice(),
        b"RFPM\x01\x03",
        b"single frame payload",
        &[0, 1, 2, 3, 250, 251, 252, 253, 254, 255],
    ] {
        assert_eq!(crc32(bytes), bitwise_crc32(bytes));
    }
}

#[test]
fn running_crc32_over_split_slices_matches_one_shot_crc32() {
    let mut running = RunningCrc32::new();
    running.update(b"1234");
    running.update(b"");
    running.update(b"56789");

    assert_eq!(running.value(), crc32(b"123456789"));
}

#[test]
fn running_crc32_value_is_readable_mid_stream() {
    let mut running = RunningCrc32::new();
    running.update(b"partial");

    assert_eq!(running.value(), crc32(b"partial"));

    running.update(b" snapshot bytes");
    assert_eq!(running.value(), crc32(b"partial snapshot bytes"));
}

#[test]
fn running_crc32_of_empty_stream_matches_empty_crc32() {
    assert_eq!(RunningCrc32::new().value(), crc32(b""));
}
