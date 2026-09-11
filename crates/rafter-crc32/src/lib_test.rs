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

#[test]
fn every_short_length_and_alignment_matches_independent_reference() {
    let bytes: Vec<u8> = (0_u32..528)
        .map(|index| {
            index
                .wrapping_mul(197)
                .wrapping_add(index >> 2)
                .to_le_bytes()[0]
        })
        .collect();
    for offset in 0..16 {
        for len in 0..=512 {
            let input = &bytes[offset..offset + len];
            assert_eq!(
                crc32(input),
                bitwise_crc32(input),
                "offset={offset}, len={len}"
            );
        }
    }
}

#[test]
fn every_split_preserves_the_streaming_state_and_scalar_tails() {
    let bytes: Vec<u8> = (0_u32..257)
        .map(|i| i.wrapping_mul(73).to_le_bytes()[0])
        .collect();
    let expected = bitwise_crc32(&bytes);
    for split in 0..=bytes.len() {
        let mut running = RunningCrc32::new();
        running.update(&bytes[..split]);
        assert_eq!(running.value(), bitwise_crc32(&bytes[..split]));
        running.update(&[]);
        running.update(&bytes[split..]);
        assert_eq!(running.value(), expected, "split={split}");
    }
}

#[test]
fn snapshot_sized_streams_match_independent_reference() {
    let mut seed = 0xE761_4B93_u32;
    let bytes: Vec<u8> = (0..256 * 1024 + 7)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            seed.to_le_bytes()[0]
        })
        .collect();
    let expected = bitwise_crc32(&bytes);
    for chunk_len in [1, 7, 8, 9, 63, 64, 65, 4096, 65536, bytes.len()] {
        let mut running = RunningCrc32::new();
        for chunk in bytes.chunks(chunk_len) {
            running.update(chunk);
        }
        assert_eq!(running.value(), expected, "chunk_len={chunk_len}");
    }
    for byte in [0, 1, 128, 255] {
        let repeated = vec![byte; 4097];
        assert_eq!(crc32(&repeated), bitwise_crc32(&repeated));
    }
}
