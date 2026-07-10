//! Table-driven CRC-32 for Rafter's corruption-detection envelopes.
//!
//! This crate is intentionally tiny and dependency-free. It centralizes the
//! IEEE CRC-32 implementation used by storage envelopes and peer-message frames
//! so both boundaries keep byte-compatible checksums without duplicating the hot
//! byte-scanning code.

const CRC32_TABLE: [u32; 256] = build_crc32_table();

/// Computes CRC-32 (IEEE 802.3) for accidental corruption detection.
///
/// This is not a cryptographic digest or authentication tag.
#[must_use]
pub fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = RunningCrc32::new();
    crc.update(bytes);
    crc.value()
}

/// Incremental CRC-32 (IEEE 802.3, the polynomial behind [`crc32`]) over a byte
/// stream fed in arbitrary slices.
///
/// Feeding slices through [`RunningCrc32::update`] and reading
/// [`RunningCrc32::value`] yields exactly `crc32(concatenation)`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RunningCrc32 {
    state: u32,
}

impl RunningCrc32 {
    /// Starts a new streaming CRC-32 accumulator.
    ///
    /// The initial state matches [`crc32`] over an empty byte slice.
    #[must_use]
    pub fn new() -> Self {
        Self { state: 0xFFFF_FFFF }
    }

    /// Extends the checksum with `bytes`.
    ///
    /// Calling this repeatedly is equivalent to checksumming the concatenation
    /// of every slice in order.
    pub fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            let index = ((self.state ^ u32::from(*byte)) & 0xFF) as usize;
            self.state = (self.state >> 8) ^ CRC32_TABLE[index];
        }
    }

    /// The checksum of every byte fed so far. Reading it does not end the
    /// stream; later [`RunningCrc32::update`] calls keep extending it.
    #[must_use]
    pub fn value(self) -> u32 {
        !self.state
    }
}

impl Default for RunningCrc32 {
    fn default() -> Self {
        Self::new()
    }
}

const fn build_crc32_table() -> [u32; 256] {
    let mut table = [0; 256];
    let mut i: u32 = 0;
    while i < 256 {
        let mut crc = i;
        let mut j = 0;
        while j < 8 {
            if crc & 1 == 1 {
                crc = (crc >> 1) ^ 0xEDB8_8320;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i as usize] = crc;
        i += 1;
    }
    table
}

#[cfg(test)]
mod tests {
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
}
