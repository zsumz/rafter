//! Slicing-by-eight IEEE CRC-32 over slices and incremental byte streams.
//!
//! This module owns the polynomial table and the byte scan that reads it, so
//! every caller shares one set of checksum bytes. It decides nothing about what
//! is checksummed or how a digest is framed on disk or on the wire.

const CRC32_TABLES: [[u32; 256]; 8] = build_crc32_tables();

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
        // Each table advances one byte through a different number of trailing
        // zero bytes. XORing their contributions processes eight bytes without
        // the scalar loop's chain of eight dependent lookups. Explicit little-
        // endian assembly makes the reflected polynomial independent of host
        // endianness and alignment; short slices retain the scalar path.
        let (blocks, tail) = bytes.as_chunks::<8>();
        for &[a, b, c, d, e, f, g, h] in blocks {
            let state = self.state ^ u32::from_le_bytes([a, b, c, d]);
            let [a, b, c, d] = state.to_le_bytes();
            self.state = CRC32_TABLES[7][usize::from(a)]
                ^ CRC32_TABLES[6][usize::from(b)]
                ^ CRC32_TABLES[5][usize::from(c)]
                ^ CRC32_TABLES[4][usize::from(d)]
                ^ CRC32_TABLES[3][usize::from(e)]
                ^ CRC32_TABLES[2][usize::from(f)]
                ^ CRC32_TABLES[1][usize::from(g)]
                ^ CRC32_TABLES[0][usize::from(h)];
        }
        for byte in tail {
            let index = ((self.state ^ u32::from(*byte)) & 0xFF) as usize;
            self.state = (self.state >> 8) ^ CRC32_TABLES[0][index];
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

const fn build_crc32_tables() -> [[u32; 256]; 8] {
    let mut tables = [[0; 256]; 8];
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
        tables[0][i as usize] = crc;
        i += 1;
    }
    let mut slice = 1;
    while slice < 8 {
        let mut byte = 0;
        while byte < 256 {
            let previous = tables[slice - 1][byte];
            tables[slice][byte] = (previous >> 8) ^ tables[0][(previous & 0xFF) as usize];
            byte += 1;
        }
        slice += 1;
    }
    tables
}
