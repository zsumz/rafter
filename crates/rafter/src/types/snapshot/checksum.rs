//! Dependency-free CRC-32 for snapshot payload descriptors.

const TABLES: [[u32; 256]; 8] = build_tables();

#[must_use]
pub(crate) fn crc32(bytes: &[u8]) -> u32 {
    let mut state = 0xFFFF_FFFFu32;
    let (blocks, tail) = bytes.as_chunks::<8>();
    for &[a, b, c, d, e, f, g, h] in blocks {
        let word = state ^ u32::from_le_bytes([a, b, c, d]);
        let [a, b, c, d] = word.to_le_bytes();
        state = TABLES[7][usize::from(a)]
            ^ TABLES[6][usize::from(b)]
            ^ TABLES[5][usize::from(c)]
            ^ TABLES[4][usize::from(d)]
            ^ TABLES[3][usize::from(e)]
            ^ TABLES[2][usize::from(f)]
            ^ TABLES[1][usize::from(g)]
            ^ TABLES[0][usize::from(h)];
    }
    for byte in tail {
        let index = ((state ^ u32::from(*byte)) & 0xFF) as usize;
        state = (state >> 8) ^ TABLES[0][index];
    }
    !state
}

const fn build_tables() -> [[u32; 256]; 8] {
    let mut tables = [[0; 256]; 8];
    let mut byte: u32 = 0;
    while byte < 256 {
        let mut crc = byte;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
            bit += 1;
        }
        tables[0][byte as usize] = crc;
        byte += 1;
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
