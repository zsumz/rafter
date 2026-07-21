//! Trailing CRC32 creation and verification for storage envelopes.

use crate::checksum::crc32;

use super::Writer;

const CHECKSUM_LEN: usize = 4;

/// Structural failures shared by checksummed storage envelopes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ChecksumError {
    /// The input is too short to contain its trailing checksum.
    UnexpectedEof { needed: usize, remaining: usize },
    /// The stored checksum does not cover the preceding bytes.
    Mismatch { expected: u32, actual: u32 },
}

/// Appends a big-endian CRC32 over every byte currently in `writer`.
pub(crate) fn finish_checksummed(mut writer: Writer) -> Vec<u8> {
    let checksum = crc32(writer.as_slice());
    writer.u32(checksum);
    writer.finish()
}

/// Verifies and removes one trailing big-endian CRC32 field.
pub(crate) fn verify_checksum(input: &[u8]) -> Result<&[u8], ChecksumError> {
    let checksum_offset =
        input
            .len()
            .checked_sub(CHECKSUM_LEN)
            .ok_or(ChecksumError::UnexpectedEof {
                needed: CHECKSUM_LEN,
                remaining: input.len(),
            })?;
    let body = &input[..checksum_offset];
    let checksum = &input[checksum_offset..];
    let expected = u32::from_be_bytes([checksum[0], checksum[1], checksum[2], checksum[3]]);
    let actual = crc32(body);
    if expected == actual {
        Ok(body)
    } else {
        Err(ChecksumError::Mismatch { expected, actual })
    }
}
