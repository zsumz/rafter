//! Grow checkpoint entry buffers only after their bytes arrive.
use crate::checksum::RunningCrc32;
use std::io::{self, BufRead, ErrorKind};

pub(super) fn read_tracked_into(
    file: &mut impl BufRead,
    crc: &mut RunningCrc32,
    size: usize,
    bytes: &mut Vec<u8>,
) -> io::Result<()> {
    bytes.clear();
    while bytes.len() < size {
        let available = match file.fill_buf() {
            Ok(available) => available,
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        if available.is_empty() {
            return Err(io::Error::new(
                ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ));
        }
        let count = available.len().min(size - bytes.len());
        let required = bytes.len() + count;
        if required > bytes.capacity() {
            // Geometric growth amortizes copies, but neither trusts unread
            // bytes nor reserves beyond the complete encoded entry's size.
            let capacity = size.min(required.saturating_mul(2));
            bytes.reserve_exact(capacity - bytes.len());
        }
        bytes.extend_from_slice(&available[..count]);
        file.consume(count);
    }
    crc.update(bytes);
    Ok(())
}

#[cfg(test)]
#[path = "checkpoint_reader_test.rs"]
mod tests;
