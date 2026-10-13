//! Verify each frame, then decode its image without retaining encoded bytes.

use std::{
    io::{self, BufReader, Read, Seek, SeekFrom},
    path::Path,
};

use rafter::LogIndex;

use crate::{adapter::codec::decode_snapshot_stream, Ledger, LedgerConfig};

use super::{
    damage::TornTail,
    error::LedgerStoreError,
    format::{Checksum, BEGIN_LEN, HEADER_LEN},
    frame::verify_header,
    frame_input::{read_frame, BUFFER_BYTES},
};

/// Result of scanning a journal's bytes.
pub(super) struct JournalScan {
    /// Newest committed image, if the journal holds one.
    pub(super) image: Option<(Ledger, LogIndex)>,
    /// Number of committed frames.
    pub(super) committed_frames: u64,
    /// Byte length of the committed prefix; independent of address-space size.
    pub(super) committed_len: u64,
    /// Residue found after the committed prefix.
    pub(super) torn_tail: Option<TornTail>,
}

pub(super) fn scan_journal(
    reader: &mut (impl Read + Seek),
    length: u64,
    config: LedgerConfig,
    path: &Path,
) -> Result<JournalScan, LedgerStoreError> {
    let map_error = |source| LedgerStoreError::Io {
        operation: "read the ledger journal",
        path: path.to_path_buf(),
        source,
    };
    reader.seek(SeekFrom::Start(0)).map_err(map_error)?;
    let mut header = [0; HEADER_LEN];
    let present = usize::try_from(length.min(HEADER_LEN as u64)).expect("fixed header width");
    reader
        .read_exact(&mut header[..present])
        .map_err(map_error)?;
    verify_header(&header[..present], config)?;

    let mut offset = HEADER_LEN as u64;
    let mut image = None;
    let mut committed_frames = 0_u64;
    let mut previous_index: Option<LogIndex> = None;

    let torn_tail = loop {
        if offset == length {
            break None;
        }
        let frame = match read_frame(reader, length - offset).map_err(map_error)? {
            Ok(frame) => frame,
            Err(tail) => break Some(tail),
        };
        // Preserve refusal/repair classification: all physical checksums pass
        // before the schema or model can return a different kind of error.
        reader
            .seek(SeekFrom::Start(offset + BEGIN_LEN as u64))
            .map_err(map_error)?;
        let mut source = CheckedRead {
            reader: reader.take(frame.image_len),
            checksum: Checksum::new(),
        };
        let decoded = decode_snapshot_stream(
            &mut BufReader::with_capacity(BUFFER_BYTES, &mut source),
            frame.image_len,
            config,
            map_error,
        )?;
        // Exclusive directory ownership is required. Also detect an image
        // changing across the two reads instead of adopting unchecked bytes.
        if source.checksum.value() != frame.image_crc {
            return Err(map_error(io::Error::new(
                io::ErrorKind::InvalidData,
                "ledger image changed during recovery",
            )));
        }
        let (applied_index, ledger) = decoded;
        let applied_index = LogIndex(applied_index);
        if let Some(previous) = previous_index {
            if applied_index < previous {
                return Err(LedgerStoreError::NonMonotonicAppliedIndex {
                    previous,
                    found: applied_index,
                });
            }
        }
        previous_index = Some(applied_index);
        image = Some((ledger, applied_index));
        committed_frames += 1;
        offset += frame.len;
        reader.seek(SeekFrom::Start(offset)).map_err(map_error)?;
    };
    Ok(JournalScan {
        image,
        committed_frames,
        committed_len: offset,
        torn_tail,
    })
}

struct CheckedRead<R> {
    reader: R,
    checksum: Checksum,
}
impl<R: Read> Read for CheckedRead<R> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let read = self.reader.read(bytes)?;
        self.checksum.update(&bytes[..read]);
        Ok(read)
    }
}

#[cfg(test)]
#[path = "scan_tests.rs"]
mod tests;
