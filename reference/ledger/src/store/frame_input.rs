//! Verify a frame with fixed buffers before any application decoding.
//!
//! The order matches the whole-slice test oracle: zeros, identity, seal,
//! version, begin checksum, image checksum, then commit checks. I/O errors
//! remain errors, never evidence of a tail that recovery may truncate.

use std::io::{self, Read};

use super::{
    damage::TornTail,
    format::{
        crc32, read_u32, Checksum, BEGIN_LEN, COMMIT_LEN, COMMIT_MAGIC, JOURNAL_FORMAT_VERSION,
        SEALED_FRAME_MARK, UNSEALED_FRAME_MARK,
    },
    frame::verify_identity,
};

pub(super) const BUFFER_BYTES: usize = 16 * 1024;

#[derive(Debug)]
pub(super) struct Frame {
    pub(super) image_len: u64,
    pub(super) image_crc: u32,
    pub(super) len: u64,
}

pub(super) fn read_frame(
    reader: &mut impl Read,
    available: u64,
) -> io::Result<Result<Frame, TornTail>> {
    let mut begin = [0; BEGIN_LEN];
    let present = usize::try_from(available.min(BEGIN_LEN as u64)).expect("fixed header width");
    reader.read_exact(&mut begin[..present])?;
    if begin[..present].iter().all(|byte| *byte == 0)
        && zeros_to_end(reader, available - present as u64)?
    {
        return Ok(Err(TornTail::ZeroFilledToEnd { present: available }));
    }
    if let Err(tail) = verify_identity(&begin[..present]) {
        return Ok(Err(tail));
    }
    let unsealed = begin[0] == UNSEALED_FRAME_MARK;
    // Restore only the fixed begin record, so both checksums see the sealed
    // value without copying the image or the rest of the journal.
    begin[0] = SEALED_FRAME_MARK;
    let verdict = read_sealed(reader, &begin[..present], available)?;
    Ok(if unsealed {
        Err(match verdict {
            Ok(frame) => TornTail::UnsealedCompleteFrame { len: frame.len },
            Err(TornTail::UnsupportedFrameVersion { version }) => {
                TornTail::UnsupportedFrameVersion { version }
            }
            Err(_) => TornTail::UnsealedAppend { present: available },
        })
    } else {
        verdict
    })
}

fn zeros_to_end(reader: &mut impl Read, mut remaining: u64) -> io::Result<bool> {
    let mut buffer = [0; BUFFER_BYTES];
    while remaining != 0 {
        let count = chunk_len(remaining);
        reader.read_exact(&mut buffer[..count])?;
        if buffer[..count].iter().any(|byte| *byte != 0) {
            return Ok(false);
        }
        remaining -= count as u64;
    }
    Ok(true)
}

fn read_sealed(
    reader: &mut impl Read,
    begin: &[u8],
    available: u64,
) -> io::Result<Result<Frame, TornTail>> {
    if let Some(&version) = begin.get(4) {
        if version != JOURNAL_FORMAT_VERSION {
            return Ok(Err(TornTail::UnsupportedFrameVersion { version }));
        }
    }
    if begin.len() != BEGIN_LEN {
        return Ok(Err(TornTail::PartialBeginRecord));
    }
    if read_u32(&begin[13..17]) != crc32(&begin[..13]) {
        return Ok(Err(TornTail::BeginRecordCorrupt));
    }
    let image_len = u64::from(read_u32(&begin[5..9]));
    let commit_start = BEGIN_LEN as u64 + image_len;
    if available < commit_start {
        return Ok(Err(TornTail::PartialImage));
    }
    let image_crc = read_u32(&begin[9..13]);
    let mut frame_checksum = Checksum::new();
    frame_checksum.update(begin);
    let mut image_checksum = Checksum::new();
    let mut buffer = [0; BUFFER_BYTES];
    let mut remaining = image_len;
    while remaining != 0 {
        let count = chunk_len(remaining);
        reader.read_exact(&mut buffer[..count])?;
        image_checksum.update(&buffer[..count]);
        frame_checksum.update(&buffer[..count]);
        remaining -= count as u64;
    }
    if image_checksum.value() != image_crc {
        return Ok(Err(TornTail::ImageCorrupt));
    }
    let commit_bytes = available - commit_start;
    if commit_bytes == 0 {
        return Ok(Err(TornTail::MissingCommitRecord));
    }
    if commit_bytes < COMMIT_LEN as u64 {
        return Ok(Err(TornTail::PartialCommitRecord));
    }
    let mut commit = [0; COMMIT_LEN];
    reader.read_exact(&mut commit)?;
    if commit[..4] != COMMIT_MAGIC
        || commit[4] != JOURNAL_FORMAT_VERSION
        || read_u32(&commit[9..13]) != crc32(&commit[..9])
        || read_u32(&commit[5..9]) != frame_checksum.value()
    {
        return Ok(Err(TornTail::CommitRecordCorrupt));
    }
    Ok(Ok(Frame {
        image_len,
        image_crc,
        len: commit_start + COMMIT_LEN as u64,
    }))
}

fn chunk_len(remaining: u64) -> usize {
    usize::try_from(remaining.min(BUFFER_BYTES as u64)).expect("fixed buffer width")
}
