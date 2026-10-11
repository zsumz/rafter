//! Two-pass v1 frame emission over a borrowed, immutable application image.
//!
//! The begin record carries the image's length and checksum before any image
//! bytes, so the first pass measures the image without retaining it. The second pass
//! emits it while checksumming the frame. Only fixed-size framing is retained;
//! the publication layer owns buffering, durability, and fault injection.

use std::{
    convert::Infallible,
    io::{self, Write},
};

use rafter::LogIndex;

use crate::{
    adapter::codec::{self, Output, SnapshotEncodingError},
    Ledger,
};

use super::{
    format::{
        crc32, Checksum, BEGIN_LEN, BEGIN_MAGIC, COMMIT_LEN, COMMIT_MAGIC, JOURNAL_FORMAT_VERSION,
    },
    LedgerStoreError,
};

pub(super) struct FramePlan<'a> {
    ledger: &'a Ledger,
    at: LogIndex,
    begin: [u8; BEGIN_LEN],
    len: u64,
}

impl<'a> FramePlan<'a> {
    /// The borrow keeps both passes on exactly the same model state.
    pub(super) fn new(ledger: &'a Ledger, at: LogIndex) -> Result<Self, LedgerStoreError> {
        let mut image = ImageDigest {
            len: 0,
            checksum: Checksum::new(),
        };
        match codec::encode_snapshot_with(&mut image, at.0, ledger) {
            Ok(()) => {}
            Err(SnapshotEncodingError::Codec(error)) => return Err(LedgerStoreError::Image(error)),
            Err(SnapshotEncodingError::Sink(never)) => match never {},
        }
        let image_len = u32::try_from(image.len)
            .map_err(|_| LedgerStoreError::ImageTooLarge { length: image.len })?;
        Ok(Self {
            ledger,
            at,
            begin: begin_record(image_len, image.checksum.value()),
            len: image.len + BEGIN_LEN as u64 + COMMIT_LEN as u64,
        })
    }

    pub(super) const fn len(&self) -> u64 {
        self.len
    }

    pub(super) fn write_to(&self, sink: &mut impl Write) -> Result<(), SnapshotEncodingError> {
        let mut checked = Checked {
            sink,
            checksum: Checksum::new(),
        };
        checked
            .write_all(&self.begin)
            .map_err(SnapshotEncodingError::Sink)?;
        codec::encode_snapshot_stream(&mut checked, self.at.0, self.ledger)?;
        let commit = commit_record(checked.checksum.value());
        checked
            .sink
            .write_all(&commit)
            .map_err(SnapshotEncodingError::Sink)
    }
}

struct ImageDigest {
    len: u64,
    checksum: Checksum,
}
impl Output for ImageDigest {
    type Error = Infallible;
    fn emit(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        // The grammar checks both collection counts fit u32 before emission.
        // Even maximal counts and maximal cached records fit well within u64.
        self.len += bytes.len() as u64;
        self.checksum.update(bytes);
        Ok(())
    }
}

struct Checked<'a, W> {
    sink: &'a mut W,
    checksum: Checksum,
}
impl<W: Write> Write for Checked<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let accepted = self.sink.write(bytes)?;
        self.checksum.update(&bytes[..accepted]);
        Ok(accepted)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.sink.flush()
    }
}

pub(super) fn begin_record(image_len: u32, image_checksum: u32) -> [u8; BEGIN_LEN] {
    let mut bytes = [0; BEGIN_LEN];
    bytes[..4].copy_from_slice(&BEGIN_MAGIC);
    bytes[4] = JOURNAL_FORMAT_VERSION;
    bytes[5..9].copy_from_slice(&image_len.to_be_bytes());
    bytes[9..13].copy_from_slice(&image_checksum.to_be_bytes());
    let checksum = crc32(&bytes[..13]);
    bytes[13..].copy_from_slice(&checksum.to_be_bytes());
    bytes
}

pub(super) fn commit_record(frame_checksum: u32) -> [u8; COMMIT_LEN] {
    let mut bytes = [0; COMMIT_LEN];
    bytes[..4].copy_from_slice(&COMMIT_MAGIC);
    bytes[4] = JOURNAL_FORMAT_VERSION;
    bytes[5..9].copy_from_slice(&frame_checksum.to_be_bytes());
    let checksum = crc32(&bytes[..9]);
    bytes[9..].copy_from_slice(&checksum.to_be_bytes());
    bytes
}
