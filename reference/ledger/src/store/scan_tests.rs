//! Bounded reads, physical-error precedence, and refusal of unstable input.

use std::{
    io::{self, Cursor, Read, Seek, SeekFrom},
    path::Path,
};

use rafter::LogIndex;

use super::scan_journal;
use crate::store::{
    format::{crc32, BEGIN_LEN, HEADER_LEN},
    frame::encode_header,
    frame_input::read_frame,
    frame_output::{begin_record, commit_record},
    LedgerStoreError, TornTail,
};
use crate::{LedgerCodecError, LedgerConfig, SnapshotError};

const PATH: &str = "fixture/ledger.journal";

// Independent v1 grammar: zero-balance accounts, no sessions, zero deposits.
fn image(at: u64, accounts: u32) -> Vec<u8> {
    let mut bytes = vec![1];
    bytes.extend_from_slice(&at.to_be_bytes());
    bytes.extend_from_slice(&0_u128.to_be_bytes());
    bytes.extend_from_slice(&accounts.to_be_bytes());
    for id in 0..u64::from(accounts) {
        bytes.extend_from_slice(&id.to_be_bytes());
        bytes.extend_from_slice(&0_u64.to_be_bytes());
    }
    bytes.extend_from_slice(&0_u32.to_be_bytes());
    bytes
}

fn frame(image: &[u8]) -> Vec<u8> {
    let mut bytes = begin_record(u32::try_from(image.len()).unwrap(), crc32(image)).to_vec();
    bytes.extend_from_slice(image);
    bytes.extend_from_slice(&commit_record(crc32(&bytes)));
    bytes
}

fn journal(config: LedgerConfig, payload: &[u8]) -> Vec<u8> {
    let mut bytes = encode_header(config);
    bytes.extend_from_slice(&frame(payload));
    bytes
}

#[derive(Clone, Copy)]
enum DecodeFault {
    None,
    Seek,
    Read,
    ChangedImage,
}

struct Reader {
    inner: Cursor<Vec<u8>>,
    max_request: usize,
    total: u64,
    calls: usize,
    partial: bool,
    fail_at: Option<u64>,
    decode_fault: DecodeFault,
    decoding: bool,
}
impl Reader {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            inner: Cursor::new(bytes),
            max_request: 0,
            total: 0,
            calls: 0,
            partial: false,
            fail_at: None,
            decode_fault: DecodeFault::None,
            decoding: false,
        }
    }
    fn scan(&mut self, config: LedgerConfig) -> Result<super::JournalScan, LedgerStoreError> {
        let length = self.inner.get_ref().len() as u64;
        scan_journal(self, length, config, Path::new(PATH))
    }
}
impl Read for Reader {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.max_request = self.max_request.max(bytes.len());
        assert!(bytes.len() <= 16 * 1024, "unbounded journal read");
        self.calls += 1;
        if self.partial && self.calls % 3 == 0 {
            return Err(io::ErrorKind::Interrupted.into());
        }
        if (self.decoding && matches!(self.decode_fault, DecodeFault::Read))
            || self.fail_at == Some(self.inner.position())
        {
            return Err(io::Error::other("fixture read failure"));
        }
        let mut count = bytes.len();
        if let Some(offset) = self.fail_at {
            count =
                count.min(usize::try_from(offset.saturating_sub(self.inner.position())).unwrap());
        }
        if self.partial {
            count = count.min(3);
        }
        let read = self.inner.read(&mut bytes[..count])?;
        self.total += read as u64;
        Ok(read)
    }
}
impl Seek for Reader {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        if position == SeekFrom::Start((HEADER_LEN + BEGIN_LEN) as u64) {
            if matches!(self.decode_fault, DecodeFault::Seek) {
                return Err(io::ErrorKind::PermissionDenied.into());
            }
            self.decoding = true;
            if matches!(self.decode_fault, DecodeFault::ChangedImage) {
                // A different, still schema-valid applied index on the second read.
                self.inner.get_mut()[HEADER_LEN + BEGIN_LEN + 8] ^= 1;
            }
        }
        self.inner.seek(position)
    }
}

#[test]
fn multi_mebibyte_images_and_multiple_frames_use_bounded_reads() {
    let config = LedgerConfig::new(1, 131_072).unwrap();
    let mut bytes = encode_header(config);
    let mut image_bytes = 0;
    for at in 1..=3 {
        let payload = image(at, 131_072);
        image_bytes += payload.len() as u64;
        bytes.extend_from_slice(&frame(&payload));
    }
    let length = bytes.len() as u64;
    assert!(length > 6 * 1024 * 1024);
    let mut reader = Reader::new(bytes);
    let scan = reader.scan(config).unwrap();
    assert_eq!(scan.committed_frames, 3);
    assert_eq!(scan.committed_len, length);
    assert_eq!(scan.torn_tail, None);
    let (ledger, at) = scan.image.unwrap();
    assert_eq!(at, LogIndex(3));
    assert_eq!(ledger.view().accounts.len(), 131_072);
    assert_eq!(reader.total, length + image_bytes);
    assert_eq!(reader.max_request, 16 * 1024);
}

#[test]
fn partial_and_interrupted_reads_preserve_complete_recovery() {
    let config = LedgerConfig::new(1, 4).unwrap();
    let mut reader = Reader::new(journal(config, &image(19, 4)));
    reader.partial = true;
    let scan = reader.scan(config).unwrap();
    assert_eq!(scan.image.unwrap().1, LogIndex(19));
    assert_eq!(scan.committed_frames, 1);
    assert_eq!(scan.torn_tail, None);
    assert!(reader.calls > 50);
}

#[test]
fn io_failures_during_verification_rewind_or_decode_never_become_repairable_tails() {
    let config = LedgerConfig::new(1, 4).unwrap();
    let bytes = journal(config, &image(19, 4));
    for mark in [b'R', 0] {
        let mut bytes = bytes.clone();
        bytes[HEADER_LEN] = mark;
        for stop in [
            1,
            HEADER_LEN + 1,
            HEADER_LEN + BEGIN_LEN + 1,
            bytes.len() - 1,
        ] {
            let mut reader = Reader::new(bytes.clone());
            reader.fail_at = Some(stop as u64);
            assert!(
                matches!(reader.scan(config), Err(LedgerStoreError::Io { path, source, .. })
            if path == Path::new(PATH) && source.kind() == io::ErrorKind::Other)
            );
        }
    }
    for fault in [DecodeFault::Seek, DecodeFault::Read] {
        let mut reader = Reader::new(bytes.clone());
        reader.decode_fault = fault;
        assert!(matches!(
            reader.scan(config),
            Err(LedgerStoreError::Io { .. })
        ));
    }
    let mut short = Cursor::new(&bytes[..bytes.len() - 1]);
    assert!(
        matches!(scan_journal(&mut short, bytes.len() as u64, config, Path::new(PATH)),
        Err(LedgerStoreError::Io { source, .. }) if source.kind() == io::ErrorKind::UnexpectedEof)
    );
}

#[test]
fn changed_bytes_between_verification_and_decoding_are_refused() {
    let config = LedgerConfig::new(1, 4).unwrap();
    let mut reader = Reader::new(journal(config, &image(18, 4)));
    reader.decode_fault = DecodeFault::ChangedImage;
    assert!(
        matches!(reader.scan(config), Err(LedgerStoreError::Io { source, .. })
        if source.kind() == io::ErrorKind::InvalidData)
    );
}

#[test]
fn physical_checksums_precede_semantic_decoding_and_resource_admission() {
    let config = LedgerConfig::new(1, 1).unwrap();
    for bad_version in [true, false] {
        let mut payload = image(1, if bad_version { 1 } else { 4 });
        if bad_version {
            payload[0] = 9;
        }
        let mut bytes = journal(config, &payload);
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        let scan = Reader::new(bytes.clone()).scan(config).unwrap();
        assert_eq!(scan.torn_tail, Some(TornTail::CommitRecordCorrupt));
        assert!(scan.image.is_none());
        bytes[last] ^= 1;
        let error = Reader::new(bytes).scan(config).err().unwrap();
        if bad_version {
            assert!(matches!(
                error,
                LedgerStoreError::Image(LedgerCodecError::UnsupportedSnapshotVersion {
                    version: 9
                })
            ));
        } else {
            assert!(matches!(
                error,
                LedgerStoreError::Snapshot(SnapshotError::TooManyAccounts)
            ));
        }
    }
    let mut bytes = journal(config, &image(1, 4));
    bytes[HEADER_LEN + BEGIN_LEN] = 9; // Invalid schema and invalid physical checksum.
    let scan = Reader::new(bytes).scan(config).unwrap();
    assert_eq!(scan.torn_tail, Some(TornTail::ImageCorrupt));
}

#[test]
fn zero_tails_are_scanned_to_the_end_and_io_failure_is_not_residue() {
    let config = LedgerConfig::new(1, 1).unwrap();
    let mut bytes = encode_header(config);
    bytes.resize(HEADER_LEN + 5 * 1024 * 1024, 0);
    let scan = Reader::new(bytes.clone()).scan(config).unwrap();
    assert_eq!(
        scan.torn_tail,
        Some(TornTail::ZeroFilledToEnd {
            present: 5 * 1024 * 1024
        })
    );
    let mut failed = Reader::new(bytes.clone());
    failed.fail_at = Some(100);
    assert!(matches!(
        failed.scan(config),
        Err(LedgerStoreError::Io { .. })
    ));
    *bytes.last_mut().unwrap() = 1;
    let scan = Reader::new(bytes).scan(config).unwrap();
    assert!(matches!(
        scan.torn_tail,
        Some(TornTail::NotALedgerFrame { .. })
    ));
}

#[test]
fn a_maximum_declared_image_length_does_not_allocate_or_read_missing_bytes() {
    let begin = begin_record(u32::MAX, 0);
    let mut reader = Reader::new(begin.to_vec());
    assert!(matches!(
        read_frame(&mut reader, BEGIN_LEN as u64).unwrap(),
        Err(TornTail::PartialImage)
    ));
    assert_eq!(reader.total, BEGIN_LEN as u64);
    assert_eq!(reader.calls, 1);
}
