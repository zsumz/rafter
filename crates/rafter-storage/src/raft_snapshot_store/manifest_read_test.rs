//! Bounded RFSM admission preserves format errors and authoritative selection.
use super::*;
use crate::{crc32, format::v1::snapshot_manifest::decode_manifest};
use std::io::{self, Cursor};

fn signed_body(body: &[u8]) -> Vec<u8> {
    let mut bytes = body.to_vec();
    bytes.extend_from_slice(&crc32(body).to_be_bytes());
    bytes
}

fn encoded_name(name: &str, tail: usize) -> Vec<u8> {
    let bytes = encode_manifest(&SnapshotManifest {
        sequence: u64::MAX,
        file_name: name.into(),
    })
    .unwrap();
    let mut body = bytes[..bytes.len() - 4].to_vec();
    body.resize(body.len() + tail, 0);
    signed_body(&body)
}

#[test]
fn oversized_snapshot_manifest_uses_bounded_read_requests() {
    let bytes = encoded_name("snapshot.rfsn", 256 * 1024);
    let mut reader = BudgetReader(Cursor::new(bytes));
    assert!(matches!(
        read_manifest_from(&mut reader),
        Err(ReadManifestError::Decode(
            RaftSnapshotManifestDecodeError::TrailingBytes(262_144)
        ))
    ));
    assert_eq!(reader.0.position(), reader.0.get_ref().len() as u64);
}

struct BudgetReader(Cursor<Vec<u8>>);
impl Read for BudgetReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        assert!(
            out.len() <= 4096,
            "unbounded snapshot-manifest read request"
        );
        self.0.read(out)
    }
}

fn assert_matches_slice(bytes: &[u8]) {
    let expected = decode_manifest(bytes);
    for chunk in [1, 3, 4, 7, 4096] {
        let reader = ChunkedReader {
            inner: Cursor::new(bytes),
            chunk,
            interrupt: false,
        };
        let actual = match read_manifest_from(reader) {
            Ok(manifest) => Ok(manifest),
            Err(ReadManifestError::Decode(error)) => Err(error),
            Err(ReadManifestError::Io(error)) => panic!("unexpected I/O: {error}"),
        };
        assert_eq!(actual, expected, "length {}, chunk {chunk}", bytes.len());
    }
}

#[test]
fn snapshot_manifest_stream_preserves_truncation_corruption_and_filename_errors() {
    for name in [
        "snapshot.rfsn",
        "",
        ".",
        "..",
        "../snapshot",
        "dir\\snapshot",
        "é.rfsn",
    ] {
        let bytes = encoded_name(name, 0);
        for end in 0..=bytes.len() {
            assert_matches_slice(&bytes[..end]);
        }
        for end in 0..bytes.len() - 4 {
            assert_matches_slice(&signed_body(&bytes[..end]));
        }
        for offset in 0..bytes.len() {
            let mut corrupt = bytes.clone();
            corrupt[offset] ^= 0x80;
            assert_matches_slice(&corrupt);
            let body_len = corrupt.len() - 4;
            assert_matches_slice(&signed_body(&corrupt[..body_len]));
        }
    }
}

#[test]
fn snapshot_manifest_stream_accepts_maximum_names_and_counts_the_full_suffix() {
    for len in [1, 65_534, 65_535] {
        let name = "a".repeat(len);
        for tail in [0, 1, 2, 4095, 4096, 4097, 65_550, 65_551] {
            let bytes = encoded_name(&name, tail);
            assert_matches_slice(&bytes);
        }
    }
    // Schema errors retain priority even when bytes beyond the maximum prefix
    // were verified and discarded. The checksum remains the first admission gate.
    for name in ["../bad", "dir\\bad", "", ".", ".."] {
        let bytes = encoded_name(name, 70_000);
        assert_matches_slice(&bytes);
    }
    let bytes = encoded_name("valid.rfsn", 70_000);
    for offset in [0, 4, 15] {
        let mut invalid = bytes.clone();
        invalid[offset] = 0xff;
        assert_matches_slice(&invalid);
        let body_len = invalid.len() - 4;
        assert_matches_slice(&signed_body(&invalid[..body_len]));
    }
}

#[test]
fn snapshot_manifest_stream_preserves_io_errors_including_the_eof_probe() {
    let bytes = encoded_name("snapshot.rfsn", 0);
    for cut in 0..=bytes.len() {
        let reader = (&bytes[..cut]).chain(FailingRead);
        let ReadManifestError::Io(error) = read_manifest_from(reader).unwrap_err() else {
            panic!("read failure hidden at {cut}");
        };
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(error.to_string(), "injected snapshot-manifest read failure");
    }
}

#[test]
fn oversized_snapshot_manifest_preserves_selection_and_pending_recovery_files() {
    use super::super::{
        test_support::{
            assert_current_snapshot, remove_test_dir, snapshot, staged_chunk, test_store_dir,
        },
        FileRaftSnapshotStore, RaftSnapshotStore,
    };
    use std::{collections::BTreeMap, fs};
    let directory = test_store_dir("oversized-current-manifest");
    let selected = snapshot(5, 4, b"selected");
    {
        let mut store = FileRaftSnapshotStore::open(&directory).unwrap();
        store.write_snapshot(snapshot(3, 2, b"obsolete")).unwrap();
        store.write_snapshot(selected.clone()).unwrap();
        store
            .stage_snapshot_chunk(&staged_chunk(0, b"part", 64))
            .unwrap();
    }
    // This incomplete body would normally trigger cleanup during successful open.
    fs::write(directory.join("pending.snapshot-transfer.body"), b"x").unwrap();
    let path = manifest_path(&directory);
    let valid = fs::read(&path).unwrap();
    let mut body = valid[..valid.len() - 4].to_vec();
    body.resize(body.len() + 8 * 1024 * 1024, 0);
    fs::write(&path, signed_body(&body)).unwrap();
    let inventory = || -> BTreeMap<_, _> {
        fs::read_dir(&directory)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                (entry.file_name(), fs::read(entry.path()).unwrap())
            })
            .collect()
    };
    let before = inventory();
    for _ in 0..2 {
        assert!(matches!(
            FileRaftSnapshotStore::open(&directory),
            Err(OpenRaftSnapshotStoreError::Manifest(
                RaftSnapshotManifestDecodeError::TrailingBytes(8_388_608)
            ))
        ));
        assert_eq!(inventory(), before);
    }
    fs::write(&path, valid).unwrap();
    let reopened = FileRaftSnapshotStore::open(&directory).unwrap();
    assert_current_snapshot(&reopened, &selected);
    assert!(!directory.join("pending.snapshot-transfer").exists());
    assert!(!directory.join("pending.snapshot-transfer.body").exists());
    remove_test_dir(directory);
}

struct ChunkedReader<'a> {
    inner: Cursor<&'a [u8]>,
    chunk: usize,
    interrupt: bool,
}
impl Read for ChunkedReader<'_> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        self.interrupt = !self.interrupt;
        if self.interrupt {
            return Err(io::ErrorKind::Interrupted.into());
        }
        let len = out.len().min(self.chunk);
        self.inner.read(&mut out[..len])
    }
}
struct FailingRead;
impl Read for FailingRead {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "injected snapshot-manifest read failure",
        ))
    }
}
