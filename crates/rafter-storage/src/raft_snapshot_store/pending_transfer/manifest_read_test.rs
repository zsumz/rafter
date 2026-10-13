//! RFPT streaming admission preserves corruption precedence and resumable files.
use super::*;
use crate::format::v1::pending_transfer::{
    decode_pending_snapshot_transfer_manifest, encode_pending_snapshot_transfer_manifest,
    MAX_PENDING_SNAPSHOT_METADATA_ENVELOPE_BYTES,
};
use crate::raft_snapshot_store::test_support::transfer_metadata;
use crate::{crc32, encode_raft_snapshot, PersistedRaftSnapshot};
use rafter::{NodeId, RaftSnapshot};
use std::io::{self, Cursor};

fn valid_frame() -> Vec<u8> {
    let metadata = transfer_metadata();
    let manifest = PendingTransferManifest {
        leader_id: NodeId(1),
        transfer_id: RaftSnapshot::new(metadata.clone(), 64, 0).transfer_id(),
        metadata,
        total_payload_len: 64,
        application_payload_crc32: 0,
        received_payload_len: 4,
        body_checksum: crc32(b"part"),
    };
    encode_pending_snapshot_transfer_manifest(&manifest).unwrap()
}
fn signed_body(body: &[u8]) -> Vec<u8> {
    let mut bytes = body.to_vec();
    bytes.extend_from_slice(&crc32(body).to_be_bytes());
    bytes
}
fn with_tail(bytes: &[u8], tail: usize) -> Vec<u8> {
    let mut body = bytes[..bytes.len() - 4].to_vec();
    body.resize(body.len() + tail, 0);
    signed_body(&body)
}

#[test]
fn oversized_pending_manifest_uses_bounded_read_requests() {
    let bytes = with_tail(&valid_frame(), 256 * 1024);
    let mut reader = BudgetReader(Cursor::new(bytes));
    assert!(matches!(
        read_manifest_from(&mut reader),
        Err(ReadManifestError::Decode(
            DecodePendingSnapshotTransferError::TrailingBytes(262_144)
        ))
    ));
    assert_eq!(reader.0.position(), reader.0.get_ref().len() as u64);
}
struct BudgetReader(Cursor<Vec<u8>>);
impl Read for BudgetReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        assert!(out.len() <= 4096, "unbounded pending-manifest read request");
        self.0.read(out)
    }
}

fn assert_matches_slice(bytes: &[u8]) {
    let expected = decode_pending_snapshot_transfer_manifest(bytes);
    let chunks: &[usize] = if bytes.len() <= 8192 {
        &[1, 3, 4, 7, 4096]
    } else {
        &[4093, 4096]
    };
    for &chunk in chunks {
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
fn pending_manifest_stream_preserves_truncation_corruption_and_descriptor_errors() {
    let bytes = valid_frame();
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
        assert_matches_slice(&signed_body(&corrupt[..corrupt.len() - 4]));
    }
    for tail in [1, 4095, 4096, 4097, 4 * 1024 * 1024 + 1] {
        let bytes = with_tail(&bytes, tail);
        assert_matches_slice(&bytes);
        for offset in [0, 4, 13, 33, 45] {
            let mut bad = bytes.clone();
            bad[offset] = 0xff;
            assert_matches_slice(&bad);
            assert_matches_slice(&signed_body(&bad[..bad.len() - 4]));
        }
    }
}

#[test]
fn pending_manifest_stream_preserves_nested_metadata_limits_and_payload_refusal() {
    let valid = valid_frame();
    let nested_payload = encode_raft_snapshot(&PersistedRaftSnapshot {
        metadata: transfer_metadata(),
        application_payload: b"not metadata".to_vec(),
    })
    .unwrap();
    let at_limit = vec![0; usize::try_from(MAX_PENDING_SNAPSHOT_METADATA_ENVELOPE_BYTES).unwrap()];
    for (declared, nested) in [
        (nested_payload.len() as u64, nested_payload.as_slice()),
        (
            MAX_PENDING_SNAPSHOT_METADATA_ENVELOPE_BYTES,
            at_limit.as_slice(),
        ),
        (MAX_PENDING_SNAPSHOT_METADATA_ENVELOPE_BYTES + 1, &[][..]),
        (u64::MAX, &[][..]),
    ] {
        let mut body = valid[..45].to_vec();
        body.extend_from_slice(&declared.to_be_bytes());
        body.extend_from_slice(nested);
        let bytes = signed_body(&body);
        assert_matches_slice(&bytes);
        assert_matches_slice(&with_tail(&bytes, 4097));
    }
}

#[test]
fn pending_manifest_trailing_bytes_precede_progress_and_identity_errors() {
    for offset in [13, 33] {
        let mut invalid = valid_frame();
        invalid[offset] ^= 0x80;
        for tail in [1, 4 * 1024 * 1024 + 1] {
            let bytes = with_tail(&invalid, tail);
            let ReadManifestError::Decode(error) =
                read_manifest_from(Cursor::new(bytes)).unwrap_err()
            else {
                panic!("unexpected I/O error");
            };
            assert_eq!(
                error,
                DecodePendingSnapshotTransferError::TrailingBytes(tail)
            );
        }
    }
}

#[test]
fn pending_manifest_stream_preserves_io_errors_including_the_eof_probe() {
    let bytes = valid_frame();
    for cut in 0..=bytes.len() {
        let reader = (&bytes[..cut]).chain(FailingRead);
        let ReadManifestError::Io(error) = read_manifest_from(reader).unwrap_err() else {
            panic!("read failure hidden at {cut}");
        };
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(error.to_string(), "injected pending-manifest read failure");
    }
}

#[test]
fn oversized_pending_manifest_preserves_snapshot_and_resumable_transfer_files() {
    use crate::raft_snapshot_store::{
        test_support::{
            assert_current_snapshot, remove_test_dir, snapshot, staged_chunk, test_store_dir,
        },
        FileRaftSnapshotStore, RaftSnapshotStore,
    };
    use std::{collections::BTreeMap, fs};
    let directory = test_store_dir("oversized-pending-manifest");
    let selected = snapshot(5, 4, b"selected");
    {
        let mut store = FileRaftSnapshotStore::open(&directory).unwrap();
        store.write_snapshot(selected.clone()).unwrap();
        store
            .stage_snapshot_chunk(&staged_chunk(0, b"part", 64))
            .unwrap();
    }
    let path = directory.join("pending.snapshot-transfer");
    let valid = fs::read(&path).unwrap();
    fs::write(&path, with_tail(&valid, 8 * 1024 * 1024)).unwrap();
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
            Err(OpenRaftSnapshotStoreError::PendingTransfer(
                DecodePendingSnapshotTransferError::TrailingBytes(8_388_608)
            ))
        ));
        assert_eq!(inventory(), before);
    }
    fs::write(&path, valid).unwrap();
    let reopened = FileRaftSnapshotStore::open(&directory).unwrap();
    assert_current_snapshot(&reopened, &selected);
    assert_eq!(
        reopened
            .current_pending_snapshot_transfer()
            .unwrap()
            .received_len,
        4
    );
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
            "injected pending-manifest read failure",
        ))
    }
}
