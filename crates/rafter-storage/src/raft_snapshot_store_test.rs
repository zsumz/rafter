//! Current-snapshot publication, reopening, and payload-source scenarios.

use std::fs;

use rafter::{RaftSnapshot, SnapshotChunkRequest, SnapshotChunkSource};
use rafter_invariant_test::{oracle_assert, oracle_assert_eq};

use super::*;
use crate::{
    crc32, encode_raft_snapshot, DecodeRaftSnapshotError, RaftSnapshotManifestDecodeError,
};

use super::test_support::{
    assert_current_snapshot, descriptor, read_current_payload, remove_test_dir, snapshot,
    test_store_dir,
};

#[test]
fn in_memory_snapshot_store_returns_latest_snapshot() {
    let first = snapshot(3, 2, b"first");
    let second = snapshot(5, 4, b"second");
    let mut store = InMemoryRaftSnapshotStore::new();

    store.write_snapshot(first).expect("first snapshot writes");
    store
        .write_snapshot(second.clone())
        .expect("second snapshot writes");

    assert_eq!(store.current(), Some(&second));
}

#[test]
fn file_snapshot_store_reopens_manifest_selected_snapshot() {
    let directory = test_store_dir("reopen");
    let expected = snapshot(3, 2, b"application snapshot");
    {
        let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
        store
            .write_snapshot(expected.clone())
            .expect("snapshot writes");
    }

    let reopened = FileRaftSnapshotStore::open(&directory).expect("store reopens");

    oracle_assert_eq!(reopened.current_snapshot(), Some(descriptor(&expected)));
    oracle_assert_eq!(
        read_current_payload(&reopened).as_deref(),
        Some(expected.application_payload.as_slice())
    );
    oracle_assert!(reopened.current_snapshot_file_name().is_some());
    remove_test_dir(directory);
}

#[test]
fn file_snapshot_store_retains_previous_complete_snapshots() {
    let directory = test_store_dir("retains-previous");
    let first = snapshot(3, 2, b"first");
    let second = snapshot(5, 4, b"second");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");

    store
        .write_snapshot(first.clone())
        .expect("first snapshot writes");
    let first_file = store
        .current_snapshot_file_name()
        .expect("first snapshot file is current")
        .to_string();
    store
        .write_snapshot(second.clone())
        .expect("second snapshot writes");
    let second_file = store
        .current_snapshot_file_name()
        .expect("second snapshot file is current")
        .to_string();

    assert_ne!(first_file, second_file);
    assert!(directory.join(&first_file).exists());
    assert!(directory.join(&second_file).exists());
    assert_current_snapshot(
        &FileRaftSnapshotStore::open(&directory).expect("store reopens"),
        &second,
    );
    remove_test_dir(directory);
}

#[test]
fn file_snapshot_store_ignores_unmanifested_complete_snapshot_on_open() {
    let directory = test_store_dir("unmanifested");
    fs::create_dir_all(&directory).expect("test directory creates");
    let unmanifested = snapshot(3, 2, b"unmanifested");
    fs::write(
        directory.join("snapshot-1-3-2-1.rfsn"),
        encode_raft_snapshot(&unmanifested).expect("unmanifested snapshot encodes"),
    )
    .expect("unmanifested snapshot writes");

    let store = FileRaftSnapshotStore::open(&directory).expect("store opens");

    oracle_assert_eq!(store.current_snapshot(), None);
    remove_test_dir(directory);
}

#[test]
fn file_snapshot_store_ignores_abandoned_snapshot_and_manifest_temps() {
    let directory = test_store_dir("abandoned-temps");
    let store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    fs::write(
        store.temp_snapshot_path(),
        encode_raft_snapshot(&snapshot(3, 2, b"abandoned")).expect("abandoned snapshot encodes"),
    )
    .expect("abandoned snapshot temp writes");
    fs::write(store.temp_manifest_path(), b"abandoned manifest temp")
        .expect("abandoned manifest temp writes");
    drop(store);

    let reopened = FileRaftSnapshotStore::open(&directory).expect("store reopens");

    assert_eq!(reopened.current_snapshot(), None);
    remove_test_dir(directory);
}

#[test]
fn file_snapshot_store_rejects_missing_manifest_selected_snapshot() {
    let directory = test_store_dir("missing-selected");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    store
        .write_snapshot(snapshot(3, 2, b"present"))
        .expect("snapshot writes");
    let selected = store
        .current_snapshot_file_name()
        .expect("snapshot file is selected")
        .to_string();
    fs::remove_file(directory.join(selected)).expect("selected snapshot is removed");

    assert!(matches!(
        FileRaftSnapshotStore::open(&directory),
        Err(OpenRaftSnapshotStoreError::MissingSnapshot { .. })
    ));
    remove_test_dir(directory);
}

#[cfg(unix)]
#[test]
fn snapshot_open_does_not_treat_manifest_metadata_failure_as_absence() {
    use std::os::unix::fs::symlink;

    let directory = test_store_dir("manifest-metadata-failure");
    fs::create_dir_all(&directory).expect("test directory creates");
    let manifest_path = directory.join("current.snapshot");
    symlink(&manifest_path, &manifest_path).expect("self-referential manifest symlink creates");

    let result = FileRaftSnapshotStore::open(&directory);
    fs::remove_file(&manifest_path).expect("test symlink removes");

    assert!(matches!(
        result,
        Err(OpenRaftSnapshotStoreError::Io {
            operation: "open raft snapshot manifest",
            ..
        })
    ));
    remove_test_dir(directory);
}

#[cfg(unix)]
#[test]
fn snapshot_open_distinguishes_selected_file_metadata_failure_from_missing() {
    use std::os::unix::fs::symlink;

    let directory = test_store_dir("selected-metadata-failure");
    let selected_path = {
        let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
        store
            .write_snapshot(snapshot(3, 2, b"present"))
            .expect("snapshot writes");
        directory.join(
            store
                .current_snapshot_file_name()
                .expect("snapshot file is selected"),
        )
    };
    fs::remove_file(&selected_path).expect("selected snapshot removes");
    symlink(&selected_path, &selected_path)
        .expect("self-referential selected snapshot symlink creates");

    let result = FileRaftSnapshotStore::open(&directory);
    fs::remove_file(&selected_path).expect("test symlink removes");

    assert!(matches!(
        result,
        Err(OpenRaftSnapshotStoreError::Io {
            operation: "stat raft snapshot",
            ..
        })
    ));
    remove_test_dir(directory);
}

#[test]
fn file_snapshot_store_rejects_corrupt_manifest_selected_snapshot() {
    let directory = test_store_dir("corrupt-selected");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    store
        .write_snapshot(snapshot(3, 2, b"present"))
        .expect("snapshot writes");
    let selected = store
        .current_snapshot_file_name()
        .expect("snapshot file is selected")
        .to_string();
    let path = directory.join(selected);
    let mut bytes = fs::read(&path).expect("snapshot reads");
    let corrupt_byte = bytes.len() - 5;
    bytes[corrupt_byte] ^= 0xFF;
    fs::write(path, bytes).expect("corrupt snapshot writes");

    assert!(matches!(
        FileRaftSnapshotStore::open(&directory),
        Err(OpenRaftSnapshotStoreError::Snapshot(
            DecodeRaftSnapshotError::EnvelopeChecksumMismatch { .. }
        ))
    ));
    remove_test_dir(directory);
}

fn chunk_request(
    snapshot: &PersistedRaftSnapshot,
    offset: u64,
    len: u32,
) -> SnapshotChunkRequest<'_> {
    SnapshotChunkRequest {
        transfer_id: RaftSnapshot::from_payload(
            snapshot.metadata.clone(),
            &snapshot.application_payload,
        )
        .transfer_id(),
        metadata: &snapshot.metadata,
        total_payload_len: snapshot.application_payload.len() as u64,
        application_payload_crc32: crate::crc32(&snapshot.application_payload),
        offset,
        len,
    }
}

#[test]
fn file_snapshot_store_serves_chunks_of_the_current_snapshot() {
    let directory = test_store_dir("chunk-source");
    let current = snapshot(3, 2, b"application snapshot");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    store
        .write_snapshot(current.clone())
        .expect("snapshot writes");

    assert_eq!(
        store.snapshot_chunk(chunk_request(&current, 0, 11)),
        Some(b"application".to_vec())
    );
    assert_eq!(
        store.snapshot_chunk(chunk_request(&current, 12, 8)),
        Some(b"snapshot".to_vec())
    );
    remove_test_dir(directory);
}

#[test]
fn in_memory_snapshot_store_serves_chunks_of_the_current_snapshot() {
    let current = snapshot(3, 2, b"application snapshot");
    let store = InMemoryRaftSnapshotStore::with_snapshot(current.clone());

    assert_eq!(
        store.snapshot_chunk(chunk_request(&current, 12, 8)),
        Some(b"snapshot".to_vec())
    );
}

#[test]
fn snapshot_store_chunk_source_rejects_other_transfers_and_invalid_ranges() {
    let current = snapshot(3, 2, b"application snapshot");
    let store = InMemoryRaftSnapshotStore::with_snapshot(current.clone());
    let other = snapshot(5, 4, b"a different snapshot payload");

    assert_eq!(store.snapshot_chunk(chunk_request(&other, 0, 4)), None);
    let mut wrong_length = chunk_request(&current, 0, 4);
    wrong_length.total_payload_len += 1;
    assert_eq!(store.snapshot_chunk(wrong_length), None);
    let mut wrong_checksum = chunk_request(&current, 0, 4);
    wrong_checksum.application_payload_crc32 ^= 0xff;
    assert_eq!(store.snapshot_chunk(wrong_checksum), None);
    assert_eq!(store.snapshot_chunk(chunk_request(&current, 12, 9)), None);
    assert_eq!(store.snapshot_chunk(chunk_request(&current, 21, 1)), None);
    assert_eq!(
        InMemoryRaftSnapshotStore::new().snapshot_chunk(chunk_request(&current, 0, 4)),
        None
    );
}

#[test]
fn in_memory_snapshot_store_rejects_streamed_snapshot_payload_checksum_mismatch() {
    struct WrongPayloadSource(Vec<u8>);
    impl SnapshotChunkSource for WrongPayloadSource {
        fn snapshot_chunk(&self, request: SnapshotChunkRequest<'_>) -> Option<Vec<u8>> {
            let start = usize::try_from(request.offset).ok()?;
            let end = start.checked_add(request.len as usize)?;
            self.0.get(start..end).map(<[u8]>::to_vec)
        }
    }

    let expected = b"application snapshot";
    let actual = b"Application snapshot";
    assert_eq!(expected.len(), actual.len());
    let requested = RaftSnapshot::from_payload(snapshot(3, 2, expected).metadata, expected);
    let source = WrongPayloadSource(actual.to_vec());
    let mut store = InMemoryRaftSnapshotStore::new();

    assert_eq!(
        store.write_snapshot_from_source(&requested, &source),
        Err(
            RaftSnapshotStoreWriteError::SnapshotPayloadChecksumMismatch {
                expected: requested.application_payload_crc32,
                actual: crc32(actual),
            }
        )
    );
    assert_eq!(store.current_snapshot(), None);
}

#[test]
fn file_snapshot_store_rejects_corrupt_manifest() {
    let directory = test_store_dir("corrupt-manifest");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    store
        .write_snapshot(snapshot(3, 2, b"present"))
        .expect("snapshot writes");
    let manifest = directory.join("current.snapshot");
    let mut bytes = fs::read(&manifest).expect("manifest reads");
    bytes[0] ^= 0xFF;
    fs::write(manifest, bytes).expect("corrupt manifest writes");

    assert!(matches!(
        FileRaftSnapshotStore::open(&directory),
        Err(OpenRaftSnapshotStoreError::Manifest(
            RaftSnapshotManifestDecodeError::ManifestChecksumMismatch { .. }
        ))
    ));
    remove_test_dir(directory);
}

#[test]
fn file_store_serves_chunks_by_positioned_reads_after_reopen() {
    let directory = test_store_dir("chunks-after-reopen");
    // A few hundred KiB, so the payload spans more than one stream chunk.
    let payload: Vec<u8> = (0_u32..80 * 1024).flat_map(u32::to_be_bytes).collect();
    let current = snapshot(3, 2, &payload);
    {
        let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
        store
            .write_snapshot(current.clone())
            .expect("snapshot writes");
    }

    let reopened = FileRaftSnapshotStore::open(&directory).expect("store reopens");

    assert_eq!(
        reopened.snapshot_chunk(chunk_request(&current, 8, 32)),
        Some(payload[8..40].to_vec())
    );
    let tail = payload.len() - 4096;
    assert_eq!(
        reopened.snapshot_chunk(chunk_request(&current, tail as u64, 4096)),
        Some(payload[tail..].to_vec())
    );
    assert_eq!(
        reopened.snapshot_chunk(chunk_request(&current, payload.len() as u64 - 1, 2)),
        None
    );
    remove_test_dir(directory);
}
