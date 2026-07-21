//! Observable snapshot-open recovery and publication-sequence exhaustion scenarios.

use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, LogIndex,
    NodeId, RaftSnapshot, RaftSnapshotMetadata, SnapshotGroupId, StagedSnapshotChunk, Term,
};
use rafter_storage::{
    crc32, FileRaftSnapshotStore, PendingSnapshotTransferRecovery, PersistedRaftSnapshot,
    RaftSnapshotStore, RaftSnapshotStoreWriteError,
};

static TEST_DIRECTORY_ID: AtomicU64 = AtomicU64::new(0);

#[test]
fn open_report_records_directory_creation() {
    let directory = test_directory("created-directory");

    let opened = FileRaftSnapshotStore::open_with_report(&directory).expect("store opens");
    assert!(opened.report().created_directory);
    assert_eq!(opened.report().pending_transfer_recovery, None);
    drop(opened);

    let reopened = FileRaftSnapshotStore::open_with_report(&directory).expect("store reopens");
    assert!(!reopened.report().created_directory);
    remove_test_directory(directory);
}

#[test]
fn open_report_records_missing_pending_body_discard() {
    let directory = test_directory("missing-pending-body");
    stage_partial_transfer(&directory, b"abcdefgh", 4);
    fs::remove_file(directory.join("pending.snapshot-transfer.body")).expect("staged body removes");

    let opened = FileRaftSnapshotStore::open_with_report(&directory).expect("store recovers");
    let (store, report) = opened.into_parts();

    assert_eq!(
        report.pending_transfer_recovery,
        Some(PendingSnapshotTransferRecovery::DiscardedMissingBody)
    );
    assert!(store.current_pending_snapshot_transfer().is_none());
    assert!(!directory.join("pending.snapshot-transfer").exists());
    remove_test_directory(directory);
}

#[test]
fn open_report_records_short_pending_body_discard() {
    let directory = test_directory("short-pending-body");
    stage_partial_transfer(&directory, b"abcdefgh", 4);
    OpenOptions::new()
        .write(true)
        .open(directory.join("pending.snapshot-transfer.body"))
        .expect("staged body opens")
        .set_len(2)
        .expect("staged body truncates");

    let opened = FileRaftSnapshotStore::open_with_report(&directory).expect("store recovers");

    assert_eq!(
        opened.report().pending_transfer_recovery,
        Some(PendingSnapshotTransferRecovery::DiscardedShortBody {
            expected_bytes: 4,
            actual_bytes: 2,
        })
    );
    assert!(opened.store().current_pending_snapshot_transfer().is_none());
    remove_test_directory(directory);
}

#[test]
fn open_report_records_checksum_mismatch_discard() {
    let directory = test_directory("checksum-mismatch");
    stage_partial_transfer(&directory, b"abcdefgh", 4);
    let body_path = directory.join("pending.snapshot-transfer.body");
    let mut body = fs::read(&body_path).expect("staged body reads");
    body[0] ^= 0xff;
    fs::write(&body_path, &body).expect("staged body corrupts");

    let opened = FileRaftSnapshotStore::open_with_report(&directory).expect("store recovers");

    assert!(matches!(
        opened.report().pending_transfer_recovery.clone(),
        Some(PendingSnapshotTransferRecovery::DiscardedChecksumMismatch {
            expected,
            actual,
        }) if expected != actual
    ));
    assert!(opened.store().current_pending_snapshot_transfer().is_none());
    remove_test_directory(directory);
}

#[test]
fn open_report_records_ignored_unpublished_body_suffix() {
    let directory = test_directory("unpublished-suffix");
    stage_partial_transfer(&directory, b"abcdefgh", 4);
    let body_path = directory.join("pending.snapshot-transfer.body");
    OpenOptions::new()
        .append(true)
        .open(&body_path)
        .expect("staged body opens")
        .write_all(b"ghost")
        .expect("unpublished suffix appends");

    let opened = FileRaftSnapshotStore::open_with_report(&directory).expect("store recovers");

    assert_eq!(
        opened.report().pending_transfer_recovery,
        Some(PendingSnapshotTransferRecovery::IgnoredUnpublishedSuffix {
            published_bytes: 4,
            actual_bytes: 9,
        })
    );
    assert_eq!(
        opened
            .store()
            .current_pending_snapshot_transfer()
            .map(|transfer| transfer.received_len),
        Some(4)
    );
    remove_test_directory(directory);
}

#[test]
fn maximum_manifest_sequence_exhausts_future_publication_without_poisoning() {
    let directory = test_directory("sequence-exhausted");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    let first = snapshot(1, 1, b"first");
    store
        .write_snapshot(first.clone())
        .expect("first snapshot publishes");
    rewrite_manifest_sequence(&directory, &store, u64::MAX);
    drop(store);

    let opened = FileRaftSnapshotStore::open_with_report(&directory).expect("store reopens");
    let (mut store, report) = opened.into_parts();
    let before = store.current_snapshot();

    assert_eq!(report.pending_transfer_recovery, None);
    assert_eq!(
        store.write_snapshot(snapshot(2, 2, b"second")),
        Err(RaftSnapshotStoreWriteError::SnapshotSequenceExhausted)
    );
    assert_eq!(store.current_snapshot(), before);
    assert!(!store.requires_reopen());
    remove_test_directory(directory);
}

#[test]
fn penultimate_manifest_sequence_allows_one_final_publication() {
    let directory = test_directory("final-sequence");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    store
        .write_snapshot(snapshot(1, 1, b"first"))
        .expect("first snapshot publishes");
    rewrite_manifest_sequence(&directory, &store, u64::MAX - 1);
    drop(store);

    let mut store = FileRaftSnapshotStore::open(&directory).expect("store reopens");
    store
        .write_snapshot(snapshot(2, 2, b"last representable"))
        .expect("maximum sequence publishes");

    assert!(store
        .current_snapshot_file_name()
        .expect("current filename exists")
        .starts_with(&format!("snapshot-{}-", u64::MAX)));
    assert_eq!(
        store.write_snapshot(snapshot(3, 3, b"too late")),
        Err(RaftSnapshotStoreWriteError::SnapshotSequenceExhausted)
    );
    assert!(!store.requires_reopen());
    remove_test_directory(directory);
}

fn stage_partial_transfer(directory: &Path, payload: &[u8], received: usize) {
    assert!(received > 0 && received < payload.len());
    let descriptor = RaftSnapshot::from_payload(snapshot_metadata(7, 6), payload);
    let mut store = FileRaftSnapshotStore::open(directory).expect("store opens");
    store
        .stage_snapshot_chunk(&StagedSnapshotChunk {
            leader_id: NodeId(2),
            transfer_id: descriptor.transfer_id(),
            metadata: descriptor.metadata.clone(),
            total_payload_len: descriptor.application_payload_len,
            application_payload_crc32: descriptor.application_payload_crc32,
            offset: 0,
            bytes: payload[..received].to_vec(),
            done: false,
        })
        .expect("partial transfer stages");
}

fn rewrite_manifest_sequence(directory: &Path, store: &FileRaftSnapshotStore, sequence: u64) {
    let file_name = store
        .current_snapshot_file_name()
        .expect("current filename exists");
    fs::write(
        directory.join("current.snapshot"),
        encode_manifest(sequence, file_name),
    )
    .expect("manifest rewrites");
}

fn encode_manifest(sequence: u64, file_name: &str) -> Vec<u8> {
    let file_name = file_name.as_bytes();
    let file_name_len = u16::try_from(file_name.len()).expect("test filename fits");
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"RFSM");
    bytes.push(1);
    bytes.extend_from_slice(&sequence.to_be_bytes());
    bytes.extend_from_slice(&file_name_len.to_be_bytes());
    bytes.extend_from_slice(file_name);
    bytes.extend_from_slice(&crc32(&bytes).to_be_bytes());
    bytes
}

fn snapshot(index: u64, term: u64, payload: &[u8]) -> PersistedRaftSnapshot {
    PersistedRaftSnapshot {
        metadata: snapshot_metadata(index, term),
        application_payload: payload.to_vec(),
    }
}

fn snapshot_metadata(index: u64, term: u64) -> RaftSnapshotMetadata {
    RaftSnapshotMetadata::new(
        SnapshotGroupId::new("open-recovery-report").expect("valid group id"),
        NodeId(1),
        LogIndex(index),
        Term(term),
        Term(term),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("kv").expect("valid snapshot kind"),
            ApplicationSnapshotVersion::new(1).expect("valid snapshot version"),
        ),
    )
    .expect("valid snapshot metadata")
}

fn test_directory(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "rafter-storage-open-report-{name}-{}-{}",
        std::process::id(),
        TEST_DIRECTORY_ID.fetch_add(1, Ordering::Relaxed)
    ))
}

fn remove_test_directory(directory: PathBuf) {
    let _ = fs::remove_dir_all(directory);
}
