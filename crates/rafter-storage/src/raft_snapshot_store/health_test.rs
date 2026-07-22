//! File-backed snapshot failure, commit-point, and reopen scenarios.

use std::fs;

use rafter::{MembershipConfig, MembershipSet, NodeId, RaftSnapshot, StagedSnapshotChunk};

use super::test_support::{
    assert_current_snapshot, remove_test_dir, snapshot, staged_chunk, test_store_dir,
    transfer_metadata,
};
use super::*;

#[test]
fn validation_failure_keeps_snapshot_store_writable() {
    let directory = test_store_dir("health-validation");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");

    assert!(matches!(
        store.stage_snapshot_chunk(&staged_chunk(7, b"tail", 64)),
        Err(RaftSnapshotStoreWriteError::StagedChunkWithoutTransfer { .. })
    ));
    assert!(!store.requires_reopen());

    let expected = snapshot(3, 2, b"still writable");
    store
        .write_snapshot(expected.clone())
        .expect("validation failure does not poison the store");
    assert_current_snapshot(&store, &expected);
    remove_test_dir(directory);
}

#[test]
fn snapshot_write_io_failure_requires_reopen() {
    let directory = test_store_dir("health-write-io");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    fs::remove_dir_all(&directory).expect("snapshot directory removes");
    let expected = snapshot(3, 2, b"durable after reopen");

    assert!(matches!(
        store.write_snapshot(expected.clone()),
        Err(RaftSnapshotStoreWriteError::Io {
            operation: "open raft snapshot temp file",
            ..
        })
    ));
    assert!(store.requires_reopen());

    fs::create_dir_all(&directory).expect("snapshot directory recreates");
    assert_eq!(
        store.write_snapshot(expected.clone()),
        Err(RaftSnapshotStoreWriteError::StoreRequiresReopen)
    );

    drop(store);
    let mut reopened = FileRaftSnapshotStore::open(&directory).expect("store reopens");
    assert!(!reopened.requires_reopen());
    reopened
        .write_snapshot(expected.clone())
        .expect("fresh handle writes");
    assert_current_snapshot(&reopened, &expected);
    remove_test_dir(directory);
}

#[cfg(unix)]
#[test]
fn snapshot_candidate_metadata_failure_requires_reopen() {
    use std::os::unix::fs::symlink;

    let directory = test_store_dir("health-candidate-metadata");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    let candidate_path = directory.join("snapshot-1-3-2-1.rfsn");
    symlink(&candidate_path, &candidate_path)
        .expect("self-referential candidate snapshot symlink creates");
    let expected = snapshot(3, 2, b"durable after reopen");

    let result = store.write_snapshot(expected.clone());
    fs::remove_file(&candidate_path).expect("test symlink removes");

    assert!(matches!(
        result,
        Err(RaftSnapshotStoreWriteError::Io {
            operation: "stat candidate raft snapshot path",
            ..
        })
    ));
    assert!(store.requires_reopen());
    assert_eq!(
        store.write_snapshot(expected),
        Err(RaftSnapshotStoreWriteError::StoreRequiresReopen)
    );
    remove_test_dir(directory);
}

#[test]
fn pending_stage_io_failure_requires_reopen() {
    let directory = test_store_dir("health-stage-io");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    fs::remove_dir_all(&directory).expect("snapshot directory removes");

    assert!(matches!(
        store.stage_snapshot_chunk(&staged_chunk(0, b"partial", 64)),
        Err(RaftSnapshotStoreWriteError::Io { .. })
    ));
    assert!(store.requires_reopen());

    fs::create_dir_all(&directory).expect("snapshot directory recreates");
    assert_eq!(
        store.clear_pending_snapshot_transfer(),
        Err(RaftSnapshotStoreWriteError::StoreRequiresReopen)
    );

    drop(store);
    let mut reopened = FileRaftSnapshotStore::open(&directory).expect("store reopens");
    reopened
        .stage_snapshot_chunk(&staged_chunk(0, b"fresh", 64))
        .expect("fresh handle stages");
    assert!(!reopened.requires_reopen());
    remove_test_dir(directory);
}

#[test]
fn current_manifest_commit_is_reported_when_staging_cleanup_fails() {
    let directory = test_store_dir("health-committed-cleanup");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    store
        .stage_snapshot_chunk(&staged_chunk(0, b"partial", 64))
        .expect("pending transfer stages");

    let body_path = directory.join("pending.snapshot-transfer.body");
    fs::remove_file(&body_path).expect("staged body file removes");
    fs::create_dir(&body_path).expect("directory replaces staged body file");
    fs::write(body_path.join("keep"), b"not removable as a file")
        .expect("body directory is non-empty");

    let expected = snapshot(9, 8, b"new current snapshot");
    let error = store
        .write_snapshot(expected.clone())
        .expect_err("staging cleanup fails after current manifest publication");
    let committed_file_name = match error {
        RaftSnapshotStoreWriteError::SnapshotCommittedButReopenRequired {
            file_name,
            operation,
            path,
            ..
        } => {
            assert_eq!(operation, "remove pending snapshot transfer body");
            assert_eq!(path, body_path);
            file_name
        }
        other => panic!("expected committed snapshot outcome, got {other:?}"),
    };

    assert!(store.requires_reopen());
    assert_eq!(
        store.current_snapshot_file_name(),
        Some(committed_file_name.as_str())
    );
    assert_current_snapshot(&store, &expected);
    assert_eq!(
        store.stage_snapshot_chunk(&staged_chunk(0, b"retry", 64)),
        Err(RaftSnapshotStoreWriteError::StoreRequiresReopen)
    );

    drop(store);
    let reopened = FileRaftSnapshotStore::open(&directory).expect("committed snapshot reopens");
    assert!(!reopened.requires_reopen());
    assert_current_snapshot(&reopened, &expected);
    assert_eq!(reopened.current_pending_snapshot_transfer(), None);
    remove_test_dir(directory);
}

#[test]
fn pending_manifest_encoding_fails_before_body_mutation() {
    let directory = test_store_dir("health-prepare-before-body");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    let voters = (1..=(u64::from(u16::MAX) + 1))
        .map(NodeId)
        .collect::<Vec<_>>();
    let membership = MembershipSet::new(voters, Vec::new()).expect("membership is valid");
    let metadata =
        transfer_metadata().with_committed_membership(MembershipConfig::stable(membership));
    let transfer_id = RaftSnapshot::new(metadata.clone(), 0, 0).transfer_id();
    let chunk = StagedSnapshotChunk {
        leader_id: NodeId(1),
        transfer_id,
        metadata,
        total_payload_len: 0,
        application_payload_crc32: 0,
        offset: 0,
        bytes: Vec::new(),
        done: true,
    };

    assert!(matches!(
        store.stage_snapshot_chunk(&chunk),
        Err(RaftSnapshotStoreWriteError::EncodeSnapshot(
            EncodeRaftSnapshotError::TooManyMembers {
                member_kind: "voters",
                ..
            }
        ))
    ));
    assert!(!store.requires_reopen());
    assert!(!directory.join("pending.snapshot-transfer").exists());
    assert!(!directory.join("pending.snapshot-transfer.body").exists());
    remove_test_dir(directory);
}
