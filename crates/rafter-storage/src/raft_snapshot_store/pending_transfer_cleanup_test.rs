//! Pending-transfer cleanup, inconsistent-residue recovery, and replacement scenarios.

use std::fs;

use super::*;

use super::test_support::{
    pending_transfer, remove_test_dir, snapshot, staged_chunk, test_store_dir,
};

#[test]
fn file_snapshot_store_ignores_abandoned_pending_snapshot_body_without_manifest() {
    let directory = test_store_dir("pending-abandoned-body");
    fs::create_dir_all(&directory).expect("test directory creates");
    fs::write(
        directory.join("pending.snapshot-transfer.body"),
        b"abandoned",
    )
    .expect("abandoned body writes");

    let store = FileRaftSnapshotStore::open(&directory).expect("store opens");

    assert_eq!(store.current_pending_snapshot_transfer(), None);
    remove_test_dir(directory);
}

#[test]
fn file_snapshot_store_removes_abandoned_pending_snapshot_body() {
    let directory = test_store_dir("pending-remove-abandoned-body");
    fs::create_dir_all(&directory).expect("test directory creates");
    let body_path = directory.join("pending.snapshot-transfer.body");
    fs::write(&body_path, b"abandoned").expect("abandoned body writes");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");

    assert!(store
        .remove_abandoned_pending_snapshot_transfer_staging()
        .expect("abandoned body removes"));

    assert!(!body_path.exists());
    assert_eq!(store.current_pending_snapshot_transfer(), None);
    assert!(
        !store
            .pending_snapshot_transfer_staging_status()
            .body_present
    );
    remove_test_dir(directory);
}

#[test]
fn file_snapshot_store_preserves_valid_pending_snapshot_transfer_during_abandoned_cleanup() {
    let directory = test_store_dir("pending-cleanup-preserves-valid");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    store
        .stage_snapshot_chunk(&staged_chunk(0, b"partial snapshot bytes", 64))
        .expect("chunk stages");

    assert!(!store
        .remove_abandoned_pending_snapshot_transfer_staging()
        .expect("valid pending transfer is left untouched"));

    assert_eq!(
        store.current_pending_snapshot_transfer(),
        Some(pending_transfer(22, 64))
    );
    assert_eq!(
        FileRaftSnapshotStore::open(&directory)
            .expect("store reopens")
            .current_pending_snapshot_transfer(),
        Some(pending_transfer(22, 64))
    );
    remove_test_dir(directory);
}

#[test]
fn file_snapshot_store_discards_pending_staging_with_short_body() {
    // A body shorter than the staged length the manifest records is the
    // inconsistent leftover of a crash between the body replace and the
    // manifest replace. Staged progress is optional state: the store must
    // discard it and open with no pending transfer, not refuse to open.
    let directory = test_store_dir("pending-short-body");
    {
        let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
        store
            .stage_snapshot_chunk(&staged_chunk(0, b"partial snapshot bytes", 64))
            .expect("chunk stages");
    }
    fs::write(directory.join("pending.snapshot-transfer.body"), b"partial")
        .expect("short body writes");

    let store =
        FileRaftSnapshotStore::open(&directory).expect("inconsistent staging discards at open");

    assert_eq!(store.current_pending_snapshot_transfer(), None);
    assert!(!directory.join("pending.snapshot-transfer").exists());
    assert!(!directory.join("pending.snapshot-transfer.body").exists());
    remove_test_dir(directory);
}

#[test]
fn file_snapshot_store_discards_pending_staging_with_body_checksum_mismatch() {
    // A body failing the manifest's checksum no longer holds the bytes the
    // manifest describes — the same crash-between-renames shape as a short
    // body. The staging is discarded and the store opens empty.
    let directory = test_store_dir("pending-body-checksum");
    {
        let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
        store
            .stage_snapshot_chunk(&staged_chunk(0, b"partial snapshot bytes", 64))
            .expect("chunk stages");
    }
    let body_path = directory.join("pending.snapshot-transfer.body");
    let mut body = fs::read(&body_path).expect("body reads");
    body[0] ^= 0xFF;
    fs::write(&body_path, body).expect("corrupt body writes");

    let store =
        FileRaftSnapshotStore::open(&directory).expect("inconsistent staging discards at open");

    assert_eq!(store.current_pending_snapshot_transfer(), None);
    assert!(!directory.join("pending.snapshot-transfer").exists());
    assert!(!directory.join("pending.snapshot-transfer.body").exists());
    remove_test_dir(directory);
}

#[test]
fn file_snapshot_store_recovers_from_crash_between_body_and_manifest_replace() {
    // Restarting a transfer at offset zero replaces the body (temp+rename)
    // and then the manifest. Crash between the two renames: the new
    // transfer's shorter body sits under the old transfer's manifest. The
    // reopen must discard the staging, open with no pending transfer, and
    // accept a fresh transfer from offset zero.
    let directory = test_store_dir("pending-crash-between-renames");
    {
        let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
        store
            .stage_snapshot_chunk(&staged_chunk(0, b"first transfer prefix", 64))
            .expect("first transfer stages");
    }
    fs::write(directory.join("pending.snapshot-transfer.body"), b"xy")
        .expect("replaced body writes without touching the manifest");

    let mut store =
        FileRaftSnapshotStore::open(&directory).expect("inconsistent staging discards at open");
    assert_eq!(store.current_pending_snapshot_transfer(), None);
    assert!(!directory.join("pending.snapshot-transfer").exists());
    assert!(!directory.join("pending.snapshot-transfer.body").exists());

    store
        .stage_snapshot_chunk(&staged_chunk(0, b"fresh start", 64))
        .expect("a fresh transfer stages from offset zero");

    assert_eq!(
        store.current_pending_snapshot_transfer(),
        Some(pending_transfer(11, 64))
    );
    assert_eq!(
        FileRaftSnapshotStore::open(&directory)
            .expect("store reopens")
            .current_pending_snapshot_transfer(),
        Some(pending_transfer(11, 64))
    );
    remove_test_dir(directory);
}

#[test]
fn file_snapshot_store_clears_pending_snapshot_transfer() {
    let directory = test_store_dir("pending-clear");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    store
        .stage_snapshot_chunk(&staged_chunk(0, b"partial snapshot bytes", 64))
        .expect("chunk stages");

    store
        .clear_pending_snapshot_transfer()
        .expect("pending transfer clears");

    assert_eq!(store.current_pending_snapshot_transfer(), None);
    assert_eq!(
        FileRaftSnapshotStore::open(&directory)
            .expect("store reopens")
            .current_pending_snapshot_transfer(),
        None
    );
    remove_test_dir(directory);
}

#[test]
fn file_snapshot_store_write_snapshot_clears_pending_transfer() {
    let directory = test_store_dir("pending-cleared-by-current");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    store
        .stage_snapshot_chunk(&staged_chunk(0, b"partial", 64))
        .expect("chunk stages");

    store
        .write_snapshot(snapshot(5, 4, b"complete snapshot"))
        .expect("current snapshot writes");

    assert_eq!(store.current_pending_snapshot_transfer(), None);
    assert_eq!(
        FileRaftSnapshotStore::open(&directory)
            .expect("store reopens")
            .current_pending_snapshot_transfer(),
        None
    );
    remove_test_dir(directory);
}
