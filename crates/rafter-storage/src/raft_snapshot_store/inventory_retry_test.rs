//! Maintenance retries must finish directory synchronization even with no residue.

use std::fs;

use super::test_support::{
    assert_current_snapshot, remove_test_dir, snapshot, staged_chunk, test_store_dir,
};
use super::*;
use crate::storage_failpoint_test::{arm, DurabilityPoint};

#[test]
fn pruning_retries_directory_sync_after_all_deletions_on_the_same_handle() {
    retry_cleanup(false, false);
}

#[test]
fn pruning_retries_directory_sync_after_all_deletions_on_a_reopened_handle() {
    retry_cleanup(false, true);
}

#[test]
fn temporary_cleanup_retries_directory_sync_after_all_deletions_on_the_same_handle() {
    retry_cleanup(true, false);
}

#[test]
fn temporary_cleanup_retries_directory_sync_after_all_deletions_on_a_reopened_handle() {
    retry_cleanup(true, true);
}

fn retry_cleanup(temporary: bool, reopen: bool) {
    let directory = test_store_dir(&format!("inventory-sync-retry-{temporary}-{reopen}"));
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    store.write_snapshot(snapshot(1, 1, b"old")).unwrap();
    let current = snapshot(2, 2, b"selected");
    store.write_snapshot(current.clone()).unwrap();
    store
        .stage_snapshot_chunk(&staged_chunk(0, b"partial", 64))
        .unwrap();
    let selected = store.current_snapshot_file_name().unwrap().to_owned();
    let preserved = [
        "current.snapshot",
        &selected,
        "pending.snapshot-transfer",
        "pending.snapshot-transfer.body",
        "operator-note.txt",
    ];
    fs::write(directory.join("operator-note.txt"), b"operator owned").unwrap();
    let original: Vec<_> = preserved
        .iter()
        .map(|name| fs::read(directory.join(name)).unwrap())
        .collect();
    let pending = store.current_pending_snapshot_transfer();
    let expected = if temporary {
        for path in [
            store.temp_snapshot_path(),
            store.temp_manifest_path(),
            store.temp_pending_snapshot_transfer_path(),
        ] {
            fs::write(path, b"abandoned").unwrap();
        }
        (0, 3)
    } else {
        (1, 0)
    };

    let fault = arm(DurabilityPoint::DirectoryBeforeSync);
    assert_sync_failure(cleanup(&mut store, temporary), expected);
    fault.assert_triggered();
    drop(fault);
    assert!(!store.requires_reopen(), "maintenance is safe to retry");
    let inventory = store.snapshot_inventory().unwrap();
    if temporary {
        assert!(inventory.temporary.is_empty());
        assert_eq!(inventory.retained.len(), 1);
    } else {
        assert!(inventory.retained.is_empty());
    }
    if reopen {
        drop(store);
        store = FileRaftSnapshotStore::open(&directory).expect("store reopens");
    }

    // A repeated fsync failure must remain visible even though the preceding
    // attempt already removed every eligible file. Reopen must not forget it.
    let fault = arm(DurabilityPoint::DirectoryBeforeSync);
    assert_sync_failure(cleanup(&mut store, temporary), (0, 0));
    fault.assert_triggered();
    drop(fault);
    assert!(!store.requires_reopen());
    assert_eq!(
        cleanup(&mut store, temporary),
        Ok(SnapshotPruneReport::default())
    );
    assert_current_snapshot(&store, &current);
    assert_eq!(store.current_pending_snapshot_transfer(), pending);
    for (name, bytes) in preserved.into_iter().zip(original) {
        assert_eq!(fs::read(directory.join(name)).unwrap(), bytes, "{name}");
    }
    drop(store);
    let store = FileRaftSnapshotStore::open(&directory).unwrap();
    assert_current_snapshot(&store, &current);
    assert_eq!(store.current_pending_snapshot_transfer(), pending);
    remove_test_dir(directory);
}

fn cleanup(
    store: &mut FileRaftSnapshotStore,
    temporary: bool,
) -> Result<SnapshotPruneReport, SnapshotPruneError> {
    if temporary {
        store.cleanup_abandoned_snapshot_temporary_files()
    } else {
        store.prune_snapshots(SnapshotRetention::CurrentOnly)
    }
}

fn assert_sync_failure(
    result: Result<SnapshotPruneReport, SnapshotPruneError>,
    expected: (usize, usize),
) {
    match result.expect_err("directory sync failure must be reported") {
        SnapshotPruneError::Io {
            operation, removed, ..
        } => {
            assert_eq!(operation, "sync raft snapshot directory after maintenance");
            assert_eq!(removed.removed_snapshots.len(), expected.0);
            assert_eq!(removed.removed_temporary_files.len(), expected.1);
        }
        other => panic!("unexpected maintenance failure: {other:?}"),
    }
}

#[test]
fn keep_all_remains_a_noop_without_directory_sync() {
    let directory = test_store_dir("inventory-keep-all-no-sync");
    let mut store = FileRaftSnapshotStore::open(&directory).unwrap();
    store.write_snapshot(snapshot(1, 1, b"one")).unwrap();
    store.write_snapshot(snapshot(2, 2, b"two")).unwrap();
    let fault = arm(DurabilityPoint::DirectoryBeforeSync);
    assert_eq!(
        store.prune_snapshots(SnapshotRetention::KeepAll),
        Ok(SnapshotPruneReport::default())
    );
    assert_eq!(store.snapshot_inventory().unwrap().retained.len(), 1);
    assert_sync_failure(
        store.prune_snapshots(SnapshotRetention::CurrentOnly),
        (1, 0),
    );
    fault.assert_triggered();
    remove_test_dir(directory);
}
