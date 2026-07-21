//! Snapshot inventory, retention, residue cleanup, and partial-report scenarios.

use std::fs;

use super::test_support::{
    assert_current_snapshot, remove_test_dir, snapshot, staged_chunk, test_store_dir,
};
use super::*;

#[test]
fn inventory_classifies_current_unreferenced_temporary_and_foreign_artifacts() {
    let directory = test_store_dir("inventory-classification");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    for expected in [
        snapshot(1, 1, b"one"),
        snapshot(2, 2, b"two"),
        snapshot(3, 3, b"three"),
    ] {
        store.write_snapshot(expected).expect("snapshot publishes");
    }
    store
        .stage_snapshot_chunk(&staged_chunk(0, b"partial", 64))
        .expect("partial transfer stages");

    fs::write(store.temp_snapshot_path(), b"snapshot temp").expect("snapshot temp writes");
    fs::write(store.temp_manifest_path(), b"manifest temp").expect("manifest temp writes");
    fs::write(
        store.temp_pending_snapshot_transfer_path(),
        b"pending manifest temp",
    )
    .expect("pending temp writes");
    fs::write(directory.join("operator-note.txt"), b"foreign").expect("foreign file writes");
    fs::write(directory.join(".snapshot-not-a-pid.tmp"), b"foreign").expect("foreign temp writes");
    fs::create_dir(directory.join("foreign-directory")).expect("foreign directory creates");

    let inventory = store.snapshot_inventory().expect("inventory succeeds");

    assert_eq!(
        inventory
            .current
            .as_ref()
            .and_then(|file| file.identity)
            .map(|identity| identity.sequence),
        Some(3)
    );
    assert_eq!(sequences(&inventory.retained), vec![1, 2]);
    assert!(inventory.unreferenced.is_empty());
    assert_eq!(inventory.temporary.len(), 3);
    let snapshot_temp = file_name(&store.temp_snapshot_path());
    let manifest_temp = file_name(&store.temp_manifest_path());
    let pending_temp = file_name(&store.temp_pending_snapshot_transfer_path());
    assert!(inventory.temporary.iter().any(|file| {
        file.kind == SnapshotTemporaryFileKind::SnapshotEnvelope && file.file_name == snapshot_temp
    }));
    assert!(inventory.temporary.iter().any(|file| {
        file.kind == SnapshotTemporaryFileKind::CurrentManifest && file.file_name == manifest_temp
    }));
    assert!(inventory.temporary.iter().any(|file| {
        file.kind == SnapshotTemporaryFileKind::PendingTransferManifest
            && file.file_name == pending_temp
    }));
    assert!(inventory
        .unrecognized
        .contains(&"operator-note.txt".to_string()));
    assert!(inventory
        .unrecognized
        .contains(&".snapshot-not-a-pid.tmp".to_string()));
    assert!(inventory
        .unrecognized
        .contains(&"foreign-directory".to_string()));
    assert!(inventory.pending_transfer.manifest_present);
    assert!(inventory.pending_transfer.body_present);

    remove_test_dir(directory);
}

#[test]
fn prune_keeps_current_and_requested_number_of_previous_snapshots() {
    let directory = test_store_dir("inventory-retention");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    let snapshots = [
        snapshot(1, 1, b"one"),
        snapshot(2, 2, b"two"),
        snapshot(3, 3, b"three"),
        snapshot(4, 4, b"four"),
    ];
    for expected in snapshots.iter().cloned() {
        store.write_snapshot(expected).expect("snapshot publishes");
    }

    let report = store
        .prune_snapshots(SnapshotRetention::KeepPrevious(1))
        .expect("old snapshots prune");

    assert_eq!(sequences(&report.removed_snapshots), vec![1, 2]);
    assert!(report.removed_temporary_files.is_empty());
    let inventory = store.snapshot_inventory().expect("inventory succeeds");
    assert_eq!(sequences(&inventory.retained), vec![3]);
    assert!(inventory.unreferenced.is_empty());
    assert_eq!(current_sequence(&inventory), Some(4));
    assert_current_snapshot(&store, &snapshots[3]);

    drop(store);
    let reopened = FileRaftSnapshotStore::open(&directory).expect("store reopens");
    assert_eq!(
        sequences(&reopened.snapshot_inventory().unwrap().retained),
        vec![3]
    );
    assert_current_snapshot(&reopened, &snapshots[3]);
    remove_test_dir(directory);
}

#[test]
fn retention_keeps_previous_snapshots_but_removes_future_crash_orphans() {
    let directory = test_store_dir("inventory-future-orphan");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    store
        .write_snapshot(snapshot(1, 1, b"one"))
        .expect("first snapshot publishes");
    store
        .write_snapshot(snapshot(2, 2, b"two"))
        .expect("second snapshot publishes");
    fs::write(directory.join("snapshot-9-9-9-9.rfsn"), b"future orphan")
        .expect("future orphan writes");

    let before = store.snapshot_inventory().expect("inventory succeeds");
    assert_eq!(sequences(&before.retained), vec![1]);
    assert_eq!(sequences(&before.unreferenced), vec![9]);

    let report = store
        .prune_snapshots(SnapshotRetention::KeepPrevious(1))
        .expect("future orphan prunes");

    assert_eq!(sequences(&report.removed_snapshots), vec![9]);
    let after = store.snapshot_inventory().expect("inventory succeeds");
    assert_eq!(sequences(&after.retained), vec![1]);
    assert!(after.unreferenced.is_empty());
    remove_test_dir(directory);
}

#[test]
fn keep_all_is_a_noop_and_current_only_removes_every_previous_snapshot() {
    let directory = test_store_dir("inventory-current-only");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    for expected in [
        snapshot(1, 1, b"one"),
        snapshot(2, 2, b"two"),
        snapshot(3, 3, b"three"),
    ] {
        store.write_snapshot(expected).expect("snapshot publishes");
    }

    assert_eq!(
        store.prune_snapshots(SnapshotRetention::KeepAll),
        Ok(SnapshotPruneReport::default())
    );
    assert_eq!(
        sequences(&store.snapshot_inventory().unwrap().retained),
        vec![1, 2]
    );

    let report = store
        .prune_snapshots(SnapshotRetention::CurrentOnly)
        .expect("previous snapshots prune");
    assert_eq!(sequences(&report.removed_snapshots), vec![1, 2]);
    let inventory = store.snapshot_inventory().expect("inventory succeeds");
    assert!(inventory.retained.is_empty());
    assert!(inventory.unreferenced.is_empty());
    assert_eq!(current_sequence(&inventory), Some(3));
    remove_test_dir(directory);
}

#[test]
fn cleanup_removes_only_recognized_temps_and_preserves_stable_staging() {
    let directory = test_store_dir("inventory-temp-cleanup");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    store
        .stage_snapshot_chunk(&staged_chunk(0, b"partial", 64))
        .expect("partial transfer stages");
    fs::write(store.temp_snapshot_path(), b"snapshot temp").expect("snapshot temp writes");
    fs::write(store.temp_manifest_path(), b"manifest temp").expect("manifest temp writes");
    fs::write(
        store.temp_pending_snapshot_transfer_path(),
        b"pending manifest temp",
    )
    .expect("pending temp writes");
    fs::write(directory.join(".snapshot-not-a-pid.tmp"), b"foreign").expect("foreign temp writes");

    let report = store
        .cleanup_abandoned_snapshot_temporary_files()
        .expect("recognized temps clean up");

    assert_eq!(report.removed_temporary_files.len(), 3);
    assert!(report.removed_snapshots.is_empty());
    let inventory = store.snapshot_inventory().expect("inventory succeeds");
    assert!(inventory.temporary.is_empty());
    assert!(inventory
        .unrecognized
        .contains(&".snapshot-not-a-pid.tmp".to_string()));
    assert!(inventory.pending_transfer.manifest_present);
    assert!(inventory.pending_transfer.body_present);
    assert!(store.current_pending_snapshot_transfer().is_some());
    remove_test_dir(directory);
}

#[test]
fn current_only_prunes_canonical_orphans_but_never_unknown_files() {
    let directory = test_store_dir("inventory-orphan-prune");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    fs::write(directory.join("snapshot-7-8-9-10.rfsn"), b"orphan one").expect("orphan writes");
    fs::write(directory.join("snapshot-8-9-10-11.rfsn"), b"orphan two").expect("orphan writes");
    fs::write(directory.join("manual.rfsn"), b"operator owned").expect("foreign file writes");

    let report = store
        .prune_snapshots(SnapshotRetention::CurrentOnly)
        .expect("canonical orphans prune");

    assert_eq!(sequences(&report.removed_snapshots), vec![7, 8]);
    assert!(directory.join("manual.rfsn").is_file());
    let inventory = store.snapshot_inventory().expect("inventory succeeds");
    assert!(inventory.retained.is_empty());
    assert!(inventory.unreferenced.is_empty());
    assert_eq!(inventory.unrecognized, vec!["manual.rfsn".to_string()]);
    remove_test_dir(directory);
}

#[test]
fn maintenance_refuses_a_handle_that_requires_reopen() {
    let directory = test_store_dir("inventory-reopen-required");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    fs::remove_dir_all(&directory).expect("snapshot directory removes");

    assert!(matches!(
        store.write_snapshot(snapshot(1, 1, b"fails")),
        Err(RaftSnapshotStoreWriteError::Io { .. })
    ));
    assert_eq!(
        store.snapshot_inventory(),
        Err(SnapshotInventoryError::StoreRequiresReopen)
    );
    assert_eq!(
        store.prune_snapshots(SnapshotRetention::CurrentOnly),
        Err(SnapshotPruneError::StoreRequiresReopen)
    );
    assert_eq!(
        store.cleanup_abandoned_snapshot_temporary_files(),
        Err(SnapshotPruneError::StoreRequiresReopen)
    );

    fs::create_dir_all(&directory).expect("snapshot directory recreates");
    remove_test_dir(directory);
}

#[test]
fn inventory_fails_loudly_when_the_selected_snapshot_disappears() {
    let directory = test_store_dir("inventory-current-missing");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    store
        .write_snapshot(snapshot(1, 1, b"current"))
        .expect("snapshot publishes");
    let selected = store
        .current_snapshot_file_name()
        .expect("current filename exists")
        .to_string();
    let selected_path = directory.join(selected);
    fs::remove_file(&selected_path).expect("selected snapshot removes");

    assert_eq!(
        store.snapshot_inventory(),
        Err(SnapshotInventoryError::CurrentSnapshotMissing {
            path: selected_path,
        })
    );
    remove_test_dir(directory);
}

#[test]
fn failed_cleanup_reports_the_completed_deletion_prefix() {
    let directory = test_store_dir("inventory-partial-report");
    let store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    fs::write(directory.join("snapshot-1-1-1-1.rfsn"), b"one").expect("orphan writes");
    fs::write(directory.join("snapshot-2-2-2-1.rfsn"), b"two").expect("orphan writes");
    let inventory = store.snapshot_inventory().expect("inventory succeeds");
    fs::remove_file(directory.join("snapshot-2-2-2-1.rfsn")).expect("second orphan removes");

    let error = store
        .remove_inventory_artifacts(inventory.unreferenced, Vec::new())
        .expect_err("second removal fails");

    assert!(matches!(
        error,
        SnapshotPruneError::Io {
            operation: "remove unreferenced raft snapshot",
            removed,
            ..
        } if sequences(&removed.removed_snapshots) == vec![1]
    ));
    assert!(!store.requires_reopen());
    assert!(!directory.join("snapshot-1-1-1-1.rfsn").exists());
    remove_test_dir(directory);
}

fn sequences(files: &[SnapshotFileInfo]) -> Vec<u64> {
    files
        .iter()
        .map(|file| file.identity.expect("canonical identity").sequence)
        .collect()
}

fn current_sequence(inventory: &SnapshotInventory) -> Option<u64> {
    inventory
        .current
        .as_ref()
        .and_then(|file| file.identity)
        .map(|identity| identity.sequence)
}

fn file_name(path: &std::path::Path) -> String {
    path.file_name()
        .expect("path has a filename")
        .to_string_lossy()
        .into_owned()
}
