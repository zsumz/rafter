//! Parent-directory synchronization and batching scenarios.

use super::*;
use std::{
    fs::{self, OpenOptions},
    io::Write,
    sync::atomic::{AtomicU64, Ordering},
};

static TEST_FILE_ID: AtomicU64 = AtomicU64::new(0);

#[test]
fn sync_parent_directory_accepts_existing_parent() {
    let path = std::env::temp_dir().join(format!(
        "rafter-storage-parent-sync-{}-{}",
        std::process::id(),
        TEST_FILE_ID.fetch_add(1, Ordering::Relaxed)
    ));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .expect("test file creates");
    file.write_all(b"durable").expect("test file writes");
    file.sync_data().expect("test file syncs");

    sync_parent_directory(&path).expect("parent directory syncs");

    fs::remove_file(path).expect("test file removes");
}

#[test]
fn relative_file_parent_defaults_to_current_directory() {
    assert_eq!(
        parent_directory(Path::new("hard-state")),
        PathBuf::from(".")
    );
}

#[test]
fn parent_directory_sync_batch_deduplicates_common_parents() {
    let mut batch = ParentDirectorySyncBatch::new();

    batch.record_parent_of(Path::new("/tmp/group-1/log"));
    batch.record_parent_of(Path::new("/tmp/group-1/snapshots"));
    batch.record_parent_of(Path::new("/tmp/group-2/log"));

    assert_eq!(batch.pending_count(), 2);
}

#[test]
fn nested_snapshot_creation_schedules_every_new_directory_parent() {
    let anchor = std::env::temp_dir().join(format!(
        "rafter-storage-nested-sync-{}-{}",
        std::process::id(),
        TEST_FILE_ID.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&anchor).expect("existing test anchor creates");
    let directory = anchor.join("group/node/snapshots");
    let mut batch = ParentDirectorySyncBatch::new();
    let store = crate::FileRaftSnapshotStore::open_with_parent_sync_batch(&directory, &mut batch)
        .expect("nested snapshot store opens");
    assert_eq!(
        batch.parents,
        BTreeSet::from([
            anchor.clone(),
            anchor.join("group"),
            anchor.join("group/node")
        ]),
        "every newly created directory entry needs its own parent synchronized"
    );
    batch
        .flush()
        .expect("all new directory parents synchronize");
    assert_eq!(batch.pending_count(), 0);
    drop(store);
    fs::remove_dir_all(anchor).expect("test hierarchy removes");
}

#[test]
fn nested_snapshot_creation_stops_at_existing_ancestor_and_reopen_schedules_nothing() {
    let anchor = std::env::temp_dir().join(format!(
        "rafter-storage-existing-sync-{}-{}",
        std::process::id(),
        TEST_FILE_ID.fetch_add(1, Ordering::Relaxed)
    ));
    let existing = anchor.join("group");
    fs::create_dir_all(&existing).expect("existing hierarchy creates");
    let directory = existing.join("node/snapshots");
    let mut batch = ParentDirectorySyncBatch::new();
    let store = crate::FileRaftSnapshotStore::open_with_parent_sync_batch(&directory, &mut batch)
        .expect("nested snapshot store opens");
    assert_eq!(
        batch.parents,
        BTreeSet::from([existing.clone(), existing.join("node")])
    );
    batch.flush().expect("new directory parents synchronize");
    drop(store);
    let reopened =
        crate::FileRaftSnapshotStore::open_with_parent_sync_batch(&directory, &mut batch)
            .expect("existing snapshot store reopens");
    assert_eq!(batch.pending_count(), 0);
    drop(reopened);
    fs::remove_dir_all(anchor).expect("test hierarchy removes");
}

#[test]
fn nested_snapshot_creation_refuses_file_ancestors_without_changing_them() {
    let anchor = std::env::temp_dir().join(format!(
        "rafter-storage-file-sync-{}-{}",
        std::process::id(),
        TEST_FILE_ID.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&anchor).expect("test anchor creates");
    let file = anchor.join("occupied");
    fs::write(&file, b"keep me").expect("existing file creates");
    for directory in [file.clone(), file.join("node/snapshots")] {
        let mut batch = ParentDirectorySyncBatch::new();
        assert!(
            crate::FileRaftSnapshotStore::open_with_parent_sync_batch(directory, &mut batch)
                .is_err()
        );
        assert_eq!(batch.pending_count(), 0);
        assert_eq!(fs::read(&file).expect("existing file reads"), b"keep me");
    }
    fs::remove_dir_all(anchor).expect("test hierarchy removes");
}
