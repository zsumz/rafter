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
