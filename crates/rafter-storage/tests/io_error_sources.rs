//! Public error-chain scenarios for file-backed storage failures.

use std::{
    error::Error as _,
    fs, io,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use rafter_storage::{
    FileRaftHardStateStore, FileRaftNodeStores, OpenFileRaftNodeStoresError, RaftHardState,
    RaftHardStateStore,
};

static TEST_PATH_ID: AtomicU64 = AtomicU64::new(0);

#[test]
fn mutating_store_error_exposes_the_original_io_error() {
    let directory = test_path("hard-state-source");
    let path = directory.join("missing").join("hard-state");
    let mut store =
        FileRaftHardStateStore::open(&path).expect("absent store opens logically empty");

    let error = store
        .write_hard_state(RaftHardState::default())
        .expect_err("missing parent rejects the temp-file open");
    let source = error
        .source()
        .expect("storage error retains its I/O source")
        .downcast_ref::<io::Error>()
        .expect("immediate source is std::io::Error");

    assert_eq!(source.kind(), io::ErrorKind::NotFound);
    let cloned = error.clone();
    assert_eq!(cloned, error);
    assert_eq!(
        cloned
            .source()
            .and_then(|source| source.downcast_ref::<io::Error>())
            .map(io::Error::kind),
        Some(io::ErrorKind::NotFound)
    );
}

#[test]
fn bundle_open_error_exposes_the_original_io_error() {
    let directory = test_path("bundle-open-source");

    let error =
        FileRaftNodeStores::open(&directory).expect_err("missing replica directory is rejected");
    assert!(matches!(error, OpenFileRaftNodeStoresError::Io { .. }));

    let source = error
        .source()
        .expect("bundle error retains its I/O source")
        .downcast_ref::<io::Error>()
        .expect("immediate source is std::io::Error");
    assert_eq!(source.kind(), io::ErrorKind::NotFound);
}

fn test_path(name: &str) -> PathBuf {
    let id = TEST_PATH_ID.fetch_add(1, Ordering::Relaxed);
    let path =
        std::env::temp_dir().join(format!("rafter-storage-{name}-{}-{id}", std::process::id()));
    if path.exists() {
        fs::remove_dir_all(&path).expect("stale test path removes");
    }
    path
}
