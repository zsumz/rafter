//! File publication, reopen, corruption, and post-I/O health scenarios.

use std::fs;

use crate::{encode_raft_hard_state, DecodeRaftHardStateError, RaftHardState};
use rafter_invariant_test::oracle_assert_eq;

use super::test_support::{
    hard_state, hard_state_temp_path, remove_test_file, test_store_directory, test_store_path,
};
use super::{
    FileRaftHardStateStore, OpenRaftHardStateStoreError, RaftHardStateStore,
    RaftHardStateStoreWriteError,
};

#[test]
fn empty_file_store_opens_as_default_hard_state() {
    let path = test_store_path("empty");

    let store = FileRaftHardStateStore::open(&path).expect("store opens");

    assert_eq!(store.current(), RaftHardState::default());
    remove_test_file(path);
}

#[cfg(unix)]
#[test]
fn hard_state_open_does_not_treat_metadata_failure_as_absence() {
    use std::os::unix::fs::symlink;

    let path = test_store_path("metadata-failure");
    symlink(&path, &path).expect("self-referential test symlink creates");

    let result = FileRaftHardStateStore::open(&path);
    fs::remove_file(&path).expect("test symlink removes");

    assert!(matches!(
        result,
        Err(OpenRaftHardStateStoreError::Io {
            operation: "open raft hard state",
            ..
        })
    ));
}

#[test]
fn file_store_reopens_latest_written_hard_state() {
    let path = test_store_path("latest");
    {
        let mut store = FileRaftHardStateStore::open(&path).expect("store opens");
        store
            .write_hard_state(hard_state(1, Some(7)))
            .expect("state writes");
        store
            .write_hard_state(hard_state(2, Some(8)))
            .expect("state writes");
    }

    let reopened = FileRaftHardStateStore::open(&path).expect("store reopens");

    oracle_assert_eq!(reopened.current(), hard_state(2, Some(8)));
    remove_test_file(path);
}

#[test]
fn file_store_replaces_state_through_temp_file() {
    let path = test_store_path("replace");
    let temp_path = hard_state_temp_path(&path);
    let mut store = FileRaftHardStateStore::open(&path).expect("store opens");

    store
        .write_hard_state(hard_state(3, Some(9)))
        .expect("state writes");

    assert!(!temp_path.exists());
    assert_eq!(
        FileRaftHardStateStore::open(&path).unwrap().current(),
        hard_state(3, Some(9))
    );
    remove_test_file(path);
}

#[test]
fn file_store_ignores_abandoned_temp_file_before_rename() {
    let path = test_store_path("abandoned-temp");
    let temp_path = hard_state_temp_path(&path);
    {
        let mut store = FileRaftHardStateStore::open(&path).expect("store opens");
        store
            .write_hard_state(hard_state(1, Some(7)))
            .expect("initial state writes");
    }
    fs::write(&temp_path, encode_raft_hard_state(&hard_state(2, Some(8))))
        .expect("abandoned temp state is written");

    let reopened = FileRaftHardStateStore::open(&path).expect("store reopens");

    assert_eq!(reopened.current(), hard_state(1, Some(7)));
    remove_test_file(path);
}

#[test]
fn file_store_rejects_reuse_after_a_mutating_io_failure() {
    let directory = test_store_directory("reopen-required");
    fs::create_dir_all(&directory).expect("test directory creates");
    let path = directory.join("hard-state");
    let mut store = FileRaftHardStateStore::open(&path).expect("store opens");

    fs::remove_dir_all(&directory).expect("test directory removes");
    let first_error = store
        .write_hard_state(hard_state(1, Some(7)))
        .expect_err("missing parent makes the temp-file open fail");

    assert!(matches!(
        first_error,
        RaftHardStateStoreWriteError::Io {
            operation: "open raft hard state temp file",
            ..
        }
    ));
    assert!(store.requires_reopen());
    assert_eq!(store.current(), RaftHardState::default());

    fs::create_dir_all(&directory).expect("test directory recreates");
    assert_eq!(
        store.write_hard_state(hard_state(2, Some(8))),
        Err(RaftHardStateStoreWriteError::StoreRequiresReopen)
    );
    assert!(!path.exists(), "poisoned handle performs no later write");

    let mut reopened = FileRaftHardStateStore::open(&path).expect("store reopens");
    assert!(!reopened.requires_reopen());
    reopened
        .write_hard_state(hard_state(3, Some(9)))
        .expect("fresh handle writes");
    assert_eq!(reopened.current(), hard_state(3, Some(9)));
    fs::remove_dir_all(directory).expect("test directory removes");
}

#[test]
fn corrupt_hard_state_fails_loudly_on_open() {
    let path = test_store_path("corrupt");
    fs::write(&path, b"bad").expect("corrupt store is written");

    assert_eq!(
        FileRaftHardStateStore::open(&path).map(|store| store.current()),
        Err(OpenRaftHardStateStoreError::Decode(
            DecodeRaftHardStateError::UnexpectedEof {
                needed: 4,
                remaining: 3,
            }
        ))
    );
    remove_test_file(path);
}
