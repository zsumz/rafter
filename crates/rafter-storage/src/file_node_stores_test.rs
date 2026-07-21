//! Standard node-store layout and commit-floor repair scenarios.

use super::*;
use crate::{
    file_store_ownership::{acquire_file_store_ownership, FILE_STORE_OWNERSHIP_LOCK_NAME},
    PersistedRaftLogEntry, RaftHardState, RaftHardStateStore, RaftLogSegment, RaftSnapshotStore,
};
use rafter::{LogIndex, Term};
use std::{
    fs,
    io::Write,
    sync::atomic::{AtomicU64, Ordering},
};

static TEST_DIRECTORY_ID: AtomicU64 = AtomicU64::new(0);

#[test]
fn file_raft_node_stores_open_standard_layout() {
    let directory = test_directory("standard-layout");
    fs::create_dir_all(&directory).expect("replica directory creates");

    let stores = FileRaftNodeStores::open(&directory).expect("node stores open");
    let (hard_state, log_segment, snapshot_store) = stores.into_parts();

    assert_eq!(hard_state.current(), RaftHardState::default());
    assert_eq!(log_segment.next_index(), rafter::LogIndex(1));
    assert!(snapshot_store.current_snapshot().is_none());
    assert!(directory.join(FILE_STORE_OWNERSHIP_LOCK_NAME).is_file());
    assert!(directory.join("log").is_file());
    assert!(directory.join("snapshots").is_dir());

    drop(hard_state);
    drop(log_segment);
    drop(snapshot_store);
    fs::remove_dir_all(directory).expect("test directory removes");
}

#[test]
fn split_stores_retain_exclusive_directory_ownership_until_every_part_drops() {
    let directory = test_directory("exclusive-ownership");
    fs::create_dir_all(&directory).expect("replica directory creates");

    let stores = FileRaftNodeStores::open(&directory).expect("first owner opens");
    let error = FileRaftNodeStores::open(&directory).expect_err("second owner is rejected");
    assert_eq!(
        error,
        OpenFileRaftNodeStoresError::AlreadyOpen {
            directory: directory.clone(),
        }
    );

    let (hard_state, log_segment, snapshot_store) = stores.into_parts();
    assert!(matches!(
        FileRaftNodeStores::open(&directory),
        Err(OpenFileRaftNodeStoresError::AlreadyOpen { .. })
    ));

    drop(hard_state);
    assert!(matches!(
        FileRaftNodeStores::open(&directory),
        Err(OpenFileRaftNodeStoresError::AlreadyOpen { .. })
    ));
    drop(log_segment);
    assert!(matches!(
        FileRaftNodeStores::open(&directory),
        Err(OpenFileRaftNodeStoresError::AlreadyOpen { .. })
    ));
    drop(snapshot_store);

    let reopened =
        FileRaftNodeStores::open(&directory).expect("last guard drop releases ownership");
    drop(reopened);
    fs::remove_dir_all(directory).expect("test directory removes");
}

#[test]
fn file_raft_node_stores_requires_existing_directory() {
    let directory = test_directory("missing-directory");

    let error = FileRaftNodeStores::open(&directory).expect_err("missing directory is rejected");

    assert!(matches!(
        error,
        OpenFileRaftNodeStoresError::Io {
            operation: "open raft node store directory",
            ..
        }
    ));
}

#[test]
fn file_raft_node_stores_repair_uses_hard_state_commit_floor() {
    let directory = test_directory("repair-uses-commit");
    fs::create_dir_all(&directory).expect("replica directory creates");

    {
        let mut hard_state =
            FileRaftHardStateStore::open(directory.join("hard-state")).expect("hard state opens");
        hard_state
            .write_hard_state(RaftHardState {
                commit_index: LogIndex(1),
                ..RaftHardState::default()
            })
            .expect("hard state writes");

        let mut log_segment = FileRaftLogSegment::open(directory.join("log")).expect("log opens");
        log_segment
            .append_entries(&[PersistedRaftLogEntry::application(
                LogIndex(1),
                Term(7),
                b"committed".to_vec(),
            )])
            .expect("committed entry appends");
    }
    let mut log = fs::OpenOptions::new()
        .append(true)
        .open(directory.join("log"))
        .expect("log opens for partial tail append");
    log.write_all(&[0, 0])
        .expect("uncommitted partial tail writes");

    let stores = FileRaftNodeStores::open_repairing_uncommitted_log_tail(&directory)
        .expect("node stores repair uncommitted log tail");
    let (hard_state, log_segment, snapshot_store) = stores.into_parts();

    assert_eq!(hard_state.current().commit_index, LogIndex(1));
    assert_eq!(log_segment.next_index(), LogIndex(2));
    assert_eq!(
        log_segment.replay_entries(),
        vec![PersistedRaftLogEntry::application(
            LogIndex(1),
            Term(7),
            b"committed".to_vec(),
        )]
    );
    assert!(snapshot_store.current_snapshot().is_none());
    assert!(matches!(
        FileRaftNodeStores::open(&directory),
        Err(OpenFileRaftNodeStoresError::AlreadyOpen { .. })
    ));

    drop(hard_state);
    drop(log_segment);
    drop(snapshot_store);
    let reopened = FileRaftNodeStores::open(&directory).expect("repaired stores open strictly");
    drop(reopened);
    fs::remove_dir_all(directory).expect("test directory removes");
}

#[test]
fn repairing_open_checks_ownership_before_truncating_an_uncommitted_tail() {
    let directory = test_directory("ownership-before-repair");
    fs::create_dir_all(&directory).expect("replica directory creates");

    {
        let mut hard_state =
            FileRaftHardStateStore::open(directory.join("hard-state")).expect("hard state opens");
        hard_state
            .write_hard_state(RaftHardState {
                commit_index: LogIndex(1),
                ..RaftHardState::default()
            })
            .expect("hard state writes");
        let mut log_segment = FileRaftLogSegment::open(directory.join("log")).expect("log opens");
        log_segment
            .append_entries(&[PersistedRaftLogEntry::application(
                LogIndex(1),
                Term(7),
                b"committed".to_vec(),
            )])
            .expect("committed entry appends");
    }
    fs::OpenOptions::new()
        .append(true)
        .open(directory.join("log"))
        .expect("log opens for partial tail append")
        .write_all(&[0, 0])
        .expect("uncommitted partial tail writes");

    let before = fs::read(directory.join("log")).expect("partial log reads");
    let ownership = acquire_file_store_ownership(&directory).expect("test owner acquires lock");

    assert!(matches!(
        FileRaftNodeStores::open_repairing_uncommitted_log_tail(&directory),
        Err(OpenFileRaftNodeStoresError::AlreadyOpen { .. })
    ));
    assert_eq!(
        fs::read(directory.join("log")).expect("contended log reads"),
        before,
        "a contended repairing open must not touch the retained log"
    );

    drop(ownership);
    let repaired = FileRaftNodeStores::open_repairing_uncommitted_log_tail(&directory)
        .expect("repair proceeds after ownership is released");
    drop(repaired);
    assert!(
        fs::read(directory.join("log"))
            .expect("repaired log reads")
            .len()
            < before.len(),
        "the released repairing open removes the partial tail"
    );
    let reopened = FileRaftNodeStores::open(&directory).expect("strict reopen succeeds");
    drop(reopened);
    fs::remove_dir_all(directory).expect("test directory removes");
}

fn test_directory(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "rafter-storage-node-stores-{name}-{}-{}",
        std::process::id(),
        TEST_DIRECTORY_ID.fetch_add(1, Ordering::Relaxed)
    ))
}
