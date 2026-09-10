//! Journal bundles share ownership and reject accidental backend switches.

use crate::raft_hard_state_store::test_support::test_store_path;
use crate::{
    FileRaftNodeStores, JournalRaftNodeStores, OpenFileRaftNodeStoresError,
    OpenJournalRaftNodeStoresError, RaftHardState, RaftHardStateStore,
};

#[test]
fn every_split_store_retains_exclusive_directory_ownership() {
    for last in 0..3 {
        let path = test_store_path("journal-owned");
        std::fs::create_dir(&path).expect("replica directory");
        let (hard, log, snapshots) = JournalRaftNodeStores::open(&path)
            .expect("open")
            .into_parts();
        let mut parts: Vec<Box<dyn std::fmt::Debug>> =
            vec![Box::new(hard), Box::new(log), Box::new(snapshots)];
        let survivor = parts.remove(last);
        drop(parts);
        assert!(matches!(
            JournalRaftNodeStores::open(&path),
            Err(OpenJournalRaftNodeStoresError::Stores(
                OpenFileRaftNodeStoresError::AlreadyOpen { .. }
            ))
        ));
        assert!(matches!(
            FileRaftNodeStores::open(&path),
            Err(OpenFileRaftNodeStoresError::AlreadyOpen { .. })
        ));
        drop(survivor);
        drop(JournalRaftNodeStores::open(&path).expect("last owner released"));
        assert!(matches!(
            FileRaftNodeStores::open(&path),
            Err(OpenFileRaftNodeStoresError::HardState(_))
        ));
        std::fs::remove_dir_all(&path).expect("cleanup");
    }
}

#[test]
fn selecting_journal_for_existing_legacy_replica_fails_before_other_stores_open() {
    let path = test_store_path("journal-legacy-bundle");
    std::fs::create_dir(&path).expect("replica directory");
    let (mut hard, log, snapshots) = FileRaftNodeStores::open(&path)
        .expect("legacy")
        .into_parts();
    hard.write_hard_state(RaftHardState::default())
        .expect("legacy bytes");
    drop((hard, log, snapshots));
    let bytes = std::fs::read(path.join("hard-state")).expect("read");
    assert!(matches!(
        JournalRaftNodeStores::open_repairing_uncommitted_log_tail(&path),
        Err(OpenJournalRaftNodeStoresError::HardState(_))
    ));
    assert_eq!(
        std::fs::read(path.join("hard-state")).expect("unchanged"),
        bytes
    );
    drop(FileRaftNodeStores::open(&path).expect("legacy still opens"));
    std::fs::remove_dir_all(path).expect("cleanup");
}

#[test]
fn journal_commit_floor_protects_log_repair() {
    use crate::{PersistedRaftLogEntry, RaftLogSegment};
    use rafter::{LogIndex, Term};
    use std::io::Write;
    for commit in [1, 2] {
        let path = test_store_path("journal-repair-floor");
        std::fs::create_dir(&path).expect("replica directory");
        let (mut hard, mut log, snapshots) = JournalRaftNodeStores::open(&path)
            .expect("open")
            .into_parts();
        log.append_entries(&[PersistedRaftLogEntry::application(
            LogIndex(1),
            Term(1),
            b"committed".to_vec(),
        )])
        .expect("log append");
        hard.write_hard_state(RaftHardState {
            commit_index: LogIndex(commit),
            ..RaftHardState::default()
        })
        .expect("commit floor");
        drop((hard, log, snapshots));
        let log_path = path.join("log");
        std::fs::OpenOptions::new()
            .append(true)
            .open(&log_path)
            .expect("open log")
            .write_all(&[0, 0])
            .expect("torn log tail");
        let before = std::fs::read(&log_path).expect("read log");
        let recovered = JournalRaftNodeStores::open_repairing_uncommitted_log_tail(&path);
        if commit == 1 {
            let (hard, log, snapshots) = recovered.expect("uncommitted tail repaired").into_parts();
            assert_eq!(hard.current().commit_index, LogIndex(1));
            assert_eq!(log.next_index(), LogIndex(2));
            drop((hard, log, snapshots));
            assert_eq!(
                std::fs::read(&log_path).expect("repaired log").len(),
                before.len() - 2
            );
        } else {
            assert!(matches!(
                recovered,
                Err(OpenJournalRaftNodeStoresError::Stores(
                    OpenFileRaftNodeStoresError::Log(_)
                ))
            ));
            assert_eq!(std::fs::read(&log_path).expect("protected log"), before);
        }
        std::fs::remove_dir_all(path).expect("cleanup");
    }
}
