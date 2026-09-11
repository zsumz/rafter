//! Real-file durability, recovery, and domain-identity tests.
use super::*;
use crate::{
    BorrowedPersistedRaftLogEntry, PersistedRaftLogEntry, RaftHardState, RaftHardStateStore,
    RaftLogSegment,
};
use rafter::{LogIndex, Term};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
mod recovery;

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "rafter-wal-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn open(
        &self,
    ) -> (
        WalRaftHardStateStore,
        WalRaftLogSegment,
        crate::FileRaftSnapshotStore,
    ) {
        WalRaftNodeStores::open(&self.0).unwrap().into_parts()
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn entry(index: u64, value: &[u8]) -> PersistedRaftLogEntry {
    PersistedRaftLogEntry::application(LogIndex(index), Term(1), value.to_vec())
}
fn hard(commit: u64) -> RaftHardState {
    RaftHardState {
        current_term: Term(1),
        commit_index: LogIndex(commit),
        ..RaftHardState::default()
    }
}
fn publish(
    h: &WalRaftHardStateStore,
    l: &mut WalRaftLogSegment,
    entries: &[PersistedRaftLogEntry],
    commit: u64,
    truncate: Option<LogIndex>,
) -> Result<DurableReceipt, RaftPersistenceBatchError> {
    let borrowed: Vec<_> = entries
        .iter()
        .map(|e| BorrowedPersistedRaftLogEntry::new(e.index, e.term, &e.kind))
        .collect();
    l.persist_batch(
        &h.persistence_domain().unwrap(),
        RaftPersistenceBatch {
            truncate_from: truncate,
            entries: &borrowed,
            hard_state: Some(hard(commit)),
        },
    )
    .map(Option::unwrap)
}

#[test]
fn combined_append_and_commit_has_one_sync_and_survives_reopen() {
    let dir = Directory::new();
    let (h, mut l, s) = dir.open();
    let receipt = publish(&h, &mut l, &[entry(1, b"one"), entry(2, b"two")], 2, None).unwrap();
    assert_eq!(l.sync_count(), 1);
    assert_eq!(receipt.operation, 1);
    assert_eq!(receipt.hard_state, h.current());
    assert_eq!(receipt.next_index, LogIndex(3));
    let domain = receipt.domain;
    drop((h, l, s));
    let (h, l, _s) = dir.open();
    assert_eq!(h.current(), hard(2));
    assert_eq!(l.replay_entries(), vec![entry(1, b"one"), entry(2, b"two")]);
    assert_ne!(domain, h.persistence_domain().unwrap());
}
#[test]
fn suffix_replacement_is_atomic_and_cannot_erase_committed_entries() {
    let dir = Directory::new();
    let (h, mut l, _s) = dir.open();
    publish(&h, &mut l, &[entry(1, b"one"), entry(2, b"old")], 1, None).unwrap();
    publish(&h, &mut l, &[entry(2, b"new")], 2, Some(LogIndex(2))).unwrap();
    assert_eq!(l.sync_count(), 2);
    assert_eq!(l.replay_entries(), vec![entry(1, b"one"), entry(2, b"new")]);
    assert!(publish(&h, &mut l, &[entry(2, b"bad")], 2, Some(LogIndex(2))).is_err());
    assert_eq!(l.sync_count(), 2);
}
#[test]
fn another_coordinators_receipt_domain_cannot_publish() {
    let dir = Directory::new();
    let (h, mut l, _s) = dir.open();
    let bytes = fs::read(dir.0.join("hard-state")).unwrap();
    let result = l.persist_batch(
        &PersistenceDomain::new(),
        RaftPersistenceBatch {
            truncate_from: None,
            entries: &[],
            hard_state: Some(hard(0)),
        },
    );
    assert_eq!(result, Err(RaftPersistenceBatchError::DomainMismatch));
    assert_eq!(h.current(), RaftHardState::default());
    assert_eq!(fs::read(dir.0.join("hard-state")).unwrap(), bytes);
}
#[test]
fn failed_sync_keeps_acknowledged_views_and_poisons_both_handles() {
    let dir = Directory::new();
    let (mut h, mut l, s) = dir.open();
    publish(&h, &mut l, &[entry(1, b"one")], 1, None).unwrap();
    l.0.lock().unwrap().fail_after_write = true;
    assert!(publish(&h, &mut l, &[entry(2, b"uncertain")], 2, None).is_err());
    assert_eq!(h.current(), hard(1));
    assert_eq!(l.next_index(), LogIndex(2));
    assert_eq!(l.sync_count(), 1);
    assert_eq!(
        publish(&h, &mut l, &[], 1, None),
        Err(RaftPersistenceBatchError::StoreRequiresReopen)
    );
    assert_eq!(
        h.write_hard_state(hard(1)),
        Err(crate::RaftHardStateStoreWriteError::StoreRequiresReopen)
    );
    drop((h, l, s));
    // A complete write whose sync failed may recover; it was never acknowledged.
    let (h, l, _s) = dir.open();
    assert_eq!(h.current(), hard(2));
    assert_eq!(l.next_index(), LogIndex(3));
}
#[test]
fn standalone_writes_remain_synchronous_and_noops_do_not_resync() {
    let dir = Directory::new();
    let (mut h, mut l, _s) = dir.open();
    h.write_hard_state(hard(0)).unwrap();
    l.append_entries(&[entry(1, b"one")]).unwrap();
    h.write_hard_state(hard(1)).unwrap();
    assert_eq!(l.sync_count(), 3);
    publish(&h, &mut l, &[], 1, None).unwrap();
    assert_eq!(l.sync_count(), 3);
}
#[test]
fn commit_beyond_log_and_maximum_index_are_rejected_before_writing() {
    let dir = Directory::new();
    let (h, mut l, _s) = dir.open();
    assert!(publish(&h, &mut l, &[], 1, None).is_err());
    assert!(publish(&h, &mut l, &[entry(u64::MAX, b"max")], 0, None).is_err());
    assert_eq!(l.sync_count(), 0);
    assert_eq!(h.current(), RaftHardState::default());
}
