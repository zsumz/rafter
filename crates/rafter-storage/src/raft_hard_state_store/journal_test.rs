//! Journal recovery distinguishes incomplete appends from complete corruption.

use super::test_support::{remove_test_file, test_store_path};
use crate::{
    encode_raft_hard_state,
    format::v1::hard_state_journal::{HEADER_LEN, RECORD_LEN},
    FileRaftHardStateStore, JournalRaftHardStateStore as Journal,
    OpenJournalRaftHardStateStoreError as OpenError, RaftHardState, RaftHardStateStore,
    RaftHardStateStoreWriteError,
};
use rafter::{CommittedConfiguration, ConfigurationId, LogIndex, NodeId, Term};
use std::fs;

fn state(n: u64) -> RaftHardState {
    RaftHardState {
        current_term: Term(n),
        voted_for: Some(NodeId(2)),
        commit_index: LogIndex(n * 5),
        committed_configuration: Some(CommittedConfiguration {
            index: LogIndex(n),
            config_id: ConfigurationId(n),
        }),
    }
}

#[test]
fn complete_records_recover_every_hard_state_field() {
    let path = test_store_path("journal-roundtrip");
    let mut store = Journal::open(&path).expect("create");
    assert_eq!(store.current(), RaftHardState::default());
    for n in 1..=8 {
        store.write_hard_state(state(n)).expect("append");
    }
    assert_eq!(store.current(), state(8));
    assert_eq!(
        fs::metadata(&path).expect("metadata").len(),
        (HEADER_LEN + 8 * RECORD_LEN) as u64
    );
    drop(store);
    assert_eq!(Journal::open(&path).expect("reopen").current(), state(8));
    remove_test_file(path.clone());
}

#[test]
fn every_incomplete_tail_is_repaired_before_the_next_append() {
    let path = test_store_path("journal-torn-tail");
    let mut store = Journal::open(&path).expect("create");
    store.write_hard_state(state(1)).expect("first");
    drop(store);
    let prefix = fs::read(&path).expect("read");
    let next = encode_raft_hard_state(&state(2));
    assert_eq!(next.len(), RECORD_LEN);
    for cut in 1..RECORD_LEN {
        let mut bytes = prefix.clone();
        bytes.extend_from_slice(&next[..cut]);
        fs::write(&path, bytes).expect("crashed append");
        let mut store = Journal::open(&path).expect("recover tail");
        assert_eq!(store.current(), state(1), "cut {cut}");
        assert_eq!(
            fs::metadata(&path).expect("metadata").len(),
            prefix.len() as u64
        );
        store
            .write_hard_state(state(3))
            .expect("append after repair");
        drop(store);
        assert_eq!(
            Journal::open(&path)
                .expect("reopen repaired append")
                .current(),
            state(3)
        );
    }
    remove_test_file(path.clone());
}

#[test]
fn complete_corruption_anywhere_is_rejected_without_truncation() {
    let path = test_store_path("journal-corrupt");
    let mut store = Journal::open(&path).expect("create");
    store.write_hard_state(state(1)).expect("first");
    store.write_hard_state(state(2)).expect("second");
    drop(store);
    let valid = fs::read(&path).expect("read");
    for record in 0..2 {
        for field in 0..RECORD_LEN {
            let mut bytes = valid.clone();
            bytes[HEADER_LEN + record * RECORD_LEN + field] ^= 1;
            bytes.extend_from_slice(&[1, 2, 3]);
            fs::write(&path, &bytes).expect("corrupt");
            assert!(
                matches!(Journal::open(&path), Err(OpenError::Record { offset, .. }) if offset == (HEADER_LEN + record * RECORD_LEN) as u64)
            );
            assert_eq!(fs::read(&path).expect("unchanged"), bytes);
        }
    }
    remove_test_file(path.clone());
}

#[test]
fn incomplete_and_corrupt_headers_are_never_initialized_as_empty() {
    let path = test_store_path("journal-header");
    drop(Journal::open(&path).expect("create"));
    let header = fs::read(&path).expect("header");
    for cut in 0..HEADER_LEN {
        fs::write(&path, &header[..cut]).expect("truncate header");
        assert!(matches!(
            Journal::open(&path),
            Err(OpenError::InvalidHeader)
        ));
        assert_eq!(fs::read(&path).expect("unchanged"), header[..cut]);
    }
    for byte in 0..HEADER_LEN {
        let mut bytes = header.clone();
        bytes[byte] ^= 1;
        fs::write(&path, &bytes).expect("corrupt header");
        assert!(matches!(
            Journal::open(&path),
            Err(OpenError::InvalidHeader)
        ));
    }
    remove_test_file(path.clone());
}

#[test]
fn legacy_and_journal_backends_reject_each_others_format() {
    let path = test_store_path("journal-format-switch");
    let mut legacy = FileRaftHardStateStore::open(&path).expect("legacy create");
    legacy.write_hard_state(state(7)).expect("legacy write");
    drop(legacy);
    let bytes = fs::read(&path).expect("read legacy");
    assert!(matches!(
        Journal::open(&path),
        Err(OpenError::InvalidHeader)
    ));
    assert_eq!(fs::read(&path).expect("unchanged"), bytes);
    remove_test_file(path.clone());
    let mut journal = Journal::open(&path).expect("create journal");
    assert!(FileRaftHardStateStore::open(&path).is_err());
    journal.write_hard_state(state(8)).expect("journal write");
    assert!(FileRaftHardStateStore::open(&path).is_err());
    drop(journal);
    remove_test_file(path.clone());
}

#[test]
fn ambiguous_append_and_sync_failures_poison_until_reopen() {
    use crate::storage_failpoint_test::{arm, DurabilityPoint};
    let path = test_store_path("journal-poison");
    for point in [
        DurabilityPoint::JournalAfterAppend,
        DurabilityPoint::JournalAfterSync,
    ] {
        let mut store = Journal::open(&path).expect("create");
        store.write_hard_state(state(1)).expect("first");
        let guard = arm(point);
        assert!(matches!(
            store.write_hard_state(state(2)),
            Err(RaftHardStateStoreWriteError::Io { .. })
        ));
        guard.assert_triggered();
        assert!(store.requires_reopen());
        assert_eq!(store.current(), state(1));
        let bytes = fs::read(&path).expect("after ambiguous write");
        assert_eq!(
            store.write_hard_state(state(3)),
            Err(RaftHardStateStoreWriteError::StoreRequiresReopen)
        );
        assert_eq!(fs::read(&path).expect("no additional write"), bytes);
        drop(store);
        // The complete unacknowledged record survived this injected error.
        let mut recovered = Journal::open(&path).expect("reopen resyncs");
        assert_eq!(recovered.current(), state(2));
        recovered
            .write_hard_state(state(3))
            .expect("usable after reopen");
        drop(recovered);
        remove_test_file(path.clone());
    }
}

#[test]
fn interrupted_creation_and_repair_can_be_reopened() {
    use crate::storage_failpoint_test::{arm, DurabilityPoint};
    let path = test_store_path("journal-open-retry");
    let guard = arm(DurabilityPoint::JournalOpenAfterSync);
    assert!(matches!(Journal::open(&path), Err(OpenError::Io { .. })));
    guard.assert_triggered();
    let mut store = Journal::open(&path).expect("retry creation directory sync");
    store.write_hard_state(state(1)).expect("first");
    drop(store);
    let mut bytes = fs::read(&path).expect("read");
    bytes.extend_from_slice(&[42; 13]);
    fs::write(&path, bytes).expect("torn tail");
    let guard = arm(DurabilityPoint::JournalOpenAfterSync);
    assert!(matches!(Journal::open(&path), Err(OpenError::Io { .. })));
    guard.assert_triggered();
    assert_eq!(
        Journal::open(&path).expect("retry repair").current(),
        state(1)
    );
    remove_test_file(path.clone());
}
