//! Rotation preserves acknowledged state through each publication failure boundary.
use super::{
    journal_checkpoint::{temp_path, RECORD_LIMIT},
    test_support::{remove_test_file, test_store_path},
};
use crate::{
    encode_raft_hard_state,
    format::v1::hard_state_journal::{self as format, HEADER_LEN, RECORD_LEN},
    storage_failpoint_test::{arm, DurabilityPoint},
    JournalRaftHardStateStore as Journal, OpenJournalRaftHardStateStoreError as OpenError,
    RaftHardState, RaftHardStateStore, RaftHardStateStoreWriteError,
};
use rafter::{CommittedConfiguration, ConfigurationId, LogIndex, NodeId, Term};
use std::{fs, path::Path};

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
fn journal(path: &Path, records: u64) -> Vec<u8> {
    let mut bytes = format::header().to_vec();
    for _ in 0..records {
        bytes.extend_from_slice(&encode_raft_hard_state(&state(1)));
    }
    fs::write(path, &bytes).expect("fixture");
    bytes
}
fn size(path: &Path) -> u64 {
    fs::metadata(path).expect("metadata").len()
}
fn cleanup(path: &Path) {
    let _ = fs::remove_file(temp_path(path));
    remove_test_file(path.to_path_buf());
}

#[test]
fn threshold_checkpoints_all_fields_and_later_appends_use_the_published_file() {
    let path = test_store_path("journal-checkpoint-limit");
    journal(&path, RECORD_LIMIT - 1);
    let mut store = Journal::open(&path).expect("open");
    store
        .write_hard_state(state(2))
        .expect("last ordinary append");
    assert_eq!(
        size(&path),
        HEADER_LEN as u64 + RECORD_LIMIT * RECORD_LEN as u64
    );
    crate::telemetry::set_enabled(true);
    store.write_hard_state(state(3)).expect("checkpoint");
    let metrics = crate::telemetry::snapshot();
    crate::telemetry::set_enabled(false);
    assert_eq!(
        metrics
            .iter()
            .find(|(name, _)| *name == "hard_state_sync")
            .expect("sync")
            .1
            .calls,
        1
    );
    assert_eq!(
        metrics
            .iter()
            .find(|(name, _)| *name == "hard_state_directory_sync")
            .expect("directory")
            .1
            .calls,
        1
    );
    assert_eq!(size(&path), (HEADER_LEN + RECORD_LEN) as u64);
    assert_eq!(store.current(), state(3));
    assert!(!temp_path(&path).exists());
    store
        .write_hard_state(state(4))
        .expect("append after checkpoint");
    assert_eq!(size(&path), (HEADER_LEN + RECORD_LEN * 2) as u64);
    drop(store);
    let reopened = Journal::open(&path).expect("reopen");
    assert_eq!(reopened.current(), state(4));
    drop(reopened);
    cleanup(&path);
}

#[test]
fn older_oversized_journal_repairs_tail_then_checkpoints_on_first_write() {
    let path = test_store_path("journal-checkpoint-legacy");
    let mut bytes = journal(&path, RECORD_LIMIT * 2);
    bytes.extend_from_slice(&encode_raft_hard_state(&state(2))[..7]);
    fs::write(&path, bytes).expect("torn legacy tail");
    let mut store = Journal::open(&path).expect("validate and repair");
    assert_eq!(store.current(), state(1));
    store.write_hard_state(state(3)).expect("checkpoint");
    drop(store);
    assert_eq!(size(&path), (HEADER_LEN + RECORD_LEN) as u64);
    let reopened = Journal::open(&path).expect("reopen");
    assert_eq!(reopened.current(), state(3));
    drop(reopened);
    cleanup(&path);
}

#[test]
fn every_checkpoint_publication_failure_poisons_without_advancing_cached_state() {
    for (point, recovered) in [
        (DurabilityPoint::JournalCheckpointAfterWrite, state(1)),
        (DurabilityPoint::JournalCheckpointAfterSync, state(1)),
        (DurabilityPoint::JournalCheckpointAfterRename, state(2)),
        (
            DurabilityPoint::JournalCheckpointAfterDirectorySync,
            state(2),
        ),
    ] {
        let path = test_store_path("journal-checkpoint-failure");
        journal(&path, RECORD_LIMIT);
        let mut store = Journal::open(&path).expect("open");
        let guard = arm(point);
        assert!(matches!(
            store.write_hard_state(state(2)),
            Err(RaftHardStateStoreWriteError::Io { .. })
        ));
        guard.assert_triggered();
        assert!(store.requires_reopen());
        assert_eq!(store.current(), state(1));
        assert_eq!(
            store.write_hard_state(state(3)),
            Err(RaftHardStateStoreWriteError::StoreRequiresReopen)
        );
        drop(store);
        let mut reopened = Journal::open(&path).expect("reopen resyncs published path");
        assert_eq!(reopened.current(), recovered);
        reopened.write_hard_state(state(3)).expect("resume safely");
        drop(reopened);
        let final_store = Journal::open(&path).expect("reopen resumed");
        assert_eq!(final_store.current(), state(3));
        drop(final_store);
        cleanup(&path);
    }
}

#[test]
fn every_partial_or_complete_unpublished_checkpoint_is_ignored() {
    let path = test_store_path("journal-checkpoint-orphan");
    let original = journal(&path, RECORD_LIMIT);
    let mut unpublished = format::header().to_vec();
    unpublished.extend_from_slice(&encode_raft_hard_state(&state(99)));
    for cut in 0..=unpublished.len() {
        fs::write(&path, &original).expect("restore durable prefix");
        fs::write(temp_path(&path), &unpublished[..cut]).expect("orphan");
        let mut store = Journal::open(&path).expect("ignore orphan");
        assert_eq!(store.current(), state(1), "cut={cut}");
        assert_eq!(fs::read(&path).expect("durable bytes"), original);
        store
            .write_hard_state(state(2))
            .expect("replace orphan before publication");
        drop(store);
        let reopened = Journal::open(&path).expect("published recovery");
        assert_eq!(reopened.current(), state(2));
        drop(reopened);
    }
    cleanup(&path);
}

#[test]
fn complete_corruption_is_not_hidden_by_checkpointing() {
    let path = test_store_path("journal-checkpoint-corrupt");
    let mut bytes = journal(&path, RECORD_LIMIT);
    bytes[HEADER_LEN + 7] ^= 1;
    fs::write(&path, &bytes).expect("corrupt complete record");
    assert!(matches!(
        Journal::open(&path),
        Err(OpenError::Record { .. })
    ));
    assert_eq!(fs::read(&path).expect("unchanged"), bytes);
    cleanup(&path);
}
