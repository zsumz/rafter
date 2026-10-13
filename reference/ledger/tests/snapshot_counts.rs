//! Impossible collection counts are refused before record decoding or publication.
//!
//! These fixtures keep a real account, session, and nonzero applied floor while
//! changing only a count. The modest-count regression also runs safely against
//! the old eager-allocation decoder; maximum-count cases run after the fix.

#[path = "support/scratch.rs"]
mod scratch;

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, LogIndex,
    NodeId, RaftSnapshot, RaftSnapshotMetadata, SnapshotGroupId, Term,
};
use rafter_app::state_machine::{
    ApplicationSnapshot, ApplicationSnapshotError, ReplicatedStateMachine,
};
use rafter_reference_ledger::{
    store::{raw_journal, LedgerStore},
    DurableLedgerError, DurableLedgerStateMachine, LedgerAdapterError, LedgerCodecError,
    LedgerConfig, LedgerStateMachine,
};
use rafter_storage::{FileRaftSnapshotStore, PersistedRaftSnapshot, RaftSnapshotStore};
use scratch::ScratchDir;

const FLOOR: LogIndex = LogIndex(7);
const ACCOUNT_COUNT: usize = 25;
const SESSION_COUNT: usize = 45;

fn config() -> LedgerConfig {
    LedgerConfig::new(2, 4).unwrap()
}

// Independent v1 frame: floor 7, zero supply, account 5 with zero balance,
// and client 0 at epoch 1 with no cached completion.
fn payload() -> Vec<u8> {
    let mut bytes = vec![1];
    bytes.extend_from_slice(&FLOOR.0.to_be_bytes());
    bytes.extend_from_slice(&0_u128.to_be_bytes());
    bytes.extend_from_slice(&1_u32.to_be_bytes());
    bytes.extend_from_slice(&5_u64.to_be_bytes());
    bytes.extend_from_slice(&0_u64.to_be_bytes());
    bytes.extend_from_slice(&1_u32.to_be_bytes());
    bytes.extend_from_slice(&0_u32.to_be_bytes());
    bytes.extend_from_slice(&1_u64.to_be_bytes());
    bytes.push(0);
    bytes
}

fn inline(payload: Vec<u8>) -> ApplicationSnapshot {
    ApplicationSnapshot {
        applied_index: FLOOR,
        payload,
        raft_snapshot: None,
    }
}

fn corrupted_count(offset: usize, count: u32) -> (Vec<u8>, LedgerCodecError) {
    let mut bytes = payload();
    bytes[offset..offset + 4].copy_from_slice(&count.to_be_bytes());
    let required = u64::from(count) * if offset == ACCOUNT_COUNT { 16 } else { 13 }
        + if offset == ACCOUNT_COUNT { 4 } else { 0 };
    let error = match usize::try_from(required) {
        Ok(required) => LedgerCodecError::TruncatedFrame {
            required,
            available: bytes.len() - offset - 4,
        },
        Err(_) => LedgerCodecError::LengthOutOfRange {
            length: u64::from(count),
        },
    };
    (bytes, error)
}

fn check_inline_counts(count: u32) {
    let mut app = LedgerStateMachine::new(config());
    app.install_snapshot(inline(payload())).unwrap();
    let before = app.ledger().view();
    for offset in [ACCOUNT_COUNT, SESSION_COUNT] {
        let (bytes, expected) = corrupted_count(offset, count);
        let result = app.install_snapshot(inline(bytes));
        assert!(
            matches!(result, Err(ApplicationSnapshotError::StateMachine(
                LedgerAdapterError::Codec(found)
            )) if found == expected),
            "count {count} at {offset}: expected {expected:?}, got {result:?}"
        );
        assert_eq!(app.ledger().view(), before);
        assert_eq!(app.applied_index().unwrap(), FLOOR);
    }
    assert_eq!(app.build_snapshot(FLOOR).unwrap().payload, payload());
}

#[test]
fn counts_that_cannot_fit_are_refused_before_record_decoding() {
    check_inline_counts(4096);
}

#[test]
fn maximum_encoded_counts_cannot_expand_a_tiny_snapshot() {
    check_inline_counts(u32::MAX);
}

#[test]
fn valid_minimum_records_round_trip_and_every_truncated_prefix_preserves_state() {
    let mut app = LedgerStateMachine::new(config());
    let bytes = payload();
    app.install_snapshot(inline(bytes.clone())).unwrap();
    assert_eq!(app.build_snapshot(FLOOR).unwrap().payload, bytes);
    let before = app.ledger().view();
    for end in 0..bytes.len() {
        assert!(app.install_snapshot(inline(bytes[..end].to_vec())).is_err());
        assert_eq!(app.ledger().view(), before);
        assert_eq!(app.applied_index().unwrap(), FLOOR);
    }
    // Accounts must leave room for the following session-count field.
    assert!(matches!(
        app.install_snapshot(inline(bytes[..SESSION_COUNT].to_vec())),
        Err(ApplicationSnapshotError::StateMachine(
            LedgerAdapterError::Codec(LedgerCodecError::TruncatedFrame {
                required: 20,
                available: 16
            })
        ))
    ));
}

#[test]
fn promoted_bad_counts_preserve_the_durable_image_and_floor_after_reopen() {
    for offset in [ACCOUNT_COUNT, SESSION_COUNT] {
        let scratch = ScratchDir::new("snapshot-counts");
        let snapshot_dir = scratch.path().join("raft/snapshots");
        let mut app = DurableLedgerStateMachine::new(
            LedgerStore::open(scratch.path(), config()).unwrap(),
            snapshot_dir.clone(),
        );
        app.install_snapshot(inline(payload())).unwrap();
        let before = app.ledger().view();
        let journal = raw_journal::read(scratch.path()).unwrap();
        let (bytes, expected) = corrupted_count(offset, u32::MAX);
        let metadata = RaftSnapshotMetadata::new(
            SnapshotGroupId::new("ledger").unwrap(),
            NodeId(2),
            FLOOR,
            Term(1),
            Term(1),
            ApplicationSnapshotMetadata::new(
                ApplicationSnapshotKind::new("ledger").unwrap(),
                ApplicationSnapshotVersion::new(1).unwrap(),
            ),
        )
        .unwrap();
        let snapshot = RaftSnapshot::from_payload(metadata.clone(), &bytes);
        let mut source = FileRaftSnapshotStore::open(snapshot_dir).unwrap();
        source
            .write_snapshot(PersistedRaftSnapshot {
                metadata,
                application_payload: bytes,
            })
            .unwrap();
        let result = app.install_snapshot(ApplicationSnapshot {
            applied_index: FLOOR,
            payload: Vec::new(),
            raft_snapshot: Some(snapshot),
        });
        assert!(
            matches!(result, Err(ApplicationSnapshotError::StateMachine(
            DurableLedgerError::Adapter(LedgerAdapterError::Codec(found))
        )) if found == expected),
            "{result:?}"
        );
        assert_eq!(app.ledger().view(), before);
        assert_eq!(app.applied_index().unwrap(), FLOOR);
        assert_eq!(raw_journal::read(scratch.path()).unwrap(), journal);
        drop(app);
        let reopened = LedgerStore::open(scratch.path(), config()).unwrap();
        assert_eq!(reopened.ledger().view(), before);
        assert_eq!(reopened.applied_index(), FLOOR);
        assert_eq!(raw_journal::read(scratch.path()).unwrap(), journal);
    }
}
