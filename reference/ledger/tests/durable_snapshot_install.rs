//! What a Raft-driven install is, against a real snapshot store on disk.
//!
//! Every other snapshot test in this crate installs bytes the test itself is
//! holding. That is the shape a *local* restore takes and it is not the shape
//! Rafter's own install path produces: there, the payload has already been
//! staged and promoted into the replica's snapshot store, and the application
//! is handed a [`rafter::RaftSnapshot`] descriptor with an empty payload. A
//! machine that declares `SnapshotSupport::Supported` and refuses that shape
//! poisons its group the first time a follower falls behind a compaction, and
//! nothing that only round-trips a payload in memory would notice.
//!
//! So these tests write real envelopes into a real [`FileRaftSnapshotStore`]
//! under a real scratch directory and hand the machine only the descriptor.
//!
//! Reading a snapshot off the store is a new way for bytes to arrive; it is not
//! a new authority. The applied-floor refusal still runs first — before the
//! store is opened at all — and the decode and index check the bytes pass are
//! the ones an inline payload passes, because they are the same code.

#[allow(dead_code)]
mod support;

#[path = "support/scratch.rs"]
mod scratch;

use std::path::{Path, PathBuf};

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, LogIndex,
    NodeId, RaftSnapshot, RaftSnapshotMetadata, SnapshotGroupId, Term,
};
use rafter_app::state_machine::{
    ApplicationSnapshot, ApplicationSnapshotError, ApplyBatch, ApplyEntry, ReplicatedStateMachine,
};
use rafter_reference_ledger::{
    store::LedgerStore, AccountId, ApplyDisposition, Command, DurableLedgerError,
    DurableLedgerStateMachine, LedgerAdapterError, LedgerStateMachine, Mutation,
};
use rafter_storage::{FileRaftSnapshotStore, PersistedRaftSnapshot, RaftSnapshotStore};

use scratch::ScratchDir;
use support::{amount, config, execute, open_session};

const ALPHA: AccountId = AccountId::new(11);
const BETA: AccountId = AccountId::new(12);
const GAMMA: AccountId = AccountId::new(13);

fn workload() -> Vec<Command> {
    vec![
        open_session(0, 1),
        execute(0, 1, 1, Mutation::OpenAccount { account_id: ALPHA }),
        execute(0, 1, 2, Mutation::OpenAccount { account_id: BETA }),
        execute(
            0,
            1,
            3,
            Mutation::Deposit {
                account_id: ALPHA,
                amount: amount(40),
            },
        ),
        execute(
            0,
            1,
            4,
            Mutation::Transfer {
                from: ALPHA,
                to: BETA,
                amount: amount(15),
            },
        ),
    ]
}

/// A second history, so a store can hold a snapshot that is not the one under
/// test.
fn other_workload() -> Vec<Command> {
    vec![
        open_session(1, 1),
        execute(1, 1, 1, Mutation::OpenAccount { account_id: GAMMA }),
        execute(
            1,
            1,
            2,
            Mutation::Deposit {
                account_id: GAMMA,
                amount: amount(7),
            },
        ),
    ]
}

/// Where a replica rooted at `directory` keeps its Raft snapshots.
///
/// The production replica hands the machine the `snapshots` child of the
/// directory its `FileRaftNodeStores` owns; these tests keep the same shape so
/// the path under test is the path that ships.
fn snapshot_dir(directory: &Path) -> PathBuf {
    directory.join("raft/snapshots")
}

fn open(directory: &Path) -> DurableLedgerStateMachine {
    let store = LedgerStore::open(directory, config(2, 4)).expect("a ledger store opens");
    DurableLedgerStateMachine::new(store, snapshot_dir(directory))
}

fn apply_all(app: &mut DurableLedgerStateMachine, commands: &[Command]) {
    for (position, command) in commands.iter().enumerate() {
        app.apply_batch(ApplyBatch {
            entries: vec![ApplyEntry {
                index: LogIndex(position as u64 + 1),
                term: Term(1),
                command: command.clone(),
                local_proposal_id: None,
            }],
        })
        .unwrap_or_else(|error| panic!("transaction {} must commit: {error}", position + 1));
    }
}

/// The Raft-visible descriptor a leader's compaction publishes for `payload`.
///
/// `writer_id` is another node on purpose: a promoted snapshot is by definition
/// one this replica did not write.
fn descriptor(at: LogIndex, payload: &[u8]) -> RaftSnapshot {
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("ledger").expect("a stable group id"),
        NodeId(2),
        at,
        Term(1),
        Term(1),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("ledger").expect("a stable kind"),
            ApplicationSnapshotVersion::new(1).expect("a non-zero version"),
        ),
    )
    .expect("a snapshot boundary above zero in a visible term");
    RaftSnapshot::from_payload(metadata, payload)
}

/// The install Rafter itself produces: the descriptor, and no bytes.
fn promoted(descriptor: &RaftSnapshot) -> ApplicationSnapshot {
    ApplicationSnapshot {
        applied_index: descriptor.metadata.last_included_index,
        payload: Vec::new(),
        raft_snapshot: Some(descriptor.clone()),
    }
}

/// Stages `payload` in `directory`'s snapshot store the way a completed
/// transfer's promotion leaves it: current, verified, and readable by
/// descriptor.
fn promote_into_store(directory: &Path, snapshot: &RaftSnapshot, payload: Vec<u8>) {
    let mut store =
        FileRaftSnapshotStore::open(snapshot_dir(directory)).expect("a snapshot store opens");
    store
        .write_snapshot(PersistedRaftSnapshot {
            metadata: snapshot.metadata.clone(),
            application_payload: payload,
        })
        .expect("a complete snapshot is publishable");
}

/// Runs `commands` on a throwaway replica and returns the snapshot it can build
/// at its own applied index, with the descriptor a leader would publish for it.
fn snapshot_from(label: &str, commands: &[Command]) -> (RaftSnapshot, Vec<u8>) {
    let scratch = ScratchDir::new(label);
    let mut app = open(scratch.path());
    apply_all(&mut app, commands);
    let at = LogIndex(commands.len() as u64);
    let built = app
        .build_snapshot(at)
        .expect("a state machine snapshots its own applied index");
    let payload = built.payload;
    (descriptor(at, &payload), payload)
}

/// The whole point: a descriptor with no bytes installs, because the bytes are
/// in the store the descriptor names.
#[test]
fn a_promoted_snapshot_installs_from_the_replicas_own_snapshot_store() {
    let (snapshot, payload) = snapshot_from("promoted-install-source", &workload());
    let expected = {
        let scratch = ScratchDir::new("promoted-install-expected");
        let mut app = open(scratch.path());
        apply_all(&mut app, &workload());
        app.ledger().view()
    };

    let scratch = ScratchDir::new("promoted-install-target");
    promote_into_store(scratch.path(), &snapshot, payload);
    let mut app = open(scratch.path());
    assert_eq!(app.store().applied_index(), LogIndex::ZERO);

    app.install_snapshot(promoted(&snapshot))
        .expect("the promoted payload installs");

    assert_eq!(app.store().applied_index(), LogIndex(5));
    assert_eq!(
        app.ledger().view(),
        expected,
        "every account, session, and cached result came back off the store"
    );

    // Durability, not just adoption: reopening reads what the install published
    // rather than what this handle happens to remember.
    drop(app);
    let reopened = open(scratch.path());
    assert_eq!(reopened.store().applied_index(), LogIndex(5));
    assert_eq!(reopened.ledger().view(), expected);
}

/// A store that cannot serve the transfer is a refusal, not a guess.
#[test]
fn a_promoted_install_the_store_cannot_serve_is_refused_and_changes_nothing() {
    let (snapshot, payload) = snapshot_from("promoted-missing-source", &workload());

    let scratch = ScratchDir::new("promoted-missing-target");
    let mut app = open(scratch.path());
    apply_all(&mut app, &workload()[..2]);
    let applied_before = app.store().applied_index();
    let view_before = app.ledger().view();

    // Nothing has been promoted into this replica's store. The directory does
    // not even exist yet, which is the state a replica that has never received
    // a transfer is in.
    assert!(
        matches!(
            app.install_snapshot(promoted(&snapshot)),
            Err(ApplicationSnapshotError::StateMachine(
                DurableLedgerError::Adapter(LedgerAdapterError::SnapshotPayloadUnavailable { .. })
            ))
        ),
        "an install whose bytes are nowhere is refused by name"
    );
    assert_eq!(app.store().applied_index(), applied_before);
    assert_eq!(app.ledger().view(), view_before);

    // A store holding some *other* snapshot answers for that one and no other:
    // the read is keyed by transfer id, length, and checksum together.
    let (other, other_payload) = snapshot_from("promoted-missing-other", &other_workload());
    promote_into_store(scratch.path(), &other, other_payload);
    assert!(
        matches!(
            app.install_snapshot(promoted(&snapshot)),
            Err(ApplicationSnapshotError::StateMachine(
                DurableLedgerError::Adapter(LedgerAdapterError::SnapshotPayloadUnavailable { .. })
            ))
        ),
        "a store holding a different transfer serves nothing for this one"
    );
    assert_eq!(app.store().applied_index(), applied_before);
    assert_eq!(app.ledger().view(), view_before);

    // Promote the right one and the same install succeeds, which is what makes
    // the two refusals above evidence about the transfer rather than about the
    // install path being dead.
    promote_into_store(scratch.path(), &snapshot, payload);
    app.install_snapshot(promoted(&snapshot))
        .expect("the promoted payload installs once the store holds it");
    assert_eq!(app.store().applied_index(), LogIndex(5));
}

/// The applied floor is checked before the store is ever opened.
#[test]
fn a_stale_promoted_install_is_refused_ahead_of_the_store() {
    let scratch = ScratchDir::new("promoted-stale");
    let mut app = open(scratch.path());
    apply_all(&mut app, &workload());
    let applied_before = app.store().applied_index();

    let stale = descriptor(LogIndex(2), &[0; 4]);
    assert!(
        matches!(
            app.install_snapshot(promoted(&stale)),
            Err(ApplicationSnapshotError::StateMachine(
                DurableLedgerError::Adapter(LedgerAdapterError::SnapshotBehindAppliedIndex {
                    snapshot_index: LogIndex(2),
                    ..
                })
            ))
        ),
        "an install behind the floor is refused on the index, before any read"
    );
    assert!(
        !snapshot_dir(scratch.path()).exists(),
        "and refusing on the index means the store was never opened"
    );
    assert_eq!(app.store().applied_index(), applied_before);
}

#[test]
fn a_streamed_multi_chunk_schema_preserves_state_floor_and_cached_retry_after_reopen() {
    let scratch = ScratchDir::new("streamed-ledger-schema");
    let bounds = config(1, 2048);
    let mut original = LedgerStateMachine::new(bounds);
    let mut commands = vec![open_session(0, 1)];
    for id in 0..2048 {
        commands.push(execute(
            0,
            1,
            id + 1,
            Mutation::OpenAccount {
                account_id: AccountId::new(id),
            },
        ));
    }
    let deposit = execute(
        0,
        1,
        2049,
        Mutation::Deposit {
            account_id: AccountId::new(0),
            amount: amount(9),
        },
    );
    commands.push(deposit.clone());
    let at = LogIndex(u64::try_from(commands.len()).unwrap());
    original
        .apply_batch(ApplyBatch {
            entries: commands
                .into_iter()
                .enumerate()
                .map(|(index, command)| ApplyEntry {
                    index: LogIndex(u64::try_from(index).unwrap() + 1),
                    term: Term(1),
                    command,
                    local_proposal_id: None,
                })
                .collect(),
        })
        .unwrap();
    let expected = original.ledger().view();
    let built = original.build_snapshot(at).unwrap();
    assert!(built.payload.len() > 2 * 16 * 1024);
    let snapshot = descriptor(at, &built.payload);
    promote_into_store(scratch.path(), &snapshot, built.payload);
    let mut app = DurableLedgerStateMachine::new(
        LedgerStore::open(scratch.path(), bounds).unwrap(),
        snapshot_dir(scratch.path()),
    );
    app.install_snapshot(promoted(&snapshot)).unwrap();
    assert_eq!(app.ledger().view(), expected);
    assert_eq!(app.store().applied_index(), at);
    drop(app);
    let mut reopened = DurableLedgerStateMachine::new(
        LedgerStore::open(scratch.path(), bounds).unwrap(),
        snapshot_dir(scratch.path()),
    );
    assert_eq!(reopened.ledger().view(), expected);
    assert_eq!(reopened.store().applied_index(), at);
    let retry = reopened
        .apply_batch(ApplyBatch {
            entries: vec![ApplyEntry {
                index: LogIndex(at.0 + 1),
                term: Term(1),
                command: deposit,
                local_proposal_id: None,
            }],
        })
        .unwrap();
    assert_eq!(retry[0].result.disposition, ApplyDisposition::Replayed);
    assert_eq!(reopened.ledger().view(), expected);
}

#[test]
fn inadmissible_descriptors_do_not_open_or_create_the_snapshot_store() {
    let (snapshot, _) = snapshot_from("streamed-descriptor", &workload());
    for mode in 0..4 {
        let scratch = ScratchDir::new("streamed-descriptor-refusal");
        let mut app = open(scratch.path());
        let mut offered = promoted(&snapshot);
        let descriptor = offered.raft_snapshot.as_mut().unwrap();
        match mode {
            0 => offered.applied_index = LogIndex(snapshot.metadata.last_included_index.0 + 1),
            1 => {
                descriptor.metadata.application.kind =
                    ApplicationSnapshotKind::new("other").unwrap();
            }
            2 => {
                descriptor.metadata.application.version =
                    ApplicationSnapshotVersion::new(2).unwrap();
            }
            _ => descriptor.application_payload_len = u64::MAX,
        }
        assert!(matches!(
            app.install_snapshot(offered),
            Err(ApplicationSnapshotError::StateMachine(
                DurableLedgerError::Adapter(LedgerAdapterError::SnapshotPayloadUnavailable { .. })
            ))
        ));
        assert!(!snapshot_dir(scratch.path()).exists());
        assert_eq!(app.store().applied_index(), LogIndex::ZERO);
        assert_eq!(app.ledger().summary().open_accounts, 0);
    }
}
