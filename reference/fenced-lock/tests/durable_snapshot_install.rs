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
//! # Fencing is preserved by refusal, not by trust
//!
//! Reading a snapshot off the store is a new way for bytes to arrive; it is not
//! a new authority. Both refusals a fenced lock lives by still run, in the same
//! order and on the same values:
//!
//! - [`crate::adapter::discipline::admit_install`] refuses a snapshot behind
//!   this replica's applied floor *before* the store is opened, so a stale
//!   promoted install never reaches the state.
//! - `LockStore::install` refuses any publication that would lower an
//!   acknowledged fencing high-water mark, before it writes a byte, whatever
//!   produced the state.
//!
//! The last test here is the one that matters most: a promoted install whose
//! state would drop a mark is refused exactly as a locally built one would be.
//! Without it, "the bytes now come from the store" would be indistinguishable
//! from "the store is a way around the mark discipline".

#[allow(dead_code)]
mod support;

#[path = "support/scratch.rs"]
mod scratch;

use std::path::Path;

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, LogIndex,
    NodeId, RaftSnapshot, RaftSnapshotMetadata, SnapshotGroupId, Term,
};
use rafter_app::state_machine::{
    ApplicationSnapshot, ApplicationSnapshotError, ApplyBatch, ApplyEntry, ReplicatedStateMachine,
};
use rafter_reference_fenced_lock::{
    store::{LockStore, LockStoreError},
    Command, DurableLockError, DurableLockStateMachine, FencingToken, LockAdapterError,
};
use rafter_storage::{FileRaftSnapshotStore, PersistedRaftSnapshot, RaftSnapshotStore};

use scratch::ScratchDir;
use support::{acquire, config, open_session, release, resource, submit};

const RESOURCE: &str = "orders/shard-0";
const OTHER_RESOURCE: &str = "audit/log";

/// A history whose mark outruns its first token: acquire, release, acquire.
///
/// The second tenure is what makes a lost high-water mark visible rather than
/// merely possible — a snapshot that dropped the marks would still restore a
/// held lock, and only the floor would be wrong.
fn workload() -> Vec<Command> {
    vec![
        open_session(0, 1),
        submit(0, 1, 1, acquire(RESOURCE, 10)),
        submit(0, 1, 2, release(RESOURCE, 1)),
        submit(0, 1, 3, acquire(RESOURCE, 10)),
    ]
}

/// A history over a different resource, so its snapshot carries no mark for
/// `RESOURCE` at all.
fn foreign_workload() -> Vec<Command> {
    vec![
        open_session(1, 1),
        submit(1, 1, 1, acquire(OTHER_RESOURCE, 10)),
        submit(1, 1, 2, release(OTHER_RESOURCE, 1)),
        submit(1, 1, 3, acquire(OTHER_RESOURCE, 10)),
        submit(1, 1, 4, release(OTHER_RESOURCE, 2)),
    ]
}

fn open(directory: &Path) -> DurableLockStateMachine {
    let store = LockStore::open(directory, config(2, 4)).expect("a lock store opens");
    DurableLockStateMachine::new(store, snapshot_dir(directory))
}

/// Where a replica rooted at `directory` keeps its Raft snapshots.
///
/// The production replica hands the machine the `snapshots` child of the
/// directory its `FileRaftNodeStores` owns; these tests keep the same shape so
/// the path under test is the path that ships.
fn snapshot_dir(directory: &Path) -> std::path::PathBuf {
    directory.join("raft/snapshots")
}

fn apply_all(app: &mut DurableLockStateMachine, commands: &[Command]) {
    for (position, command) in commands.iter().enumerate() {
        app.apply_batch(ApplyBatch {
            entries: vec![ApplyEntry {
                index: LogIndex(position as u64 + 1),
                term: Term(1),
                command: *command,
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
        SnapshotGroupId::new("fenced-lock").expect("a stable group id"),
        NodeId(2),
        at,
        Term(1),
        Term(1),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("fenced_lock").expect("a stable kind"),
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

fn mark(app: &DurableLockStateMachine, name: &str) -> Option<FencingToken> {
    app.store().acknowledged_mark(resource(name))
}

/// The whole point: a descriptor with no bytes installs, because the bytes are
/// in the store the descriptor names.
#[test]
fn a_promoted_snapshot_installs_from_the_replicas_own_snapshot_store() {
    let (snapshot, payload) = snapshot_from("promoted-install-source", &workload());

    let scratch = ScratchDir::new("promoted-install-target");
    promote_into_store(scratch.path(), &snapshot, payload);
    let mut app = open(scratch.path());
    assert_eq!(app.store().applied_index(), LogIndex::ZERO);

    app.install_snapshot(promoted(&snapshot))
        .expect("the promoted payload installs");

    assert_eq!(app.store().applied_index(), LogIndex(4));
    assert_eq!(
        mark(&app, RESOURCE),
        Some(FencingToken::new(2).expect("a non-zero token")),
        "the mark the second tenure issued came back off the store"
    );
    assert!(
        app.service().status(resource(RESOURCE)).holder.is_some(),
        "and so did the tenure holding it"
    );

    // Durability, not just adoption: reopening reads what the install published
    // rather than what this handle happens to remember.
    drop(app);
    let reopened = open(scratch.path());
    assert_eq!(reopened.store().applied_index(), LogIndex(4));
    assert_eq!(
        mark(&reopened, RESOURCE),
        Some(FencingToken::new(2).expect("a non-zero token"))
    );
}

/// A store that cannot serve the transfer is a refusal, not a guess.
#[test]
fn a_promoted_install_the_store_cannot_serve_is_refused_and_changes_nothing() {
    let (snapshot, payload) = snapshot_from("promoted-missing-source", &workload());

    let scratch = ScratchDir::new("promoted-missing-target");
    let mut app = open(scratch.path());
    apply_all(&mut app, &workload()[..2]);
    let applied_before = app.store().applied_index();
    let mark_before = mark(&app, RESOURCE);
    assert!(mark_before.is_some(), "the fixture holds a mark to lose");

    // Nothing has been promoted into this replica's store. The directory does
    // not even exist yet, which is the state a replica that has never received
    // a transfer is in.
    assert!(
        matches!(
            app.install_snapshot(promoted(&snapshot)),
            Err(ApplicationSnapshotError::StateMachine(
                DurableLockError::Adapter(LockAdapterError::SnapshotPayloadUnavailable { .. })
            ))
        ),
        "an install whose bytes are nowhere is refused by name"
    );
    assert_eq!(app.store().applied_index(), applied_before);
    assert_eq!(mark(&app, RESOURCE), mark_before);

    // A store holding some *other* snapshot answers for that one and no other:
    // the read is keyed by transfer id, length, and checksum together.
    let (other, other_payload) = snapshot_from("promoted-missing-other", &foreign_workload());
    promote_into_store(scratch.path(), &other, other_payload);
    assert!(
        matches!(
            app.install_snapshot(promoted(&snapshot)),
            Err(ApplicationSnapshotError::StateMachine(
                DurableLockError::Adapter(LockAdapterError::SnapshotPayloadUnavailable { .. })
            ))
        ),
        "a store holding a different transfer serves nothing for this one"
    );
    assert_eq!(app.store().applied_index(), applied_before);
    assert_eq!(mark(&app, RESOURCE), mark_before);

    // Promote the right one and the same install succeeds, which is what makes
    // the two refusals above evidence about the transfer rather than about the
    // install path being dead.
    promote_into_store(scratch.path(), &snapshot, payload);
    app.install_snapshot(promoted(&snapshot))
        .expect("the promoted payload installs once the store holds it");
    assert_eq!(app.store().applied_index(), LogIndex(4));
}

/// A promoted install is subject to the mark discipline, not exempt from it.
///
/// This is the test that keeps "the bytes come from the store now" from
/// becoming "the store is how you get around the high-water marks". The
/// snapshot below is legal, decodes, and is *ahead* of this replica's applied
/// floor — so every check that could plausibly stop it on index alone passes —
/// and it still must not be published, because it tracks no mark for a resource
/// this store has acknowledged one for. A guarded resource downstream has
/// already accepted that token, and no replay reaches it.
#[test]
fn a_promoted_install_that_would_lower_a_fencing_mark_is_still_refused() {
    let scratch = ScratchDir::new("promoted-mark-regression");
    let mut app = open(scratch.path());
    apply_all(&mut app, &workload());
    let acknowledged = mark(&app, RESOURCE).expect("the workload acknowledged a mark");
    let applied_before = app.store().applied_index();

    // Built on a different replica, one entry further along, over a resource
    // this one has never heard of. Nothing about it is malformed.
    let (foreign, payload) = snapshot_from("promoted-mark-regression-source", &foreign_workload());
    assert!(
        foreign.metadata.last_included_index > applied_before,
        "the fixture must clear the applied-floor refusal so the mark check is what fires"
    );
    promote_into_store(scratch.path(), &foreign, payload);

    let refusal = app
        .install_snapshot(promoted(&foreign))
        .expect_err("a state that loses an acknowledged mark is not publishable");
    match refusal {
        ApplicationSnapshotError::StateMachine(DurableLockError::Store(
            LockStoreError::MarkRegression {
                resource: named,
                acknowledged: lost,
                offered,
            },
        )) => {
            assert_eq!(named, resource(RESOURCE));
            assert_eq!(lost, acknowledged);
            assert_eq!(
                offered, None,
                "the promoted state tracks the name not at all"
            );
        }
        other => panic!("expected a mark regression, got {other:?}"),
    }

    assert_eq!(
        mark(&app, RESOURCE),
        Some(acknowledged),
        "a refused install moved no mark"
    );
    assert_eq!(app.store().applied_index(), applied_before);
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
                DurableLockError::Adapter(LockAdapterError::SnapshotBehindAppliedIndex {
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
