//! Regression suite, adopted from the gen-6 hunt: `discard_and_reseed` over a
//! replica whose Raft log has been compacted.
//!
//! `LockStore::discard_and_reseed` used to justify deleting durable state with:
//!
//! > What refills it is mostly this replica's own retained log rather than the
//! > group: this call empties the application store and touches nothing else, so
//! > the Raft log and snapshot beside it survive, the reopened store reports
//! > `LogIndex::ZERO`, and the entries replay. The group supplies only what local
//! > compaction has dropped, as a snapshot.
//!
//! The first half is true and `a_reseeded_replica_recovers_its_marks_from_the_group`
//! proves it — over a log that has never been compacted, so every entry is still
//! there to replay. The second sentence was false: a re-seeded store's honest
//! `LogIndex::ZERO` used to be raised to the snapshot boundary, and nothing ever
//! supplied the dropped prefix, because a follower whose log matches the leader's
//! is never sent a snapshot. The replica then reissued a fencing token its
//! guarded downstream had already accepted — the exact outcome the same method
//! gives as its reason for refusing to repair a `NoReadableImage`.
//!
//! The composition now refuses instead. These tests pin the refusal, pin that
//! the damage is unreachable behind it, and pin the control that the refusal is
//! scoped to compaction rather than to re-seeding.
//!
//! The *permanent* refusal falls on the reopened group's first use rather than
//! on the `apply_raft_outputs` call that drains its recovery outputs. That pump
//! is also what a replica which crashed between promoting an inbound snapshot
//! and installing it drains before restoring, so poisoning there would poison a
//! recoverable replica; `crates/rafter-app/tests/gen7_boundary_probe.rs` pins
//! that direction.
//!
//! # What "nothing is given up" left out
//!
//! This file used to argue that deferring the whole verdict past the pump cost
//! nothing, because minting a fencing token needs a session, a session needs a
//! committed proposal, and a proposal needs a step. That is true of the two
//! fixtures below, and they are the reason it read as true: both compact
//! *through* the applied index and stop, so the commit index lands exactly on
//! the boundary and the pump has nothing to hand over.
//!
//! One more commit breaks the argument. A replica compacted at 5 that then
//! commits 6 hands its reopened group a suffix, and the re-seeded store is at
//! zero — so the pump applied 6 onto an empty lock service and produced a store
//! holding the tail of a history whose head it had deleted, with an applied
//! index that matched the commit index and a fencing floor derived from
//! nothing. No step was needed and no token was minted through a session,
//! because the laundering happened inside the apply itself.
//! `gen6_a_reseed_with_a_commit_above_the_boundary_is_refused_before_it_applies`
//! is that variant, and it is refused now, before `apply_batch`.

#[allow(dead_code)]
mod support;

#[path = "support/scratch.rs"]
mod scratch;

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion,
    LocalProposalId, LogIndex, NodeConfig, NodeId, RaftSnapshotMetadata, Role, SnapshotGroupId,
};
use rafter_app::{
    group::{GroupInput, RaftGroup, RaftGroupParts},
    proposal::{Proposal, ProposalEvent},
    state_machine::ReplicatedStateMachine,
};
use rafter_reference_fenced_lock::{
    store::LockStore, ApplyOutcome, Command, DurableLockStateMachine, LockConfig,
};
use rafter_runtime::{DurableRaftNode, DurableRaftNodeStorage, RaftRuntimeError};
use rafter_storage::{
    InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore,
    PersistedRaftSnapshot,
};

use scratch::ScratchDir;
use support::{acquire, config, open_session, release, resource, submit};

const RESOURCE: &str = "orders/shard-0";
const GROUP_ID: u64 = 1;

type Storage = DurableRaftNodeStorage<
    InMemoryRaftHardStateStore,
    InMemoryRaftLogSegment,
    InMemoryRaftSnapshotStore,
>;
type Runtime =
    DurableRaftNode<InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore>;
type LockGroup = RaftGroup<u64, DurableLockStateMachine, Runtime>;
type LockGroupError = rafter_app::error::GroupError<
    <DurableLockStateMachine as ReplicatedStateMachine>::Error,
    RaftRuntimeError,
>;

fn empty_storage() -> Storage {
    DurableRaftNodeStorage {
        hard_state_store: InMemoryRaftHardStateStore::default(),
        log_segment: InMemoryRaftLogSegment::default(),
        snapshot_store: InMemoryRaftSnapshotStore::new(),
    }
}

/// Opens a one-voter group over `app` and `storage`, exactly as the three-node
/// driver's `open_group` does: the application's own durable applied index is
/// the floor handed to the runtime and to the group.
fn try_open_group(
    app: DurableLockStateMachine,
    storage: Storage,
) -> Result<LockGroup, LockGroupError> {
    let node_config = NodeConfig::new(NodeId(1), Vec::new(), 3).expect("single-voter config");
    let applied_index = app
        .applied_index()
        .expect("lock state machines always report an applied index");
    // The Raft node itself still opens. It raises the declared floor to its
    // snapshot boundary and documents that it does; the composition is where
    // the gap that raise leaves is answerable.
    let recovered = DurableRaftNode::recover_with_storage_and_snapshot_store_applied_through(
        node_config,
        storage.hard_state_store,
        storage.log_segment,
        storage.snapshot_store,
        applied_index,
    )
    .expect("the kernel accepts any floor at or below its commit index");
    let (runtime, recovery_outputs) = recovered.into_parts();
    let mut group = RaftGroup::with_applied_index(GROUP_ID, NodeId(1), runtime, app, applied_index);
    // The driver routes the recovery outputs rather than dropping them; so does
    // this, or the replay would never reach the state machine at all. This is
    // the same call `TransportRaftDriver::apply_recovery_outputs` makes.
    group.apply_raft_outputs(recovery_outputs)?;
    Ok(group)
}

fn open_group(app: DurableLockStateMachine, storage: Storage) -> LockGroup {
    try_open_group(app, storage).expect("retained durable state reopens")
}

fn tick(group: &mut LockGroup) {
    let report = group.step(GroupInput::Tick).expect("tick succeeds");
    // One voter: nothing leaves the node.
    assert!(report.peer_messages.is_empty(), "a lone voter has no peers");
}

fn become_leader(group: &mut LockGroup) {
    for _ in 0..16 {
        if group.metrics().role == Role::Leader {
            return;
        }
        tick(group);
    }
    panic!("the lone voter never became leader");
}

fn commit(group: &mut LockGroup, id: u64, command: Command) -> Option<ApplyOutcome> {
    let mut outcome = None;
    let report = group
        .step(GroupInput::Proposal {
            proposal: Proposal {
                local_proposal_id: LocalProposalId(id),
                client_request_id: None,
                command,
            },
        })
        .expect("the leader accepts the proposal");
    for event in &report.proposal_events {
        if let ProposalEvent::Applied { result, .. } = event {
            outcome = Some(*result);
        }
    }
    for _ in 0..8 {
        let report = group.step(GroupInput::Tick).expect("tick succeeds");
        assert!(report.peer_messages.is_empty(), "a lone voter has no peers");
        for event in &report.proposal_events {
            if let ProposalEvent::Applied { result, .. } = event {
                outcome = Some(*result);
            }
        }
    }
    outcome
}

fn workload() -> Vec<Command> {
    vec![
        open_session(1, 1),
        // Acquire, release, acquire on the same resource, so the fencing
        // high-water mark is raised twice and lands strictly above the token a
        // fresh store hands out.
        submit(1, 1, 1, acquire(RESOURCE, 4)),
        submit(1, 1, 2, release(RESOURCE, 1)),
        submit(1, 1, 3, acquire(RESOURCE, 4)),
    ]
}

fn mark(group: &LockGroup) -> Option<u64> {
    group
        .state_machine()
        .service()
        .status(resource(RESOURCE))
        .token_floor
        .map(rafter_reference_fenced_lock::FencingToken::get)
}

/// Compacts the replica's Raft log through its applied index, carrying the
/// application's own snapshot payload — the documented compaction recipe from
/// `ReplicatedStateMachine::build_snapshot`.
fn compact_through_applied(
    state_machine: &mut DurableLockStateMachine,
    runtime: &mut Runtime,
    boundary: LogIndex,
) {
    let application_snapshot = state_machine
        .build_snapshot(boundary)
        .expect("the state machine snapshots at its own applied index");
    let term = runtime.current_term();
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("gen6-lock").expect("valid group id"),
        NodeId(1),
        boundary,
        term,
        term,
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("fenced_lock").expect("valid kind"),
            ApplicationSnapshotVersion::new(1).expect("valid version"),
        ),
    )
    .expect("valid snapshot metadata");
    runtime
        .compact_log_with_snapshot(PersistedRaftSnapshot {
            metadata,
            application_payload: application_snapshot.payload,
        })
        .expect("the replica compacts through its own applied index");
}

/// Runs the workload on a fresh replica and returns the mark it established,
/// the applied index it reached, and its Raft storage.
struct Established {
    mark: u64,
    applied: LogIndex,
    /// The snapshot boundary the storage carries, which is `applied` for
    /// [`establish`] and stays there while later entries commit above it.
    boundary: LogIndex,
    storage: Storage,
}

fn establish(
    scratch: &ScratchDir,
    lock_config: LockConfig,
    base_id: u64,
    compact: bool,
) -> Established {
    let store = LockStore::open(scratch.path(), lock_config).expect("a fresh store opens");
    let mut group = open_group(
        DurableLockStateMachine::new(store, scratch.path().join("raft/snapshots")),
        empty_storage(),
    );
    become_leader(&mut group);
    for (offset, command) in workload().into_iter().enumerate() {
        commit(&mut group, base_id + offset as u64, command);
    }

    let established = mark(&group).expect("the workload acquired this resource");
    assert!(
        established > 1,
        "the workload must raise the mark above the token a fresh store issues, got {established}"
    );
    let applied = group
        .state_machine()
        .applied_index()
        .expect("the state machine reports its applied index");
    assert!(
        applied > LogIndex::ZERO,
        "the workload must have been applied"
    );

    let RaftGroupParts {
        mut state_machine,
        mut runtime,
        ..
    } = group.into_parts();
    if compact {
        compact_through_applied(&mut state_machine, &mut runtime, applied);
        assert_eq!(
            runtime.snapshot_index(),
            applied,
            "the log is compacted through the applied index"
        );
    }
    let boundary = runtime.snapshot_index();
    let storage = runtime.into_storage();
    // Reading and rewriting the store's directory needs it closed.
    drop(state_machine);

    Established {
        mark: established,
        applied,
        boundary,
        storage,
    }
}

/// The same replica, plus one committed application entry *above* the snapshot
/// boundary.
///
/// This is the shape the two fixtures above cannot reach and the whole reason
/// the pump had to take a verdict of its own: the compaction stops at the
/// applied index, and then ordinary traffic carries the commit index past it.
/// Every replica that compacts and keeps serving is here within one write.
fn establish_with_commit_above_the_boundary(
    scratch: &ScratchDir,
    lock_config: LockConfig,
    base_id: u64,
) -> Established {
    let established = establish(scratch, lock_config, base_id, true);

    let store = LockStore::open(scratch.path(), lock_config).expect("the compacted store reopens");
    let mut group = open_group(DurableLockStateMachine::new(store), established.storage);
    become_leader(&mut group);
    commit(&mut group, base_id + 90, open_session(2, 1));

    let applied = group
        .state_machine()
        .applied_index()
        .expect("the state machine reports its applied index");
    assert!(
        applied > established.boundary,
        "the extra commit must land above the boundary: applied {applied}, boundary {}",
        established.boundary
    );

    let RaftGroupParts {
        state_machine,
        runtime,
        ..
    } = group.into_parts();
    assert_eq!(
        runtime.snapshot_index(),
        established.boundary,
        "and it must not have moved the boundary"
    );
    let storage = runtime.into_storage();
    drop(state_machine);

    Established {
        applied,
        storage,
        ..established
    }
}

/// The re-seed itself still succeeds — it is a decision about the application
/// store — and still reports the floor it deleted. What is refused is the
/// composition: a Raft node whose snapshot boundary is above the floor the
/// emptied store can honestly declare will not run.
#[test]
fn gen6_a_reseed_over_a_compacted_log_is_refused_at_the_composition_seam() {
    let scratch = ScratchDir::new("gen6-reseed-compaction");
    let lock_config: LockConfig = config(2, 4);
    let established = establish(&scratch, lock_config, 100, true);

    let reseeded = LockStore::discard_and_reseed(scratch.path(), lock_config)
        .expect("the re-seed empties the directory and opens it");
    assert_eq!(
        reseeded.applied_index(),
        LogIndex::ZERO,
        "a re-seeded store reports a zero applied floor"
    );
    let reported_floor = reseeded
        .recovery()
        .reseed()
        .expect("the opening that came back was a re-seed")
        .discarded_applied_index()
        .expect("the deleted store held a whole image");
    assert_eq!(reported_floor, established.applied);

    // Draining the recovery outputs is not the seam *for this fixture*, and the
    // reason is a property of the fixture rather than of the pump: the
    // compaction stopped at the applied index, so the commit index is the
    // boundary and there is nothing above it to hand over. One more commit and
    // the pump is the seam — see
    // `gen6_a_reseed_with_a_commit_above_the_boundary_is_refused_before_it_applies`.
    let mut group = try_open_group(
        DurableLockStateMachine::new(reseeded, scratch.path().join("raft/snapshots")),
        established.storage,
    )
    .expect("the recovery pump takes no boundary verdict");
    assert_eq!(
        group
            .state_machine()
            .applied_index()
            .expect("the state machine reports its applied index"),
        LogIndex::ZERO,
        "the recovery outputs carried nothing: the entries are compacted away"
    );

    let error = group
        .step(GroupInput::Tick)
        .expect_err("the composition must refuse a state machine below the snapshot boundary");
    assert!(
        matches!(
            &error,
            rafter_app::error::GroupError::AppliedIndexBelowSnapshotBoundary {
                app_applied_index: LogIndex(0),
                snapshot_index,
            } if *snapshot_index == established.applied
        ),
        "the refusal must name both indexes so an operator can act on it, got {error}"
    );
}

/// The consequence, in the vocabulary the lock exists to serve. Pre-fix this
/// replica handed out token 1 for a resource whose guarded downstream had
/// already accepted 2. It cannot: the group refuses every step, so it never
/// wins an election, never commits the session that a token is minted under,
/// and never mints one.
#[test]
fn gen6_a_reseed_over_a_compacted_log_cannot_reissue_a_spent_fencing_token() {
    let scratch = ScratchDir::new("gen6-reseed-token");
    let lock_config: LockConfig = config(2, 4);
    let established = establish(&scratch, lock_config, 300, true);

    let reseeded = LockStore::discard_and_reseed(scratch.path(), lock_config)
        .expect("the re-seed empties the directory and opens it");
    let mut group = try_open_group(
        DurableLockStateMachine::new(reseeded, scratch.path().join("raft/snapshots")),
        established.storage,
    )
    .expect("the recovery pump takes no boundary verdict");

    // Every route to a token is a step, and every step is refused. Driving the
    // whole workload proves it rather than asserting it of the first one.
    for (offset, command) in workload().into_iter().enumerate() {
        let error = group
            .step(GroupInput::Tick)
            .expect_err("a lone voter cannot campaign out of this state");
        assert!(matches!(
            error,
            rafter_app::error::GroupError::AppliedIndexBelowSnapshotBoundary { .. }
                | rafter_app::error::GroupError::Poisoned { .. }
        ));
        let error = group
            .step(GroupInput::Proposal {
                proposal: Proposal {
                    local_proposal_id: LocalProposalId(700 + offset as u64),
                    client_request_id: None,
                    command,
                },
            })
            .expect_err("and it cannot propose into the group either");
        assert!(matches!(
            error,
            rafter_app::error::GroupError::AppliedIndexBelowSnapshotBoundary { .. }
                | rafter_app::error::GroupError::Poisoned { .. }
        ));
    }

    assert!(
        mark(&group).is_none(),
        "a fencing token must never be reissued, and this replica would hand out 1 for a \
         resource whose guarded downstream has already accepted {}",
        established.mark
    );
    assert!(
        matches!(
            group.fatal_state(),
            rafter_app::group::GroupFatalState::Poisoned { .. }
        ),
        "and the refusal is permanent rather than a state to retry out of"
    );
}

/// The variant the "nothing is given up" argument did not cover: a compacted
/// replica that kept serving, so its commit index sits above its boundary.
///
/// Here the re-seeded store is at zero, the boundary is at the compaction
/// point, and the recovery drain carries a committed application entry above
/// it. Applying that entry is the laundering — an empty lock service plus the
/// tail of a history whose head was deleted, published with an applied index
/// that matches the commit index — and it needs no step, no session, and no
/// election, so nothing downstream of the pump can prevent it. The pump refuses
/// it instead, before `apply_batch`, and says which entry it stopped in front
/// of.
#[test]
fn gen6_a_reseed_with_a_commit_above_the_boundary_is_refused_before_it_applies() {
    let scratch = ScratchDir::new("gen6-reseed-above-boundary");
    let lock_config: LockConfig = config(2, 4);
    let established = establish_with_commit_above_the_boundary(&scratch, lock_config, 900);

    let reseeded = LockStore::discard_and_reseed(scratch.path(), lock_config)
        .expect("the re-seed empties the directory and opens it");
    assert_eq!(reseeded.applied_index(), LogIndex::ZERO);

    let boundary = established.boundary;
    let error = try_open_group(DurableLockStateMachine::new(reseeded), established.storage)
        .expect_err("a committed suffix must not be applied over the deleted prefix");
    assert!(
        matches!(
            &error,
            LockGroupError::SnapshotRestoreRequired {
                app_applied_index: LogIndex(0),
                snapshot_index,
                entry_index,
            } if *snapshot_index == boundary && *entry_index > boundary
        ),
        "the refusal must name the gap and the entry it stopped in front of, got {error}"
    );

    // And it is refused rather than repaired-in-place: the store on disk is
    // still the empty one the re-seed created, so nothing has been published
    // from a partial history.
    let reopened = LockStore::open(scratch.path(), lock_config).expect("the store reopens");
    assert_eq!(
        reopened.applied_index(),
        LogIndex::ZERO,
        "the application must be exactly what the re-seed left"
    );
}

/// The control, and the boundary the refusal is scoped to. The same re-seed
/// over a replica that never compacted still replays from its own retained log
/// and recovers the mark it deleted — which is the half of
/// `discard_and_reseed`'s premise that was always true.
#[test]
fn gen6_a_reseed_over_an_uncompacted_log_still_recovers_its_marks() {
    let scratch = ScratchDir::new("gen6-reseed-uncompacted");
    let lock_config: LockConfig = config(2, 4);
    let established = establish(&scratch, lock_config, 500, false);

    let reseeded = LockStore::discard_and_reseed(scratch.path(), lock_config)
        .expect("the re-seed empties the directory and opens it");
    assert_eq!(reseeded.applied_index(), LogIndex::ZERO);

    let mut group = open_group(
        DurableLockStateMachine::new(reseeded, scratch.path().join("raft/snapshots")),
        established.storage,
    );
    become_leader(&mut group);
    for _ in 0..32 {
        tick(&mut group);
    }

    assert_eq!(
        group
            .state_machine()
            .applied_index()
            .expect("the state machine reports its applied index"),
        established.applied,
        "the retained log carries the re-seeded replica back to the floor it deleted"
    );
    assert_eq!(
        mark(&group),
        Some(established.mark),
        "and the fencing high-water mark comes back with it"
    );
}
