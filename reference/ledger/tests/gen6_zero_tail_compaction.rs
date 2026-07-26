//! Regression suite, adopted from the gen-6 hunt: the ledger's "the entries
//! above it are re-applied" over a replica that has compacted its Raft log.
//!
//! CONTRACT.md, on the one place opening is not a read:
//!
//! > And the loss is local rather than final: the application journal is a
//! > projection of the replicated log, its applied index is the join point, and
//! > the entries above it are re-applied on the next recovery. That second half
//! > is a fact about the composition, so it is tested end to end rather than
//! > asserted, and **it stops holding exactly when the group can no longer
//! > supply the entries.**
//!
//! The escape clause named one way the re-apply can fail: the group losing the
//! entries. There is a second, and it is local. A replica that has compacted its
//! own Raft log carries a snapshot, and `Node::from_bootstrap_applied_through`
//! used to take `max(snapshot_index, applied_through)` as its floor — so a
//! declared floor *below* the snapshot boundary was silently raised rather than
//! honoured, and one acknowledged transaction went missing on the ordinary
//! `open`, with no flag, on a replica whose group still held every entry.
//!
//! The seam now refuses that floor by name. These tests pin the refusal on the
//! compacted replica and the unchanged recovery on the uncompacted one, which
//! is the boundary the escape clause now has to state.
//!
//! The refusal falls on the reopened group's first *use* — its first step, and
//! its first read — rather than on the `apply_raft_outputs` call that drains
//! its recovery outputs. That pump is also what a replica which crashed
//! between promoting an inbound snapshot and installing it drains before
//! restoring, so refusing there would refuse a recoverable replica;
//! `crates/rafter-app/tests/gen7_boundary_probe.rs` pins that direction.
//! Nothing is given up here: the loss this file exists for is a balance read
//! back short, and no read gets through.

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
    proposal::Proposal,
    read::ReadRequest,
    state_machine::ReplicatedStateMachine,
};
use rafter_reference_ledger::{
    store::{raw_journal, LedgerStore, TornTail},
    AccountId, Command, DurableLedgerStateMachine, LedgerConfig, LedgerQuery, Mutation,
};
use rafter_runtime::{DurableRaftNode, DurableRaftNodeStorage, RaftRuntimeError};
use rafter_storage::{
    InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore,
    PersistedRaftSnapshot,
};

use scratch::ScratchDir;
use support::{amount, config, execute, open_session};

const ALPHA: AccountId = AccountId::new(11);
const GROUP_ID: u64 = 1;

type Storage = DurableRaftNodeStorage<
    InMemoryRaftHardStateStore,
    InMemoryRaftLogSegment,
    InMemoryRaftSnapshotStore,
>;
type Runtime =
    DurableRaftNode<InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore>;
type LedgerGroup = RaftGroup<u64, DurableLedgerStateMachine, Runtime>;
type LedgerGroupError = rafter_app::error::GroupError<
    <DurableLedgerStateMachine as ReplicatedStateMachine>::Error,
    RaftRuntimeError,
>;

fn empty_storage() -> Storage {
    DurableRaftNodeStorage {
        hard_state_store: InMemoryRaftHardStateStore::default(),
        log_segment: InMemoryRaftLogSegment::default(),
        snapshot_store: InMemoryRaftSnapshotStore::new(),
    }
}

/// Opens a one-voter group over `app` and `storage`, as `support/cluster.rs`
/// does: the application's own durable applied index is the floor handed both
/// to the runtime and to the group.
fn try_open_group(
    app: DurableLedgerStateMachine,
    storage: Storage,
) -> Result<LedgerGroup, LedgerGroupError> {
    let node_config = NodeConfig::new(NodeId(1), Vec::new(), 3).expect("single-voter config");
    let applied_index = app
        .applied_index()
        .expect("ledger state machines always report an applied index");
    // The Raft node itself still opens; it raises a low declared floor to its
    // snapshot boundary and documents that it does.
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
    group.apply_raft_outputs(recovery_outputs)?;
    Ok(group)
}

fn open_group(app: DurableLedgerStateMachine, storage: Storage) -> LedgerGroup {
    try_open_group(app, storage).expect("retained durable state reopens")
}

fn tick(group: &mut LedgerGroup) {
    group.step(GroupInput::Tick).expect("tick succeeds");
}

fn become_leader(group: &mut LedgerGroup) {
    for _ in 0..16 {
        if group.metrics().role == Role::Leader {
            return;
        }
        tick(group);
    }
    panic!("the lone voter never became leader");
}

fn commit(group: &mut LedgerGroup, id: u64, command: Command) {
    group
        .step(GroupInput::Proposal {
            proposal: Proposal {
                local_proposal_id: LocalProposalId(id),
                client_request_id: None,
                command,
            },
        })
        .expect("the leader accepts the proposal");
    for _ in 0..8 {
        tick(group);
    }
}

fn workload() -> Vec<Command> {
    vec![
        open_session(0, 1),
        execute(0, 1, 1, Mutation::OpenAccount { account_id: ALPHA }),
        execute(
            0,
            1,
            2,
            Mutation::Deposit {
                account_id: ALPHA,
                amount: amount(2),
            },
        ),
        execute(
            0,
            1,
            3,
            Mutation::Deposit {
                account_id: ALPHA,
                amount: amount(5),
            },
        ),
    ]
}

fn compact_through_applied(
    state_machine: &mut DurableLedgerStateMachine,
    runtime: &mut Runtime,
    boundary: LogIndex,
) {
    let application_snapshot = state_machine
        .build_snapshot(boundary)
        .expect("the state machine snapshots at its own applied index");
    let term = runtime.current_term();
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("gen6-ledger").expect("valid group id"),
        NodeId(1),
        boundary,
        term,
        term,
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("ledger").expect("valid kind"),
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

/// Runs the workload on a fresh replica, optionally compacts, then zero-fills
/// the journal's final frame — the ordinary crash rule two exists for.
struct TornReplica {
    acknowledged_index: LogIndex,
    acknowledged_balance: u64,
    truncated_index: LogIndex,
    storage: Storage,
    store: LedgerStore,
}

fn torn_replica(
    scratch: &ScratchDir,
    ledger_config: LedgerConfig,
    base_id: u64,
    compact: bool,
) -> TornReplica {
    let store = LedgerStore::open(scratch.path(), ledger_config).expect("a fresh store opens");
    let mut group = open_group(DurableLedgerStateMachine::new(store), empty_storage());
    become_leader(&mut group);
    for (offset, command) in workload().into_iter().enumerate() {
        commit(&mut group, base_id + offset as u64, command);
    }

    let acknowledged_index = group
        .state_machine()
        .applied_index()
        .expect("the state machine reports its applied index");
    let acknowledged_balance = balance(&group);
    assert!(
        acknowledged_balance > 0,
        "the workload must leave a balance to lose"
    );

    let RaftGroupParts {
        mut state_machine,
        mut runtime,
        ..
    } = group.into_parts();
    if compact {
        compact_through_applied(&mut state_machine, &mut runtime, acknowledged_index);
    }
    let storage = runtime.into_storage();
    // Reading the journal needs the store closed, and the frame offsets come
    // off the medium rather than from the store's own accounting.
    drop(state_machine);

    let mut bytes = raw_journal::read(scratch.path()).expect("the journal reads");
    let last_start = last_frame_start(scratch.path(), ledger_config);
    for byte in &mut bytes[last_start..] {
        *byte = 0;
    }
    raw_journal::write(scratch.path(), &bytes).expect("the journal rewrites");

    let after = LedgerStore::open(scratch.path(), ledger_config)
        .expect("`open` accepts a zero-filled tail rather than refusing it");
    assert!(
        matches!(
            after.recovery().torn_tail(),
            Some(TornTail::ZeroFilledToEnd { .. })
        ),
        "the crash must be the zero-filled tail rule two covers, got {:?}",
        after.recovery().torn_tail()
    );
    let truncated_index = after.applied_index();
    assert!(
        truncated_index < acknowledged_index,
        "the truncation must have cost the replica its floor"
    );

    TornReplica {
        acknowledged_index,
        acknowledged_balance,
        truncated_index,
        storage,
        store: after,
    }
}

/// On a compacted replica the frames the tear deleted are below the snapshot
/// boundary and no longer exist in the log. Pre-fix the composition raised the
/// floor to the boundary and the acknowledged transaction was gone for good;
/// now the replica refuses to run and names both indexes.
#[test]
fn gen6_a_zero_tail_on_a_compacted_replica_is_refused_rather_than_silently_lost() {
    let scratch = ScratchDir::new("gen6-zero-tail-compaction");
    let ledger_config: LedgerConfig = config(2, 4);
    let torn = torn_replica(&scratch, ledger_config, 100, true);

    // Draining the recovery outputs is not the seam: it supplies nothing here,
    // because every frame the tear cost is below the boundary and compacted
    // away. The seam is the first use of what it left behind.
    let mut group = try_open_group(DurableLedgerStateMachine::new(torn.store), torn.storage)
        .expect("the recovery pump takes no boundary verdict");
    assert_eq!(
        group
            .state_machine()
            .applied_index()
            .expect("the state machine reports its applied index"),
        torn.truncated_index,
        "the recovery outputs carried nothing back"
    );

    let error = group
        .step(GroupInput::Tick)
        .expect_err("the composition must refuse a state machine below the snapshot boundary");
    assert!(
        matches!(
            &error,
            rafter_app::error::GroupError::AppliedIndexBelowSnapshotBoundary {
                app_applied_index,
                snapshot_index,
            } if *app_applied_index == torn.truncated_index
                && *snapshot_index == torn.acknowledged_index
        ),
        "the refusal must name the floor the store reached and the boundary it \
         is short of, got {error}"
    );
}

/// The consequence, in the vocabulary the ledger exists to serve. The loss this
/// file is about is a balance that reads back short of what was acknowledged,
/// and the group refuses the read that would report it — before any step has
/// poisoned it, so the refusal is the boundary check rather than an aftershock.
#[test]
fn gen6_a_zero_tail_on_a_compacted_replica_cannot_report_the_short_balance() {
    let scratch = ScratchDir::new("gen6-zero-tail-read");
    let ledger_config: LedgerConfig = config(2, 4);
    let torn = torn_replica(&scratch, ledger_config, 400, true);
    let acknowledged_balance = torn.acknowledged_balance;

    let mut group = try_open_group(DurableLedgerStateMachine::new(torn.store), torn.storage)
        .expect("the recovery pump takes no boundary verdict");
    // The truncated store really is short: this is the answer a served read
    // would have carried.
    let short_balance = balance(&group);
    assert!(
        short_balance < acknowledged_balance,
        "the tear must have cost this replica an acknowledged deposit, {short_balance} vs \
         {acknowledged_balance}"
    );

    let error = group
        .read(ReadRequest::Local {
            group_id: GROUP_ID,
            query: LedgerQuery::GetAccount { account_id: ALPHA },
            min_applied_index: None,
        })
        .expect_err("a truncated replica must not answer a local read");
    assert!(
        matches!(
            &error,
            rafter_app::error::GroupError::AppliedIndexBelowSnapshotBoundary {
                app_applied_index,
                snapshot_index,
            } if *app_applied_index == torn.truncated_index
                && *snapshot_index == torn.acknowledged_index
        ),
        "the read must refuse by name rather than report the short balance, got {error}"
    );
}

/// The control, and the scope the CONTRACT.md escape clause now has to state.
/// The same zero-filled tail on a replica that never compacted is re-applied
/// from the retained log, and the acknowledged transaction comes back.
#[test]
fn gen6_a_zero_tail_on_an_uncompacted_replica_is_re_applied() {
    let scratch = ScratchDir::new("gen6-zero-tail-uncompacted");
    let ledger_config: LedgerConfig = config(2, 4);
    let torn = torn_replica(&scratch, ledger_config, 200, false);

    let mut group = open_group(DurableLedgerStateMachine::new(torn.store), torn.storage);
    become_leader(&mut group);
    for _ in 0..32 {
        tick(&mut group);
    }

    assert_eq!(
        group
            .state_machine()
            .applied_index()
            .expect("the state machine reports its applied index"),
        torn.acknowledged_index,
        "the retained log still holds the frames the tear deleted"
    );
    assert_eq!(
        balance(&group),
        torn.acknowledged_balance,
        "and the acknowledged transaction comes back with them"
    );
}

fn balance(group: &LedgerGroup) -> u64 {
    group
        .state_machine()
        .ledger()
        .account_balance(ALPHA)
        .unwrap_or(0)
}

/// Byte offset the journal's final committed frame begins at.
fn last_frame_start(directory: &std::path::Path, ledger_config: LedgerConfig) -> usize {
    // Replaying the same workload one command short into a scratch copy would
    // be one way; reading the store's own frame accounting is the direct one.
    let bytes = raw_journal::read(directory).expect("the journal reads");
    let store = LedgerStore::open(directory, ledger_config).expect("the store reopens clean");
    let frames = store.recovery().committed_frames();
    drop(store);
    assert!(frames >= 2, "the workload must leave at least two frames");
    // Frames are equal-shaped only by accident, so walk back one frame by
    // truncating and reopening until the committed count drops by one.
    for cut in (1..bytes.len()).rev() {
        let mut probe = bytes.clone();
        probe.truncate(cut);
        raw_journal::write(directory, &probe).expect("the probe journal writes");
        let reopened = LedgerStore::open(directory, ledger_config);
        let dropped = reopened.is_ok_and(|store| store.recovery().committed_frames() == frames - 1);
        if dropped {
            raw_journal::write(directory, &bytes).expect("the journal is restored");
            return cut;
        }
    }
    panic!("no truncation point dropped exactly one frame");
}
