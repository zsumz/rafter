#![allow(clippy::wildcard_imports)]
//! The restart transaction, over the real durable stack.
//!
//! `gen7_boundary_probe` pins the ordering rule against a scripted runtime,
//! where the boundary and the suffix are whatever a fixture says they are. This
//! file pins the same rule against `DurableRaftNode` over in-memory stores,
//! because the scenario that produced the defect is not a fixture — it is what
//! the shipped recovery constructor actually hands a caller.
//!
//! The shape, which is the crash window between promoting an inbound snapshot
//! and installing it into the application:
//!
//! * the application is durable through index 3 and says so;
//! * the Raft node holds a promoted snapshot at index 5, so entries 4 and 5 are
//!   compacted and exist in no form this replica can reach;
//! * index 6 is committed above the boundary.
//!
//! `recover_with_storage_and_snapshot_store_applied_through` raises the
//! declared floor of 3 to the boundary of 5 — it documents that it does, and it
//! has no choice, holding a descriptor rather than payload bytes — and drains
//! `Apply(6)`. Draining that onto the application writes `{1,2,3}`, a hole
//! where `4..=5` were, and `6`. The result reports applied index 6 against a
//! committed application index of 6, so readiness passes and reads are served.
//! Nothing later can find the hole.

mod support;

use rafter_runtime::DurableRaftNode;
use rafter_storage::{
    InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore,
    PersistedRaftLogEntry, PersistedRaftSnapshot, RaftHardState, RaftHardStateStore,
    RaftLogSegment,
};
use support::*;

const BOUNDARY: u64 = 5;
const COMMITTED_SUFFIX: u64 = 6;

type Runtime =
    DurableRaftNode<InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore>;

fn node_config() -> NodeConfig {
    NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 1).expect("test node config is valid")
}

fn boundary_snapshot() -> PersistedRaftSnapshot {
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("recovery-transaction").expect("snapshot group id is valid"),
        NodeId(1),
        LogIndex(BOUNDARY),
        Term(1),
        Term(1),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("kv").expect("snapshot kind is valid"),
            ApplicationSnapshotVersion::new(1).expect("snapshot version is valid"),
        ),
    )
    .expect("snapshot metadata is valid");
    PersistedRaftSnapshot {
        metadata,
        application_payload: b"boundary-image".to_vec(),
    }
}

/// Durable Raft state for a replica whose snapshot boundary is 5 and whose
/// committed index reaches `commit_index`.
///
/// The retained log still carries every entry, which is the shape a crash
/// between persisting a snapshot and compacting the log leaves and which the
/// runtime supports directly; the boundary is what makes 4 and 5 unreachable,
/// not their absence from this segment.
fn durable_state(
    commit_index: u64,
) -> (
    InMemoryRaftHardStateStore,
    InMemoryRaftLogSegment,
    InMemoryRaftSnapshotStore,
) {
    let mut hard_state_store = InMemoryRaftHardStateStore::new();
    hard_state_store
        .write_hard_state(RaftHardState {
            current_term: Term(1),
            voted_for: None,
            commit_index: LogIndex(commit_index),
            committed_configuration: None,
        })
        .expect("durable commit floor writes");

    let mut log_segment = InMemoryRaftLogSegment::new();
    let entries = (1..=commit_index)
        .map(|index| {
            PersistedRaftLogEntry::application(
                LogIndex(index),
                Term(1),
                format!("cmd{index}").into_bytes(),
            )
        })
        .collect::<Vec<_>>();
    log_segment
        .append_entries(&entries)
        .expect("committed application entries persist");

    (
        hard_state_store,
        log_segment,
        InMemoryRaftSnapshotStore::with_snapshot(boundary_snapshot()),
    )
}

/// Reopens the runtime exactly as every shipped restart path does: the
/// application's own durable applied index is the floor handed to the kernel.
fn recover(commit_index: u64, applied_through: u64) -> (Runtime, Vec<RaftOutput>) {
    let (hard_state_store, log_segment, snapshot_store) = durable_state(commit_index);
    DurableRaftNode::recover_with_storage_and_snapshot_store_applied_through(
        node_config(),
        hard_state_store,
        log_segment,
        snapshot_store,
        LogIndex(applied_through),
    )
    .expect("the kernel accepts a floor at or below its commit index")
    .into_parts()
}

/// A state machine that stopped at 3, holding the three commands it applied.
fn application_at_three() -> RecordingStateMachine {
    RecordingStateMachine {
        applied_index: LogIndex(3),
        applied: vec![b"cmd1".to_vec(), b"cmd2".to_vec(), b"cmd3".to_vec()],
        ..RecordingStateMachine::default()
    }
}

fn open_group(
    runtime: Runtime,
    app: RecordingStateMachine,
) -> RaftGroup<u64, RecordingStateMachine, Runtime> {
    let applied_index = app.applied_index().expect("the fake always reports one");
    RaftGroup::with_applied_index(7, NodeId(1), runtime, app, applied_index)
}

/// The kernel half of the reproduction, stated on its own so the rest of this
/// file is not also asserting it. The floor is raised silently and the drained
/// suffix starts above the gap rather than at it.
#[test]
fn the_kernel_raises_the_declared_floor_and_drains_only_above_the_boundary() {
    // The raise, observed before anything drains: a floor of 3 goes in and the
    // node opens at 5, with nothing in the return value reporting the change.
    let (hard_state_store, log_segment, snapshot_store) = durable_state(COMMITTED_SUFFIX);
    let unopened = DurableRaftNode::with_storage_and_snapshot_store_applied_through(
        node_config(),
        hard_state_store,
        log_segment,
        snapshot_store,
        LogIndex(3),
    )
    .expect("the kernel accepts a floor at or below its commit index");
    assert_eq!(unopened.snapshot_index(), LogIndex(BOUNDARY));
    assert_eq!(
        unopened.applied_index(),
        LogIndex(BOUNDARY),
        "the declared floor of 3 was raised to the boundary, silently"
    );

    let (_runtime, recovery_outputs) = recover(COMMITTED_SUFFIX, 3);
    let applied = recovery_outputs
        .iter()
        .filter_map(|output| match output {
            RaftOutput::Apply { index, .. } => Some(*index),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        applied,
        vec![LogIndex(COMMITTED_SUFFIX)],
        "the suffix skips 4 and 5 entirely: they are compacted, and the kernel holds no payload"
    );
    assert!(
        !recovery_outputs
            .iter()
            .any(|output| matches!(output, RaftOutput::ApplySnapshot { .. })),
        "and it cannot carry the install that would make applying 6 safe"
    );
}

/// The defect itself, as a test. The raw pump used to accept this batch and
/// leave a hole in durable application state; it now refuses before
/// `apply_batch` is reached.
#[test]
fn the_raw_pump_refuses_the_committed_suffix_over_the_crash_window() {
    let (runtime, recovery_outputs) = recover(COMMITTED_SUFFIX, 3);
    let mut group = open_group(runtime, application_at_three());

    let error = group
        .apply_raft_outputs(recovery_outputs)
        .expect_err("a committed suffix must not be laid over a snapshot that was never installed");
    assert!(
        matches!(
            error,
            GroupError::SnapshotRestoreRequired {
                app_applied_index: LogIndex(3),
                snapshot_index: LogIndex(BOUNDARY),
                entry_index: LogIndex(COMMITTED_SUFFIX),
            }
        ),
        "got {error:?}"
    );
    assert_eq!(
        group.state_machine().applied,
        vec![b"cmd1".to_vec(), b"cmd2".to_vec(), b"cmd3".to_vec()],
        "the application must be exactly what it opened with — this is the corruption itself"
    );
    assert!(group.state_machine().operations.is_empty());
}

/// The repair, and the whole point of the operation: one call, and the order
/// inside it is install-at-the-boundary then apply-the-suffix.
#[test]
fn the_recovery_operation_installs_the_boundary_then_applies_the_suffix() {
    let (runtime, recovery_outputs) = recover(COMMITTED_SUFFIX, 3);
    let mut group = open_group(runtime, application_at_three());

    let report = group
        .apply_recovery_outputs(recovery_outputs)
        .expect("the ordered recovery restores and then applies");

    assert_eq!(
        group.state_machine().operations,
        vec![
            RecordedOperation::InstallSnapshot(LogIndex(BOUNDARY)),
            RecordedOperation::ApplyBatch(vec![LogIndex(COMMITTED_SUFFIX)]),
        ],
        "install at 5, then apply 6 — in that order, which is the entire contract"
    );
    assert_eq!(
        group.state_machine().installed_snapshots.len(),
        1,
        "and the install happened once"
    );
    assert!(
        report
            .snapshot_events
            .iter()
            .any(|event| matches!(event, SnapshotEvent::Apply { .. })),
        "the routed report carries the install, so a driver forwarding events sees it"
    );

    // The state machine that took the install is the authority on what it now
    // holds. What this composition owes is that it was given the boundary
    // before the suffix, which the trace above states.
    assert_eq!(
        group.state_machine().applied_index,
        LogIndex(COMMITTED_SUFFIX)
    );
    assert!(
        group.state_machine().applied_index().expect("floor reads")
            >= group.committed_application_index(),
        "and the replica is honestly ready rather than numerically ready"
    );
    assert!(matches!(group.fatal_state(), GroupFatalState::Healthy));
}

/// Restarting after a successful recovery installs nothing and applies nothing.
///
/// The second incarnation declares the index the first one reached, so the
/// kernel's floor lands above the boundary and the drain is empty — and the
/// operation finds no restore owed. That is what makes it safe to put on an
/// unconditional restart path instead of behind a caller's judgement about
/// whether *this* boot is the one that needs it.
#[test]
fn a_restart_after_a_successful_recovery_repeats_neither_half() {
    let (runtime, recovery_outputs) = recover(COMMITTED_SUFFIX, 3);
    let mut group = open_group(runtime, application_at_three());
    group
        .apply_recovery_outputs(recovery_outputs)
        .expect("the first boot recovers");
    let RaftGroupParts { state_machine, .. } = group.into_parts();

    let (reopened_runtime, second_outputs) = recover(COMMITTED_SUFFIX, COMMITTED_SUFFIX);
    assert!(
        second_outputs.is_empty(),
        "a replica that declares 6 has nothing above its floor to replay, got {second_outputs:?}"
    );
    let mut reopened = open_group(reopened_runtime, state_machine);

    let report = reopened
        .apply_recovery_outputs(second_outputs)
        .expect("a restored replica recovers into a no-op");

    assert_eq!(
        reopened.state_machine().operations,
        vec![
            RecordedOperation::InstallSnapshot(LogIndex(BOUNDARY)),
            RecordedOperation::ApplyBatch(vec![LogIndex(COMMITTED_SUFFIX)]),
        ],
        "the second boot must add nothing to the trace the first one left"
    );
    assert!(report.applied.is_empty() && report.snapshot_events.is_empty());
    reopened
        .step(GroupInput::Tick)
        .expect("and the replica runs");
    assert!(matches!(reopened.fatal_state(), GroupFatalState::Healthy));
}

/// A later restart that *does* carry a suffix still installs nothing.
///
/// The empty-drain case above is protected by there being no suffix at all, so
/// on its own it says nothing about a replica that keeps committing after it
/// has been restored. This one has both: a boundary at 5, an application at 6,
/// and index 7 committed above it. Re-installing here would not merely be
/// wasteful — it would walk the application *backwards* to the boundary and
/// then apply 7 over the hole that reopened, which is the original defect
/// arriving through the fix.
#[test]
fn a_restart_that_carries_a_new_suffix_does_not_reinstall_the_boundary() {
    let (runtime, recovery_outputs) = recover(COMMITTED_SUFFIX + 1, COMMITTED_SUFFIX);
    let applied = recovery_outputs
        .iter()
        .filter_map(|output| match output {
            RaftOutput::Apply { index, .. } => Some(*index),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        applied,
        vec![LogIndex(COMMITTED_SUFFIX + 1)],
        "the fixture needs exactly the newly committed entry"
    );

    let app = RecordingStateMachine {
        applied_index: LogIndex(COMMITTED_SUFFIX),
        applied: vec![b"restored".to_vec()],
        ..RecordingStateMachine::default()
    };
    let mut group = open_group(runtime, app);

    group
        .apply_recovery_outputs(recovery_outputs)
        .expect("a restored replica applies its new suffix");

    assert_eq!(
        group.state_machine().operations,
        vec![RecordedOperation::ApplyBatch(vec![LogIndex(
            COMMITTED_SUFFIX + 1
        )])],
        "the apply alone: an application already at or above the boundary owes no restore"
    );
    assert_eq!(
        group.state_machine().applied_index,
        LogIndex(COMMITTED_SUFFIX + 1)
    );
    assert!(matches!(group.fatal_state(), GroupFatalState::Healthy));
}

/// An install that fails ends the transaction with the suffix unapplied. A
/// failed install poisons — that is existing snapshot semantics and it stays —
/// but the group must not be left healthy over an application that took the
/// suffix and not the snapshot, which is the state the whole defect consisted
/// of.
#[test]
fn a_failed_install_refuses_the_recovery_and_leaves_the_application_at_three() {
    let (runtime, recovery_outputs) = recover(COMMITTED_SUFFIX, 3);
    let mut app = application_at_three();
    app.fail_install_snapshot = true;
    let mut group = open_group(runtime, app);

    let error = group
        .apply_recovery_outputs(recovery_outputs)
        .expect_err("a recovery whose install fails cannot continue to the suffix");
    assert!(
        matches!(
            error,
            GroupError::StateMachine {
                operation: StateMachineOperation::InstallSnapshot,
                ..
            }
        ),
        "got {error:?}"
    );

    let app = group.state_machine();
    assert_eq!(
        app.applied,
        vec![b"cmd1".to_vec(), b"cmd2".to_vec(), b"cmd3".to_vec()],
        "the suffix must never have touched the application"
    );
    assert_eq!(app.applied_index, LogIndex(3));
    assert!(app.batches.is_empty(), "apply_batch was never called");
    assert!(
        matches!(group.fatal_state(), GroupFatalState::Poisoned { .. }),
        "and the group is poisoned rather than reporting a healthy replica it cannot serve"
    );
}

/// The empty-drain case, where the boundary *is* the last committed entry. The
/// operation has no suffix to protect, so it installs nothing and changes
/// nothing — and the replica is left in the state the permanent verdict already
/// owns: it opens, and refuses the first thing it does that would let the
/// truncated state machine answer for it.
#[test]
fn an_empty_drain_at_the_boundary_keeps_its_existing_behavior() {
    let (runtime, recovery_outputs) = recover(BOUNDARY, 3);
    assert!(
        recovery_outputs.is_empty(),
        "a replica compacted through its commit index has nothing to replay"
    );
    let mut group = open_group(runtime, application_at_three());

    group
        .apply_recovery_outputs(recovery_outputs)
        .expect("an empty recovery must not refuse a replica that may still be restored");
    assert!(
        group.state_machine().operations.is_empty(),
        "and it must not take a decision about discarded application state on its own"
    );
    assert!(matches!(group.fatal_state(), GroupFatalState::Healthy));

    let error = group
        .step(GroupInput::Tick)
        .expect_err("the permanent verdict still falls on the first use");
    assert!(
        matches!(
            error,
            GroupError::AppliedIndexBelowSnapshotBoundary {
                app_applied_index: LogIndex(3),
                snapshot_index: LogIndex(BOUNDARY),
            }
        ),
        "got {error:?}"
    );
}
