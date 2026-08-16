#![allow(clippy::wildcard_imports)]
//! Regression suite, adopted from the gen-7 hunt: *where* the snapshot-boundary
//! verdict is taken.
//!
//! `a_state_machine_below_the_snapshot_boundary_refuses_to_run` pins the
//! refusal itself. This file pins its scope, because the first placement got
//! the scope wrong in both directions: it refused the raw output pump a
//! recovering replica drains before it restores, and it let every
//! `ReadRequest::Local` and every linearizable retry serve a query from a state
//! machine short of acknowledged entries.
//!
//! The invariant is a statement about *moments*, not about vectors: at every
//! moment the group would let the state machine answer for this replica, that
//! state machine is at or above its own Raft snapshot boundary. Stepping and
//! reading are those moments; draining outputs and reporting metrics are not.
//!
//! # The correction this file carries
//!
//! Deferring the *permanent* verdict past the output pump was right. Deferring
//! everything past it was not, and this file used to pin the mistake by name:
//! `the_recovery_batch_a_crash_window_replica_drains_is_accepted` asserted that
//! a committed entry at 6 lands on a state machine at 3 under a boundary of 5,
//! and called the resulting group healthy. It was. It was also holding
//! `{1,2,3,6}` and reporting itself caught up, because the applied index, the
//! readiness predicate, and the metrics snapshot all compare numbers and all
//! three numbers were right.
//!
//! Applying is a moment too — it is the moment the gap stops being recoverable
//! and becomes durable — so the pump refuses it, with
//! `GroupError::SnapshotRestoreRequired` and without poisoning, because the
//! repair is still available. `RaftGroup::apply_recovery_outputs` performs it:
//! install the snapshot the boundary names, then apply the suffix, in one
//! operation. What is still deferred past the pump is only the permanent
//! verdict, and only for a batch that would apply nothing.

mod support;

use support::*;

/// The exact fixture from the K2 regression test: a runtime whose snapshot
/// boundary is 5 over a state machine that only reached 3. The entries 4..=5
/// are compacted out of the log and nothing will ever supply them — except the
/// snapshot at the boundary, which is the one thing that can, and which the
/// runtime therefore hands back.
fn below_boundary_group() -> RaftGroup<u64, RecordingStateMachine, ScriptedRuntime> {
    group_below_boundary(Some(test_snapshot(5)))
}

/// The same fixture with the descriptor withheld, modelling a runtime that
/// reports a boundary whose snapshot it cannot produce.
fn below_boundary_group_without_descriptor(
) -> RaftGroup<u64, RecordingStateMachine, ScriptedRuntime> {
    group_below_boundary(None)
}

fn group_below_boundary(
    snapshot: Option<RaftSnapshot>,
) -> RaftGroup<u64, RecordingStateMachine, ScriptedRuntime> {
    let mut runtime = ScriptedRuntime::with_step_outputs([Vec::new()]);
    runtime.snapshot_index = LogIndex(5);
    runtime.snapshot = snapshot;
    RaftGroup::with_applied_index(
        7,
        NodeId(1),
        runtime,
        RecordingStateMachine {
            applied_index: LogIndex(3),
            applied: vec![b"stale".to_vec()],
            ..RecordingStateMachine::default()
        },
        LogIndex(3),
    )
}

/// The same shape, reached the way a live replica reaches it: the group opens
/// healthy, a barrier is started, and the boundary rises past the state machine
/// while that barrier is in flight. This is the only way to park a *completed*
/// or *pending* proof on a group that is below its boundary, since a group that
/// is already below it cannot start a barrier at all.
fn group_with_barrier_stranded_below_the_boundary(
    read_id: ReadId,
) -> RaftGroup<u64, RecordingStateMachine, ScriptedRuntime> {
    let mut runtime = ScriptedRuntime::with_step_outputs([Vec::new()]);
    runtime.step_log_shapes.push_back(ScriptedLogShape {
        application_entries: None,
        commit_index: LogIndex(5),
        snapshot_index: LogIndex(5),
    });
    let mut group = RaftGroup::with_applied_index(
        7,
        NodeId(1),
        runtime,
        RecordingStateMachine {
            applied_index: LogIndex(3),
            applied: vec![b"stale".to_vec()],
            ..RecordingStateMachine::default()
        },
        LogIndex(3),
    );

    // The barrier starts while the boundary is still at zero, and the step that
    // starts it is the one that compacts past the state machine.
    let outcome = group
        .read(read_helper_request(
            read_id,
            ReadConsistency::Linearizable,
            None,
        ))
        .expect("the barrier starts while the group is still above its boundary")
        .outcome;
    assert!(
        matches!(outcome, ReadOutcome::Pending { .. }),
        "the fixture needs a barrier left in flight, got {outcome:?}"
    );
    assert_eq!(group.metrics().snapshot_index, LogIndex(5));
    assert!(matches!(group.fatal_state(), GroupFatalState::Healthy));
    group
}

/// PROBE A. A `Local` read is served straight off the state machine without
/// touching Raft, which is exactly why it must be refused: there is no barrier
/// to stall and no later step to catch it, so an unguarded local read is the
/// truncated state handed to a client verbatim.
#[test]
fn a_local_read_below_the_boundary_is_refused() {
    let mut group = below_boundary_group();

    let error = group
        .read(read_helper_request(ReadId(1), ReadConsistency::Local, None))
        .expect_err("a group below its snapshot boundary must not answer a local read");
    assert!(
        matches!(
            error,
            GroupError::AppliedIndexBelowSnapshotBoundary {
                app_applied_index: LogIndex(3),
                snapshot_index: LogIndex(5),
            }
        ),
        "the refusal names both indexes so an operator can act on it, got {error:?}"
    );
    assert!(
        matches!(group.fatal_state(), GroupFatalState::Poisoned { .. }),
        "and it is fatal rather than transient, like every other statement of it"
    );
}

/// PROBE A2. The refusal precedes the freshness lever. A local read that asks
/// for nothing more than the state machine already claims used to be served,
/// and one that asks for the boundary itself used to stall as merely stale —
/// two different answers to one broken composition. Neither is available now.
#[test]
fn a_local_read_below_the_boundary_is_refused_whatever_freshness_it_asks_for() {
    for min_applied_index in [None, Some(LogIndex(3)), Some(LogIndex(5))] {
        let mut group = below_boundary_group();
        let error = group
            .read(read_helper_request(
                ReadId(1),
                ReadConsistency::Local,
                min_applied_index,
            ))
            .expect_err("the boundary refusal is not a freshness stall");
        assert!(
            matches!(error, GroupError::AppliedIndexBelowSnapshotBoundary { .. }),
            "min_applied_index={min_applied_index:?} got {error:?}"
        );
    }
}

/// PROBE A3. A *fresh* linearizable read reaches the guard through
/// `begin_read_barrier` -> `step` as well as through `read`'s own entry, so it
/// was already refused; it stays refused, and this pins that the added entry
/// did not change which error a caller sees.
#[test]
fn a_fresh_linearizable_read_below_the_boundary_is_refused() {
    let mut group = below_boundary_group();
    let error = group
        .read(read_helper_request(
            ReadId(1),
            ReadConsistency::Linearizable,
            None,
        ))
        .expect_err("a fresh barrier must not be started for a truncated state machine");
    assert!(
        matches!(error, GroupError::AppliedIndexBelowSnapshotBoundary { .. }),
        "got {error:?}"
    );
    assert!(matches!(
        group.fatal_state(),
        GroupFatalState::Poisoned { .. }
    ));
}

/// PROBE A3, second branch. A retry that consumes an already completed proof
/// returns an unstepped report, so it never reached the step guard. The proof
/// is honest about the read index it was granted at and says nothing about the
/// compaction that happened afterwards, so consuming it would answer from the
/// truncated state machine with a receipt attached.
#[test]
fn a_linearizable_retry_consuming_a_completed_proof_is_refused() {
    let read_id = ReadId(11);
    let mut group = group_with_barrier_stranded_below_the_boundary(read_id);

    // The pump completes the proof without taking a verdict, which is what
    // leaves a completed proof on a below-boundary group at all.
    group
        .apply_raft_outputs(vec![RaftOutput::ReadIndexGranted {
            read_id,
            read_index: LogIndex(3),
        }])
        .expect("the raw output pump takes no boundary verdict");
    assert_eq!(group.metrics().completed_query_reads, 1);

    let error = group
        .read(read_helper_request(
            read_id,
            ReadConsistency::Linearizable,
            None,
        ))
        .expect_err("a completed proof does not license reading a truncated state machine");
    assert!(
        matches!(
            error,
            GroupError::AppliedIndexBelowSnapshotBoundary {
                app_applied_index: LogIndex(3),
                snapshot_index: LogIndex(5),
            }
        ),
        "got {error:?}"
    );
    assert!(matches!(
        group.fatal_state(),
        GroupFatalState::Poisoned { .. }
    ));
}

/// PROBE A3, third branch. A retry of a still-pending barrier also returns an
/// unstepped report. It cannot serve a query, but it can report `Pending`,
/// which tells a caller to keep waiting on a replica that will never recover.
#[test]
fn a_linearizable_retry_of_a_pending_barrier_is_refused() {
    let read_id = ReadId(12);
    let mut group = group_with_barrier_stranded_below_the_boundary(read_id);

    let error = group
        .read(read_helper_request(
            read_id,
            ReadConsistency::Linearizable,
            None,
        ))
        .expect_err("a pending retry must refuse rather than counsel more waiting");
    assert!(
        matches!(error, GroupError::AppliedIndexBelowSnapshotBoundary { .. }),
        "got {error:?}"
    );
}

/// PROBE B, and the first boundary of the scope. `metrics` deliberately takes
/// no verdict: it is the supported way to *see* a raised declaration, and an
/// observability call that poisoned the group it reports on would destroy the
/// evidence an operator called it for. What it owes is honesty, and it pays it
/// by reporting the state machine's real position beside the boundary.
#[test]
fn metrics_reports_the_gap_instead_of_taking_a_verdict_on_it() {
    let group = below_boundary_group();
    let metrics = group.metrics();

    assert_eq!(
        metrics.applied_index,
        LogIndex(3),
        "metrics must report where the state machine actually is"
    );
    assert_eq!(
        metrics.snapshot_index,
        LogIndex(5),
        "beside the boundary it is short of, which is the whole detection recipe"
    );
    assert!(
        matches!(group.fatal_state(), GroupFatalState::Healthy),
        "and it must not poison the group an operator is inspecting"
    );
}

/// PROBE C. The verdict is keyed on where the state machine actually ended up,
/// not on whether an `ApplySnapshot` was present in some vector. An install
/// that lands at 4 leaves the replica below a boundary of 5, so it buys
/// nothing: the very next step refuses, naming 4.
#[test]
fn an_install_that_cannot_clear_the_boundary_buys_nothing() {
    let mut group = below_boundary_group();

    group
        .apply_raft_outputs(vec![RaftOutput::ApplySnapshot {
            snapshot: test_snapshot(4),
        }])
        .expect("the pump installs what it was handed");

    let error = group
        .step(GroupInput::Tick)
        .expect_err("an install short of the boundary does not clear it");
    assert!(
        matches!(
            error,
            GroupError::AppliedIndexBelowSnapshotBoundary {
                app_applied_index: LogIndex(4),
                snapshot_index: LogIndex(5),
            }
        ),
        "the refusal names where the install actually left the state machine, got {error:?}"
    );
}

/// PROBE C, the other half. An install that *does* reach the boundary clears
/// it, and it clears it because of where it left the state machine rather than
/// because of the variant it arrived as.
#[test]
fn an_install_that_reaches_the_boundary_clears_it() {
    let mut group = below_boundary_group();

    group
        .apply_raft_outputs(vec![RaftOutput::ApplySnapshot {
            snapshot: test_snapshot(5),
        }])
        .expect("the pump installs what it was handed");
    group
        .step(GroupInput::Tick)
        .expect("a state machine lifted to the boundary runs");
    assert!(matches!(group.fatal_state(), GroupFatalState::Healthy));
}

/// PROBE D, and the second boundary of the scope. The verdict is a fact about
/// the replica, so it cannot depend on how a caller chunked one runtime step's
/// outputs into calls. Handing the same outputs over in one call or in two must
/// reach the same place.
#[test]
fn the_verdict_does_not_depend_on_how_the_caller_chunks_the_batch() {
    let install = || RaftOutput::ApplySnapshot {
        snapshot: test_snapshot(5),
    };

    let mut whole = below_boundary_group();
    whole
        .apply_raft_outputs(vec![install()])
        .expect("one batch carrying the install passes");

    let mut chunked = below_boundary_group();
    chunked
        .apply_raft_outputs(Vec::new())
        .expect("an empty leading chunk must not poison a group whose install is next");
    chunked
        .apply_raft_outputs(vec![install()])
        .expect("and the install still lands");

    for (label, group) in [("whole", &mut whole), ("chunked", &mut chunked)] {
        group
            .step(GroupInput::Tick)
            .unwrap_or_else(|error| panic!("{label} must run after the install: {error:?}"));
        assert!(
            matches!(group.fatal_state(), GroupFatalState::Healthy),
            "{label} must be healthy"
        );
    }
}

/// PROBE E, and the third boundary of the scope — the severe one, and the one
/// this file used to get backwards.
///
/// The batch is `RecoveredDurableRaftNode::into_parts`' `recovery_outputs`, and
/// `apply_committed_into` never pushes an `Output::ApplySnapshot` — the kernel
/// holds a descriptor rather than payload bytes — so such a batch can never
/// carry the install that would make it safe. It is drained one line after
/// construction, ahead of any chance to restore. Accepting it wrote the gap
/// into the application: `{1,2,3}` then nothing for `4..=5` then `6`, durable,
/// with every index agreeing the replica was current.
///
/// So the raw pump refuses it, and refuses it *before* `apply_batch` is called
/// — the application must come out of this untouched, because a partial write
/// here is the same corruption arriving through the error path.
#[test]
fn the_recovery_batch_a_crash_window_replica_drains_is_refused_by_the_raw_pump() {
    let mut group = below_boundary_group();

    // A recovery batch of the shape `drain_committed_outputs` actually
    // produces: committed applies above the boundary, no install.
    let recovery_outputs = vec![apply_output(6, b"committed-after-the-boundary", None)];

    let error = group
        .apply_raft_outputs(recovery_outputs)
        .expect_err("the raw pump must not lay a committed suffix over a gap");
    assert!(
        matches!(
            error,
            GroupError::SnapshotRestoreRequired {
                app_applied_index: LogIndex(3),
                snapshot_index: LogIndex(5),
                entry_index: LogIndex(6),
            }
        ),
        "the refusal names the gap and the entry it stopped in front of, got {error:?}"
    );

    let app = group.state_machine();
    assert_eq!(
        app.applied,
        vec![b"stale".to_vec()],
        "the application must hold exactly what it opened with"
    );
    assert_eq!(app.applied_index, LogIndex(3), "and still report 3");
    assert!(
        app.batches.is_empty(),
        "apply_batch must never have been called"
    );
    assert!(
        app.installed_snapshots.is_empty(),
        "and the raw pump installs nothing on its own"
    );

    assert!(
        matches!(group.fatal_state(), GroupFatalState::Healthy),
        "the refusal is recoverable, so it must not poison the group that can still be repaired"
    );
}

/// PROBE E, the other half: the same fixture, through the operation that owns
/// the ordering. The claim is not "it works" but *what order it worked in* —
/// install at the boundary, then the suffix, one call, one report.
#[test]
fn the_recovery_operation_installs_the_boundary_before_it_applies_the_suffix() {
    let mut group = below_boundary_group();

    let report = group
        .apply_recovery_outputs(vec![apply_output(6, b"committed-after-the-boundary", None)])
        .expect("the ordered recovery restores the state machine and then applies");

    assert_eq!(
        group.state_machine().operations,
        vec![
            RecordedOperation::InstallSnapshot(LogIndex(5)),
            RecordedOperation::ApplyBatch(vec![LogIndex(6)]),
        ],
        "the install must precede the apply, and this is the only assertion that can say so"
    );
    assert_eq!(
        group.state_machine().applied_index,
        LogIndex(6),
        "and the state machine ends where the committed suffix ends"
    );

    // The install travels in the same report the caller routes, so a driver
    // that forwards snapshot events sees this one.
    assert!(
        report.snapshot_events.iter().any(
            |event| matches!(event, SnapshotEvent::Apply { snapshot, .. }
                if snapshot.metadata.last_included_index == LogIndex(5))
        ),
        "the routed report must carry the install it performed, got {:?}",
        report.snapshot_events
    );
    assert_eq!(
        report.applied.len(),
        1,
        "and the applied entry, once: {:?}",
        report.applied
    );

    group
        .step(GroupInput::Tick)
        .expect("a replica restored through the operation runs");
    assert!(matches!(group.fatal_state(), GroupFatalState::Healthy));
}

/// A restart after a successful recovery re-installs nothing and re-applies
/// nothing. The second incarnation opens over a state machine that is already
/// at the boundary, so the restore is not owed — which is the property that
/// makes the operation safe to put on an unconditional restart path rather than
/// behind a caller's judgement about whether this boot needs it.
#[test]
fn a_second_recovery_over_a_restored_state_machine_does_nothing() {
    let mut group = below_boundary_group();
    group
        .apply_recovery_outputs(vec![apply_output(6, b"committed-after-the-boundary", None)])
        .expect("the first recovery restores and applies");

    let RaftGroupParts {
        runtime,
        state_machine,
        ..
    } = group.into_parts();
    let applied_index = state_machine
        .applied_index()
        .expect("the state machine reports where it ended up");
    let mut reopened =
        RaftGroup::with_applied_index(7, NodeId(1), runtime, state_machine, applied_index);

    // The suffix is not re-emitted after a restart — the kernel's floor is at
    // 6 — so the honest second-boot batch is empty.
    let report = reopened
        .apply_recovery_outputs(Vec::new())
        .expect("a restored replica recovers into a no-op");

    assert_eq!(
        reopened.state_machine().operations,
        vec![
            RecordedOperation::InstallSnapshot(LogIndex(5)),
            RecordedOperation::ApplyBatch(vec![LogIndex(6)]),
        ],
        "the second boot must add nothing to the trace the first one left"
    );
    assert!(report.applied.is_empty() && report.snapshot_events.is_empty());
    assert!(matches!(reopened.fatal_state(), GroupFatalState::Healthy));
}

/// The install is the transaction's first act, so a failure in it must end the
/// operation with the suffix unapplied. A failed install legitimately poisons —
/// that is existing snapshot-install semantics and stays — but the entry the
/// batch carried must never have reached the application.
#[test]
fn a_failed_restore_leaves_the_suffix_unapplied() {
    let mut group = below_boundary_group();
    group.state_machine_mut().fail_install_snapshot = true;

    let error = group
        .apply_recovery_outputs(vec![apply_output(6, b"committed-after-the-boundary", None)])
        .expect_err("an install that fails ends the recovery");
    assert!(
        matches!(
            error,
            GroupError::StateMachine {
                operation: StateMachineOperation::InstallSnapshot,
                ..
            }
        ),
        "the install failure travels as itself, got {error:?}"
    );

    let app = group.state_machine();
    assert_eq!(
        app.applied,
        vec![b"stale".to_vec()],
        "the suffix must not have been applied over the gap the install failed to close"
    );
    assert_eq!(app.applied_index, LogIndex(3));
    assert!(app.batches.is_empty(), "apply_batch was never called");
    assert!(
        matches!(group.fatal_state(), GroupFatalState::Poisoned { .. }),
        "and a failed install poisons, as it does on every other path"
    );
}

/// A runtime that reports a boundary it cannot describe cannot be recovered
/// past, and the composition says so instead of applying the suffix anyway.
/// This is the missing-or-unreadable snapshot case as the group sees it: the
/// descriptor never arrives, so the install cannot even be attempted.
#[test]
fn a_boundary_without_a_descriptor_refuses_rather_than_applies() {
    let mut group = below_boundary_group_without_descriptor();

    let error = group
        .apply_recovery_outputs(vec![apply_output(6, b"committed-after-the-boundary", None)])
        .expect_err("a restore that cannot be performed must not be skipped");
    assert!(
        matches!(
            error,
            GroupError::SnapshotRestoreRequired {
                app_applied_index: LogIndex(3),
                snapshot_index: LogIndex(5),
                entry_index: LogIndex(6),
            }
        ),
        "got {error:?}"
    );

    let app = group.state_machine();
    assert!(app.batches.is_empty() && app.installed_snapshots.is_empty());
    assert_eq!(app.applied_index, LogIndex(3));
    assert!(
        matches!(group.fatal_state(), GroupFatalState::Healthy),
        "an operator who repairs the snapshot store can retry, so this does not poison"
    );
}

/// PROBE E, the empty case — the shape a fully compacted replica actually
/// hands over, since `drain_committed_outputs` has nothing above the boundary
/// to emit. The pump accepts it, the caller restores, and the replica runs.
#[test]
fn a_crash_window_replica_that_restores_after_opening_runs() {
    let mut group = below_boundary_group();

    group
        .apply_raft_outputs(Vec::new())
        .expect("an empty recovery batch must not poison a recovering replica");
    assert!(matches!(group.fatal_state(), GroupFatalState::Healthy));

    // The repair for a crash between promoting an inbound snapshot and
    // installing it: the caller restores the state machine from the snapshot
    // the boundary names, after opening.
    group.state_machine_mut().applied_index = LogIndex(5);

    group
        .step(GroupInput::Tick)
        .expect("a restored replica runs, whatever floor it opened with");
    assert!(matches!(group.fatal_state(), GroupFatalState::Healthy));
}

/// And the reason waiting costs nothing. A replica that drains its recovery
/// outputs and never restores is refused at the first thing it does that would
/// let the truncated state machine answer for it — whichever of the two that
/// is. Deferring the verdict past the pump defers it past nothing observable.
#[test]
fn a_crash_window_replica_that_never_restores_is_refused_at_its_first_use() {
    let mut stepped = below_boundary_group();
    stepped
        .apply_raft_outputs(Vec::new())
        .expect("the pump takes no verdict");
    assert!(matches!(
        stepped.step(GroupInput::Tick),
        Err(GroupError::AppliedIndexBelowSnapshotBoundary { .. })
    ));

    let mut read = below_boundary_group();
    read.apply_raft_outputs(Vec::new())
        .expect("the pump takes no verdict");
    assert!(matches!(
        read.read(read_helper_request(ReadId(1), ReadConsistency::Local, None)),
        Err(GroupError::AppliedIndexBelowSnapshotBoundary { .. })
    ));
}

/// `begin_proposal` and `begin_proposal_batch` step the runtime without routing
/// through `step_with_options`, so "a step is refused" has to be checked at
/// them by name rather than inferred from the one entry point that spells the
/// word. A proposal is how a state machine's contents are *extended*, and on
/// this replica it would extend a state machine missing acknowledged entries.
#[test]
fn the_proposal_entry_points_that_bypass_step_are_refused_too() {
    let mut single = below_boundary_group();
    let error = single
        .begin_proposal(Proposal {
            local_proposal_id: LocalProposalId(1),
            client_request_id: None,
            command: b"write".to_vec(),
        })
        .expect_err("begin_proposal steps the runtime and must take the verdict");
    assert!(
        matches!(
            error,
            GroupError::AppliedIndexBelowSnapshotBoundary {
                app_applied_index: LogIndex(3),
                snapshot_index: LogIndex(5),
            }
        ),
        "got {error:?}"
    );

    let mut batch = below_boundary_group();
    let error = batch
        .begin_proposal_batch(vec![Proposal {
            local_proposal_id: LocalProposalId(1),
            client_request_id: None,
            command: b"write".to_vec(),
        }])
        .expect_err("begin_proposal_batch steps the runtime and must take the verdict");
    assert!(
        matches!(error, GroupError::AppliedIndexBelowSnapshotBoundary { .. }),
        "got {error:?}"
    );
}
