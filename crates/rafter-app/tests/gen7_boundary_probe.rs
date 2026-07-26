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

mod support;

use support::*;

/// The exact fixture from the K2 regression test: a runtime whose snapshot
/// boundary is 5 over a state machine that only reached 3. The entries 4..=5
/// are compacted out of the log and nothing will ever supply them.
fn below_boundary_group() -> RaftGroup<u64, RecordingStateMachine, ScriptedRuntime> {
    let mut runtime = ScriptedRuntime::with_step_outputs([Vec::new()]);
    runtime.snapshot_index = LogIndex(5);
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

/// PROBE E, and the third boundary of the scope — the severe one. The recovery
/// batch this refusal's own design says it accommodates is
/// `RecoveredDurableRaftNode::into_parts`' `recovery_outputs`, and
/// `apply_committed_into` never pushes an `Output::ApplySnapshot` — the kernel
/// holds a descriptor rather than payload bytes — so such a batch can never
/// carry an install. Both reference drivers pump it one line after
/// construction, ahead of any chance to restore. It must not poison the group.
#[test]
fn the_recovery_batch_a_crash_window_replica_drains_is_accepted() {
    let mut group = below_boundary_group();

    // A recovery batch of the shape `drain_committed_outputs` actually
    // produces: committed applies above the boundary, no install.
    let recovery_outputs = vec![apply_output(6, b"committed-after-the-boundary", None)];

    group
        .apply_raft_outputs(recovery_outputs)
        .expect("a recovering replica must not be poisoned before it can restore");
    assert!(matches!(group.fatal_state(), GroupFatalState::Healthy));
    group
        .step(GroupInput::Tick)
        .expect("and it runs once it is no longer short of the boundary");
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
