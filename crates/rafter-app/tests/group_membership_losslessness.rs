#![allow(clippy::wildcard_imports)]

//! A membership fact this group moved through is owed until it is *reported*.
//!
//! Its sibling [`group_membership_stream`] pins what a **successful** step
//! reports. This one pins the other half of the same contract, and it is the
//! half a per-step comparison cannot keep: the runtime moves its configuration
//! before the group has finished the step, and everything the group does after
//! that — applying committed entries, completing granted read barriers,
//! deciding whether a proposal started — can fail. A step that fails returns no
//! report at all, so a group that compared against a *pre-step* snapshot
//! reported the move into a value nobody received and then compared the next
//! step against the configuration it had already moved to. The transition was
//! unreportable from then on, and the consumer's peer set and fences stayed on
//! the old membership for the life of the incarnation.
//!
//! So the comparison is against durable state — the memberships as of the last
//! report this group *handed back* — and it advances only when a report is
//! returned. Everything here is one shape said five ways: **the delta survives
//! the failure.**

mod support;

use support::*;

/// A group whose runtime reports `effective`/`committed` and moves to each
/// scripted pair on its next step, with `outputs` released by that same step.
fn losslessness_group(
    app: RecordingStateMachine,
    effective: &[u64],
    committed: &[u64],
    steps: impl IntoIterator<Item = (Vec<u64>, Vec<u64>)>,
    outputs: impl IntoIterator<Item = Vec<RaftOutput>>,
) -> RaftGroup<u64, RecordingStateMachine, ScriptedRuntime> {
    let mut runtime = ScriptedRuntime::with_terms([
        (LogIndex(1), Term(1)),
        (LogIndex(2), Term(1)),
        (LogIndex(3), Term(1)),
        (LogIndex(4), Term(1)),
    ]);
    runtime.membership = membership(effective, &[]);
    runtime.committed_membership = membership(committed, &[]);
    runtime.last_log_index = LogIndex(2);
    runtime.commit_index = LogIndex(2);
    runtime.step_memberships = steps
        .into_iter()
        .map(|(effective, committed)| (membership(&effective, &[]), membership(&committed, &[])))
        .collect();
    runtime.step_outputs = outputs.into_iter().collect();
    scripted_group_with_runtime(app, runtime)
}

/// The frame a follower is stepped with; its contents do not matter, because
/// every configuration move here is scripted onto the runtime.
fn replicated_frame() -> GroupInput<u64, Vec<u8>> {
    GroupInput::PeerMessage {
        envelope: PeerEnvelope {
            group_id: 7,
            from: NodeId(2),
            to: NodeId(1),
            message: Message::AppendEntries(AppendEntries {
                term: Term(1),
                leader_id: NodeId(2),
                prev_log_index: LogIndex(2),
                prev_log_term: Term(1),
                sequence: 1,
                entries: Vec::new().into(),
                leader_commit: LogIndex(2),
            }),
        },
    }
}

/// The membership any of the three configuration-carrying events names.
///
/// All three are accepted because this file asks what a *delta survives*, not
/// which provenance carried it. The distinction between an exact crossing and an
/// endpoint observation is asserted where it changes a consumer's conclusion —
/// `crates/rafter-service/tests/transport_cursor_provenance.rs` — and here it
/// would only make every case restate its fixture's shape.
fn voters(event: &MembershipEvent<u64>) -> Vec<NodeId> {
    match event {
        MembershipEvent::EffectiveChanged { membership, .. }
        | MembershipEvent::Applied { membership, .. }
        | MembershipEvent::CommittedEndpoint { membership, .. } => membership.replica_ids(),
        other => panic!("expected a membership configuration event, got {other:?}"),
    }
}

/// Whether an event is one of the two committed facts.
///
/// **Both, deliberately.** These fixtures drive a scripted runtime that emits no
/// `ConfigurationCommitted` output, so every committed fact they produce reaches
/// the report through the endpoint comparison — the shape a snapshot install and
/// a group opened over an already-moved runtime also produce. Asserting
/// `Applied` here would have been asserting the fixture rather than the
/// property, which is that a committed membership move is never lost.
fn is_committed_fact(event: &MembershipEvent<u64>) -> bool {
    matches!(
        event,
        MembershipEvent::Applied { .. } | MembershipEvent::CommittedEndpoint { .. }
    )
}

/// Leaves the group holding a granted barrier this replica has not applied
/// through, so every later step reads the state machine's applied index.
///
/// This is how a fixture arms a *late* failure: the group has already recorded
/// every output the step released and is finishing the report when the state
/// machine refuses. The barrier's floor is deliberately unreachable, so it stays
/// reserved and the read is retried — and therefore fails — on every step.
///
/// It costs one step, which is why every fixture that calls it scripts an
/// unchanged membership pair ahead of the one under test.
fn arm_granted_read(group: &mut RaftGroup<u64, RecordingStateMachine, ScriptedRuntime>) {
    begin_pending_read_barrier(group, ReadId(1), Some(LogIndex(50)));
    let report = group
        .apply_raft_outputs(vec![RaftOutput::ReadIndexGranted {
            read_id: ReadId(1),
            read_index: LogIndex(2),
        }])
        .expect("the grant is recorded");
    assert!(
        matches!(
            report.read_events.as_slice(),
            [ReadEvent::FreshnessUnavailable { .. }]
        ),
        "the barrier is granted and short of its floor, so it stays reserved: {:?}",
        report.read_events
    );
    assert!(
        report.membership_events.is_empty(),
        "and arming the barrier moved no configuration: {:?}",
        report.membership_events
    );
}

/// The unchanged membership pair an arming step consumes.
fn unchanged(effective: &[u64], committed: &[u64]) -> (Vec<u64>, Vec<u64>) {
    (effective.to_vec(), committed.to_vec())
}

/// A later step that moves nothing, so the only membership it can report is a
/// delta an earlier step left owed.
///
/// **The discriminating half of the failure cases here.** A drain taken
/// immediately after a failed step is satisfied by a *pre-step snapshot* too:
/// that snapshot was taken before the runtime moved, so it still differs. What a
/// snapshot cannot survive is a second step, which re-snapshots from the
/// configuration the runtime already moved to and has nothing left to compare
/// against. The clauses below that go through this helper are the ones that say
/// the comparison is durable rather than merely well timed.
fn report_after_an_unrelated_step(
    group: &mut RaftGroup<u64, RecordingStateMachine, ScriptedRuntime>,
) -> Vec<MembershipEvent<u64>> {
    group
        .step(replicated_frame())
        .expect("a later step the state machine does not refuse")
        .membership_events
}

/// A committed removal survives a step that fails after the runtime moved.
///
/// The reviewer's first case, and the one that costs the most: a peer delivery
/// commits node 3 out of the configuration, and the same step then fails
/// completing an unrelated read barrier. The removal is the only fact that
/// licenses narrowing a peer set and fencing what left, and a consumer that
/// never hears it keeps authorizing a replica the cluster retired.
#[test]
fn a_committed_removal_survives_a_step_that_failed_after_it_moved() {
    let mut group = losslessness_group(
        RecordingStateMachine::default(),
        &[1, 2, 3],
        &[1, 2, 3],
        [unchanged(&[1, 2, 3], &[1, 2, 3]), (vec![1, 2], vec![1, 2])],
        [],
    );
    arm_granted_read(&mut group);
    group.state_machine_mut().fail_applied_index = true;

    let error = group
        .step(replicated_frame())
        .expect_err("the state machine refuses to report its applied index");
    assert!(
        matches!(
            error,
            GroupError::StateMachine {
                operation: StateMachineOperation::AppliedIndex,
                ..
            }
        ),
        "the step failed completing the barrier, not applying: {error:?}"
    );

    group.state_machine_mut().fail_applied_index = false;
    let owed = group.drain_membership_events();

    assert_eq!(
        owed.len(),
        2,
        "both facts moved and neither was reported: {owed:?}"
    );
    assert_eq!(voters(&owed[0]), vec![NodeId(1), NodeId(2)]);
    assert_eq!(voters(&owed[1]), vec![NodeId(1), NodeId(2)]);
    assert!(matches!(owed[0], MembershipEvent::EffectiveChanged { .. }));
    assert!(is_committed_fact(&owed[1]));
}

/// The same removal survives an intervening step that reported nothing.
///
/// The clause a pre-step snapshot cannot satisfy, and therefore the one that
/// says the comparison is durable rather than merely well timed. See
/// [`report_after_an_unrelated_step`].
#[test]
fn a_committed_removal_survives_a_later_step_that_moved_nothing() {
    let mut group = losslessness_group(
        RecordingStateMachine::default(),
        &[1, 2, 3],
        &[1, 2, 3],
        [
            unchanged(&[1, 2, 3], &[1, 2, 3]),
            (vec![1, 2], vec![1, 2]),
            unchanged(&[1, 2], &[1, 2]),
        ],
        [],
    );
    arm_granted_read(&mut group);
    group.state_machine_mut().fail_applied_index = true;
    let _ = group
        .step(replicated_frame())
        .expect_err("the state machine refuses to report its applied index");
    group.state_machine_mut().fail_applied_index = false;

    let reported = report_after_an_unrelated_step(&mut group);

    assert_eq!(
        reported.len(),
        2,
        "the delta the failed step owed arrives on the next report: {reported:?}"
    );
    assert_eq!(voters(&reported[0]), vec![NodeId(1), NodeId(2)]);
    assert!(matches!(
        reported[0],
        MembershipEvent::EffectiveChanged { .. }
    ));
    assert!(is_committed_fact(&reported[1]));
    assert!(
        group.drain_membership_events().is_empty(),
        "and that report advanced the mark, so it is owed once"
    );
}

/// An effective rollback survives the same failure.
///
/// The other direction, and the one no consumer can re-derive: a new leader
/// truncated an uncommitted addition back off the log, nothing committed, and
/// no later event names the configuration that was taken away.
#[test]
fn an_effective_rollback_survives_a_step_that_failed_after_it_moved() {
    let mut group = losslessness_group(
        RecordingStateMachine::default(),
        &[1, 2, 3],
        &[1, 2],
        [unchanged(&[1, 2, 3], &[1, 2]), (vec![1, 2], vec![1, 2])],
        [],
    );
    arm_granted_read(&mut group);
    group.state_machine_mut().fail_applied_index = true;

    let _ = group
        .step(replicated_frame())
        .expect_err("the state machine refuses to report its applied index");

    group.state_machine_mut().fail_applied_index = false;
    let owed = group.drain_membership_events();

    assert_eq!(
        owed.len(),
        1,
        "the effective fact moved and the committed one did not: {owed:?}"
    );
    assert!(matches!(owed[0], MembershipEvent::EffectiveChanged { .. }));
    assert_eq!(voters(&owed[0]), vec![NodeId(1), NodeId(2)]);
}

/// A membership move survives a failing state-machine apply.
///
/// The third producer on the reviewer's list. `apply_entries` runs before the
/// membership comparison and can fail on its own, and this is the arm where the
/// configuration entry and the application entry commit in the same step.
#[test]
fn a_membership_move_survives_a_failing_apply() {
    let mut group = losslessness_group(
        RecordingStateMachine {
            apply_mode: ApplyMode::Fail,
            ..RecordingStateMachine::default()
        },
        &[1, 2],
        &[1, 2],
        [(vec![1, 2, 3], vec![1, 2, 3])],
        [vec![apply_output(3, b"command", None)]],
    );

    let error = group
        .step(replicated_frame())
        .expect_err("the state machine refuses the batch");
    assert!(
        matches!(error, GroupError::StateMachine { .. }),
        "the step failed applying: {error:?}"
    );

    let owed = group.drain_membership_events();

    assert_eq!(
        owed.len(),
        2,
        "both facts moved and the failed apply reported neither: {owed:?}"
    );
    assert_eq!(voters(&owed[0]), vec![NodeId(1), NodeId(2), NodeId(3)]);
}

/// A membership move survives a failing snapshot install.
///
/// The one path on which a replica's entire configuration is replaced at once,
/// paired with the one failure that can interrupt it. The runtime has already
/// promoted the snapshot durably when the application refuses it, so the
/// configuration has moved and the report never arrives.
#[test]
fn a_membership_move_survives_a_failing_snapshot_install() {
    let mut group = losslessness_group(
        RecordingStateMachine {
            fail_install_snapshot: true,
            ..RecordingStateMachine::default()
        },
        &[1, 2],
        &[1, 2],
        [(vec![1, 4, 5], vec![1, 4, 5])],
        [vec![RaftOutput::ApplySnapshot {
            snapshot: test_snapshot(9),
        }]],
    );

    let _ = group
        .step(replicated_frame())
        .expect_err("the state machine refuses the snapshot");

    let owed = group.drain_membership_events();

    assert_eq!(
        owed.len(),
        2,
        "the snapshot replaced both facts and neither was reported: {owed:?}"
    );
    assert_eq!(voters(&owed[0]), vec![NodeId(1), NodeId(4), NodeId(5)]);
    assert_eq!(voters(&owed[1]), vec![NodeId(1), NodeId(4), NodeId(5)]);
}

/// The raw output pump discharges a delta a failed step left owed.
///
/// The second hole in the same contract, and it is not a failure path of its
/// own: [`RaftGroup::apply_raft_outputs`] is the advanced pump a caller driving
/// a [`PersistedRaftRuntime`] uses, and both shipped drivers call it one line
/// after construction to drain a recovery report. It used to be handed no
/// membership context at all, so it could not report a configuration move under
/// any circumstances — including the one that matters, where a step that failed
/// left the move owed and this pump is the next report the caller receives.
#[test]
fn the_raw_output_pump_discharges_a_membership_delta_a_failed_step_left_owed() {
    let mut group = losslessness_group(
        RecordingStateMachine::default(),
        &[1, 2],
        &[1, 2],
        [unchanged(&[1, 2], &[1, 2]), (vec![1, 2, 3], vec![1, 2, 3])],
        [],
    );
    arm_granted_read(&mut group);
    group.state_machine_mut().fail_applied_index = true;
    let _ = group
        .step(replicated_frame())
        .expect_err("the state machine refuses to report its applied index");
    group.state_machine_mut().fail_applied_index = false;

    let report = group
        .apply_raft_outputs(Vec::new())
        .expect("the pump applies the caller's outputs");

    assert_eq!(
        report.membership_events.len(),
        2,
        "the pump reports what the failed step could not: {:?}",
        report.membership_events
    );
    assert_eq!(
        voters(&report.membership_events[0]),
        vec![NodeId(1), NodeId(2), NodeId(3)]
    );
    assert!(matches!(
        report.membership_events[0],
        MembershipEvent::EffectiveChanged { .. }
    ));
    assert!(is_committed_fact(&report.membership_events[1]));
    assert!(
        group.drain_membership_events().is_empty(),
        "and the pump's report advanced the mark, so nothing is owed twice"
    );
}

/// A reported move is reported once.
///
/// The control, and the reason the comparison is against durable state rather
/// than a running diff: a successful step advances the mark, so the next step
/// and the next drain have nothing to say. A group that re-reported the same
/// configuration on every step would make "the membership changed" unreadable.
#[test]
fn a_reported_move_is_not_reported_again() {
    let mut group = losslessness_group(
        RecordingStateMachine::default(),
        &[1, 2],
        &[1, 2],
        [
            (vec![1, 2, 3], vec![1, 2, 3]),
            (vec![1, 2, 3], vec![1, 2, 3]),
        ],
        [],
    );

    let report = group.step(replicated_frame()).expect("the follower steps");
    assert_eq!(report.membership_events.len(), 2);

    assert!(
        group.drain_membership_events().is_empty(),
        "the delta was reported, so nothing is owed"
    );
    let report = group.step(replicated_frame()).expect("the follower steps");
    assert!(
        report.membership_events.is_empty(),
        "and the next step has nothing to add: {:?}",
        report.membership_events
    );
}

/// A group reports nothing for the configuration it was constructed over.
///
/// Pre-existing state is not an event. A group built over a recovered runtime
/// already holding a three-node configuration has moved through nothing, and a
/// consumer told otherwise would republish a peer set on every restart.
#[test]
fn construction_owes_no_event_for_the_configuration_it_opened_over() {
    let mut group = losslessness_group(
        RecordingStateMachine::default(),
        &[1, 2, 3],
        &[1, 2, 3],
        [],
        [],
    );

    assert!(
        group.drain_membership_events().is_empty(),
        "the configuration this group opened over is not a transition"
    );
}

/// A discarded report leaves its membership delta owed.
///
/// `GroupError::ProposalDidNotStart` throws away a report the group had already
/// built, which is a known hole in its own right. Its membership half is closed
/// here by construction: the mark advances when a report is *returned*, so a
/// report the caller never sees advances nothing and the next drain still owes
/// the move.
#[test]
fn a_report_discarded_by_a_proposal_verdict_still_owes_its_membership_delta() {
    let mut group = losslessness_group(
        RecordingStateMachine::default(),
        &[1, 2],
        &[1, 2],
        [(vec![1, 2, 3], vec![1, 2]), unchanged(&[1, 2, 3], &[1, 2])],
        // No lifecycle output for the proposal, which is what makes the group
        // discard the report it built.
        [vec![]],
    );

    let error = group
        .step(GroupInput::Proposal {
            proposal: Proposal {
                local_proposal_id: LocalProposalId(1),
                client_request_id: None,
                command: b"command".to_vec(),
            },
        })
        .expect_err("the runtime released no lifecycle event for the proposal");
    assert!(matches!(error, GroupError::ProposalDidNotStart { .. }));

    // Through a later step rather than a drain, because the discard has to
    // survive the *mark* rather than merely the moment: a group that advanced
    // the mark into the report it threw away has nothing left to report here.
    let reported = report_after_an_unrelated_step(&mut group);

    assert_eq!(
        reported.len(),
        1,
        "the discarded report's membership delta is still owed: {reported:?}"
    );
    assert_eq!(voters(&reported[0]), vec![NodeId(1), NodeId(2), NodeId(3)]);
}

/// Decomposition carries the owed delta, and the rebuild still owes it.
///
/// **The boundary a failing step's losslessness could not cross.** A membership-
/// moving step fails, so the transition is owed; the caller then does what the
/// decomposition contract tells it to do — take the parts, keep the runtime,
/// build a new group — and the owed transition is gone. `RaftGroupParts` carried
/// no mark, so `with_applied_index` seeded a fresh comparison from the runtime it
/// was handed, and that runtime had already *moved*. The new group's first
/// comparison read "nothing has changed" and every later one agreed.
///
/// The decomposition contract says no protocol effect can be lost by calling
/// `into_parts`. This is the clause that made that false.
#[test]
fn a_rebuild_from_parts_still_owes_a_failed_steps_membership_delta() {
    let mut group = losslessness_group(
        RecordingStateMachine::default(),
        &[1, 2, 3],
        &[1, 2, 3],
        [unchanged(&[1, 2, 3], &[1, 2, 3]), (vec![1, 2], vec![1, 2])],
        [],
    );
    arm_granted_read(&mut group);
    group.state_machine_mut().fail_applied_index = true;
    group
        .step(replicated_frame())
        .expect_err("the state machine refuses to report its applied index");

    let mut parts = group.into_parts();
    parts.state_machine.fail_applied_index = false;
    let mut rebuilt = RaftGroup::from_parts(parts, LogIndex::ZERO);

    let owed = rebuilt.drain_membership_events();
    assert_eq!(
        owed.len(),
        2,
        "the delta the failed step left owed survived decomposition: {owed:?}"
    );
    assert_eq!(voters(&owed[0]), vec![NodeId(1), NodeId(2)]);
    assert!(matches!(owed[0], MembershipEvent::EffectiveChanged { .. }));
    assert!(is_committed_fact(&owed[1]));
    assert!(
        rebuilt.drain_membership_events().is_empty(),
        "and the drain advanced the rebuilt group's mark"
    );
}

/// The control for the rebuild: a group that owed nothing still owes nothing.
///
/// Without it, a `from_parts` that simply re-reported the runtime's whole
/// configuration would pass the clause above. Pre-existing state is not an event
/// on either side of a decomposition.
#[test]
fn a_rebuild_from_parts_owes_nothing_when_nothing_was_owed() {
    let mut group = losslessness_group(
        RecordingStateMachine::default(),
        &[1, 2, 3],
        &[1, 2, 3],
        [(vec![1, 2], vec![1, 2])],
        [],
    );
    let report = group.step(replicated_frame()).expect("the follower steps");
    assert_eq!(
        report.membership_events.len(),
        2,
        "and the step reported it"
    );

    let mut rebuilt = RaftGroup::from_parts(group.into_parts(), LogIndex::ZERO);
    assert!(
        rebuilt.drain_membership_events().is_empty(),
        "a reported delta does not come back through decomposition"
    );
}

/// The outcome-only proposal helper leaves its membership delta owed.
///
/// It discards a whole report by design and documents that it does. What it must
/// not discard is the mark that report advanced: a caller that never received
/// the report never received the membership event in it, so the delta is owed
/// exactly as it is after any other discarded report.
#[test]
fn the_outcome_only_proposal_helper_still_owes_its_membership_delta() {
    let mut group = losslessness_group(
        RecordingStateMachine::default(),
        &[1, 2],
        &[1, 2],
        [
            (vec![1, 2, 3], vec![1, 2, 3]),
            unchanged(&[1, 2, 3], &[1, 2, 3]),
        ],
        [vec![RaftOutput::LocalProposalAppended {
            proposal_id: LocalProposalId(1),
            index: LogIndex(3),
            term: Term(1),
        }]],
    );

    let begin = group
        .begin_proposal_outcome(Proposal {
            local_proposal_id: LocalProposalId(1),
            client_request_id: None,
            command: b"command".to_vec(),
        })
        .expect("the proposal starts");
    assert!(matches!(begin, ProposalBegin::Appended { .. }));

    // Through a later step, for the reason `report_after_an_unrelated_step`
    // gives: a mark that advanced into the discarded report has nothing left.
    let reported = report_after_an_unrelated_step(&mut group);
    assert_eq!(
        reported.len(),
        2,
        "the discarded report's membership delta is still owed: {reported:?}"
    );
    assert_eq!(voters(&reported[0]), vec![NodeId(1), NodeId(2), NodeId(3)]);
    assert!(matches!(
        reported[0],
        MembershipEvent::EffectiveChanged { .. }
    ));
    assert!(is_committed_fact(&reported[1]));
}

/// The outcome-only read-barrier helper leaves its membership delta owed.
///
/// The same clause on the other outcome-only surface. Both were named as
/// non-blocking in review; both made the documented mark invariant — every
/// report discarded before reaching a caller restores the mark — false.
#[test]
fn the_outcome_only_read_barrier_helper_still_owes_its_membership_delta() {
    let mut group = losslessness_group(
        RecordingStateMachine::default(),
        &[1, 2],
        &[1, 2],
        [
            (vec![1, 2, 3], vec![1, 2, 3]),
            unchanged(&[1, 2, 3], &[1, 2, 3]),
        ],
        [],
    );

    group
        .begin_read_barrier_outcome(read_request(ReadId(1), None))
        .expect("the barrier starts");

    let reported = report_after_an_unrelated_step(&mut group);
    assert_eq!(
        reported.len(),
        2,
        "the discarded report's membership delta is still owed: {reported:?}"
    );
    assert_eq!(voters(&reported[0]), vec![NodeId(1), NodeId(2), NodeId(3)]);
}

/// One configuration entry, as a leader would replicate it.
fn configuration_entry(config_id: u64, node_ids: &[u64]) -> rafter::LogEntry {
    rafter::LogEntry::configuration(
        Term(1),
        rafter::ConfigurationEntry::stable(
            rafter::ConfigurationId(config_id),
            MembershipSet::new(node_ids.iter().copied().map(NodeId).collect(), Vec::new())
                .expect("test membership is valid"),
        ),
    )
}

/// A configuration the same commit crossed survives an entry the state machine
/// cannot decode.
///
/// The producer the other five miss, and the one that needed a real kernel to
/// state. Every failure above fails *after* the whole output vector has been
/// walked — `apply_entries` runs past the loop, and so do the read completion
/// and the readiness probe — so the crossing queue was already full when they
/// raised. Decoding is different: it runs *inside* the loop, once per `Apply`,
/// so an undecodable payload at a lower index than a configuration entry
/// abandons the scan with that configuration still unvisited.
///
/// Nothing queued it, so nothing owed it, and the endpoint comparison cannot
/// rescue it: a commit that admits node 5 and removes it again lands on the
/// membership it started from, so the comparison sees no move at all. The
/// identity the cluster spent is spent nowhere here.
#[test]
fn a_crossed_configuration_survives_an_undecodable_entry_ahead_of_it() {
    let mut group = RaftGroup::new(
        7,
        NodeId(1),
        runtime(1, &[2, 3]),
        RecordingStateMachine {
            fail_decode: true,
            ..RecordingStateMachine::default()
        },
    );

    let error = group
        .step(GroupInput::PeerMessage {
            envelope: PeerEnvelope {
                group_id: 7,
                from: NodeId(2),
                to: NodeId(1),
                message: Message::AppendEntries(AppendEntries {
                    term: Term(1),
                    leader_id: NodeId(2),
                    prev_log_index: LogIndex::ZERO,
                    prev_log_term: Term(0),
                    sequence: 1,
                    entries: vec![
                        rafter::LogEntry::application(Term(1), b"command".to_vec()),
                        configuration_entry(1, &[1, 2, 3, 5]),
                        configuration_entry(2, &[1, 2, 3]),
                    ]
                    .into(),
                    leader_commit: LogIndex(3),
                }),
            },
        })
        .expect_err("the state machine refuses to decode the payload");
    assert!(
        matches!(
            error,
            GroupError::StateMachine {
                operation: StateMachineOperation::DecodeCommand,
                ..
            }
        ),
        "the step failed decoding: {error:?}"
    );

    let owed = group.drain_membership_events();

    assert_eq!(
        owed.len(),
        2,
        "the commit crossed two configurations and the decode failure reported \
         neither: {owed:?}"
    );
    assert!(matches!(owed[0], MembershipEvent::Applied { .. }));
    assert!(matches!(owed[1], MembershipEvent::Applied { .. }));
    assert_eq!(
        voters(&owed[0]),
        vec![NodeId(1), NodeId(2), NodeId(3), NodeId(5)],
        "the admission is reported first, in index order"
    );
    assert_eq!(
        voters(&owed[1]),
        vec![NodeId(1), NodeId(2), NodeId(3)],
        "the removal that spent node 5 is reported second"
    );
}
