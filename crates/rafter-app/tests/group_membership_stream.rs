#![allow(clippy::wildcard_imports)]

//! The completeness contract for a group's membership event stream.
//!
//! A transport driver follows these events to keep its peer set current, so the
//! stream has to name *every* configuration this replica moves through — not
//! only the ones a local membership request caused. A follower learns about a
//! configuration change by replication, a new leader can take one back by
//! truncation, and a snapshot install can replace both facts at once; none of
//! those carries a membership request, and a driver that heard nothing for them
//! would authorize the wrong set of replicas until some later local change
//! happened to arrive.
//!
//! Two facts travel here and they are reported separately.
//! [`MembershipEvent::EffectiveChanged`] says which configuration this replica
//! is *operating under*, which may still be uncommitted and may still be taken
//! back; [`MembershipEvent::Applied`] says which one the cluster has committed.
//! A consumer may only widen for the first and may only narrow for the second,
//! so collapsing them would lose the distinction that makes either safe.

mod support;

use support::*;

/// A group whose runtime reports `effective`/`committed` and then moves to each
/// scripted pair on its next step.
///
/// The moves are applied from inside `step`, which is what makes the group
/// observe them as a *change* rather than as a value that was always this way —
/// the same distinction a real follower sees between opening a log that already
/// held a configuration entry and receiving one.
fn membership_group(
    effective: &[u64],
    committed: &[u64],
    steps: impl IntoIterator<Item = (Vec<u64>, Vec<u64>)>,
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
    scripted_group_with_runtime(RecordingStateMachine::default(), runtime)
}

/// The frame a follower is stepped with. Its contents do not matter here: the
/// configuration move is scripted onto the runtime, and what is under test is
/// that a step carrying no membership *request* still reports what moved.
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

fn effective_voters(event: &MembershipEvent<u64>) -> Vec<NodeId> {
    match event {
        MembershipEvent::EffectiveChanged { membership, .. }
        | MembershipEvent::Applied { membership, .. } => membership.replica_ids(),
        other => panic!("expected a membership configuration event, got {other:?}"),
    }
}

/// A follower that receives a configuration entry by replication reports it.
///
/// The joiner has to be able to speak before the change commits, or it can
/// never catch up and the change can never commit. Nothing on this path carries
/// a membership request — the entry arrived in an `AppendEntries` — so a stream
/// that spoke only for local requests left this follower's driver authorizing
/// the old set for the whole joint transition.
#[test]
fn a_replicated_configuration_entry_reports_an_effective_change() {
    let mut group = membership_group(&[1, 2], &[1, 2], [(vec![1, 2, 3], vec![1, 2])]);

    let report = group
        .step(replicated_frame())
        .expect("a follower steps a replicated frame");

    assert_eq!(
        report.membership_events.len(),
        1,
        "one fact moved, so one event: {:?}",
        report.membership_events
    );
    assert!(
        matches!(
            &report.membership_events[0],
            MembershipEvent::EffectiveChanged { group_id: 7, .. }
        ),
        "the effective configuration moved and nothing committed, got {:?}",
        report.membership_events[0]
    );
    assert_eq!(
        effective_voters(&report.membership_events[0]),
        vec![NodeId(1), NodeId(2), NodeId(3)],
        "and the event carries the configuration now in effect"
    );
}

/// A new leader taking back an uncommitted addition reports it too.
///
/// The mirror of the clause above, and the one a stream built around appends
/// cannot express at all: nothing was appended, an entry was *truncated*. A
/// consumer that never heard it would keep authorizing a replica the cluster
/// never admitted, for as long as this incarnation ran.
#[test]
fn a_truncated_configuration_reports_the_effective_change_that_undid_it() {
    let mut group = membership_group(
        &[1, 2, 3],
        &[1, 2],
        [
            // A new leader overwrites the uncommitted addition of node 3.
            (vec![1, 2], vec![1, 2]),
        ],
    );

    let report = group
        .step(replicated_frame())
        .expect("a follower steps a replicated frame");

    assert_eq!(
        report.membership_events.len(),
        1,
        "the rollback is one fact moving: {:?}",
        report.membership_events
    );
    assert_eq!(
        effective_voters(&report.membership_events[0]),
        vec![NodeId(1), NodeId(2)],
        "the configuration in effect is the one the truncation left behind"
    );
    assert!(matches!(
        &report.membership_events[0],
        MembershipEvent::EffectiveChanged { .. }
    ));
}

/// One step that moves both facts reports both, in the order a consumer needs.
///
/// The effective change is reported first because a consumer may only widen for
/// it and may only narrow for the committed one: a change that commits while a
/// later change is already in effect must not take the later change's joiner
/// away. Reporting the committed fact alone — which is what a stream that
/// suppressed the effective event whenever the commit index moved did — leaves a
/// consumer no way to tell the two apart.
#[test]
fn one_step_that_moves_both_facts_reports_both() {
    let mut group = membership_group(&[1, 2], &[1, 2], [(vec![1, 2, 3], vec![1, 2, 3])]);

    let report = group
        .step(replicated_frame())
        .expect("a follower steps a replicated frame");

    assert_eq!(
        report.membership_events.len(),
        2,
        "two facts moved, so two events: {:?}",
        report.membership_events
    );
    assert!(
        matches!(
            &report.membership_events[0],
            MembershipEvent::EffectiveChanged { .. }
        ),
        "the effective fact is reported first, got {:?}",
        report.membership_events[0]
    );
    assert!(
        matches!(
            &report.membership_events[1],
            MembershipEvent::Applied { .. }
        ),
        "and the committed one after it, got {:?}",
        report.membership_events[1]
    );
}

/// A local membership request that moves both facts in one step reports both.
///
/// A single-voter leader commits its own configuration change in the step that
/// appends it, and a stream that reported the effective change only when the
/// commit index stood still said nothing about the configuration this replica
/// had begun operating under.
#[test]
fn a_local_request_that_commits_immediately_still_reports_the_effective_change() {
    let mut group = group(1, &[]);
    group.step(GroupInput::Tick).expect("single node elects");

    let report = group
        .step(GroupInput::Membership {
            change: MembershipChange::AddLearner {
                node_id: NodeId(2),
                info: NodeInfo::default(),
            },
        })
        .expect("single node membership change commits");

    assert_eq!(
        report.membership_events.len(),
        2,
        "the request moved both facts, so both are reported: {:?}",
        report.membership_events
    );
    assert!(matches!(
        &report.membership_events[0],
        MembershipEvent::EffectiveChanged { .. }
    ));
    assert!(matches!(
        &report.membership_events[1],
        MembershipEvent::Applied { .. }
    ));
}

/// A commit observed on a peer step reports the committed fact.
///
/// The audit clause for the other half of the stream: a follower learns that a
/// configuration committed from a leader's commit index, not from a request of
/// its own, and that is the only fact that licenses narrowing a peer set or
/// fencing what left it.
#[test]
fn a_commit_observed_on_a_peer_step_reports_the_committed_fact() {
    let mut group = membership_group(&[1, 2], &[1, 2, 3], [(vec![1, 2], vec![1, 2])]);

    let report = group
        .step(replicated_frame())
        .expect("a follower steps a replicated frame");

    assert_eq!(
        report.membership_events.len(),
        1,
        "the effective configuration did not move, so only the commit is news: {:?}",
        report.membership_events
    );
    assert!(matches!(
        &report.membership_events[0],
        MembershipEvent::Applied { group_id: 7, .. }
    ));
    assert_eq!(
        effective_voters(&report.membership_events[0]),
        vec![NodeId(1), NodeId(2)],
        "and it carries the configuration the cluster committed"
    );
}

/// A snapshot install replaces both facts and reports both.
///
/// A replica that fell behind the leader's compaction boundary learns its whole
/// configuration from the snapshot, with no configuration entry to replicate and
/// no request of its own. It is the one path on which a consumer's entire peer
/// set can change at once.
#[test]
fn a_snapshot_install_reports_both_facts() {
    let snapshot = test_snapshot(9);
    let mut runtime = ScriptedRuntime::with_terms([(LogIndex(9), Term(1))]);
    runtime.membership = membership(&[1, 2], &[]);
    runtime.committed_membership = membership(&[1, 2], &[]);
    runtime.step_memberships = [(membership(&[1, 4, 5], &[]), membership(&[1, 4, 5], &[]))]
        .into_iter()
        .collect();
    runtime.step_outputs = [vec![RaftOutput::ApplySnapshot {
        snapshot: snapshot.clone(),
    }]]
    .into_iter()
    .collect();
    let mut group = scripted_group_with_runtime(RecordingStateMachine::default(), runtime);

    let report = group
        .step(replicated_frame())
        .expect("a follower installs a snapshot");

    assert_eq!(
        report.snapshot_events.len(),
        1,
        "the install happened, which is the fixture's premise"
    );
    assert_eq!(
        report.membership_events.len(),
        2,
        "the snapshot replaced both facts: {:?}",
        report.membership_events
    );
    assert_eq!(
        effective_voters(&report.membership_events[0]),
        vec![NodeId(1), NodeId(4), NodeId(5)],
    );
    assert_eq!(
        effective_voters(&report.membership_events[1]),
        vec![NodeId(1), NodeId(4), NodeId(5)],
    );
}

/// A step that moves nothing reports nothing.
///
/// The control. An event stream that fired on every step would make "the
/// configuration changed" unreadable, and a consumer republishing an unchanged
/// peer set on every tick is how a link layer stops being able to tell a real
/// change from noise.
#[test]
fn a_step_that_moves_no_membership_reports_no_membership_event() {
    let mut group = membership_group(&[1, 2], &[1, 2], [(vec![1, 2], vec![1, 2])]);

    let report = group
        .step(replicated_frame())
        .expect("a follower steps a replicated frame");

    assert!(
        report.membership_events.is_empty(),
        "nothing moved: {:?}",
        report.membership_events
    );
}
