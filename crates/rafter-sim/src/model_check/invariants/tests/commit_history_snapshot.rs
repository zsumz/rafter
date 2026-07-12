use rafter::{LogIndex, NodeId, RaftSnapshot, Term};

use super::super::super::{
    application::apply_to_state, helpers::elect_node_one_in_state, scheduling::Operation,
};
use super::commit_history::{app_entry, bootstrap_with_log, voter_configs};
use super::*;

#[test]
fn leader_completeness_accepts_committed_prefix_hidden_by_witnessed_snapshot() {
    let mut cluster = Cluster::new(voter_configs(&[1]));
    cluster
        .restart_node_from_bootstrap(
            NodeId(1),
            bootstrap_with_log(
                Term(2),
                LogIndex(1),
                vec![
                    app_entry(1, Term(1), b"committed"),
                    app_entry(2, Term(2), b"compacted"),
                ],
                None,
            ),
        )
        .expect("visible committed bootstrap is valid");
    let mut state = ExplorationState::new(cluster);
    assert!(
        state
            .commit_history
            .committed_prefix
            .as_ref()
            .is_some_and(|prefix| prefix.through >= LogIndex(1)),
        "test setup must record the committed prefix before compaction"
    );

    let (snapshot, payload) = test_snapshot(1, 2, 2, 3, b"snapshot through two");
    state
        .cluster
        .seed_snapshot_payload(NodeId(1), &snapshot, payload);
    state
        .cluster
        .restart_node_from_bootstrap(NodeId(1), bootstrap_with_snapshot(Term(3), snapshot, &[]))
        .expect("compacted bootstrap is valid");
    state.refresh_log_history();
    state.election_history.record_election(election_certificate(
        3,
        1,
        stable_membership(&[1], &[]),
        &[1],
    ));

    state.record_leader_completeness_observation();

    check_commit_history(&state, &[])
        .expect("snapshot witness should prove the hidden committed prefix");
}

#[test]
fn leader_completeness_rejects_unwitnessed_snapshot_with_matching_boundary() {
    let (snapshot, payload) = test_snapshot(1, 2, 2, 3, b"unwitnessed snapshot");
    let mut cluster = Cluster::new(voter_configs(&[1, 2]));
    cluster.seed_snapshot_payload(NodeId(1), &snapshot, payload);
    cluster
        .restart_node_from_bootstrap(NodeId(1), bootstrap_with_snapshot(Term(3), snapshot, &[]))
        .expect("snapshot bootstrap is valid");
    cluster
        .restart_node_from_bootstrap(
            NodeId(2),
            bootstrap_with_log(
                Term(2),
                LogIndex(2),
                vec![
                    app_entry(1, Term(1), b"committed"),
                    app_entry(2, Term(2), b"boundary"),
                ],
                None,
            ),
        )
        .expect("visible committed bootstrap is valid");
    let mut state = ExplorationState::new(cluster);
    state.election_history.record_election(election_certificate(
        3,
        1,
        stable_membership(&[1, 2], &[]),
        &[1, 2],
    ));

    state.record_leader_completeness_observation();

    let failure = check_commit_history(&state, &[])
        .expect_err("matching boundary coordinates alone must not prove a snapshot prefix");
    assert_eq!(failure.invariant(), catalog::LG_05_LEADER_COMPLETENESS);
    assert!(
        failure.message.contains("without committed prefix"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn snapshot_transfer_propagates_logical_prefix_witness_to_installed_follower() {
    let mut cluster = Cluster::new(voter_configs(&[1, 2, 3]));
    let visible_log = vec![
        app_entry(1, Term(1), b"prefix"),
        app_entry(2, Term(1), b"boundary"),
        app_entry(3, Term(2), b"suffix"),
    ];
    cluster
        .restart_node_from_bootstrap(
            NodeId(1),
            bootstrap_with_log(Term(2), LogIndex(2), visible_log.clone(), None),
        )
        .expect("leader visible bootstrap is valid");
    cluster
        .restart_node_from_bootstrap(
            NodeId(2),
            bootstrap_with_log(
                Term(2),
                LogIndex::ZERO,
                vec![app_entry(1, Term(1), b"prefix")],
                None,
            ),
        )
        .expect("behind follower bootstrap is valid");
    cluster
        .restart_node_from_bootstrap(
            NodeId(3),
            bootstrap_with_log(Term(2), LogIndex(2), visible_log, None),
        )
        .expect("voter visible bootstrap is valid");
    let mut state = ExplorationState::new(cluster);

    let (snapshot, payload) = test_snapshot(1, 2, 1, 2, b"snapshot through two");
    let suffix = b"suffix".as_slice();
    for node_id in [NodeId(1), NodeId(3)] {
        state
            .cluster
            .seed_snapshot_payload(node_id, &snapshot, payload.clone());
        state
            .cluster
            .restart_node_from_bootstrap(
                node_id,
                bootstrap_with_snapshot(Term(2), snapshot.clone(), &[(3, Term(2), suffix)]),
            )
            .expect("compacted voter bootstrap is valid");
    }
    state.refresh_log_history();

    elect_node_one_in_state(&mut state);
    drive_until_node_two_installs_snapshot(&mut state, &snapshot);
    assert_eq!(
        state.cluster.bootstrap_state(NodeId(2)).snapshot,
        Some(snapshot.clone()),
        "node 2 should install the leader's witnessed snapshot transfer"
    );

    state.election_history.record_election(election_certificate(
        4,
        2,
        stable_membership(&[1, 2, 3], &[]),
        &[1, 2],
    ));
    state.record_leader_completeness_observation();

    check_commit_history(&state, &[])
        .expect("installed snapshot transfer should carry the logical prefix witness");
}

fn drive_until_node_two_installs_snapshot(state: &mut ExplorationState, snapshot: &RaftSnapshot) {
    for _ in 0..64 {
        if state.cluster.bootstrap_state(NodeId(2)).snapshot.as_ref() == Some(snapshot) {
            return;
        }
        if let Some(position) = state
            .cluster
            .network
            .iter()
            .position(|queued| queued.ready_at <= state.cluster.clock.now())
        {
            apply_to_state(state, Operation::DeliverReadyAt(position));
        } else {
            apply_to_state(state, Operation::Tick(NodeId(1)));
        }
    }
    panic!("node 2 did not install the witnessed snapshot transfer");
}
