use std::time::Duration;

use super::super::helpers::{config, four_node_future_learner_configs, three_node_lease_configs};
use super::super::{
    check_raft_commit_safety, check_raft_election_safety,
    check_raft_joint_membership_restart_and_snapshot_safety, check_raft_membership_safety,
    check_raft_read_index_safety, check_raft_restart_and_snapshot_safety, Bounds,
};

#[test]
fn bounded_raft_election_safety_passes_for_three_node_cluster() {
    let summary = check_raft_election_safety(
        vec![
            config(1, &[2, 3], 2),
            config(2, &[1, 3], 2),
            config(3, &[1, 2], 2),
        ],
        Bounds::new(6),
    )
    .expect("bounded election safety check should pass");

    assert!(summary.explored_states() > 1);
    assert!(summary.unique_states() > 1);
    assert!(summary.unique_states() <= summary.explored_states());
    assert!(summary.explored_actions() > 1);
    assert_eq!(summary.max_depth(), 6);
}

#[test]
fn bounded_raft_commit_safety_passes_for_three_node_cluster() {
    let summary = check_raft_commit_safety(
        vec![
            config(1, &[2, 3], 2),
            config(2, &[1, 3], 2),
            config(3, &[1, 2], 2),
        ],
        Bounds::new(8).with_max_proposals(1),
    )
    .expect("bounded commit safety check should pass");

    assert!(summary.explored_states() > 1);
    assert!(summary.unique_states() > 1);
    assert!(summary.unique_states() <= summary.explored_states());
    assert!(summary.explored_actions() > 1);
    assert_eq!(summary.max_depth(), 8);
}

#[test]
fn bounded_raft_membership_safety_passes_for_future_learner_cluster() {
    let summary = check_raft_membership_safety(
        four_node_future_learner_configs(),
        Bounds::new(5)
            .with_max_proposals(1)
            .with_max_membership_changes(1),
    )
    .expect("bounded membership safety check should pass");

    assert!(summary.explored_states() > 1);
    assert!(summary.unique_states() > 1);
    assert!(summary.unique_states() <= summary.explored_states());
    assert!(summary.explored_actions() > 1);
    assert_eq!(summary.max_depth(), 5);
}

#[test]
fn bounded_raft_membership_safety_does_not_require_client_proposals() {
    let summary = check_raft_membership_safety(
        four_node_future_learner_configs(),
        Bounds::new(4).with_max_membership_changes(1),
    )
    .expect("membership actions should not depend on the client proposal budget");

    assert!(summary.explored_actions() > 1);
    assert_eq!(summary.max_depth(), 4);
}

#[test]
fn bounded_joint_membership_restart_and_snapshot_safety_passes() {
    let summary = check_raft_joint_membership_restart_and_snapshot_safety(
        Bounds::new(8).with_max_restarts(1),
    )
    .expect("joint-membership restart and snapshot safety check should pass");

    assert!(summary.explored_states() > 1);
    assert!(summary.unique_states() > 1);
    assert!(summary.unique_states() <= summary.explored_states());
    assert!(summary.explored_actions() > 1);
    assert_eq!(summary.max_depth(), 12);
}

#[test]
fn unique_state_budget_stops_expansion_without_failing() {
    let summary = check_raft_election_safety(
        vec![
            config(1, &[2, 3], 2),
            config(2, &[1, 3], 2),
            config(3, &[1, 2], 2),
        ],
        Bounds::new(6).with_max_unique_states(1),
    )
    .expect("bounded election safety check should stop at the unique-state cap");

    assert_eq!(summary.unique_states(), 1);
    assert!(summary.explored_states() > summary.unique_states());
    assert!(summary.explored_actions() > 0);
    assert_eq!(summary.max_depth(), 6);
}

#[test]
fn bounds_expose_dedup_budget_controls() {
    let bounds = Bounds::new(7)
        .with_max_unique_states(42)
        .with_max_wall_clock(Duration::from_secs(5));

    assert_eq!(bounds.max_depth(), 7);
    assert_eq!(bounds.max_unique_states(), Some(42));
    assert_eq!(bounds.max_wall_clock(), Some(Duration::from_secs(5)));
}

#[test]
fn bounded_raft_restart_and_snapshot_safety_passes() {
    let summary = check_raft_restart_and_snapshot_safety(Bounds::new(8).with_max_restarts(1))
        .expect("bounded restart and snapshot safety check should pass");

    assert!(summary.explored_states() > 1);
    assert!(summary.unique_states() > 1);
    assert!(summary.unique_states() <= summary.explored_states());
    assert!(summary.explored_actions() > 1);
    assert_eq!(summary.max_depth(), 12);
}

#[test]
fn bounded_raft_read_index_safety_passes_for_three_node_cluster() {
    let summary = check_raft_read_index_safety(
        vec![
            config(1, &[2, 3], 2),
            config(2, &[1, 3], 2),
            config(3, &[1, 2], 2),
        ],
        Bounds::new(5)
            .with_max_proposals(1)
            .with_max_read_indexes(1),
    )
    .expect("bounded read-index exploration finds no violation");
    assert!(summary.explored_states() > 1_000);
    assert!(summary.unique_states() > 100);
    assert!(summary.unique_states() <= summary.explored_states());
}

#[test]
fn bounded_raft_lease_read_safety_passes_for_production_cluster() {
    let summary = check_raft_read_index_safety(
        three_node_lease_configs(),
        Bounds::new(5)
            .with_max_proposals(1)
            .with_max_read_indexes(2),
    )
    .expect("bounded lease-read exploration finds no violation");
    assert!(summary.explored_states() > 100);
    assert!(summary.unique_states() > 100);
    assert!(summary.unique_states() <= summary.explored_states());
}
