//! Requested and effective feature-policy behavior for node configuration.

use crate::{NodeConfig, NodeId};

fn config(election_timeout_ticks: u64) -> NodeConfig {
    NodeConfig::new(
        NodeId(1),
        vec![NodeId(2), NodeId(3)],
        election_timeout_ticks,
    )
    .expect("test node configuration is valid")
}

#[test]
fn production_features_are_requested_by_default_but_leases_are_opt_in() {
    let config = config(3);

    assert!(config.pre_vote());
    assert!(config.check_quorum());
    assert!(!config.lease_reads());
}

#[test]
fn lease_request_survives_temporarily_disabled_dependencies() {
    let without_pre_vote = config(3).with_lease_reads(true).with_pre_vote(false);
    assert!(!without_pre_vote.lease_reads());

    let restored_pre_vote = without_pre_vote.with_pre_vote(true);
    assert!(restored_pre_vote.lease_reads());

    let without_check_quorum = restored_pre_vote.with_check_quorum(false);
    assert!(!without_check_quorum.lease_reads());

    let restored_check_quorum = without_check_quorum.with_check_quorum(true);
    assert!(restored_check_quorum.lease_reads());
}

#[test]
fn one_tick_timeout_keeps_requested_check_quorum_and_leases_ineffective() {
    let config = config(1).with_check_quorum(true).with_lease_reads(true);

    assert!(config.pre_vote());
    assert!(!config.check_quorum());
    assert!(!config.lease_reads());
}

#[test]
fn requested_posture_remains_part_of_configuration_value_semantics() {
    let check_quorum_requested = config(1).with_check_quorum(true);
    let check_quorum_disabled = config(1).with_check_quorum(false);

    assert!(!check_quorum_requested.check_quorum());
    assert!(!check_quorum_disabled.check_quorum());
    assert_ne!(check_quorum_requested, check_quorum_disabled);

    let long_heartbeat = config(3).with_heartbeat_interval_ticks(99);
    let clamped_heartbeat = config(3).with_heartbeat_interval_ticks(2);

    assert_eq!(long_heartbeat.heartbeat_interval_ticks(), 2);
    assert_eq!(clamped_heartbeat.heartbeat_interval_ticks(), 2);
    assert_ne!(long_heartbeat, clamped_heartbeat);
}

#[test]
fn heartbeat_accessor_exposes_effective_interval_without_erasing_request() {
    let requested = config(3).with_heartbeat_interval_ticks(99);
    assert_eq!(requested.heartbeat_interval_ticks(), 2);

    let unclamped = requested.clone().with_check_quorum(false);
    assert_eq!(unclamped.heartbeat_interval_ticks(), 99);

    let reclamped = unclamped.with_check_quorum(true);
    assert_eq!(reclamped.heartbeat_interval_ticks(), 2);

    let zero = config(3).with_heartbeat_interval_ticks(0);
    assert_eq!(zero.heartbeat_interval_ticks(), 1);
}
