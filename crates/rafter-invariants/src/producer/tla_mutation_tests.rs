#[path = "tla_mutation_tests/detector_contracts.rs"]
mod detector_contracts;
#[path = "tla_mutation_tests/lifecycle.rs"]
mod lifecycle;
#[path = "tla_mutation_tests/protocol_regressions.rs"]
mod protocol_regressions;
#[path = "tla_mutation_tests/recorder_regressions.rs"]
mod recorder_regressions;
#[path = "tla_mutation_tests/support.rs"]
mod support;

macro_rules! ignored_tlc_mutation_test {
    ($family:ident, $name:ident) => {
        #[test]
        #[ignore = "requires the pinned TLC tool and Java"]
        fn $name() {
            $family::$name();
        }
    };
}

ignored_tlc_mutation_test!(
    detector_contracts,
    recorder_only_fixtures_qualify_before_mutation
);
ignored_tlc_mutation_test!(
    detector_contracts,
    every_required_detector_probe_reaches_its_named_counterexample
);
ignored_tlc_mutation_test!(
    lifecycle,
    snapshot_lifecycle_preserves_logical_identity_through_restart
);
ignored_tlc_mutation_test!(
    lifecycle,
    stale_messages_are_retired_when_the_target_term_advances
);
ignored_tlc_mutation_test!(
    lifecycle,
    closed_term_election_history_is_retired_after_every_node_advances
);
ignored_tlc_mutation_test!(
    lifecycle,
    closed_term_prefix_history_retires_without_erasing_conflicts
);
ignored_tlc_mutation_test!(
    protocol_regressions,
    shorter_authoritative_log_repairs_an_uncommitted_suffix
);
ignored_tlc_mutation_test!(
    protocol_regressions,
    delayed_append_uses_frozen_sender_authority_after_self_removal
);
ignored_tlc_mutation_test!(
    protocol_regressions,
    removed_candidate_vote_requires_membership_and_freshness_guards
);
ignored_tlc_mutation_test!(
    protocol_regressions,
    leader_completeness_uses_commit_authority_term
);
ignored_tlc_mutation_test!(
    lifecycle,
    snapshot_compaction_pending_tracks_create_and_compact_transitions
);
ignored_tlc_mutation_test!(
    lifecycle,
    application_epoch_loss_replays_identically_without_erasing_history
);
ignored_tlc_mutation_test!(
    lifecycle,
    missing_application_epoch_recorder_cannot_qualify_state_machine_safety
);
ignored_tlc_mutation_test!(
    protocol_regressions,
    self_removing_leader_commits_final_configuration_and_steps_down
);
ignored_tlc_mutation_test!(
    protocol_regressions,
    missing_self_removal_step_down_breaks_commit_regression
);
ignored_tlc_mutation_test!(
    protocol_regressions,
    unfrozen_effective_membership_breaks_commit_witness_regression
);
ignored_tlc_mutation_test!(
    lifecycle,
    corrupted_snapshot_install_breaks_lifecycle_identity
);
ignored_tlc_mutation_test!(
    lifecycle,
    corrupted_snapshot_restored_state_breaks_empty_epoch_lifecycle
);
ignored_tlc_mutation_test!(
    detector_contracts,
    true_mutation_of_real_predicate_cannot_qualify
);
ignored_tlc_mutation_test!(detector_contracts, non_violating_fixture_cannot_qualify);
ignored_tlc_mutation_test!(
    protocol_regressions,
    applied_membership_quorum_mutation_breaks_joint_regression
);
ignored_tlc_mutation_test!(
    protocol_regressions,
    missing_effective_recomputation_breaks_overwrite_regression
);
ignored_tlc_mutation_test!(
    protocol_regressions,
    follower_recomputation_breaks_delayed_heartbeat_regression
);
ignored_tlc_mutation_test!(
    recorder_regressions,
    missing_higher_term_recorder_cannot_qualify_fencing
);
ignored_tlc_mutation_test!(
    recorder_regressions,
    missing_stale_authority_recorder_cannot_qualify_fencing
);
ignored_tlc_mutation_test!(
    recorder_regressions,
    missing_election_recorder_cannot_qualify_election_safety
);
ignored_tlc_mutation_test!(
    recorder_regressions,
    missing_application_recorder_cannot_qualify_state_machine_safety
);
ignored_tlc_mutation_test!(
    recorder_regressions,
    sanitized_application_result_cannot_qualify_detector_fixture
);
ignored_tlc_mutation_test!(
    recorder_regressions,
    missing_log_prefix_recorder_cannot_qualify_log_or_snapshot_paths
);
ignored_tlc_mutation_test!(
    recorder_regressions,
    missing_commit_ledger_recorder_cannot_qualify_history_predicates
);
ignored_tlc_mutation_test!(
    recorder_regressions,
    missing_commit_witness_recorder_cannot_qualify_quorum_predicate
);
ignored_tlc_mutation_test!(
    recorder_regressions,
    unvalidated_commit_certificate_cannot_qualify_quorum_predicate
);
ignored_tlc_mutation_test!(
    recorder_regressions,
    missing_read_grant_recorder_cannot_qualify_read_barrier_predicate
);
ignored_tlc_mutation_test!(
    recorder_regressions,
    unvalidated_read_grant_cannot_qualify_read_barrier_predicate
);
