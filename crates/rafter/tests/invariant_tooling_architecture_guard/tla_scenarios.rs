//! Scenarios: TLA+ producer and verifier policies remain independently owned.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use super::architecture_support::{
    declared_macros, display_path, function_call_counts, invariant_rust_files,
    macro_second_identifiers, public_associated_methods, public_free_functions, read,
    starts_with_module_contract, string_array_constant, workspace_root,
};

#[test]
fn tla_mutation_acceptance_has_independent_policies() {
    let root = workspace_root();
    let neutral_path = "crates/rafter-invariants/src/evidence/format/tla/mutation.rs";
    let producer_path = "crates/rafter-invariants/src/producer/tla/execution/probes.rs";
    let verifier_path = "crates/rafter-invariants/src/verification/tla/detector.rs";
    assert_neutral_tla_mutation_api(&root, neutral_path);
    assert_tla_mutation_inventory(&root, producer_path, verifier_path);
    assert_tla_mutation_policy_call_edges(&root, producer_path, verifier_path);
    assert_tla_mutation_guard_fixtures();
}

fn assert_neutral_tla_mutation_api(root: &Path, neutral_path: &str) {
    let neutral = read(&root.join(neutral_path));
    assert!(starts_with_module_contract(&neutral));
    assert!(neutral.contains("pub(crate) fn parse_mutation_transcript"));
    assert!(neutral.contains("fn parse_canonical_decimal"));
    assert!(!neutral.contains("fn mutation_suite_qualified("));
    assert_eq!(
        public_free_functions(&neutral),
        BTreeSet::from(["parse_mutation_transcript".to_owned()])
    );
    assert!(public_associated_methods(&neutral).is_empty());
    assert!(declared_macros(&neutral).is_empty());

    let neutral_root_path = "crates/rafter-invariants/src/evidence/format/tla.rs";
    let neutral_root = read(&root.join(neutral_root_path));
    assert_eq!(
        public_free_functions(&neutral_root),
        BTreeSet::from([
            "detector_config_kind".to_owned(),
            "detector_invariant".to_owned(),
            "detector_label".to_owned(),
            "detector_log_kind".to_owned(),
            "detector_observation".to_owned(),
            // Proof-obligation vocabulary. Producer and verifier share these
            // and only these: the artifact and label identities, the
            // acceptance predicate, and the observation frame. Both sides
            // parse their own bytes and reach their own verdict.
            "obligation_config_kind".to_owned(),
            "obligation_discharged".to_owned(),
            "obligation_label".to_owned(),
            "obligation_log_kind".to_owned(),
            "obligation_observation".to_owned(),
            "obligation_observations".to_owned(),
            "parse".to_owned(),
            "parse_complete_prefix".to_owned(),
            "parse_latest_progress".to_owned(),
            "probe_slug".to_owned(),
            "render_detector_config".to_owned(),
        ]),
        "{neutral_root_path} public function inventory changed"
    );
    assert!(public_associated_methods(&neutral_root).is_empty());
    assert!(declared_macros(&neutral_root).is_empty());
}

fn assert_tla_mutation_inventory(root: &Path, producer_path: &str, verifier_path: &str) {
    let reviewed = reviewed_tla_mutation_tests();
    let reviewed_set = reviewed.into_iter().collect::<BTreeSet<_>>();
    assert_eq!(reviewed_set.len(), reviewed.len());
    for path in [producer_path, verifier_path] {
        let policy_inventory =
            string_array_constant(&read(&root.join(path)), "REQUIRED_MUTATION_TESTS");
        assert_eq!(policy_inventory, reviewed);
        assert_eq!(
            policy_inventory.iter().collect::<BTreeSet<_>>().len(),
            reviewed.len()
        );
    }
    let selector_source =
        read(&root.join("crates/rafter-invariants/src/producer/tla_mutation_tests.rs"));
    let discovered = macro_second_identifiers(&selector_source, "ignored_tlc_mutation_test");
    assert_eq!(discovered.len(), reviewed.len());
    assert_eq!(
        discovered.into_iter().collect::<BTreeSet<_>>(),
        reviewed_set.into_iter().map(str::to_owned).collect()
    );
}

fn assert_tla_mutation_policy_call_edges(root: &Path, producer_path: &str, verifier_path: &str) {
    let mut policy_owners = BTreeSet::new();
    for path in invariant_rust_files(root) {
        let source = read(&path);
        if source.contains("fn mutation_suite_qualified(") {
            policy_owners.insert(display_path(root, &path));
        }
    }
    assert_eq!(
        policy_owners,
        BTreeSet::from([producer_path.to_owned(), verifier_path.to_owned()])
    );
    for (path, parser_call) in [
        (producer_path, "tla_output::parse_mutation_transcript"),
        (verifier_path, "parse_mutation_transcript"),
    ] {
        let source = read(&root.join(path));
        assert!(source.contains("parse_mutation_transcript"));
        assert!(source.contains("REQUIRED_MUTATION_TESTS"));
        assert_eq!(
            function_call_counts(&source, "mutation_suite_qualified"),
            BTreeMap::from([
                (parser_call.to_owned(), 1),
                ("Some".to_owned(), 1),
                ("macro::format".to_owned(), 1),
                ("method::all".to_owned(), 1),
                ("method::any".to_owned(), 1),
                ("method::contains".to_owned(), 1),
                ("method::count".to_owned(), 1),
                ("method::filter".to_owned(), 1),
                ("method::iter".to_owned(), 3),
                ("method::len".to_owned(), 1),
            ]),
            "{path} must reduce the decoded transcript directly"
        );
    }
}

fn assert_tla_mutation_guard_fixtures() {
    let delegated_fixture = r"
        fn mutation_suite_qualified(stdout: &str) -> bool {
            shared_acceptance(parse_mutation_transcript(stdout))
        }
    ";
    assert_ne!(
        function_call_counts(delegated_fixture, "mutation_suite_qualified"),
        BTreeMap::from([("parse_mutation_transcript".to_owned(), 1)]),
        "the call-edge ratchet must reject a thin shared-policy delegate"
    );
    for delegated_fixture in [
        r"
            fn mutation_suite_qualified(stdout: &str) -> bool {
                parse_mutation_transcript(stdout).shared_acceptance()
            }
        ",
        r"
            fn mutation_suite_qualified(stdout: &str) -> bool {
                shared_acceptance!(stdout)
            }
        ",
    ] {
        assert_ne!(
            function_call_counts(delegated_fixture, "mutation_suite_qualified"),
            BTreeMap::from([("parse_mutation_transcript".to_owned(), 1)]),
            "the call-edge ratchet must reject method and macro delegation"
        );
    }
    let method_fixture = r"
        struct MutationTranscript;
        impl MutationTranscript {
            pub(crate) fn qualified(&self) -> bool { true }
        }
    ";
    assert_eq!(
        public_associated_methods(method_fixture),
        BTreeSet::from(["MutationTranscript::qualified".to_owned()])
    );
    assert_eq!(
        declared_macros("macro_rules! qualified { () => { true } }"),
        BTreeSet::from(["qualified".to_owned()])
    );
}

#[test]
fn tla_checkpoint_contract_has_independent_derivation_policies() {
    let root = workspace_root();
    let neutral_path = "crates/rafter-invariants/src/evidence/format/tla/checkpoint.rs";
    let producer_path = "crates/rafter-invariants/src/producer/tla/checkpoint/model.rs";
    let verifier_path = "crates/rafter-invariants/src/verification/tla/checkpoint.rs";
    let neutral = read(&root.join(neutral_path));

    assert!(starts_with_module_contract(&neutral));
    assert!(neutral.contains("struct CheckpointContract"));
    assert!(neutral.contains("fn sha256(&self)"));
    assert!(!neutral.contains("fn expected_contract("));
    assert!(public_free_functions(&neutral).is_empty());
    assert_eq!(
        public_associated_methods(&neutral),
        BTreeSet::from(["CheckpointContract::sha256".to_owned()])
    );
    assert!(declared_macros(&neutral).is_empty());

    let reviewed_inputs = [
        "tla-tool",
        "tla-spec",
        "tla-trace-spec",
        "tla-detector-spec",
        "tla-runner",
        "tla-tool-asset-id",
        "tla-tool-checksums",
        "tla-config",
        "tla-trace-config",
        "tla-detector-config",
    ];
    for path in [producer_path, verifier_path] {
        let source = read(&root.join(path));
        assert!(source.contains("fn expected_contract("));
        assert_eq!(
            string_array_constant(&source, "INPUT_KINDS"),
            reviewed_inputs,
            "{path} must pin the exact reviewed checkpoint inputs independently"
        );
        assert!(source.contains("Sha256::digest"));
    }

    assert_checkpoint_derivation_calls(&root, producer_path, verifier_path);
}

fn assert_checkpoint_derivation_calls(root: &Path, producer_path: &str, verifier_path: &str) {
    assert_eq!(
        function_call_counts(&read(&root.join(producer_path)), "expected_contract"),
        BTreeMap::from([
            ("BTreeMap::new".to_owned(), 1),
            ("Err".to_owned(), 1),
            ("Ok".to_owned(), 1),
            ("macro::format".to_owned(), 2),
            ("method::as_slice".to_owned(), 1),
            ("method::clone".to_owned(), 2),
            ("method::collect".to_owned(), 1),
            ("method::filter".to_owned(), 1),
            ("method::get".to_owned(), 1),
            ("method::insert".to_owned(), 1),
            ("method::into".to_owned(), 1),
            ("method::iter".to_owned(), 1),
            ("method::ok_or".to_owned(), 1),
            ("method::to_owned".to_owned(), 2),
        ]),
        "producer checkpoint derivation gained an unreviewed delegate"
    );
    assert_eq!(
        function_call_counts(&read(&root.join(verifier_path)), "expected_contract"),
        BTreeMap::from([
            ("AggregateError::new".to_owned(), 4),
            ("BTreeMap::new".to_owned(), 1),
            ("Err".to_owned(), 1),
            ("Ok".to_owned(), 1),
            ("macro::format".to_owned(), 4),
            ("method::clone".to_owned(), 2),
            ("method::filter".to_owned(), 1),
            ("method::get".to_owned(), 1),
            ("method::insert".to_owned(), 1),
            ("method::is_some".to_owned(), 1),
            ("method::iter".to_owned(), 1),
            ("method::map_err".to_owned(), 1),
            ("method::next".to_owned(), 2),
            ("method::ok_or_else".to_owned(), 2),
            ("method::to_owned".to_owned(), 3),
            ("serde_json::to_vec".to_owned(), 1),
        ]),
        "verifier checkpoint derivation gained an unreviewed delegate"
    );
}

fn reviewed_tla_mutation_tests() -> [&'static str; 34] {
    [
        "application_epoch_loss_replays_identically_without_erasing_history",
        "applied_membership_quorum_mutation_breaks_joint_regression",
        "closed_term_election_history_is_retired_after_every_node_advances",
        "closed_term_prefix_history_retires_without_erasing_conflicts",
        "corrupted_snapshot_install_breaks_lifecycle_identity",
        "corrupted_snapshot_restored_state_breaks_empty_epoch_lifecycle",
        "delayed_append_uses_frozen_sender_authority_after_self_removal",
        "every_required_detector_probe_reaches_its_named_counterexample",
        "follower_recomputation_breaks_delayed_heartbeat_regression",
        "leader_completeness_uses_commit_authority_term",
        "missing_application_epoch_recorder_cannot_qualify_state_machine_safety",
        "missing_application_recorder_cannot_qualify_state_machine_safety",
        "missing_commit_ledger_recorder_cannot_qualify_history_predicates",
        "missing_commit_witness_recorder_cannot_qualify_quorum_predicate",
        "missing_effective_recomputation_breaks_overwrite_regression",
        "missing_election_recorder_cannot_qualify_election_safety",
        "missing_higher_term_recorder_cannot_qualify_fencing",
        "missing_log_prefix_recorder_cannot_qualify_log_or_snapshot_paths",
        "missing_read_grant_recorder_cannot_qualify_read_barrier_predicate",
        "missing_self_removal_step_down_breaks_commit_regression",
        "missing_stale_authority_recorder_cannot_qualify_fencing",
        "non_violating_fixture_cannot_qualify",
        "recorder_only_fixtures_qualify_before_mutation",
        "removed_candidate_vote_requires_membership_and_freshness_guards",
        "sanitized_application_result_cannot_qualify_detector_fixture",
        "self_removing_leader_commits_final_configuration_and_steps_down",
        "shorter_authoritative_log_repairs_an_uncommitted_suffix",
        "snapshot_compaction_pending_tracks_create_and_compact_transitions",
        "snapshot_lifecycle_preserves_logical_identity_through_restart",
        "stale_messages_are_retired_when_the_target_term_advances",
        "true_mutation_of_real_predicate_cannot_qualify",
        "unfrozen_effective_membership_breaks_commit_witness_regression",
        "unvalidated_commit_certificate_cannot_qualify_quorum_predicate",
        "unvalidated_read_grant_cannot_qualify_read_barrier_predicate",
    ]
}
