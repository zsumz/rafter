//! Qualification of TLA+ negative detectors and mutation-suite evidence.

use std::{collections::BTreeMap, path::Path};

use crate::{
    evidence::{
        format::{
            process::ProcessLog,
            tla::{
                detector_config_kind, detector_invariant, detector_label, detector_log_kind,
                detector_observation, parse_mutation_transcript, probe_slug,
                render_detector_config, MutationSummary, TlcSummary, DETECTOR_PROBES,
                MUTATION_SUITE_ARTIFACT_KIND, MUTATION_SUITE_LABEL,
            },
        },
        CheckReceipt, ResultBundle,
    },
    provenance::invocation::environment_matches_digest,
    verification::{AggregateError, AuthenticatedArtifacts},
};

use super::{
    artifact::{has_kind, read_kind},
    invocation::{read_bound_process_log, read_process_log},
};

pub(crate) const REQUIRED_MUTATION_TESTS: [&str; 34] = [
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
];

pub(super) fn verify(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    root: &Path,
    producer_repository: &Path,
    template: &str,
    authenticated: &AuthenticatedArtifacts,
) -> Result<(BTreeMap<String, u64>, bool), AggregateError> {
    let mut observations = BTreeMap::new();
    let mut all_passed = true;
    for probe in DETECTOR_PROBES {
        let identity = probe_slug(probe);
        let config_kind = detector_config_kind(probe).ok_or_else(|| {
            AggregateError::new(format!("unregistered TLA detector probe {identity}"))
        })?;
        let log_kind = detector_log_kind(probe).ok_or_else(|| {
            AggregateError::new(format!("unregistered TLA detector probe {identity}"))
        })?;
        let observation = detector_observation(probe.predicate).ok_or_else(|| {
            AggregateError::new(format!(
                "unregistered TLA detector predicate {}",
                probe.predicate
            ))
        })?;
        let has_config = has_kind(check, &config_kind)?;
        let has_log = has_kind(check, &log_kind)?;
        if has_config != has_log {
            return Err(AggregateError::new(format!(
                "TLA detector artifacts for {identity} are incomplete"
            )));
        }
        if !has_config {
            all_passed = false;
            observations.insert(observation, 0);
            continue;
        }
        let realized_config = read_kind(check, &config_kind, authenticated)?;
        let expected_config =
            render_detector_config(template, probe).map_err(AggregateError::new)?;
        if realized_config != expected_config {
            return Err(AggregateError::new(format!(
                "TLA detector config does not bind probe {identity} exactly"
            )));
        }
        let label = detector_label(probe).ok_or_else(|| {
            AggregateError::new(format!("unregistered TLA detector probe {identity}"))
        })?;
        let detector = read_process_log(
            bundle,
            check,
            &log_kind,
            &label,
            root,
            producer_repository,
            authenticated,
        )?;
        let summary = crate::evidence::format::tla::parse(detector.stdout.as_bytes()).ok();
        let expected = detector_invariant(probe).ok_or_else(|| {
            AggregateError::new(format!("unregistered TLA detector probe {identity}"))
        })?;
        let qualified = summary
            .as_ref()
            .is_some_and(|summary| successful_detector(&detector, summary, &expected));
        all_passed &= qualified;
        let predicate_qualified = observations.entry(observation).or_insert(1);
        *predicate_qualified &= u64::from(qualified);
    }
    if has_kind(check, MUTATION_SUITE_ARTIFACT_KIND)? {
        let mutation = read_bound_process_log(
            check,
            MUTATION_SUITE_ARTIFACT_KIND,
            MUTATION_SUITE_LABEL,
            authenticated,
        )?;
        verify_mutation_invocation(bundle, &mutation, producer_repository)?;
        all_passed &=
            mutation_suite_qualified(mutation.exit_code, mutation.timed_out, &mutation.stdout);
    } else {
        all_passed = false;
    }
    Ok((observations, all_passed))
}

fn verify_mutation_invocation(
    bundle: &ResultBundle,
    log: &ProcessLog,
    producer_repository: &Path,
) -> Result<(), AggregateError> {
    let expected_arguments = [
        "test",
        "--locked",
        "-p",
        "rafter-invariants",
        "producer::tla_exec::mutation_tests",
        "--lib",
        "--",
        "--ignored",
        "--test-threads=1",
    ];
    let current_dir = producer_repository.to_string_lossy();
    if log.invocation.program != "cargo"
        || log.invocation.program_sha256 != bundle.execution.source.cargo_sha256
        || log.invocation.arguments != expected_arguments
        || log.invocation.current_dir != current_dir
        || log.invocation.environment != bundle.execution.invocation.environment
        || log.invocation.environment_sha256 != bundle.execution.invocation.environment_sha256
        || !environment_matches_digest(
            &log.invocation.environment,
            &log.invocation.environment_sha256,
        )
    {
        return Err(AggregateError::new(
            "TLA mutation suite invocation is not source-bound to the exact Cargo test command"
                .to_owned(),
        ));
    }
    Ok(())
}

fn mutation_suite_qualified(exit_code: Option<i32>, timed_out: bool, stdout: &str) -> bool {
    if exit_code != Some(0) || timed_out {
        return false;
    }
    let transcript = parse_mutation_transcript(stdout);
    let required_count = REQUIRED_MUTATION_TESTS.len() as u64;
    let reports_required_count = transcript.running_counts.contains(&required_count);
    let reports_clean_result = transcript.summaries.iter().any(|summary| {
        summary
            == &MutationSummary {
                passed: required_count,
                failed: 0,
                ignored: 0,
                measured: 0,
            }
    });
    let reports_each_required_test_once = REQUIRED_MUTATION_TESTS.iter().all(|name| {
        let required = format!("producer::tla_exec::mutation_tests::{name}");
        transcript
            .passed_tests
            .iter()
            .filter(|observed| *observed == &required)
            .count()
            == 1
    });
    reports_required_count && reports_clean_result && reports_each_required_test_once
}

pub(crate) fn successful_detector(
    log: &ProcessLog,
    summary: &TlcSummary,
    expected_invariant: &str,
) -> bool {
    log.exit_code == Some(12)
        && !log.timed_out
        && !summary.completed_without_error
        && summary.process_finished
        && summary.violated_invariant.as_deref() == Some(expected_invariant)
        && summary.distinct_states >= 2
        && summary.states_left == 0
        && summary.search_depth >= 2
}

#[cfg(test)]
#[path = "tests/detector.rs"]
mod tests;
