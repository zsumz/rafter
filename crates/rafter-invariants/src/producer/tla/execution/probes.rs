//! Trace, detector, and mutation-suite qualification policy.

use std::{collections::BTreeMap, error::Error, ffi::OsString, fs, path::Path, time::Duration};

use crate::execution::filesystem::HeldDirectory;

use super::{
    super::{artifact, contract::required_configuration, process, tla_output},
    budget::{mutation_suite_timeout, probe_timeout, ExecutionBudget},
    command::{run_tlc, TlcRequest, TlcState},
    model::{DetectorProbes, DetectorRun, TlcRun},
};
use tla_output::{
    detector_config_kind, detector_invariant, detector_label, detector_log_kind,
    detector_observation, probe_slug, render_detector_config, DetectorProbe, DETECTOR_PROBES,
    MEMBERSHIP_TRACE_MIN_DEPTH, MEMBERSHIP_TRACE_MIN_DISTINCT_STATES, MUTATION_SUITE_ARTIFACT_KIND,
    MUTATION_SUITE_LABEL, REGISTERED_PREDICATES,
};

const DETECTOR_CONFIG: &str = "RafterInvariantDetectorNegative.cfg";
pub(super) const REQUIRED_MUTATION_TESTS: [&str; 34] = [
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

pub(super) fn trace_succeeded(trace: &TlcRun) -> bool {
    trace.output.status.success()
        && !trace.output.timed_out
        && tla_output::parse(&trace.output.stdout)
            .ok()
            .is_some_and(|summary| {
                summary.completed_without_error
                    && summary.process_finished
                    && summary.distinct_states >= MEMBERSHIP_TRACE_MIN_DISTINCT_STATES
                    && summary.states_left == 0
                    && summary.search_depth >= MEMBERSHIP_TRACE_MIN_DEPTH
            })
}

pub(super) fn run_trace_probe(
    profile: &str,
    source_ref: &str,
    configuration: &BTreeMap<String, String>,
    output_dir: &Path,
    timeout: Duration,
) -> Result<TlcRun, Box<dyn Error>> {
    run_tlc(TlcRequest {
        profile,
        source_ref,
        config: "RaftMembershipTraceSample.cfg",
        module: "RaftMembershipTraceSample.tla",
        workers: "1",
        seed: required_configuration(configuration, "seed")?,
        timeout,
        output_dir,
        label: "trace-sample",
        artifact_kind: "tla-trace-log",
        max_heap: None,
        fp_mem: None,
        state: TlcState::Ephemeral,
    })
}

pub(super) fn run_detector_probes(
    profile: &str,
    source_ref: &str,
    configuration: &BTreeMap<String, String>,
    output_dir: &Path,
    budget: &ExecutionBudget,
) -> Result<DetectorProbes, Box<dyn Error>> {
    let mut aggregate = DetectorProbes::default();
    for probe in DETECTOR_PROBES {
        let Some(timeout) = budget.phase_timeout(probe_timeout(profile)) else {
            aggregate.succeeded = false;
            aggregate.qualifications = empty_detector_qualifications();
            break;
        };
        let detector = run_detector_probe(
            profile,
            source_ref,
            configuration,
            output_dir,
            probe,
            timeout,
        )?;
        aggregate.peak_rss_kib = aggregate.peak_rss_kib.max(detector.run.output.peak_rss_kib);
        aggregate.duration_ms = aggregate
            .duration_ms
            .saturating_add(process::duration_ms(detector.run.output.duration));
        let expected_invariant = detector_invariant(probe).ok_or("unregistered detector probe")?;
        let summary = tla_output::parse(&detector.run.output.stdout).ok();
        let qualified = detector_qualified(
            detector.run.output.status.code(),
            detector.run.output.timed_out,
            summary.as_ref(),
            &expected_invariant,
        );
        aggregate.succeeded &= qualified;
        let observation =
            detector_observation(probe.predicate).ok_or("unregistered detector predicate")?;
        let predicate_qualified = aggregate.qualifications.entry(observation).or_insert(1);
        *predicate_qualified &= u64::from(qualified);
        aggregate.artifacts.push(detector.config_artifact);
        aggregate.artifacts.push(detector.run.artifact);
    }
    if aggregate.succeeded {
        let Some(timeout) = budget.phase_timeout(mutation_suite_timeout(profile)) else {
            aggregate.succeeded = false;
            return Ok(aggregate);
        };
        let mutation = run_mutation_suite(profile, source_ref, output_dir, timeout)?;
        aggregate.peak_rss_kib = aggregate.peak_rss_kib.max(mutation.output.peak_rss_kib);
        aggregate.duration_ms = aggregate
            .duration_ms
            .saturating_add(process::duration_ms(mutation.output.duration));
        aggregate.succeeded &= mutation_suite_qualified(
            mutation.output.status.code(),
            mutation.output.timed_out,
            &String::from_utf8_lossy(&mutation.output.stdout),
        );
        aggregate.artifacts.push(mutation.artifact);
    }
    Ok(aggregate)
}

pub(super) fn run_mutation_suite(
    profile: &str,
    source_ref: &str,
    output_dir: &Path,
    timeout: Duration,
) -> Result<TlcRun, Box<dyn Error>> {
    let arguments = [
        "test",
        "--locked",
        "-p",
        "rafter-invariants",
        "producer::tla_exec::mutation_tests",
        "--lib",
        "--",
        "--ignored",
        "--test-threads=1",
    ]
    .map(OsString::from);
    let output = process::timed_for_with_cap(
        process::ProcessKind::TlaExecution,
        "cargo",
        &arguments,
        &process::base_environment(),
        Path::new("."),
        Some(timeout),
    )?;
    let source_prefix = source_ref.get(..12).unwrap_or(source_ref);
    let artifact = artifact::write(
        output_dir,
        Path::new(&format!(
            "{profile}-tla/{source_prefix}/detector-mutation-suite.json"
        )),
        MUTATION_SUITE_ARTIFACT_KIND,
        &process::tla_json_log(MUTATION_SUITE_LABEL, &output)?,
    )?;
    Ok(TlcRun { output, artifact })
}

pub(super) fn run_detector_probe(
    profile: &str,
    source_ref: &str,
    configuration: &BTreeMap<String, String>,
    output_dir: &Path,
    probe: DetectorProbe,
    timeout: Duration,
) -> Result<DetectorRun, Box<dyn Error>> {
    let source_prefix = source_ref.get(..12).unwrap_or(source_ref);
    let template = fs::read_to_string(Path::new("specs/tla/raft").join(DETECTOR_CONFIG))?;
    let config_source = render_detector_config(&template, probe)?;
    let config_kind = detector_config_kind(probe).ok_or("unregistered detector probe")?;
    let slug = probe_slug(probe);
    let config_artifact = artifact::write(
        output_dir,
        Path::new(&format!(
            "{profile}-tla/{source_prefix}/detectors/{slug}.cfg"
        )),
        &config_kind,
        config_source.as_bytes(),
    )?;
    let config_guard = HeldDirectory::workspace()?.hold_file(Path::new(&config_artifact.path))?;
    config_guard.verify_path_binding()?;
    let config = config_guard.external_path().to_string_lossy().into_owned();
    let label = detector_label(probe).ok_or("unregistered detector probe")?;
    let artifact_kind = detector_log_kind(probe).ok_or("unregistered detector probe")?;
    let run = run_tlc(TlcRequest {
        profile,
        source_ref,
        config: &config,
        module: "RafterInvariantDetectorNegative.tla",
        workers: "1",
        seed: required_configuration(configuration, "seed")?,
        timeout,
        output_dir,
        label: &label,
        artifact_kind: &artifact_kind,
        max_heap: None,
        fp_mem: None,
        state: TlcState::Ephemeral,
    })?;
    Ok(DetectorRun {
        run,
        config_artifact,
    })
}

pub(super) fn empty_detector_qualifications() -> BTreeMap<String, u64> {
    REGISTERED_PREDICATES
        .into_iter()
        .filter_map(|predicate| detector_observation(predicate).map(|observation| (observation, 0)))
        .collect()
}

pub(in crate::producer) fn detector_qualified(
    exit_code: Option<i32>,
    timed_out: bool,
    summary: Option<&tla_output::TlcSummary>,
    expected_invariant: &str,
) -> bool {
    exit_code == Some(12)
        && !timed_out
        && summary.is_some_and(|summary| {
            !summary.completed_without_error
                && summary.process_finished
                && summary.violated_invariant.as_deref() == Some(expected_invariant)
                && summary.distinct_states >= 2
                && summary.states_left == 0
                && summary.search_depth >= 2
        })
}

pub(super) fn mutation_suite_qualified(
    exit_code: Option<i32>,
    timed_out: bool,
    stdout: &str,
) -> bool {
    if exit_code != Some(0) || timed_out {
        return false;
    }
    let transcript = tla_output::parse_mutation_transcript(stdout);
    let expected_count = REQUIRED_MUTATION_TESTS.len() as u64;
    transcript.running_counts.contains(&expected_count)
        && transcript.summaries.iter().any(|summary| {
            summary
                == &tla_output::MutationSummary {
                    passed: expected_count,
                    failed: 0,
                    ignored: 0,
                    measured: 0,
                }
        })
        && REQUIRED_MUTATION_TESTS.iter().all(|name| {
            let expected = format!("producer::tla_exec::mutation_tests::{name}");
            transcript
                .passed_tests
                .iter()
                .filter(|test| *test == &expected)
                .count()
                == 1
        })
}
