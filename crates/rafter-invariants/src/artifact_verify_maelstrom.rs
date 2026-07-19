//! End-to-end acceptance of Maelstrom trial evidence.

use std::{collections::BTreeSet, path::Path};

use crate::{
    aggregate::AggregateError,
    producer::maelstrom_edn::{MaelstromSummary, Validity},
    CheckCompletion, CheckReceipt, EvidenceStatus, ResultBundle,
};

use crate::artifact_verify_maelstrom_support::{
    add, add_summary, bind_lease_history, empty_observations, group_trials, parse_process,
    parse_results, requires_proxy, scan_node_logs, scenario_script, trial_floors_met, unique,
    verify_matches_file, LeaseArtifactStatus,
};

pub(super) fn verify(bundle: &ResultBundle, root: &Path) -> Result<Vec<String>, AggregateError> {
    let trials = configuration(bundle, "trials")?
        .parse::<u64>()
        .map_err(|parse_error| error(format!("parse Maelstrom trial count: {parse_error}")))?;
    let mut diagnostics = Vec::new();
    for check in &bundle.execution.checks {
        if verify_check(bundle, check, trials, root)? {
            diagnostics.push(format!(
                "{} preserved a Maelstrom counterexample alongside a harness error",
                check.check_id
            ));
        }
    }
    Ok(diagnostics)
}

fn verify_check(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    trials: u64,
    root: &Path,
) -> Result<bool, AggregateError> {
    let scenario = check
        .check_id
        .strip_prefix("maelstrom/")
        .ok_or_else(|| error(format!("invalid Maelstrom check ID {}", check.check_id)))?;
    let grouped = group_trials(check)?;
    let expected = (0..trials).collect::<BTreeSet<_>>();
    if grouped.keys().copied().collect::<BTreeSet<_>>() != expected {
        return Err(error("Maelstrom artifacts disagree with configured trials"));
    }

    let mut observations = empty_observations(trials);
    let mut summaries = Vec::new();
    let mut result_parse_successes = Vec::new();
    let mut process_successes = Vec::new();
    let mut coverage = Vec::new();
    let mut lease_statuses = Vec::new();
    for (trial, artifacts) in &grouped {
        verify_trial_inputs(bundle, scenario, artifacts, root)?;
        let summary = parse_results(unique(artifacts, "maelstrom-results")?, root).ok();
        let process = parse_process(unique(artifacts, "maelstrom-process-log")?, root)?;
        if process.label != scenario
            || !crate::receipt::script_invocation_matches_source(
                &process.invocation,
                &bundle.execution.source,
            )
        {
            return Err(error(
                "Maelstrom process log has wrong schema, label, or exact invocation",
            ));
        }
        verify_exact_invocation(
            bundle,
            scenario,
            *trial,
            artifacts,
            &process.invocation,
            root,
        )?;
        process_successes.push(process.exit_code == Some(0) && !process.timed_out);
        if let Some(summary) = &summary {
            add_summary(&mut observations, summary);
        }
        let mut marker_scan = scan_node_logs(artifacts, root)?;
        if scenario == "lease-isolation" {
            bind_lease_history(&mut marker_scan, artifacts, root)?;
        }
        for (&name, &value) in &marker_scan.values {
            add(&mut observations, name, value);
        }
        let durable = artifacts
            .iter()
            .any(|artifact| artifact.kind == "maelstrom-durable-file");
        coverage.push(summary.as_ref().is_some_and(|summary| {
            trial_floors_met(scenario, summary, &marker_scan.values, durable)
        }));
        lease_statuses.push(marker_scan.lease_status);
        result_parse_successes.push(summary.is_some());
        summaries.push(summary);
    }
    if observations != check.observations {
        return Err(error("Maelstrom observations disagree with artifacts"));
    }
    verify_statuses(
        bundle,
        check,
        &summaries,
        &result_parse_successes,
        &process_successes,
        &coverage,
        &lease_statuses,
    )
}

fn verify_exact_invocation(
    bundle: &ResultBundle,
    scenario: &str,
    trial: u64,
    artifacts: &[&crate::ArtifactRef],
    observed: &crate::InvocationReceipt,
    root: &Path,
) -> Result<(), AggregateError> {
    let source_prefix = bundle.source_ref.get(..12).unwrap_or(&bundle.source_ref);
    let suffix = Path::new("target/rafter-invariants/maelstrom")
        .join(source_prefix)
        .join(&bundle.profile)
        .join(scenario)
        .join(format!("trial-{trial}"));
    let repository = std::fs::canonicalize(root)
        .map_err(|error| self::error(format!("canonicalize Maelstrom root: {error}")))?;
    let state_dir = repository.join(suffix);
    let durable = state_dir.join("durable");
    let concurrency = if scenario == "membership" { "8" } else { "6" };
    let mut base_environment = observed.environment.clone();
    for name in [
        "RAFTER_MAELSTROM_ROOT",
        "RAFTER_MAELSTROM_SCRIPT_DIR",
        "RAFTER_MAELSTROM_TIME_LIMIT",
        "RAFTER_MAELSTROM_RATE",
        "RAFTER_MAELSTROM_CONCURRENCY",
        "RAFTER_MAELSTROM_RESTART_MODE",
        "RAFTER_MAELSTROM_LEASE_EVIDENCE",
        "RAFTER_MAELSTROM_TICK_INTERVAL_MS",
        "RAFTER_MAELSTROM_ELECTION_TIMEOUT_TICKS",
        "RAFTER_MAELSTROM_HEARTBEAT_INTERVAL_TICKS",
    ] {
        base_environment.remove(name);
    }
    let mut expected_environment = base_environment.clone();
    expected_environment.extend([
        (
            "RAFTER_MAELSTROM_ROOT".to_owned(),
            durable.to_string_lossy().into_owned(),
        ),
        (
            "RAFTER_MAELSTROM_SCRIPT_DIR".to_owned(),
            repository.join("scripts").to_string_lossy().into_owned(),
        ),
        (
            "RAFTER_MAELSTROM_TIME_LIMIT".to_owned(),
            configuration(bundle, "duration_seconds")?.to_owned(),
        ),
        (
            "RAFTER_MAELSTROM_RATE".to_owned(),
            configuration(bundle, "rate")?.to_owned(),
        ),
        (
            "RAFTER_MAELSTROM_CONCURRENCY".to_owned(),
            concurrency.to_owned(),
        ),
    ]);
    if scenario == "lease-isolation" {
        extend_lease_environment(bundle, &mut expected_environment)?;
    }
    let runner = unique(artifacts, "maelstrom-runner")?;
    let expected_program = std::fs::canonicalize(repository.join(scenario_script(scenario)?))
        .map_err(|error| self::error(format!("canonicalize Maelstrom script: {error}")))?;
    if observed.program != expected_program.to_string_lossy() {
        return Err(error(
            "Maelstrom process program path does not match its scenario",
        ));
    }
    if observed.program_sha256 != runner.sha256 {
        return Err(error(
            "Maelstrom process program digest does not match its runner artifact",
        ));
    }
    if observed.arguments != ["--test-count", "1"] {
        return Err(error(
            "Maelstrom process arguments do not match the exact invocation plan",
        ));
    }
    if observed.current_dir != state_dir.to_string_lossy() {
        return Err(error(
            "Maelstrom invocation working directory does not match its trial",
        ));
    }
    if observed.environment != expected_environment {
        return Err(error(
            "Maelstrom process environment does not match the exact invocation plan",
        ));
    }
    if !crate::provenance::invocation::environment_matches_digest(
        &base_environment,
        &bundle.execution.source.environment_sha256,
    ) {
        return Err(error(
            "Maelstrom base environment does not match source provenance",
        ));
    }
    Ok(())
}

fn extend_lease_environment(
    bundle: &ResultBundle,
    environment: &mut std::collections::BTreeMap<String, String>,
) -> Result<(), AggregateError> {
    environment.extend([
        (
            "RAFTER_MAELSTROM_RESTART_MODE".to_owned(),
            "lease-isolation".to_owned(),
        ),
        ("RAFTER_MAELSTROM_LEASE_EVIDENCE".to_owned(), "1".to_owned()),
        (
            "RAFTER_MAELSTROM_TICK_INTERVAL_MS".to_owned(),
            configuration(bundle, "lease_tick_interval_ms")?.to_owned(),
        ),
        (
            "RAFTER_MAELSTROM_ELECTION_TIMEOUT_TICKS".to_owned(),
            configuration(bundle, "lease_election_timeout_ticks")?.to_owned(),
        ),
        (
            "RAFTER_MAELSTROM_HEARTBEAT_INTERVAL_TICKS".to_owned(),
            configuration(bundle, "lease_heartbeat_interval_ticks")?.to_owned(),
        ),
    ]);
    Ok(())
}

fn verify_trial_inputs(
    bundle: &ResultBundle,
    scenario: &str,
    artifacts: &[&crate::ArtifactRef],
    root: &Path,
) -> Result<(), AggregateError> {
    let runner = unique(artifacts, "maelstrom-runner")?;
    verify_matches_file(runner, root.join(scenario_script(scenario)?), root)?;
    let binary = unique(artifacts, "maelstrom-binary")?;
    verify_matches_file(binary, root.join("target/debug/rafter-maelstrom"), root)?;
    let jar = unique(artifacts, "maelstrom-tool-jar")?;
    if jar.sha256 != configuration(bundle, "maelstrom_jar_sha256")? {
        return Err(error("Maelstrom tool jar does not match the profile pin"));
    }
    if requires_proxy(scenario) {
        let proxy = unique(artifacts, "maelstrom-proxy-binary")?;
        verify_matches_file(
            proxy,
            root.join("target/debug/rafter-maelstrom-leader-restart-proxy"),
            root,
        )?;
    }
    Ok(())
}

fn verify_statuses(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    summaries: &[Option<MaelstromSummary>],
    result_parse_successes: &[bool],
    process_successes: &[bool],
    coverage: &[bool],
    lease_statuses: &[LeaseArtifactStatus],
) -> Result<bool, AggregateError> {
    let statuses = bundle
        .results
        .iter()
        .filter(|result| result.execution_id == check.execution_id)
        .map(|result| (result.invariant_id.as_str(), result.status))
        .collect::<Vec<_>>();
    let non_linearizable = summaries
        .iter()
        .flatten()
        .any(|summary| summary.linearizability == Validity::Invalid);
    let all_valid = summaries.iter().all(|summary| {
        summary
            .as_ref()
            .is_some_and(|summary| summary.validity == Validity::Valid)
    });
    let owns_rd06 = statuses.iter().any(|(id, _)| *id == "RD-06");
    let lease_violation = lease_statuses.iter().any(|status| {
        matches!(
            status,
            LeaseArtifactStatus::Violation | LeaseArtifactStatus::ViolationWithHarnessError
        )
    });
    let harness_error =
        has_harness_error(result_parse_successes, process_successes, lease_statuses);
    let expected_failures =
        expected_counterexample_invariants(lease_violation, non_linearizable, owns_rd06);
    let globally_bound_rd06 = bundle
        .results
        .iter()
        .any(|result| result.invariant_id == "RD-06" && result.status == EvidenceStatus::Fail);
    let agrees = if !expected_failures.is_empty() {
        local_counterexample_agrees(
            check,
            &statuses,
            &expected_failures,
            non_linearizable,
            owns_rd06,
            globally_bound_rd06,
        )
    } else if non_linearizable {
        supporting_counterexample_statuses(bundle, check, &statuses)
    } else if harness_error {
        uniform_statuses(
            check,
            &statuses,
            CheckCompletion::HarnessError,
            EvidenceStatus::Error,
        )
    } else if !all_valid || coverage.contains(&false) {
        uniform_statuses(
            check,
            &statuses,
            CheckCompletion::CoverageNotReached,
            EvidenceStatus::Incomplete,
        )
    } else {
        uniform_statuses(
            check,
            &statuses,
            CheckCompletion::Completed,
            EvidenceStatus::Pass,
        )
    };
    if agrees {
        Ok((lease_violation || non_linearizable) && harness_error)
    } else {
        Err(error(format!(
            "{} evidence statuses disagree with Maelstrom artifacts",
            check.check_id
        )))
    }
}

fn has_harness_error(
    result_parse_successes: &[bool],
    process_successes: &[bool],
    lease_statuses: &[LeaseArtifactStatus],
) -> bool {
    result_parse_successes.contains(&false)
        || process_successes.contains(&false)
        || lease_statuses.iter().any(|status| {
            matches!(
                status,
                LeaseArtifactStatus::HarnessError | LeaseArtifactStatus::ViolationWithHarnessError
            )
        })
}

fn local_counterexample_agrees(
    check: &CheckReceipt,
    statuses: &[(&str, EvidenceStatus)],
    expected_invariants: &BTreeSet<&str>,
    non_linearizable: bool,
    owns_rd06: bool,
    globally_bound_rd06: bool,
) -> bool {
    counterexample_statuses(check, statuses, expected_invariants)
        && (!non_linearizable || owns_rd06 || globally_bound_rd06)
}

fn expected_counterexample_invariants(
    lease_violation: bool,
    non_linearizable: bool,
    owns_rd06: bool,
) -> BTreeSet<&'static str> {
    [
        (lease_violation, "RD-05"),
        (non_linearizable && owns_rd06, "RD-06"),
    ]
    .into_iter()
    .filter_map(|(required, invariant)| required.then_some(invariant))
    .collect()
}

fn counterexample_statuses(
    check: &CheckReceipt,
    statuses: &[(&str, EvidenceStatus)],
    expected_invariants: &BTreeSet<&str>,
) -> bool {
    let expected_failure = |invariant: &str, status| {
        expected_invariants.contains(invariant) && status == EvidenceStatus::Fail
    };
    check.completion == CheckCompletion::Counterexample
        && statuses
            .iter()
            .filter(|(id, status)| expected_failure(id, *status))
            .count()
            == expected_invariants.len()
        && statuses.iter().all(|(id, status)| {
            expected_failure(id, *status)
                || (!expected_invariants.contains(id) && *status == EvidenceStatus::Incomplete)
        })
}

fn supporting_counterexample_statuses(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    statuses: &[(&str, EvidenceStatus)],
) -> bool {
    bundle
        .results
        .iter()
        .any(|result| result.invariant_id == "RD-06" && result.status == EvidenceStatus::Fail)
        && uniform_statuses(
            check,
            statuses,
            CheckCompletion::CoverageNotReached,
            EvidenceStatus::Incomplete,
        )
}

fn uniform_statuses(
    check: &CheckReceipt,
    statuses: &[(&str, EvidenceStatus)],
    completion: CheckCompletion,
    status: EvidenceStatus,
) -> bool {
    check.completion == completion
        && !statuses.is_empty()
        && statuses.iter().all(|(_, observed)| *observed == status)
}

fn configuration<'a>(bundle: &'a ResultBundle, key: &str) -> Result<&'a str, AggregateError> {
    bundle
        .execution
        .plan
        .contract
        .runners
        .get(&bundle.runner)
        .ok_or_else(|| error(format!("execution plan omitted runner {}", bundle.runner)))?
        .configuration
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| error(format!("Maelstrom configuration omitted {key}")))
}

fn error(message: impl Into<String>) -> AggregateError {
    AggregateError::new(message.into())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use crate::{CheckCompletion, CheckReceipt, EvidenceStatus};

    use super::{
        counterexample_statuses, expected_counterexample_invariants, has_harness_error,
        local_counterexample_agrees, LeaseArtifactStatus,
    };

    #[test]
    fn combined_lease_violation_retains_secondary_harness_classification() {
        assert!(has_harness_error(
            &[true],
            &[true],
            &[LeaseArtifactStatus::ViolationWithHarnessError]
        ));
    }

    #[test]
    fn independent_verifier_requires_both_rd05_and_rd06_failures_when_both_rederive() {
        let expected = expected_counterexample_invariants(true, true, true);
        assert_eq!(expected, BTreeSet::from(["RD-05", "RD-06"]));
        let check = CheckReceipt {
            execution_id: "lease".to_owned(),
            check_id: "maelstrom/lease-isolation".to_owned(),
            evidence_ids: Vec::new(),
            completion: CheckCompletion::Counterexample,
            observations: BTreeMap::new(),
            simulator_liveness: None,
            duration_ms: 1,
            peak_rss_kib: 1,
            artifacts: Vec::new(),
        };
        let combined = [
            ("RD-05", EvidenceStatus::Fail),
            ("RD-06", EvidenceStatus::Fail),
            ("LG-04", EvidenceStatus::Incomplete),
        ];
        assert!(counterexample_statuses(&check, &combined, &expected));
        assert!(!counterexample_statuses(&check, &combined[..1], &expected));
    }

    #[test]
    fn local_rd05_failure_survives_harness_faults_and_external_rd06_ownership() {
        let expected = BTreeSet::from(["RD-05"]);
        let check = CheckReceipt {
            execution_id: "lease".to_owned(),
            check_id: "maelstrom/lease-isolation".to_owned(),
            evidence_ids: Vec::new(),
            completion: CheckCompletion::Counterexample,
            observations: BTreeMap::new(),
            simulator_liveness: None,
            duration_ms: 1,
            peak_rss_kib: 1,
            artifacts: Vec::new(),
        };
        let statuses = [("RD-05", EvidenceStatus::Fail)];

        assert!(local_counterexample_agrees(
            &check, &statuses, &expected, false, false, false
        ));
        assert!(local_counterexample_agrees(
            &check, &statuses, &expected, true, false, true
        ));
        assert!(!local_counterexample_agrees(
            &check, &statuses, &expected, true, false, false
        ));
    }
}
