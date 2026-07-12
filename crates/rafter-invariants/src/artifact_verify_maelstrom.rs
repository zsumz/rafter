use std::{collections::BTreeSet, path::Path};

use crate::{
    aggregate::AggregateError,
    producer::maelstrom_edn::{MaelstromSummary, Validity},
    CheckCompletion, CheckReceipt, EvidenceStatus, ResultBundle,
};

use crate::artifact_verify_maelstrom_support::{
    add, add_summary, empty_observations, group_trials, parse_process, parse_results,
    requires_proxy, scan_node_logs, scenario_script, trial_floors_met, unique, verify_matches_file,
};

pub(super) fn verify(bundle: &ResultBundle, root: &Path) -> Result<(), AggregateError> {
    let trials = configuration(bundle, "trials")?
        .parse::<u64>()
        .map_err(|parse_error| error(format!("parse Maelstrom trial count: {parse_error}")))?;
    for check in &bundle.execution.checks {
        verify_check(bundle, check, trials, root)?;
    }
    Ok(())
}

fn verify_check(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    trials: u64,
    root: &Path,
) -> Result<(), AggregateError> {
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
    let mut process_successes = Vec::new();
    let mut coverage = Vec::new();
    for artifacts in grouped.values() {
        verify_trial_inputs(bundle, scenario, artifacts, root)?;
        let summary = parse_results(unique(artifacts, "maelstrom-results")?, root)?;
        let process = parse_process(unique(artifacts, "maelstrom-process-log")?, root)?;
        if process.schema_version != 1 || process.label != scenario {
            return Err(error("Maelstrom process log has wrong schema or label"));
        }
        process_successes.push(process.exit_code == Some(0) && !process.timed_out);
        add_summary(&mut observations, &summary);
        let markers = scan_node_logs(artifacts, root)?;
        for (&name, &value) in &markers {
            add(&mut observations, name, value);
        }
        let durable = artifacts
            .iter()
            .any(|artifact| artifact.kind == "maelstrom-durable-file");
        coverage.push(trial_floors_met(scenario, &summary, &markers, durable));
        summaries.push(summary);
    }
    if observations != check.observations {
        return Err(error("Maelstrom observations disagree with artifacts"));
    }
    verify_statuses(bundle, check, &summaries, &process_successes, &coverage)
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
    summaries: &[MaelstromSummary],
    process_successes: &[bool],
    coverage: &[bool],
) -> Result<(), AggregateError> {
    let statuses = bundle
        .results
        .iter()
        .filter(|result| result.execution_id == check.execution_id)
        .map(|result| (result.invariant_id.as_str(), result.status))
        .collect::<Vec<_>>();
    let non_linearizable = summaries
        .iter()
        .any(|summary| summary.linearizability == Validity::Invalid);
    let all_valid = summaries
        .iter()
        .all(|summary| summary.validity == Validity::Valid);
    let owns_rd06 = statuses.iter().any(|(id, _)| *id == "RD-06");
    let agrees = if non_linearizable && owns_rd06 {
        counterexample_statuses(check, &statuses)
    } else if non_linearizable {
        supporting_counterexample_statuses(bundle, check, &statuses)
    } else if process_successes.contains(&false) {
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
        Ok(())
    } else {
        Err(error(format!(
            "{} evidence statuses disagree with Maelstrom artifacts",
            check.check_id
        )))
    }
}

fn counterexample_statuses(check: &CheckReceipt, statuses: &[(&str, EvidenceStatus)]) -> bool {
    let failed_rd06 =
        |invariant: &str, status| invariant == "RD-06" && status == EvidenceStatus::Fail;
    check.completion == CheckCompletion::Counterexample
        && statuses
            .iter()
            .filter(|(id, status)| failed_rd06(id, *status))
            .count()
            == 1
        && statuses.iter().all(|(id, status)| {
            failed_rd06(id, *status) || (*id != "RD-06" && *status == EvidenceStatus::Incomplete)
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
        .configuration
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| error(format!("Maelstrom configuration omitted {key}")))
}

fn error(message: impl Into<String>) -> AggregateError {
    AggregateError::new(message.into())
}
