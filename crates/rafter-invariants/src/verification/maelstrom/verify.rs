//! End-to-end acceptance of authenticated Maelstrom trial evidence.

use std::{collections::BTreeSet, path::Path};

use crate::{
    evidence::{format::maelstrom::MaelstromSummary, CheckReceipt, ResultBundle},
    verification::{AggregateError, AuthenticatedArtifacts},
};

use super::{
    artifact::{group_trials, parse_process, parse_results, unique},
    configuration, invocation, lease,
    observation::{trial_floors_met, ObservationLedger},
    scenario::Scenario,
    status::{self, TrialStatuses},
};

pub(crate) fn verify_authenticated(
    bundle: &ResultBundle,
    root: &Path,
    source_root: &Path,
    authenticated: &AuthenticatedArtifacts,
) -> Result<Vec<String>, AggregateError> {
    let trials = configuration::value(bundle, "trials")?
        .parse::<u64>()
        .map_err(|parse_error| error(format!("parse Maelstrom trial count: {parse_error}")))?;
    let mut diagnostics = Vec::new();
    for check in &bundle.execution.checks {
        if verify_check(bundle, check, trials, root, source_root, authenticated)? {
            diagnostics.push(format!(
                "{} preserved a Maelstrom counterexample alongside a harness error",
                check.check_id
            ));
        }
    }
    Ok(diagnostics)
}

#[cfg(test)]
pub(crate) fn verify(bundle: &ResultBundle, root: &Path) -> Result<Vec<String>, AggregateError> {
    let authenticated = crate::verification::snapshot_available_artifacts(bundle, root)?;
    verify_authenticated(bundle, root, root, &authenticated)
}

fn verify_check(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    trials: u64,
    root: &Path,
    source_root: &Path,
    authenticated: &AuthenticatedArtifacts,
) -> Result<bool, AggregateError> {
    let scenario = Scenario::from_check_id(&check.check_id)?;
    let grouped = group_trials(check)?;
    let expected = (0..trials).collect::<BTreeSet<_>>();
    if grouped.trials.keys().copied().collect::<BTreeSet<_>>() != expected {
        return Err(error("Maelstrom artifacts disagree with configured trials"));
    }
    invocation::verify_shared_inputs(
        bundle,
        scenario,
        &grouped.shared,
        root,
        source_root,
        authenticated,
    )?;
    let runner = unique(&grouped.shared, "maelstrom-runner")?;

    let mut observations = ObservationLedger::new(trials);
    let mut summaries = Vec::<Option<MaelstromSummary>>::new();
    let mut result_parse_successes = Vec::new();
    let mut process_successes = Vec::new();
    let mut coverage = Vec::new();
    let mut lease_statuses = Vec::new();
    for (trial, artifacts) in &grouped.trials {
        let summary = parse_results(unique(artifacts, "maelstrom-results")?, authenticated).ok();
        let process = parse_process(unique(artifacts, "maelstrom-process-log")?, authenticated)?;
        if process.label != scenario.name() {
            return Err(error(
                "Maelstrom process log has wrong schema, label, or exact invocation",
            ));
        }
        invocation::verify_process(bundle, scenario, *trial, runner, &process.invocation, root)?;
        process_successes.push(process.exit_code == Some(0) && !process.timed_out);
        if let Some(summary) = &summary {
            observations.add_summary(summary);
        }
        let mut marker_scan = lease::scan_node_logs(artifacts, authenticated)?;
        if scenario == Scenario::LeaseIsolation {
            lease::bind_history(&mut marker_scan, artifacts, authenticated)?;
        }
        observations.add_markers(&marker_scan.values);
        let durable = artifacts
            .iter()
            .any(|artifact| artifact.kind == "maelstrom-durable-file");
        coverage.push(summary.as_ref().is_some_and(|summary| {
            trial_floors_met(scenario, summary, &marker_scan.values, durable)
        }));
        lease_statuses.push(marker_scan.status);
        result_parse_successes.push(summary.is_some());
        summaries.push(summary);
    }
    if observations.into_values() != check.observations {
        return Err(error("Maelstrom observations disagree with artifacts"));
    }
    status::verify(
        bundle,
        check,
        &TrialStatuses {
            summaries: &summaries,
            result_parse_successes: &result_parse_successes,
            process_successes: &process_successes,
            coverage: &coverage,
            lease_statuses: &lease_statuses,
        },
    )
}

fn error(message: impl Into<String>) -> AggregateError {
    AggregateError::new(message.into())
}
