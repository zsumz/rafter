use std::time::Duration;

use rafter_sim::model_check::{SoakConfig, SoakSummary};
use serde_json::json;

#[cfg(test)]
use crate::profile::SoakCheckKind;
use crate::profile::SoakExecutionContract;

use super::{liveness::validate_liveness_reports, EVENT_PREFIX};

pub(crate) fn print_soak_summary(
    contract: &SoakExecutionContract,
    summary: &SoakSummary,
    config: SoakConfig,
    duration: Duration,
) {
    let name = &contract.check_id;
    println!(
        "model-check {name}: seed={:#x} steps={} observed_actions={:?} duration_ms={}",
        summary.seed().0,
        summary.steps_executed(),
        summary.observed_actions(),
        duration.as_millis()
    );
    let observed_actions = summary
        .observed_actions()
        .iter()
        .map(|kind| kind.as_str())
        .collect::<Vec<_>>();
    println!(
        "{EVENT_PREFIX}{}",
        soak_event_with_contract(contract, summary, config, &observed_actions, duration)
    );
}

#[cfg(test)]
pub(super) fn soak_event(
    name: &str,
    summary: &SoakSummary,
    config: SoakConfig,
    observed_actions: &[&str],
    duration: Duration,
) -> serde_json::Value {
    let contract = test_execution_contract(name, config);
    soak_event_with_contract(&contract, summary, config, observed_actions, duration)
}

fn soak_event_with_contract(
    contract: &SoakExecutionContract,
    summary: &SoakSummary,
    config: SoakConfig,
    observed_actions: &[&str],
    duration: Duration,
) -> serde_json::Value {
    let reports = summary.liveness_reports_json();
    soak_event_from_reports_with_contract(
        contract,
        summary,
        config,
        observed_actions,
        duration,
        &reports,
    )
}

#[cfg(test)]
pub(super) fn soak_event_from_reports(
    name: &str,
    summary: &SoakSummary,
    config: SoakConfig,
    observed_actions: &[&str],
    duration: Duration,
    reports: &[serde_json::Value],
) -> serde_json::Value {
    let contract = test_execution_contract(name, config);
    soak_event_from_reports_with_contract(
        &contract,
        summary,
        config,
        observed_actions,
        duration,
        reports,
    )
}

pub(super) fn soak_event_from_reports_with_contract(
    contract: &SoakExecutionContract,
    summary: &SoakSummary,
    config: SoakConfig,
    observed_actions: &[&str],
    duration: Duration,
    reports: &[serde_json::Value],
) -> serde_json::Value {
    let validation = summary
        .validate_liveness_report_structure()
        .and_then(|()| contract.validate_config(config))
        .and_then(|()| validate_liveness_reports(summary, config, reports));
    let passed = validation.is_ok();
    let liveness_features = if passed {
        reports
            .iter()
            .filter_map(|report| report["feature_id"].as_str())
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let observations = if passed {
        reports
            .iter()
            .filter_map(|report| report["observation_id"].as_str())
            .map(|observation| (observation.to_owned(), json!(1)))
            .collect::<serde_json::Map<_, _>>()
    } else {
        serde_json::Map::new()
    };
    json!({
        "event": "soak-check",
        "check_id": contract.check_id,
        "execution_contract": contract.to_json(),
        "status": if passed { "pass" } else { "error" },
        "classification": if passed { serde_json::Value::Null } else { json!("harness-error") },
        "message": validation.err(),
        "seed": summary.seed().0,
        "steps": summary.steps_executed(),
        "observed_actions": observed_actions,
        "liveness_features": liveness_features,
        "observations": observations,
        "liveness_reports": reports,
        "duration_ms": duration.as_millis(),
    })
}

#[cfg(test)]
pub(super) fn test_execution_contract(name: &str, config: SoakConfig) -> SoakExecutionContract {
    let kind = if name.ends_with("-membership") {
        SoakCheckKind::Membership
    } else if name.ends_with("-lease") {
        SoakCheckKind::Lease
    } else {
        SoakCheckKind::Standard
    };
    SoakExecutionContract::from_config("test-soak", name, kind, config)
}
