//! Independent parsing and semantic acceptance of raw liveness events.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use super::{
    error::{malformed, missing, LivenessReportError},
    validate::validate_liveness_report,
};
use crate::{
    contract::profile::{SimulatorExecutionContract, SimulatorLivenessContract},
    evidence::LivenessReportClaim,
};

pub(super) fn verify_run(
    profile: &str,
    check_id: &str,
    selected_feature: &str,
    expected_execution: &SimulatorExecutionContract,
    expected_reports: &BTreeMap<String, &SimulatorLivenessContract>,
    event: &Value,
) -> Result<LivenessReportClaim, LivenessReportError> {
    require_passing_status(check_id, event)?;
    validate_run_execution(profile, check_id, expected_execution, event)?;
    let by_feature = index_run_reports(check_id, expected_reports, event)?;
    let mut selected = None;
    for (feature_id, contract) in expected_reports {
        let report = by_feature[feature_id.as_str()];
        let measured = validate_liveness_report(contract, expected_execution, report)
            .map_err(|message| malformed(format!("liveness run `{check_id}`: {message}")))?;
        if feature_id == selected_feature {
            selected = Some((report, measured));
        }
    }
    let (report, (round_limit, rounds_used)) = selected.ok_or_else(|| {
        malformed(format!(
            "registry feature `{selected_feature}` is not enabled for liveness run `{check_id}`"
        ))
    })?;
    let seed = event
        .get("seed")
        .and_then(Value::as_u64)
        .ok_or_else(|| malformed(format!("liveness run `{check_id}` has no integer seed")))?;
    Ok(LivenessReportClaim {
        check_id: check_id.to_owned(),
        seed,
        execution_contract: expected_execution.clone(),
        report: report.clone(),
        round_limit,
        rounds_used,
    })
}

pub(super) fn verify_present_reports(
    profile: &str,
    check_id: &str,
    expected_execution: &SimulatorExecutionContract,
    expected_reports: &BTreeMap<String, &SimulatorLivenessContract>,
    event: &Value,
) -> Result<(), LivenessReportError> {
    match event.get("liveness_reports") {
        None | Some(Value::Null) => return Ok(()),
        Some(Value::Array(values)) if values.is_empty() => return Ok(()),
        Some(Value::Array(_)) => {}
        Some(_) => {
            return Err(malformed(format!(
                "liveness run `{check_id}` reports are not an array"
            )));
        }
    }

    validate_run_execution(profile, check_id, expected_execution, event)?;
    let by_feature = index_run_reports(check_id, expected_reports, event)?;
    for (feature_id, contract) in expected_reports {
        validate_liveness_report(
            contract,
            expected_execution,
            by_feature[feature_id.as_str()],
        )
        .map_err(|message| malformed(format!("liveness run `{check_id}`: {message}")))?;
    }
    Ok(())
}

fn require_passing_status(check_id: &str, event: &Value) -> Result<(), LivenessReportError> {
    if event.get("status").and_then(Value::as_str) == Some("pass") {
        Ok(())
    } else {
        Err(malformed(format!(
            "liveness run `{check_id}` is not a passing soak-check"
        )))
    }
}

fn validate_run_execution(
    profile: &str,
    check_id: &str,
    expected: &SimulatorExecutionContract,
    event: &Value,
) -> Result<(), LivenessReportError> {
    if event.get("check_id").and_then(Value::as_str) != Some(expected.check_id.as_str())
        || event.get("steps").and_then(Value::as_u64) != Some(expected.steps)
    {
        return Err(malformed(format!(
            "liveness run `{check_id}` does not match its expected check or step identity"
        )));
    }
    let value = event.get("execution_contract").ok_or_else(|| {
        malformed(format!(
            "liveness run `{check_id}` has no execution contract"
        ))
    })?;
    let observed =
        serde_json::from_value::<SimulatorExecutionContract>(value.clone()).map_err(|error| {
            malformed(format!(
                "liveness run `{check_id}` has malformed execution contract: {error}"
            ))
        })?;
    if observed != *expected {
        return Err(malformed(format!(
            "liveness run `{check_id}` execution contract does not match profile `{profile}`"
        )));
    }
    Ok(())
}

fn index_run_reports<'a>(
    check_id: &str,
    expected: &BTreeMap<String, &SimulatorLivenessContract>,
    event: &'a Value,
) -> Result<BTreeMap<&'a str, &'a Value>, LivenessReportError> {
    let values = match event.get("liveness_reports") {
        None | Some(Value::Null) => {
            return Err(missing(format!(
                "liveness run `{check_id}` has no structured reports"
            )));
        }
        Some(Value::Array(values)) => values,
        Some(_) => {
            return Err(malformed(format!(
                "liveness run `{check_id}` reports are not an array"
            )));
        }
    };
    let mut by_feature = BTreeMap::new();
    for report in values {
        let feature_id = report
            .get("feature_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                malformed(format!(
                    "liveness run `{check_id}` contains a report without feature identity"
                ))
            })?;
        if by_feature.insert(feature_id, report).is_some() {
            return Err(malformed(format!(
                "liveness run `{check_id}` contains duplicate feature `{feature_id}`"
            )));
        }
    }
    validate_feature_inventory(check_id, expected, &by_feature)?;
    Ok(by_feature)
}

fn validate_feature_inventory(
    check_id: &str,
    expected: &BTreeMap<String, &SimulatorLivenessContract>,
    observed: &BTreeMap<&str, &Value>,
) -> Result<(), LivenessReportError> {
    let observed_features = observed.keys().copied().collect::<BTreeSet<_>>();
    let expected_features = expected.keys().map(String::as_str).collect::<BTreeSet<_>>();
    if observed_features == expected_features {
        return Ok(());
    }
    let missing_features = expected_features
        .difference(&observed_features)
        .copied()
        .collect::<Vec<_>>();
    let unknown_features = observed_features
        .difference(&expected_features)
        .copied()
        .collect::<Vec<_>>();
    if !missing_features.is_empty() && unknown_features.is_empty() {
        return Err(missing(format!(
            "liveness run `{check_id}` is missing features {missing_features:?}"
        )));
    }
    Err(malformed(format!(
        "liveness run `{check_id}` has missing {missing_features:?} and unknown {unknown_features:?} features"
    )))
}
