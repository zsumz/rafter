use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use super::{
    liveness_validation::validate_liveness_report, LivenessReportError, LivenessReportErrorKind,
    SimulatorIdentity,
};
use crate::types::{
    SimulatorExecutionContract, SimulatorLivenessBinding, SimulatorLivenessContract,
    SimulatorLivenessReportBinding,
};

pub(crate) fn derive_liveness_binding(
    profile: &str,
    identity: &SimulatorIdentity,
    available_contracts: &[SimulatorLivenessContract],
    events: &BTreeMap<String, Vec<Value>>,
) -> Result<SimulatorLivenessBinding, LivenessReportError> {
    let contract = identity.liveness_report.as_ref().ok_or_else(|| {
        malformed("simulator identity does not declare a liveness report contract")
    })?;
    let mut reports = Vec::new();
    for check_id in &identity.checks {
        let expected_execution = expected_execution_contract(profile, check_id)
            .map_err(|message| malformed(format!("liveness run `{check_id}`: {message}")))?;
        let expected_reports = expected_report_contracts(available_contracts, &expected_execution)?;
        let runs = events.get(check_id).map(Vec::as_slice).unwrap_or_default();
        if runs.is_empty() {
            return Err(missing(format!(
                "required simulator check `{check_id}` has no liveness run"
            )));
        }
        for event in runs {
            reports.push(derive_liveness_run_binding(
                profile,
                contract,
                check_id,
                &expected_execution,
                &expected_reports,
                event,
            )?);
        }
    }
    reports.sort();
    let contract_sha256 = serialized_digest(contract);
    let reports_sha256 = serialized_digest(&reports);
    Ok(SimulatorLivenessBinding {
        schema_version: 1,
        contract: contract.clone(),
        contract_sha256,
        reports_sha256,
        reports,
    })
}

fn derive_liveness_run_binding(
    profile: &str,
    contract: &SimulatorLivenessContract,
    check_id: &str,
    expected_execution: &SimulatorExecutionContract,
    expected_reports: &BTreeMap<String, &SimulatorLivenessContract>,
    event: &Value,
) -> Result<SimulatorLivenessReportBinding, LivenessReportError> {
    validate_run_execution(profile, check_id, expected_execution, event)?;
    let by_feature = index_run_reports(check_id, expected_reports, event)?;
    let mut selected = None;
    for (feature_id, expected) in expected_reports {
        let report = by_feature[feature_id.as_str()];
        let measured = validate_liveness_report(expected, expected_execution, report)
            .map_err(|message| malformed(format!("liveness run `{check_id}`: {message}")))?;
        if feature_id == &contract.feature_id {
            selected = Some((report, measured));
        }
    }
    let (report, (round_limit, rounds_used)) = selected.ok_or_else(|| {
        malformed(format!(
            "registry feature `{}` is not enabled for liveness run `{check_id}`",
            contract.feature_id
        ))
    })?;
    let seed = event
        .get("seed")
        .and_then(Value::as_u64)
        .ok_or_else(|| malformed(format!("liveness run `{check_id}` has no integer seed")))?;
    Ok(SimulatorLivenessReportBinding {
        check_id: check_id.to_owned(),
        seed,
        execution_contract_sha256: serialized_digest(expected_execution),
        execution_contract: expected_execution.clone(),
        report_sha256: canonical_value_digest(report),
        round_limit,
        rounds_used,
    })
}

fn validate_run_execution(
    profile: &str,
    check_id: &str,
    expected: &SimulatorExecutionContract,
    event: &Value,
) -> Result<(), LivenessReportError> {
    if event.get("status").and_then(Value::as_str) != Some("pass") {
        return Err(malformed(format!(
            "liveness run `{check_id}` is not a passing soak-check"
        )));
    }
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
            )))
        }
        Some(Value::Array(values)) => values,
        Some(_) => {
            return Err(malformed(format!(
                "liveness run `{check_id}` reports are not an array"
            )))
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

fn expected_report_contracts<'a>(
    available: &'a [SimulatorLivenessContract],
    execution: &SimulatorExecutionContract,
) -> Result<BTreeMap<String, &'a SimulatorLivenessContract>, LivenessReportError> {
    let mut expected = BTreeMap::new();
    for contract in available {
        let required = match contract.feature_id.as_str() {
            "leader-convergence"
            | "quorum-only-leader-convergence"
            | "proposal-progress"
            | "proposal-termination" => true,
            "read-barrier" => execution.max_read_indexes > 0,
            "membership-transition" => execution.max_membership_changes > 0,
            "leadership-transfer" => execution.max_transfers > 0,
            "snapshot-catch-up" => execution.snapshot_catchup_probe,
            feature => {
                return Err(malformed(format!(
                    "registry contains unknown liveness feature `{feature}`"
                )))
            }
        };
        if required {
            if let Some(previous) = expected.insert(contract.feature_id.clone(), contract) {
                if previous != contract {
                    return Err(malformed(format!(
                        "registry contains conflicting liveness contracts for `{}`",
                        contract.feature_id
                    )));
                }
            }
        }
    }
    let mut required = BTreeSet::from([
        "leader-convergence",
        "quorum-only-leader-convergence",
        "proposal-progress",
        "proposal-termination",
    ]);
    if execution.max_read_indexes > 0 {
        required.insert("read-barrier");
    }
    if execution.max_membership_changes > 0 {
        required.insert("membership-transition");
    }
    if execution.max_transfers > 0 {
        required.insert("leadership-transfer");
    }
    if execution.snapshot_catchup_probe {
        required.insert("snapshot-catch-up");
    }
    if expected.keys().map(String::as_str).collect::<BTreeSet<_>>() != required {
        return Err(malformed(
            "registry does not define the complete expected liveness feature set",
        ));
    }
    Ok(expected)
}

pub(crate) fn expected_execution_contract(
    profile: &str,
    canonical_check: &str,
) -> Result<SimulatorExecutionContract, String> {
    let (profile_id, steps, maxima, tick_skew_weight) = match profile {
        "pr" => ("raft-soak", 320, [24, 12, 4, 8, 2, 2, 2], 3),
        "nightly" => ("raft-nightly-soak", 1024, [64, 32, 4, 16, 2, 2, 2], 3),
        "weekly" => ("raft-weekly-soak", 4096, [192, 96, 16, 48, 8, 8, 8], 5),
        value => return Err(format!("unsupported simulator profile `{value}`")),
    };
    let (suffix, check_kind, node_config_id, node_count) = match canonical_check {
        "raft-soak" => ("", "standard", "three-node-standard-v1", 3),
        "raft-soak-lease" => ("-lease", "lease", "three-node-lease-v1", 3),
        "raft-soak-membership" => (
            "-membership",
            "membership",
            "four-node-future-learner-v1",
            4,
        ),
        value => return Err(format!("unsupported canonical soak check `{value}`")),
    };
    Ok(SimulatorExecutionContract {
        contract_id: "rafter-soak-execution-v1".to_owned(),
        profile_id: profile_id.to_owned(),
        check_id: format!("{profile_id}{suffix}"),
        check_kind: check_kind.to_owned(),
        node_config_id: node_config_id.to_owned(),
        node_count,
        steps,
        max_proposals: maxima[0],
        max_restarts: maxima[1],
        max_read_indexes: maxima[2],
        max_membership_changes: maxima[3],
        max_transfers: maxima[4],
        max_partitions: maxima[5],
        max_lossy_restarts: maxima[6],
        snapshot_catchup_probe: true,
        tick_skew_node_id: Some(1),
        tick_skew_weight: Some(tick_skew_weight),
    })
}

pub(crate) fn liveness_contract_digest(contract: &SimulatorLivenessContract) -> String {
    serialized_digest(contract)
}

pub(crate) fn execution_contract_digest(contract: &SimulatorExecutionContract) -> String {
    serialized_digest(contract)
}

pub(crate) fn liveness_reports_digest(reports: &[SimulatorLivenessReportBinding]) -> String {
    serialized_digest(&reports)
}

fn serialized_digest(value: &impl Serialize) -> String {
    let bytes = serde_json::to_vec(value)
        .unwrap_or_else(|error| format!("liveness-serialization-error:{error}").into_bytes());
    format!("{:x}", Sha256::digest(bytes))
}

fn canonical_value_digest(value: &Value) -> String {
    let canonical = canonical_value(value);
    let bytes = serde_json::to_vec(&canonical)
        .unwrap_or_else(|error| format!("report-serialization-error:{error}").into_bytes());
    format!("{:x}", Sha256::digest(bytes))
}

fn canonical_value(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(canonical_value).collect()),
        Value::Object(values) => {
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort();
            let mut canonical = Map::new();
            for key in keys {
                canonical.insert(key.clone(), canonical_value(&values[key]));
            }
            Value::Object(canonical)
        }
        value => value.clone(),
    }
}

fn missing(message: impl Into<String>) -> LivenessReportError {
    LivenessReportError {
        kind: LivenessReportErrorKind::Missing,
        message: message.into(),
    }
}

fn malformed(message: impl Into<String>) -> LivenessReportError {
    LivenessReportError {
        kind: LivenessReportErrorKind::Malformed,
        message: message.into(),
    }
}
