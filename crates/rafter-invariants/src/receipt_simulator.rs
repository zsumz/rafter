use std::collections::{BTreeMap, BTreeSet};

use crate::{CheckReceipt, EvidenceDescriptor, EvidenceStatus, ResultBundle, SimulatorIdentity};

pub(super) fn validate(
    bundle: &ResultBundle,
    expected: &BTreeMap<String, &EvidenceDescriptor>,
) -> Result<(), &'static str> {
    let required = expected
        .iter()
        .filter_map(|(evidence_id, descriptor)| {
            descriptor.simulator.as_ref().map(|identity| {
                (
                    format!("simulator/{evidence_id}"),
                    (evidence_id.as_str(), *descriptor, identity),
                )
            })
        })
        .collect::<BTreeMap<_, _>>();
    let observed = bundle
        .execution
        .checks
        .iter()
        .map(|check| (check.check_id.as_str(), check))
        .collect::<BTreeMap<_, _>>();
    if observed.len() != bundle.execution.checks.len() || observed.len() != required.len() {
        return Err("simulator checks must uniquely cover every registry evidence record");
    }
    for (check_id, (evidence_id, descriptor, identity)) in required {
        let Some(check) = observed.get(check_id.as_str()) else {
            return Err("simulator check identity does not match the registry");
        };
        if check.evidence_ids != [evidence_id] {
            return Err("simulator evidence fanout does not match the registry");
        }
        validate_check(bundle, check, descriptor, identity)?;
    }
    Ok(())
}

fn validate_check(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    descriptor: &EvidenceDescriptor,
    identity: &SimulatorIdentity,
) -> Result<(), &'static str> {
    let statuses = bundle
        .results
        .iter()
        .filter(|result| result.execution_id == check.execution_id)
        .map(|result| result.status)
        .collect::<BTreeSet<_>>();
    if statuses.len() != 1 {
        return Err("one simulator execution cannot report conflicting statuses");
    }
    if !statuses.contains(&EvidenceStatus::Pass) {
        return Ok(());
    }
    if check
        .observations
        .get("detector_qualified")
        .copied()
        .unwrap_or_default()
        < 1
        || check
            .observations
            .get(&identity.required_observation)
            .copied()
            .unwrap_or_default()
            < identity.minimum_observation as u64
        || !has_artifact(check, "simulator-log")
        || !has_artifact(check, "simulator-binary")
    {
        return Err("passing simulator check lacks semantic coverage or executable artifacts");
    }
    if identity.negative_test.is_some()
        && (!has_artifact(check, "test-log") || !has_artifact(check, "test-binary"))
    {
        return Err("passing simulator checker lacks detector qualification artifacts");
    }
    if let (Some(protocol), Some(verifier)) = (
        identity.minimum_protocol_states,
        identity.minimum_verifier_states,
    ) {
        if observed(check, "unique_protocol_states") < protocol as u64
            || observed(check, "unique_verifier_states") < verifier as u64
            || identity
                .checks
                .iter()
                .any(|name| observed(check, &format!("passes:{name}")) < 1)
        {
            return Err("passing simulator safety check is below its state or completion floor");
        }
    }
    if let (Some(runs), Some(steps)) = (identity.minimum_runs_per_check, identity.minimum_steps) {
        if identity.liveness_report.is_some()
            && validate_liveness_binding(bundle, check, descriptor, identity).is_err()
        {
            return Err("passing simulator liveness check lacks its exact typed report binding");
        }
        if identity.checks.iter().any(|name| {
            let observed_runs = observed(check, &format!("runs:{name}"));
            observed_runs < runs as u64
                || observed(check, &format!("passes:{name}")) != observed_runs
                || observed(check, &format!("steps:{name}")) < steps as u64
        }) {
            return Err("passing simulator liveness check is below its run or step floor");
        }
    } else if check.simulator_liveness.is_some() {
        return Err("simulator safety check has an unexpected liveness report binding");
    }
    Ok(())
}

fn validate_liveness_binding(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    descriptor: &EvidenceDescriptor,
    identity: &SimulatorIdentity,
) -> Result<(), ()> {
    let contract = identity.liveness_report.as_ref().ok_or(())?;
    if contract.invariant_id != descriptor.invariant_id
        || !contract.clause_ids.contains(&descriptor.clause_id)
    {
        return Err(());
    }
    let binding = check.simulator_liveness.as_ref().ok_or(())?;
    if binding.schema_version != 1
        || binding.contract != *contract
        || binding.contract_sha256 != crate::catalog::liveness_contract_digest(contract)
        || binding.reports.is_empty()
        || binding.reports_sha256 != crate::catalog::liveness_reports_digest(&binding.reports)
        || binding.reports.windows(2).any(|pair| pair[0] >= pair[1])
        || binding.reports.iter().any(|report| {
            let expected_execution =
                crate::catalog::expected_execution_contract(&bundle.profile, &report.check_id);
            !identity.checks.contains(&report.check_id)
                || report.report_sha256.len() != 64
                || !report
                    .report_sha256
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit())
                || expected_execution.as_ref() != Ok(&report.execution_contract)
                || report.execution_contract_sha256
                    != crate::catalog::execution_contract_digest(&report.execution_contract)
                || report.rounds_used > report.round_limit
        })
    {
        return Err(());
    }
    let report_count = binding.reports.len() as u64;
    let observed_runs = identity
        .checks
        .iter()
        .map(|name| observed(check, &format!("runs:{name}")))
        .sum::<u64>();
    if report_count != observed_runs
        || observed(check, &identity.required_observation) != report_count
        || identity.checks.iter().any(|name| {
            binding
                .reports
                .iter()
                .filter(|report| report.check_id == *name)
                .count() as u64
                != observed(check, &format!("runs:{name}"))
        })
    {
        return Err(());
    }
    let mut expected_observations = BTreeSet::from([
        "detector_qualified".to_owned(),
        identity.required_observation.clone(),
    ]);
    for name in &identity.checks {
        expected_observations.extend([
            format!("runs:{name}"),
            format!("passes:{name}"),
            format!("steps:{name}"),
        ]);
    }
    if check.observations.keys().cloned().collect::<BTreeSet<_>>() != expected_observations {
        return Err(());
    }
    Ok(())
}

fn observed(check: &CheckReceipt, name: &str) -> u64 {
    check.observations.get(name).copied().unwrap_or_default()
}

fn has_artifact(check: &CheckReceipt, kind: &str) -> bool {
    check.artifacts.iter().any(|artifact| artifact.kind == kind)
}
