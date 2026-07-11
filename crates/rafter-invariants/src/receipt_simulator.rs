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
                    (evidence_id.as_str(), identity),
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
    for (check_id, (evidence_id, identity)) in required {
        let Some(check) = observed.get(check_id.as_str()) else {
            return Err("simulator check identity does not match the registry");
        };
        if check.evidence_ids != [evidence_id] {
            return Err("simulator evidence fanout does not match the registry");
        }
        validate_check(bundle, check, identity)?;
    }
    Ok(())
}

fn validate_check(
    bundle: &ResultBundle,
    check: &CheckReceipt,
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
        if identity.checks.iter().any(|name| {
            observed(check, &format!("runs:{name}")) < runs as u64
                || observed(check, &format!("passes:{name}")) < runs as u64
                || observed(check, &format!("steps:{name}")) < steps as u64
        }) {
            return Err("passing simulator liveness check is below its run or step floor");
        }
    }
    Ok(())
}

fn observed(check: &CheckReceipt, name: &str) -> u64 {
    check.observations.get(name).copied().unwrap_or_default()
}

fn has_artifact(check: &CheckReceipt, kind: &str) -> bool {
    check.artifacts.iter().any(|artifact| artifact.kind == kind)
}
