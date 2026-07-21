//! Exact binding between typed liveness reports and simulator observations.

use std::collections::BTreeSet;

use crate::{
    contract::catalog::{EvidenceDescriptor, SimulatorIdentity},
    evidence::{CheckReceipt, ResultBundle},
};

pub(super) fn validate(
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
        || binding.contract_sha256 != crate::evidence::liveness_contract_digest(contract)
        || binding.reports.is_empty()
        || binding.reports_sha256 != crate::evidence::liveness_reports_digest(&binding.reports)
        || binding.reports.windows(2).any(|pair| pair[0] >= pair[1])
        || binding.reports.iter().any(|report| {
            let expected_execution = crate::contract::profile::expected_execution_contract(
                &bundle.profile,
                &report.check_id,
            );
            !identity.checks.contains(&report.check_id)
                || report.report_sha256.len() != 64
                || !report
                    .report_sha256
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit())
                || expected_execution.as_ref() != Ok(&report.execution_contract)
                || report.execution_contract_sha256
                    != crate::evidence::execution_contract_digest(&report.execution_contract)
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
