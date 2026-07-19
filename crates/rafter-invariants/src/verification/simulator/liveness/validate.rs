//! Structural and semantic validation for one bounded-liveness report.

mod budget;
mod evidence;
mod fault_cycle;
mod json;
mod preconditions;

use serde_json::Value;

use self::{
    budget::{validate_fairness, validate_round_budget},
    evidence::{validate_operation_evidence, validate_optional_evidence},
    fault_cycle::validate_fault_cycle,
    json::{exact_string, exact_string_array, require_exact_fields, required_object, required_u64},
    preconditions::validate_preconditions,
};
use crate::contract::profile::{SimulatorExecutionContract, SimulatorLivenessContract};

const REPORT_FIELDS: &[&str] = &[
    "invariant_id",
    "clause_ids",
    "feature_id",
    "scenario_id",
    "observation_id",
    "preconditions",
    "fairness",
    "round_budget",
    "round_limit",
    "rounds_used",
    "fault_cycle",
    "stable_leader",
    "proposal",
    "operation",
];

pub(crate) fn validate_liveness_report(
    contract: &SimulatorLivenessContract,
    execution: &SimulatorExecutionContract,
    report: &Value,
) -> Result<(u64, u64), String> {
    require_exact_fields(report, REPORT_FIELDS, "liveness report")?;
    exact_string(report, "invariant_id", &contract.invariant_id)?;
    exact_string_array(report, "clause_ids", &contract.clause_ids)?;
    exact_string(report, "feature_id", &contract.feature_id)?;
    exact_string(report, "scenario_id", &contract.scenario_id)?;
    exact_string(report, "observation_id", &contract.observation_id)?;

    let voter_ids = validate_preconditions(
        contract,
        execution,
        required_object(report, "preconditions")?,
    )?;
    validate_fairness(contract, required_object(report, "fairness")?)?;
    let derived_limit = validate_round_budget(
        contract,
        execution,
        required_object(report, "round_budget")?,
    )?;

    let round_limit = required_u64(report, "round_limit")?;
    let rounds_used = required_u64(report, "rounds_used")?;
    if round_limit != derived_limit || rounds_used > round_limit {
        return Err("round limit is not the registry-derived bound".to_owned());
    }

    validate_optional_evidence(contract, report, &voter_ids, rounds_used)?;
    validate_operation_evidence(contract, report)?;
    validate_fault_cycle(contract, &voter_ids, report.get("fault_cycle"))?;
    Ok((round_limit, rounds_used))
}
