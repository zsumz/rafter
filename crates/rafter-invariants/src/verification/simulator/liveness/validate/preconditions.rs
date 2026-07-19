//! Validation of measured liveness preconditions and quorum evidence.

use std::collections::BTreeSet;

use serde_json::{Map, Value};

use super::json::{
    require_exact_object_fields, required_map_bool, required_map_str, required_map_u64,
    required_map_u64_array,
};
use crate::contract::profile::{SimulatorExecutionContract, SimulatorLivenessContract};

const PRECONDITION_FIELDS: &[&str] = &[
    "fault_requirement",
    "fault_state_satisfied",
    "fault_state_status",
    "faults_stopped",
    "partition_active",
    "mutually_reachable_quorum",
    "mutually_reachable_quorum_status",
    "stable_membership",
    "stable_membership_status",
    "stable_leader_required",
    "stable_leader_satisfied",
    "stable_leader_status",
    "accepted_proposal_required",
    "accepted_proposal_satisfied",
    "accepted_proposal_status",
    "authority_loss_required",
    "authority_loss_satisfied",
    "authority_loss_status",
    "voter_ids",
    "reachable_voters",
    "quorum_size",
    "unavailable_voters",
];

pub(super) fn validate_preconditions(
    contract: &SimulatorLivenessContract,
    execution: &SimulatorExecutionContract,
    preconditions: &Map<String, Value>,
) -> Result<Vec<u64>, String> {
    require_exact_object_fields(preconditions, PRECONDITION_FIELDS, "liveness preconditions")?;
    validate_fault_state(contract, preconditions)?;
    validate_satisfied(preconditions, "mutually_reachable_quorum")?;
    validate_satisfied(preconditions, "stable_membership")?;

    let voter_ids = validate_quorum(execution, preconditions)?;
    validate_requirement(
        preconditions,
        "stable_leader",
        contract.stable_leader_retained.is_some(),
    )?;
    validate_requirement(
        preconditions,
        "accepted_proposal",
        contract.proposal_outcome != "none",
    )?;
    validate_authority_loss(contract, preconditions)?;
    Ok(voter_ids)
}

fn validate_fault_state(
    contract: &SimulatorLivenessContract,
    preconditions: &Map<String, Value>,
) -> Result<(), String> {
    let fault_requirement = required_map_str(preconditions, "fault_requirement")?;
    let fault_state_satisfied = required_map_bool(preconditions, "fault_state_satisfied")?;
    let fault_state_status = required_map_str(preconditions, "fault_state_status")?;
    let faults_stopped = required_map_bool(preconditions, "faults_stopped")?;
    let partition_active = required_map_bool(preconditions, "partition_active")?;
    let measured_fault = match contract.fault_requirement.as_str() {
        "stopped" => faults_stopped && !partition_active,
        "active-partition" => !faults_stopped && partition_active,
        value => return Err(format!("unknown registry fault requirement `{value}`")),
    };
    if fault_requirement != contract.fault_requirement
        || !fault_state_satisfied
        || fault_state_status != "satisfied"
        || !measured_fault
    {
        return Err("fault-state precondition is inconsistent".to_owned());
    }
    Ok(())
}

fn validate_satisfied(preconditions: &Map<String, Value>, value: &str) -> Result<(), String> {
    let status = format!("{value}_status");
    if !required_map_bool(preconditions, value)?
        || required_map_str(preconditions, &status)? != "satisfied"
    {
        return Err(format!("precondition `{value}` is not satisfied"));
    }
    Ok(())
}

fn validate_quorum(
    execution: &SimulatorExecutionContract,
    preconditions: &Map<String, Value>,
) -> Result<Vec<u64>, String> {
    let voter_ids = required_map_u64_array(preconditions, "voter_ids")?;
    let expected_voter_ids = match execution.node_config_id.as_str() {
        "three-node-standard-v1" | "three-node-lease-v1" | "four-node-future-learner-v1" => {
            [1, 2, 3].as_slice()
        }
        value => return Err(format!("unknown node configuration `{value}`")),
    };
    let unique_voters = voter_ids.iter().copied().collect::<BTreeSet<_>>();
    let reachable = required_map_u64(preconditions, "reachable_voters")?;
    let quorum = required_map_u64(preconditions, "quorum_size")?;
    let unavailable = required_map_u64(preconditions, "unavailable_voters")?;
    let voter_count = voter_ids.len() as u64;
    let expected_unavailable = voter_count.checked_sub(reachable);
    if voter_ids != expected_voter_ids
        || unique_voters.len() != voter_ids.len()
        || voter_ids.contains(&0)
        || quorum != voter_count / 2 + 1
        || reachable < quorum
        || expected_unavailable != Some(unavailable)
    {
        return Err("reachable voters do not prove a quorum".to_owned());
    }
    Ok(voter_ids)
}

fn validate_requirement(
    preconditions: &Map<String, Value>,
    stem: &str,
    expected: bool,
) -> Result<(), String> {
    let required = required_map_bool(preconditions, &format!("{stem}_required"))?;
    let satisfied = required_map_bool(preconditions, &format!("{stem}_satisfied"))?;
    let status = required_map_str(preconditions, &format!("{stem}_status"))?;
    let expected_status = if expected {
        "satisfied"
    } else {
        "not-required"
    };
    if required == expected && satisfied == expected && status == expected_status {
        Ok(())
    } else {
        Err(format!("precondition `{stem}` is inconsistent"))
    }
}

fn validate_authority_loss(
    contract: &SimulatorLivenessContract,
    preconditions: &Map<String, Value>,
) -> Result<(), String> {
    let authority_required = required_map_bool(preconditions, "authority_loss_required")?;
    let authority_satisfied = required_map_bool(preconditions, "authority_loss_satisfied")?;
    let authority_status = required_map_str(preconditions, "authority_loss_status")?;
    let expected_status = if contract.authority_loss_required {
        "satisfied"
    } else {
        "not-required"
    };
    if authority_required != contract.authority_loss_required
        || authority_satisfied != contract.authority_loss_required
        || authority_status != expected_status
    {
        return Err("authority-loss precondition is inconsistent".to_owned());
    }
    Ok(())
}
