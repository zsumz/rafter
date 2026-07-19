//! Validation of stable-leader, proposal, and operation evidence.

use serde_json::Value;

use super::json::{
    require_exact_object_fields, required_map_bool, required_map_str, required_map_u64,
    required_object,
};
use crate::contract::profile::SimulatorLivenessContract;

pub(super) fn validate_optional_evidence(
    contract: &SimulatorLivenessContract,
    report: &Value,
    voter_ids: &[u64],
    rounds_used: u64,
) -> Result<(), String> {
    validate_stable_leader(contract, report, voter_ids, rounds_used)?;
    validate_proposal(contract, report)
}

fn validate_stable_leader(
    contract: &SimulatorLivenessContract,
    report: &Value,
    voter_ids: &[u64],
    rounds_used: u64,
) -> Result<(), String> {
    let Some(expected) = contract.stable_leader_retained else {
        if report
            .get("stable_leader")
            .is_some_and(|value| !value.is_null())
        {
            return Err("unexpected stable-leader evidence".to_owned());
        }
        if contract.stable_leader_rounds_relation != "none"
            || contract.stable_leader_rounds_minimum.is_some()
            || contract.stable_leader_rounds_exact.is_some()
        {
            return Err("registry stable-leader contract is inconsistent".to_owned());
        }
        return Ok(());
    };

    let leader = required_object(report, "stable_leader")?;
    require_exact_object_fields(
        leader,
        &["node_id", "stable_rounds", "remained_leader_through_probe"],
        "stable-leader evidence",
    )?;
    let node_id = required_map_u64(leader, "node_id")?;
    let stable_rounds = required_map_u64(leader, "stable_rounds")?;
    let valid_rounds = match contract.stable_leader_rounds_relation.as_str() {
        "exact" => contract.stable_leader_rounds_exact == Some(stable_rounds),
        "probe-rounds" => stable_rounds == rounds_used.max(1),
        value => return Err(format!("unknown stable-leader rounds relation `{value}`")),
    };
    if node_id == 0
        || !voter_ids.contains(&node_id)
        || contract
            .stable_leader_rounds_minimum
            .is_none_or(|minimum| stable_rounds < minimum)
        || !valid_rounds
        || required_map_bool(leader, "remained_leader_through_probe")? != expected
    {
        return Err("leader identity, stable window, or retention is inconsistent".to_owned());
    }
    Ok(())
}

fn validate_proposal(contract: &SimulatorLivenessContract, report: &Value) -> Result<(), String> {
    match contract.proposal_outcome.as_str() {
        "none" => {
            if report.get("proposal").is_some_and(|value| !value.is_null()) {
                return Err("unexpected proposal evidence".to_owned());
            }
        }
        expected @ ("committed" | "explicit-terminal") => {
            let proposal = required_object(report, "proposal")?;
            require_exact_object_fields(
                proposal,
                &["proposal_id", "terminal_outcome"],
                "proposal evidence",
            )?;
            let proposal_id = required_map_u64(proposal, "proposal_id")?;
            let outcome = required_map_str(proposal, "terminal_outcome")?;
            let valid = if expected == "committed" {
                outcome == "committed"
            } else {
                matches!(outcome, "committed" | "rejected" | "canceled" | "unknown")
            };
            if proposal_id == 0 || !valid {
                return Err("proposal terminal outcome is inconsistent".to_owned());
            }
        }
        value => return Err(format!("unknown registry proposal outcome `{value}`")),
    }
    Ok(())
}

pub(super) fn validate_operation_evidence(
    contract: &SimulatorLivenessContract,
    report: &Value,
) -> Result<(), String> {
    let (prefix, outcomes): (&str, &[&str]) = match contract.feature_id.as_str() {
        "read-barrier" => ("read:", &["completed", "rejected", "canceled"]),
        "membership-transition" => ("remove-voter:", &["committed", "rejected"]),
        "leadership-transfer" => ("transfer:", &["completed", "rejected"]),
        "snapshot-catch-up" => ("snapshot:", &["installed"]),
        _ => {
            return if report.get("operation").is_none_or(Value::is_null) {
                Ok(())
            } else {
                Err("unexpected operation evidence".to_owned())
            };
        }
    };
    let operation = required_object(report, "operation")?;
    require_exact_object_fields(
        operation,
        &["operation_id", "terminal_outcome"],
        "operation evidence",
    )?;
    let operation_id = required_map_str(operation, "operation_id")?;
    let outcome = required_map_str(operation, "terminal_outcome")?;
    if !valid_operation_id(&contract.feature_id, operation_id, prefix)
        || !outcomes.contains(&outcome)
    {
        return Err("operation identity or terminal outcome is inconsistent".to_owned());
    }
    Ok(())
}

fn valid_operation_id(feature: &str, operation_id: &str, prefix: &str) -> bool {
    let Some(identity) = operation_id.strip_prefix(prefix) else {
        return false;
    };
    match feature {
        "read-barrier" => identity.parse::<u64>().is_ok(),
        "membership-transition" => {
            let ids = identity
                .split(':')
                .map(str::parse::<u64>)
                .collect::<Result<Vec<_>, _>>();
            ids.is_ok_and(|ids| ids.len() == 2 && ids[0] != 0 && ids[1] != 0 && ids[0] != ids[1])
        }
        "leadership-transfer" => identity.split_once("->").is_some_and(|(from, to)| {
            from.parse::<u64>().is_ok_and(|id| id != 0)
                && to.parse::<u64>().is_ok_and(|id| id != 0)
                && from != to
        }),
        "snapshot-catch-up" => !identity.is_empty(),
        _ => false,
    }
}
