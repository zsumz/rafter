use std::collections::BTreeSet;

use serde_json::{Map, Value};

use crate::types::{SimulatorExecutionContract, SimulatorLivenessContract};

pub(super) fn validate_liveness_report(
    contract: &SimulatorLivenessContract,
    execution: &SimulatorExecutionContract,
    report: &Value,
) -> Result<(u64, u64), String> {
    require_exact_fields(
        report,
        &[
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
        ],
        "liveness report",
    )?;
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
    validate_fault_cycle(contract, report.get("fault_cycle"))?;
    Ok((round_limit, rounds_used))
}

const LIVENESS_PRECONDITION_FIELDS: &[&str] = &[
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

fn validate_preconditions(
    contract: &SimulatorLivenessContract,
    execution: &SimulatorExecutionContract,
    preconditions: &Map<String, Value>,
) -> Result<Vec<u64>, String> {
    require_exact_object_fields(
        preconditions,
        LIVENESS_PRECONDITION_FIELDS,
        "liveness preconditions",
    )?;
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
    for (value, status) in [
        (
            "mutually_reachable_quorum",
            "mutually_reachable_quorum_status",
        ),
        ("stable_membership", "stable_membership_status"),
    ] {
        if !required_map_bool(preconditions, value)?
            || required_map_str(preconditions, status)? != "satisfied"
        {
            return Err(format!("precondition `{value}` is not satisfied"));
        }
    }
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
    if voter_ids != expected_voter_ids
        || unique_voters.len() != voter_ids.len()
        || voter_ids.contains(&0)
        || quorum != voter_ids.len() as u64 / 2 + 1
        || reachable < quorum
        || unavailable != voter_ids.len() as u64 - reachable
    {
        return Err("reachable voters do not prove a quorum".to_owned());
    }
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

fn validate_fairness(
    contract: &SimulatorLivenessContract,
    fairness: &Map<String, Value>,
) -> Result<(), String> {
    require_exact_object_fields(
        fairness,
        &[
            "policy_id",
            "tick_bound_rounds",
            "delivery_bound_rounds",
            "max_delivery_waves_per_tick",
        ],
        "liveness fairness",
    )?;
    if required_map_str(fairness, "policy_id")? != contract.fairness_policy_id
        || required_map_u64(fairness, "tick_bound_rounds")? != contract.fairness_tick_bound_rounds
        || required_map_u64(fairness, "delivery_bound_rounds")?
            != contract.fairness_delivery_bound_rounds
        || required_map_u64(fairness, "max_delivery_waves_per_tick")?
            != contract.fairness_max_delivery_waves_per_tick
    {
        return Err("fairness policy or numeric bound is inconsistent".to_owned());
    }
    Ok(())
}

fn validate_round_budget(
    contract: &SimulatorLivenessContract,
    execution: &SimulatorExecutionContract,
    budget: &Map<String, Value>,
) -> Result<u64, String> {
    if contract.round_budget_provenance != "liveness-round-budget-v1" {
        return Err("unknown registry round-budget provenance".to_owned());
    }
    require_exact_object_fields(
        budget,
        &[
            "minimum_rounds",
            "node_count",
            "queued_messages",
            "max_proposals",
            "max_membership_changes",
            "max_partitions",
            "snapshot_catchup_probe",
            "base_rounds",
            "phase_count",
            "fixed_rounds",
        ],
        "liveness round budget",
    )?;
    let minimum_rounds = required_map_u64(budget, "minimum_rounds")?;
    let node_count = required_map_u64(budget, "node_count")?;
    let queued_messages = required_map_u64(budget, "queued_messages")?;
    let max_proposals = required_map_u64(budget, "max_proposals")?;
    let max_membership_changes = required_map_u64(budget, "max_membership_changes")?;
    let max_partitions = required_map_u64(budget, "max_partitions")?;
    let snapshot_probe = required_map_bool(budget, "snapshot_catchup_probe")?;
    let base_rounds = required_map_u64(budget, "base_rounds")?;
    let phase_count = required_map_u64(budget, "phase_count")?;
    let fixed_rounds = required_map_u64(budget, "fixed_rounds")?;
    let expected_base = contract
        .minimum_rounds
        .checked_add(weight(node_count, contract.rounds_per_node)?)
        .and_then(|value| {
            value.checked_add(weight(queued_messages, contract.rounds_per_queued_message).ok()?)
        })
        .and_then(|value| {
            value.checked_add(weight(max_proposals, contract.rounds_per_proposal).ok()?)
        })
        .and_then(|value| {
            value.checked_add(
                weight(
                    max_membership_changes,
                    contract.rounds_per_membership_change,
                )
                .ok()?,
            )
        })
        .and_then(|value| {
            value.checked_add(weight(max_partitions, contract.rounds_per_partition).ok()?)
        })
        .and_then(|value| {
            value.checked_add(if snapshot_probe {
                contract.snapshot_catchup_rounds
            } else {
                0
            })
        })
        .ok_or_else(|| "round-budget derivation overflowed".to_owned())?;
    let expected_node_count = if matches!(
        contract.feature_id.as_str(),
        "leader-convergence" | "leader-usability"
    ) {
        execution.node_count
    } else {
        3
    };
    if minimum_rounds != contract.minimum_rounds
        || node_count != expected_node_count
        || max_proposals != execution.max_proposals
        || max_membership_changes != execution.max_membership_changes
        || max_partitions != execution.max_partitions
        || snapshot_probe != execution.snapshot_catchup_probe
        || base_rounds != expected_base
        || phase_count != contract.phase_count
        || fixed_rounds != contract.fixed_rounds
    {
        return Err("round-budget provenance or derivation is inconsistent".to_owned());
    }
    base_rounds
        .checked_mul(phase_count)
        .and_then(|value| value.checked_add(fixed_rounds))
        .ok_or_else(|| "round limit overflowed".to_owned())
}

fn weight(value: u64, multiplier: u64) -> Result<u64, String> {
    value
        .checked_mul(multiplier)
        .ok_or_else(|| "round-budget component overflowed".to_owned())
}

fn validate_optional_evidence(
    contract: &SimulatorLivenessContract,
    report: &Value,
    voter_ids: &[u64],
    rounds_used: u64,
) -> Result<(), String> {
    match contract.stable_leader_retained {
        Some(expected) => {
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
                return Err(
                    "leader identity, stable window, or retention is inconsistent".to_owned(),
                );
            }
        }
        None if report
            .get("stable_leader")
            .is_some_and(|value| !value.is_null()) =>
        {
            return Err("unexpected stable-leader evidence".to_owned());
        }
        None => {
            if contract.stable_leader_rounds_relation != "none"
                || contract.stable_leader_rounds_minimum.is_some()
                || contract.stable_leader_rounds_exact.is_some()
            {
                return Err("registry stable-leader contract is inconsistent".to_owned());
            }
        }
    }
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

fn validate_operation_evidence(
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

fn validate_fault_cycle(
    contract: &SimulatorLivenessContract,
    value: Option<&Value>,
) -> Result<(), String> {
    if !contract.fault_cycle_required {
        return if value.is_none_or(Value::is_null) {
            Ok(())
        } else {
            Err("unexpected fault-cycle evidence".to_owned())
        };
    }
    let cycle = value
        .and_then(Value::as_object)
        .ok_or_else(|| "required fault-cycle evidence is missing".to_owned())?;
    require_exact_object_fields(
        cycle,
        &[
            "partition_a",
            "partition_b",
            "partition_observed",
            "partitioned_rounds",
            "nodes_exercised",
            "ticks_executed",
            "deliveries_executed",
            "drops_executed",
            "protocol_state_changed",
            "partition_active_after_exercise",
            "heal_observed",
        ],
        "fault-cycle evidence",
    )?;
    let partition_a = required_map_u64(cycle, "partition_a")?;
    let partition_b = required_map_u64(cycle, "partition_b")?;
    let partitioned_rounds = required_map_u64(cycle, "partitioned_rounds")?;
    let nodes_exercised = required_map_u64(cycle, "nodes_exercised")?;
    let ticks_executed = required_map_u64(cycle, "ticks_executed")?;
    let _deliveries = required_map_u64(cycle, "deliveries_executed")?;
    let _drops = required_map_u64(cycle, "drops_executed")?;
    let state_changed = required_map_bool(cycle, "protocol_state_changed")?;
    if partition_a == partition_b
        || !required_map_bool(cycle, "partition_observed")?
        || partitioned_rounds != contract.fixed_rounds
        || nodes_exercised < 2
        || ticks_executed != partitioned_rounds.saturating_mul(nodes_exercised)
        || !state_changed
        || !required_map_bool(cycle, "partition_active_after_exercise")?
        || !required_map_bool(cycle, "heal_observed")?
    {
        return Err("fault-cycle evidence is inconsistent".to_owned());
    }
    Ok(())
}

fn exact_string(value: &Value, field: &str, expected: &str) -> Result<(), String> {
    let actual = value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("field `{field}` is missing or not a string"))?;
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "field `{field}` expected `{expected}`, found `{actual}`"
        ))
    }
}

fn exact_string_array(value: &Value, field: &str, expected: &[String]) -> Result<(), String> {
    let observed = value
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("field `{field}` is missing or not an array"))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("field `{field}` contains a non-string"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if observed == expected {
        Ok(())
    } else {
        Err(format!("field `{field}` does not match the registry"))
    }
}

fn require_exact_fields(value: &Value, expected: &[&str], context: &str) -> Result<(), String> {
    let object = value
        .as_object()
        .ok_or_else(|| format!("{context} is not an object"))?;
    require_exact_object_fields(object, expected, context)
}

fn require_exact_object_fields(
    object: &Map<String, Value>,
    expected: &[&str],
    context: &str,
) -> Result<(), String> {
    let observed = object.keys().map(String::as_str).collect::<BTreeSet<_>>();
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
    if observed == expected {
        Ok(())
    } else {
        let missing = expected.difference(&observed).copied().collect::<Vec<_>>();
        let unknown = observed.difference(&expected).copied().collect::<Vec<_>>();
        Err(format!(
            "{context} has missing fields {missing:?} or unknown fields {unknown:?}"
        ))
    }
}

fn required_object<'a>(value: &'a Value, field: &str) -> Result<&'a Map<String, Value>, String> {
    value
        .get(field)
        .and_then(Value::as_object)
        .ok_or_else(|| format!("field `{field}` is missing or not an object"))
}

fn required_u64(value: &Value, field: &str) -> Result<u64, String> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("field `{field}` is missing or not an integer"))
}

fn required_map_str<'a>(value: &'a Map<String, Value>, field: &str) -> Result<&'a str, String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("field `{field}` is missing or not a string"))
}

fn required_map_u64(value: &Map<String, Value>, field: &str) -> Result<u64, String> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("field `{field}` is missing or not an integer"))
}

fn required_map_bool(value: &Map<String, Value>, field: &str) -> Result<bool, String> {
    value
        .get(field)
        .and_then(Value::as_bool)
        .ok_or_else(|| format!("field `{field}` is missing or not a boolean"))
}

fn required_map_u64_array(value: &Map<String, Value>, field: &str) -> Result<Vec<u64>, String> {
    value
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("field `{field}` is missing or not an array"))?
        .iter()
        .map(|value| {
            value
                .as_u64()
                .ok_or_else(|| format!("field `{field}` contains a non-integer"))
        })
        .collect()
}
