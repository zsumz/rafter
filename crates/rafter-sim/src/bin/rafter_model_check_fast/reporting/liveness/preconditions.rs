use std::collections::BTreeSet;

use super::{
    shape::{
        require_exact_object_fields, required_object_u64, required_u64_array,
        validate_required_evidence,
    },
    ExpectedLivenessFeature, FaultRequirement,
};

pub(super) fn validate_fault_cycle(
    report: &serde_json::Value,
    expected: ExpectedLivenessFeature,
) -> Result<(), String> {
    let fault_cycle = report.get("fault_cycle").filter(|value| !value.is_null());
    if !expected.fault_cycle {
        return if fault_cycle.is_none() {
            Ok(())
        } else {
            Err(format!(
                "{} has unexpected fault-cycle evidence",
                expected.feature_id
            ))
        };
    }
    let fault_cycle = fault_cycle
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| format!("{} has no fault-cycle evidence", expected.feature_id))?;
    require_exact_object_fields(
        fault_cycle,
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
    let partition_a = required_object_u64(fault_cycle, "partition_a")?;
    let partition_b = required_object_u64(fault_cycle, "partition_b")?;
    let partition_observed = fault_cycle
        .get("partition_observed")
        .and_then(serde_json::Value::as_bool);
    let heal_observed = fault_cycle
        .get("heal_observed")
        .and_then(serde_json::Value::as_bool);
    let partitioned_rounds = required_object_u64(fault_cycle, "partitioned_rounds")?;
    let nodes_exercised = required_object_u64(fault_cycle, "nodes_exercised")?;
    let ticks_executed = required_object_u64(fault_cycle, "ticks_executed")?;
    let _deliveries_executed = required_object_u64(fault_cycle, "deliveries_executed")?;
    let _drops_executed = required_object_u64(fault_cycle, "drops_executed")?;
    let protocol_state_changed = fault_cycle
        .get("protocol_state_changed")
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| {
            format!(
                "{} has no protocol-state-change evidence",
                expected.feature_id
            )
        })?;
    let partition_active_after_exercise = fault_cycle
        .get("partition_active_after_exercise")
        .and_then(serde_json::Value::as_bool);
    if partition_a == partition_b
        || partition_observed != Some(true)
        || partitioned_rounds != expected.fixed_rounds
        || nodes_exercised < 2
        || ticks_executed != partitioned_rounds.saturating_mul(nodes_exercised)
        || !protocol_state_changed
        || partition_active_after_exercise != Some(true)
        || heal_observed != Some(true)
    {
        return Err(format!(
            "{} has invalid fault-cycle evidence",
            expected.feature_id
        ));
    }
    Ok(())
}

pub(super) fn validate_liveness_preconditions(
    report: &serde_json::Value,
    expected: ExpectedLivenessFeature,
) -> Result<(), String> {
    let preconditions = report
        .get("preconditions")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| format!("{} has no precondition object", expected.feature_id))?;
    require_exact_object_fields(
        preconditions,
        &[
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
        ],
        "liveness preconditions",
    )?;
    validate_fault_precondition(preconditions, expected)?;
    for field in ["mutually_reachable_quorum", "stable_membership"] {
        if preconditions
            .get(field)
            .and_then(serde_json::Value::as_bool)
            != Some(true)
        {
            return Err(format!(
                "{} precondition `{field}` is not satisfied",
                expected.feature_id
            ));
        }
    }
    for field in [
        "mutually_reachable_quorum_status",
        "stable_membership_status",
    ] {
        if preconditions.get(field).and_then(serde_json::Value::as_str) != Some("satisfied") {
            return Err(format!(
                "{} precondition status `{field}` is invalid",
                expected.feature_id
            ));
        }
    }
    validate_quorum_counts(preconditions, expected.feature_id)?;
    validate_required_evidence(
        report,
        preconditions,
        "stable_leader",
        "stable_leader_required",
        "stable_leader_satisfied",
        expected.remained_leader_through_probe.is_some(),
    )?;
    validate_required_evidence(
        report,
        preconditions,
        "proposal",
        "accepted_proposal_required",
        "accepted_proposal_satisfied",
        expected.proposal_outcome.required(),
    )?;
    validate_authority_loss(preconditions, expected)
}

fn validate_fault_precondition(
    preconditions: &serde_json::Map<String, serde_json::Value>,
    expected: ExpectedLivenessFeature,
) -> Result<(), String> {
    let requirement = preconditions
        .get("fault_requirement")
        .and_then(serde_json::Value::as_str);
    let satisfied = preconditions
        .get("fault_state_satisfied")
        .and_then(serde_json::Value::as_bool);
    let status = preconditions
        .get("fault_state_status")
        .and_then(serde_json::Value::as_str);
    let faults_stopped = preconditions
        .get("faults_stopped")
        .and_then(serde_json::Value::as_bool);
    let partition_active = preconditions
        .get("partition_active")
        .and_then(serde_json::Value::as_bool);
    let measured_state_matches = match expected.fault_requirement {
        FaultRequirement::Stopped => {
            faults_stopped == Some(true) && partition_active == Some(false)
        }
        FaultRequirement::ActivePartition => {
            faults_stopped == Some(false) && partition_active == Some(true)
        }
    };
    if requirement == Some(expected.fault_requirement.as_str())
        && satisfied == Some(true)
        && status == Some("satisfied")
        && measured_state_matches
    {
        Ok(())
    } else {
        Err(format!(
            "{} fault-state evidence is inconsistent",
            expected.feature_id
        ))
    }
}

fn validate_quorum_counts(
    preconditions: &serde_json::Map<String, serde_json::Value>,
    feature_id: &str,
) -> Result<(), String> {
    let reachable_voters = required_object_u64(preconditions, "reachable_voters")?;
    let quorum_size = required_object_u64(preconditions, "quorum_size")?;
    let unavailable_voters = required_object_u64(preconditions, "unavailable_voters")?;
    let voters = required_u64_array(preconditions, "voter_ids")?;
    let unique = voters.iter().copied().collect::<BTreeSet<_>>();
    if voters.is_empty()
        || unique.len() != voters.len()
        || voters.contains(&0)
        || quorum_size != voters.len() as u64 / 2 + 1
        || reachable_voters < quorum_size
        || unavailable_voters != voters.len() as u64 - reachable_voters
    {
        Err(format!("{feature_id} has invalid reachable-quorum counts"))
    } else {
        Ok(())
    }
}

fn validate_authority_loss(
    preconditions: &serde_json::Map<String, serde_json::Value>,
    expected: ExpectedLivenessFeature,
) -> Result<(), String> {
    let required = preconditions
        .get("authority_loss_required")
        .and_then(serde_json::Value::as_bool);
    let satisfied = preconditions
        .get("authority_loss_satisfied")
        .and_then(serde_json::Value::as_bool);
    let expected_status = if expected.authority_loss {
        "satisfied"
    } else {
        "not-required"
    };
    let status = preconditions
        .get("authority_loss_status")
        .and_then(serde_json::Value::as_str);
    if required == Some(expected.authority_loss)
        && satisfied == Some(expected.authority_loss)
        && status == Some(expected_status)
    {
        Ok(())
    } else {
        Err(format!(
            "{} authority-loss evidence is inconsistent",
            expected.feature_id
        ))
    }
}

pub(super) fn validate_liveness_fairness(
    report: &serde_json::Value,
    feature_id: &str,
) -> Result<(), String> {
    let fairness = report
        .get("fairness")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| format!("{feature_id} has no fairness evidence"))?;
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
    let valid = fairness
        .get("policy_id")
        .and_then(serde_json::Value::as_str)
        == Some("all-node-ticks-fifo-ready-waves-v1")
        && fairness
            .get("tick_bound_rounds")
            .and_then(serde_json::Value::as_u64)
            == Some(1)
        && fairness
            .get("delivery_bound_rounds")
            .and_then(serde_json::Value::as_u64)
            == Some(1)
        && fairness
            .get("max_delivery_waves_per_tick")
            .and_then(serde_json::Value::as_u64)
            == Some(64);
    if valid {
        Ok(())
    } else {
        Err(format!("{feature_id} has invalid fairness evidence"))
    }
}
