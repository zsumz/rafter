//! Validation and derivation of fairness and round-budget bounds.

use serde_json::{Map, Value};

use super::json::{
    require_exact_object_fields, required_map_bool, required_map_str, required_map_u64,
};
use crate::contract::profile::{SimulatorExecutionContract, SimulatorLivenessContract};

pub(super) fn validate_fairness(
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

pub(super) fn validate_round_budget(
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
    let expected_base = derive_base_rounds(
        contract,
        node_count,
        queued_messages,
        max_proposals,
        max_membership_changes,
        max_partitions,
        snapshot_probe,
    )?;
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

#[allow(clippy::too_many_arguments)]
fn derive_base_rounds(
    contract: &SimulatorLivenessContract,
    node_count: u64,
    queued_messages: u64,
    max_proposals: u64,
    max_membership_changes: u64,
    max_partitions: u64,
    snapshot_probe: bool,
) -> Result<u64, String> {
    contract
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
        .ok_or_else(|| "round-budget derivation overflowed".to_owned())
}

fn weight(value: u64, multiplier: u64) -> Result<u64, String> {
    value
        .checked_mul(multiplier)
        .ok_or_else(|| "round-budget component overflowed".to_owned())
}
