//! Validation of partition, exercise, and healing cycle evidence.

use serde_json::Value;

use super::json::{require_exact_object_fields, required_map_bool, required_map_u64};
use crate::contract::profile::SimulatorLivenessContract;

pub(super) fn validate_fault_cycle(
    contract: &SimulatorLivenessContract,
    voter_ids: &[u64],
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
        || !voter_ids.contains(&partition_a)
        || !voter_ids.contains(&partition_b)
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
