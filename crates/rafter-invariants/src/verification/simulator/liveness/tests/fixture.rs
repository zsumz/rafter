//! Shared valid report fixture and mutation helpers.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use crate::{
    contract::{
        profile::{
            expected_execution_contract, SimulatorExecutionContract, SimulatorLivenessContract,
        },
        SimulatorIdentity,
    },
    evidence::SimulatorLivenessBinding,
    verification::simulator::derive_verified_liveness_binding,
};

use super::super::LivenessReportError;

pub(crate) fn fixture() -> (
    SimulatorIdentity,
    Vec<SimulatorLivenessContract>,
    BTreeMap<String, Vec<Value>>,
) {
    let (catalog, _) = crate::tests::loaded();
    let contracts = catalog
        .evidence
        .iter()
        .filter_map(|descriptor| descriptor.simulator.as_ref()?.liveness_report.clone())
        .map(|contract| (contract.feature_id.clone(), contract))
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect::<Vec<_>>();
    let mut identity = catalog
        .evidence
        .iter()
        .find_map(|descriptor| {
            let identity = descriptor.simulator.as_ref()?;
            (identity.liveness_report.as_ref()?.feature_id == "proposal-progress")
                .then(|| identity.clone())
        })
        .expect("proposal progress identity");
    identity.checks = vec!["raft-soak".to_owned()];
    identity.minimum_runs_per_check = Some(1);
    let execution = expected_execution_contract("pr", "raft-soak").expect("PR execution contract");
    let reports = contracts
        .iter()
        .map(|contract| valid_report(contract, &execution))
        .collect::<Vec<_>>();
    let liveness_features = reports
        .iter()
        .filter_map(|report| report["feature_id"].as_str())
        .collect::<Vec<_>>();
    let events = BTreeMap::from([(
        "raft-soak".to_owned(),
        vec![json!({
            "event": "soak-check",
            "check_id": "raft-soak",
            "status": "pass",
            "classification": null,
            "message": null,
            "seed": 1,
            "steps": 320,
            "duration_ms": 1,
            "execution_contract": execution,
            "observed_actions": ["tick", "deliver"],
            "liveness_features": liveness_features,
            "observations": {"accepted_completed_liveness_proposals": 99},
            "liveness_reports": reports,
        })],
    )]);
    (identity, contracts, events)
}

pub(super) fn derive(
    identity: &SimulatorIdentity,
    contracts: &[SimulatorLivenessContract],
    events: &BTreeMap<String, Vec<Value>>,
) -> Result<SimulatorLivenessBinding, LivenessReportError> {
    derive_verified_liveness_binding("pr", identity, contracts, events)
}

pub(super) fn report_array_mut(events: &mut BTreeMap<String, Vec<Value>>) -> &mut Vec<Value> {
    events.get_mut("raft-soak").expect("soak events")[0]["liveness_reports"]
        .as_array_mut()
        .expect("liveness report array")
}

pub(super) fn report_mut<'a>(
    events: &'a mut BTreeMap<String, Vec<Value>>,
    feature_id: &str,
) -> &'a mut Value {
    report_array_mut(events)
        .iter_mut()
        .find(|report| report["feature_id"] == feature_id)
        .unwrap_or_else(|| panic!("missing report {feature_id}"))
}

#[allow(clippy::too_many_lines)]
fn valid_report(
    contract: &SimulatorLivenessContract,
    execution: &SimulatorExecutionContract,
) -> Value {
    let active_partition = contract.fault_requirement == "active-partition";
    let stable_required = contract.stable_leader_retained.is_some();
    let proposal_required = contract.proposal_outcome != "none";
    let reachable_voters = if active_partition { 2 } else { 3 };
    let rounds_used = contract.stable_leader_rounds_exact.unwrap_or(1);
    let node_count = if matches!(
        contract.feature_id.as_str(),
        "leader-convergence" | "leader-usability"
    ) {
        execution.node_count
    } else {
        3
    };
    let base_rounds = contract.minimum_rounds
        + node_count * contract.rounds_per_node
        + execution.max_proposals * contract.rounds_per_proposal
        + execution.max_membership_changes * contract.rounds_per_membership_change
        + execution.max_partitions * contract.rounds_per_partition
        + contract.snapshot_catchup_rounds;
    let round_limit = base_rounds * contract.phase_count + contract.fixed_rounds;
    let fault_cycle = contract.fault_cycle_required.then(|| {
        json!({
            "partition_a": 1,
            "partition_b": 2,
            "partition_observed": true,
            "partitioned_rounds": contract.fixed_rounds,
            "nodes_exercised": 3,
            "ticks_executed": contract.fixed_rounds * 3,
            "deliveries_executed": 1,
            "drops_executed": 0,
            "protocol_state_changed": true,
            "partition_active_after_exercise": true,
            "heal_observed": true
        })
    });
    let stable_leader = stable_required.then(|| {
        json!({
            "node_id": 1,
            "stable_rounds": rounds_used.max(1),
            "remained_leader_through_probe": contract.stable_leader_retained
        })
    });
    let proposal = proposal_required.then(|| {
        json!({
            "proposal_id": 1,
            "terminal_outcome": if contract.proposal_outcome == "committed" {
                "committed"
            } else {
                "unknown"
            }
        })
    });
    let operation = match contract.feature_id.as_str() {
        "read-barrier" => Some(json!({
            "operation_id": "read:1",
            "terminal_outcome": "completed"
        })),
        "membership-transition" => Some(json!({
            "operation_id": "remove-voter:1:3",
            "terminal_outcome": "committed"
        })),
        "leadership-transfer" => Some(json!({
            "operation_id": "transfer:1->2",
            "terminal_outcome": "completed"
        })),
        "snapshot-catch-up" => Some(json!({
            "operation_id": "snapshot:fixture",
            "terminal_outcome": "installed"
        })),
        _ => None,
    };
    json!({
        "invariant_id": contract.invariant_id,
        "clause_ids": contract.clause_ids,
        "feature_id": contract.feature_id,
        "scenario_id": contract.scenario_id,
        "observation_id": contract.observation_id,
        "preconditions": {
            "fault_requirement": contract.fault_requirement,
            "fault_state_satisfied": true,
            "fault_state_status": "satisfied",
            "faults_stopped": !active_partition,
            "partition_active": active_partition,
            "mutually_reachable_quorum": true,
            "mutually_reachable_quorum_status": "satisfied",
            "stable_membership": true,
            "stable_membership_status": "satisfied",
            "stable_leader_required": stable_required,
            "stable_leader_satisfied": stable_required,
            "stable_leader_status": if stable_required { "satisfied" } else { "not-required" },
            "accepted_proposal_required": proposal_required,
            "accepted_proposal_satisfied": proposal_required,
            "accepted_proposal_status": if proposal_required { "satisfied" } else { "not-required" },
            "authority_loss_required": contract.authority_loss_required,
            "authority_loss_satisfied": contract.authority_loss_required,
            "authority_loss_status": if contract.authority_loss_required { "satisfied" } else { "not-required" },
            "voter_ids": [1, 2, 3],
            "reachable_voters": reachable_voters,
            "quorum_size": 2,
            "unavailable_voters": 3 - reachable_voters
        },
        "fairness": {
            "policy_id": contract.fairness_policy_id,
            "tick_bound_rounds": contract.fairness_tick_bound_rounds,
            "delivery_bound_rounds": contract.fairness_delivery_bound_rounds,
            "max_delivery_waves_per_tick": contract.fairness_max_delivery_waves_per_tick
        },
        "round_budget": {
            "minimum_rounds": contract.minimum_rounds,
            "node_count": node_count,
            "queued_messages": 0,
            "max_proposals": execution.max_proposals,
            "max_membership_changes": execution.max_membership_changes,
            "max_partitions": execution.max_partitions,
            "snapshot_catchup_probe": execution.snapshot_catchup_probe,
            "base_rounds": base_rounds,
            "phase_count": contract.phase_count,
            "fixed_rounds": contract.fixed_rounds
        },
        "round_limit": round_limit,
        "rounds_used": rounds_used,
        "fault_cycle": fault_cycle,
        "stable_leader": stable_leader,
        "proposal": proposal,
        "operation": operation
    })
}
