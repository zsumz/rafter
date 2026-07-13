use std::collections::BTreeMap;

use serde_json::{json, Value};

use super::*;
use crate::types::{
    SimulatorExecutionContract, SimulatorLivenessBinding, SimulatorLivenessContract,
};

#[test]
fn missing_and_duplicate_reports_fail_closed() {
    let (identity, contracts, mut missing_events) = fixture();
    report_array_mut(&mut missing_events).remove(0);
    let missing = derive(&identity, &contracts, &missing_events)
        .expect_err("missing feature report must fail");
    assert_eq!(missing.kind, LivenessReportErrorKind::Missing);

    let (_, _, mut duplicate_events) = fixture();
    let duplicate = report_array_mut(&mut duplicate_events)[0].clone();
    report_array_mut(&mut duplicate_events).push(duplicate);
    let duplicate =
        derive(&identity, &contracts, &duplicate_events).expect_err("duplicate report must fail");
    assert_eq!(duplicate.kind, LivenessReportErrorKind::Malformed);
    assert!(duplicate.message.contains("duplicate feature"));
}

#[test]
fn swapped_report_identity_is_malformed() {
    for (field, value) in [
        ("invariant_id", json!("LV-01")),
        ("feature_id", json!("invented-feature")),
        ("scenario_id", json!("accepted-proposal-authority-loss-v1")),
        ("observation_id", json!("terminated_liveness_proposals")),
        ("clause_ids", json!(["LV-02.b"])),
    ] {
        let (identity, contracts, mut events) = fixture();
        report_mut(&mut events, "proposal-progress")[field] = value;
        let error = derive(&identity, &contracts, &events).expect_err("swapped identity must fail");
        assert_eq!(error.kind, LivenessReportErrorKind::Malformed);
    }
}

#[test]
fn false_precondition_is_malformed() {
    let (identity, contracts, mut events) = fixture();
    report_mut(&mut events, "proposal-progress")["preconditions"]["mutually_reachable_quorum"] =
        json!(false);
    let error = derive(&identity, &contracts, &events).expect_err("false precondition must fail");
    assert_eq!(error.kind, LivenessReportErrorKind::Malformed);
    assert!(error.message.contains("precondition"));
}

#[test]
fn fairness_tamper_is_malformed() {
    let (identity, contracts, mut events) = fixture();
    report_mut(&mut events, "proposal-progress")["fairness"]["max_delivery_waves_per_tick"] =
        json!(65);
    let error = derive(&identity, &contracts, &events).expect_err("fairness tamper must fail");
    assert_eq!(error.kind, LivenessReportErrorKind::Malformed);
    assert!(error.message.contains("fairness"));
}

#[test]
fn bound_or_provenance_tamper_is_malformed() {
    for field in ["base_rounds", "max_proposals"] {
        let (identity, contracts, mut events) = fixture();
        report_mut(&mut events, "proposal-progress")["round_budget"][field] = json!(999);
        let error = derive(&identity, &contracts, &events).expect_err("bound tamper must fail");
        assert_eq!(error.kind, LivenessReportErrorKind::Malformed);
        assert!(error.message.contains("round"));
    }
}

#[test]
fn no_op_fault_cycle_is_malformed() {
    let (identity, contracts, mut events) = fixture();
    report_mut(&mut events, "leader-convergence")["fault_cycle"]["protocol_state_changed"] =
        json!(false);
    let error =
        derive(&identity, &contracts, &events).expect_err("a no-op partition cycle must fail");
    assert!(error.message.contains("fault-cycle"));
}

#[test]
fn wrong_leader_retention_or_proposal_outcome_is_malformed() {
    let (identity, contracts, mut leader_events) = fixture();
    report_mut(&mut leader_events, "proposal-progress")["stable_leader"]
        ["remained_leader_through_probe"] = json!(false);
    let leader_error = derive(&identity, &contracts, &leader_events)
        .expect_err("leader retention tamper must fail");
    assert!(leader_error.message.contains("retention"));

    let (_, _, mut invented_voters) = fixture();
    let report = report_mut(&mut invented_voters, "proposal-progress");
    report["preconditions"]["voter_ids"] = json!([4, 5, 6]);
    report["stable_leader"]["node_id"] = json!(4);
    let leader_error = derive(&identity, &contracts, &invented_voters)
        .expect_err("invented voter and leader identities must fail");
    assert!(leader_error.message.contains("quorum"));

    let (_, _, mut proposal_events) = fixture();
    report_mut(&mut proposal_events, "proposal-progress")["proposal"]["terminal_outcome"] =
        json!("pending");
    let proposal_error = derive(&identity, &contracts, &proposal_events)
        .expect_err("proposal outcome tamper must fail");
    assert!(proposal_error.message.contains("proposal terminal outcome"));
}

#[test]
fn coordinated_execution_contract_and_round_budget_tamper_is_rejected() {
    let (identity, contracts, mut events) = fixture();
    let event = &mut events.get_mut("raft-soak").expect("soak events")[0];
    event["execution_contract"]["max_proposals"] = json!(25);
    for report in report_array_mut(&mut events) {
        report["round_budget"]["max_proposals"] = json!(25);
        let phase_count = report["round_budget"]["phase_count"]
            .as_u64()
            .expect("phase count");
        let fixed_rounds = report["round_budget"]["fixed_rounds"]
            .as_u64()
            .expect("fixed rounds");
        report["round_budget"]["base_rounds"] = json!(600);
        report["round_limit"] = json!(600 * phase_count + fixed_rounds);
    }
    let error =
        derive(&identity, &contracts, &events).expect_err("coordinated execution tamper must fail");
    assert!(error.message.contains("execution contract"));
}

#[test]
fn unknown_fields_and_complete_set_substitution_are_rejected() {
    let (identity, contracts, mut unknown_field_events) = fixture();
    report_mut(&mut unknown_field_events, "proposal-progress")["invented"] = json!(true);
    let error = derive(&identity, &contracts, &unknown_field_events)
        .expect_err("unknown report field must fail");
    assert!(error.message.contains("unknown fields"));

    let (_, _, mut substituted_events) = fixture();
    report_mut(&mut substituted_events, "snapshot-catch-up")["feature_id"] =
        json!("invented-feature");
    let error = derive(&identity, &contracts, &substituted_events)
        .expect_err("feature-set substitution must fail");
    assert_eq!(error.kind, LivenessReportErrorKind::Malformed);
}

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
            "execution_contract": execution,
            "observations": {"accepted_completed_liveness_proposals": 99},
            "liveness_reports": reports,
        })],
    )]);
    (identity, contracts, events)
}

fn derive(
    identity: &SimulatorIdentity,
    contracts: &[SimulatorLivenessContract],
    events: &BTreeMap<String, Vec<Value>>,
) -> Result<SimulatorLivenessBinding, LivenessReportError> {
    derive_liveness_binding("pr", identity, contracts, events)
}

fn report_array_mut(events: &mut BTreeMap<String, Vec<Value>>) -> &mut Vec<Value> {
    events.get_mut("raft-soak").expect("soak events")[0]["liveness_reports"]
        .as_array_mut()
        .expect("liveness report array")
}

fn report_mut<'a>(events: &'a mut BTreeMap<String, Vec<Value>>, feature_id: &str) -> &'a mut Value {
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
        "proposal": proposal
    })
}
