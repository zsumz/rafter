use std::time::Duration;

use rafter_sim::{
    model_check::{run_raft_random_soak, FailureKind, SoakConfig, SoakSummary},
    SimSeed,
};
use serde_json::json;

use crate::raft_config::{four_node_future_learner_configs, three_node_configs};

use super::{
    failure_event, soak_event, soak_event_from_reports, soak_event_from_reports_with_contract,
    test_execution_contract,
};

#[test]
fn machine_failure_event_preserves_classification_and_message() {
    let event = failure_event(
        "raft-commit",
        FailureKind::CoverageNotReached,
        "CM-02",
        "required witness absent",
    );
    assert_eq!(event["event"], "check-failure");
    assert_eq!(event["status"], "incomplete");
    assert_eq!(event["classification"], "coverage-not-reached");
    assert_eq!(event["invariant"], "CM-02");
    assert_eq!(event["message"], "required witness absent");
}

#[test]
fn soak_event_derives_liveness_evidence_from_monitor_reports() {
    let config = SoakConfig::new(SimSeed(0x51_7e), 0);
    let summary = run_raft_random_soak(three_node_configs(2), config)
        .expect("zero-step soak should complete measured liveness monitors");
    let event = soak_event("raft-soak", &summary, config, &[], Duration::from_millis(7));

    assert_eq!(event["liveness_reports"].as_array().map(Vec::len), Some(6));
    assert_eq!(event["observations"]["post_heal_quiescent_leaders"], 1);
    assert_eq!(event["observations"]["terminated_liveness_proposals"], 1);
    assert!(event["observations"]
        .get("completed_liveness_read_barriers")
        .is_none());
    assert!(event["liveness_reports"].as_array().is_some_and(|reports| {
        reports
            .iter()
            .all(|report| report["round_limit"].is_number() && report["rounds_used"].is_number())
    }));
}

#[test]
fn pr_membership_soak_emits_a_passing_ten_report_event() {
    let config = SoakConfig::new(SimSeed(0x9104), 320)
        .with_max_proposals(24)
        .with_max_restarts(12)
        .with_max_read_indexes(4)
        .with_max_membership_changes(8)
        .with_max_transfers(2)
        .with_max_partitions(2)
        .with_max_lossy_restarts(2)
        .with_snapshot_catchup_probe()
        .with_tick_skew(rafter::NodeId(1), 3);
    let summary = run_raft_random_soak(four_node_future_learner_configs(3), config)
        .expect("the exact PR membership soak should complete");
    let event = soak_event(
        "raft-soak-membership",
        &summary,
        config,
        &[],
        Duration::ZERO,
    );

    assert_eq!(event["status"], "pass");
    assert_eq!(event["liveness_reports"].as_array().map(Vec::len), Some(10));
}

#[test]
fn soak_event_fails_closed_on_missing_liveness_report() {
    let (summary, config, mut reports) = base_soak_reports();
    reports.pop();
    let event =
        soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);

    assert_harness_error(&event, "expected 6 liveness reports");
    assert!(event["observations"]
        .as_object()
        .is_some_and(serde_json::Map::is_empty));
}

#[test]
fn soak_event_fails_closed_on_duplicate_liveness_report() {
    let (summary, config, mut reports) = base_soak_reports();
    reports[3] = reports[0].clone();
    let event =
        soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);

    assert_harness_error(&event, "duplicate liveness feature report");
}

#[test]
fn soak_event_fails_closed_on_unknown_feature_identity() {
    let (summary, config, mut reports) = base_soak_reports();
    reports[0]["feature_id"] = json!("invented-feature");
    let event =
        soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);

    assert_harness_error(&event, "missing liveness feature report");
}

#[test]
fn soak_event_fails_closed_on_malformed_report_structure() {
    let (summary, config, mut reports) = base_soak_reports();
    reports[0]
        .as_object_mut()
        .expect("report is an object")
        .remove("round_limit");
    let event =
        soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);

    assert_harness_error(&event, "round_limit");
}

#[test]
fn soak_event_fails_closed_on_missing_required_leader_evidence() {
    let (summary, config, mut reports) = base_soak_reports();
    reports[0]["stable_leader"] = serde_json::Value::Null;
    let event =
        soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);

    assert_harness_error(&event, "stable_leader");
}

#[test]
fn soak_event_fails_closed_on_wrong_scenario_identity() {
    let (summary, config, mut reports) = base_soak_reports();
    reports[0]["scenario_id"] = json!("quorum-only-output-relabelled");
    let event =
        soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);

    assert_harness_error(&event, "scenario_id");
}

#[test]
fn soak_event_fails_closed_on_false_precondition() {
    let (summary, config, mut reports) = base_soak_reports();
    reports[0]["preconditions"]["mutually_reachable_quorum"] = json!(false);
    let event =
        soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);

    assert_harness_error(&event, "mutually_reachable_quorum");
}

#[test]
fn soak_event_fails_closed_on_tampered_delivery_fairness() {
    let (summary, config, mut reports) = base_soak_reports();
    reports[0]["fairness"]["max_delivery_waves_per_tick"] = json!(65);
    let event =
        soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);

    assert_harness_error(&event, "fairness evidence");
}

#[test]
fn soak_event_accepts_every_explicit_proposal_termination_outcome() {
    for outcome in ["committed", "rejected", "canceled", "unknown"] {
        let (summary, config, mut reports) = base_soak_reports();
        let report = report_mut(&mut reports, "proposal-termination");
        report["proposal"]["terminal_outcome"] = json!(outcome);
        let event =
            soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);

        assert_eq!(event["status"], "pass", "outcome {outcome}: {event}");
    }
}

#[test]
fn soak_event_rejects_missing_or_nonterminal_proposal_outcome() {
    for (outcome, expected_message) in [
        (serde_json::Value::Null, "`proposal` evidence is malformed"),
        (json!("pending"), "proposal terminal outcome"),
    ] {
        let (summary, config, mut reports) = base_soak_reports();
        report_mut(&mut reports, "proposal-termination")["proposal"]["terminal_outcome"] = outcome;
        let event =
            soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);

        assert_harness_error(&event, expected_message);
    }
}

#[test]
fn soak_event_binds_leader_retention_to_each_scenario() {
    for (feature_id, tampered) in [
        ("leader-convergence", false),
        ("proposal-termination", true),
    ] {
        let (summary, config, mut reports) = base_soak_reports();
        report_mut(&mut reports, feature_id)["stable_leader"]["remained_leader_through_probe"] =
            json!(tampered);
        let event =
            soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);

        assert_harness_error(&event, "leader-retention evidence");
    }
}

#[test]
fn soak_event_rejects_tampered_exact_round_limit() {
    let (summary, config, mut reports) = base_soak_reports();
    let report = report_mut(&mut reports, "leader-convergence");
    report["round_limit"] = json!(report["round_limit"].as_u64().unwrap() + 1);
    let event =
        soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);

    assert_harness_error(&event, "typed execution provenance");
}

#[test]
fn soak_event_rejects_tampered_round_budget_derivation() {
    for field in ["base_rounds", "phase_count"] {
        let (summary, config, mut reports) = base_soak_reports();
        let report = report_mut(&mut reports, "leader-convergence");
        report["round_budget"][field] = json!(report["round_budget"][field].as_u64().unwrap() + 1);
        let event =
            soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);

        assert_harness_error(&event, "typed execution provenance");
    }
}

#[test]
fn soak_event_rejects_coordinated_round_budget_tampering() {
    let (summary, config, mut reports) = base_soak_reports();
    let report = report_mut(&mut reports, "leader-convergence");
    let phase_count = report["round_budget"]["phase_count"].as_u64().unwrap();
    report["round_budget"]["max_proposals"] =
        json!(report["round_budget"]["max_proposals"].as_u64().unwrap() + 1);
    report["round_budget"]["base_rounds"] =
        json!(report["round_budget"]["base_rounds"].as_u64().unwrap() + 8);
    report["round_limit"] = json!(report["round_limit"].as_u64().unwrap() + 8 * phase_count);
    let event =
        soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);

    assert_harness_error(&event, "typed SoakConfig provenance");
}

#[test]
fn soak_event_rejects_execution_contract_tampering() {
    let (summary, config, reports) = base_soak_reports();
    let mut contract = test_execution_contract("raft-soak", config);
    contract.max_proposals += 1;
    let event = soak_event_from_reports_with_contract(
        &contract,
        &summary,
        config,
        &[],
        Duration::ZERO,
        &reports,
    );

    assert_harness_error(&event, "does not match the actual SoakConfig");
}

#[test]
fn soak_event_rejects_rounds_used_not_backed_by_execution() {
    let (summary, config, mut reports) = base_soak_reports();
    let report = report_mut(&mut reports, "leader-convergence");
    report["rounds_used"] = json!(report["rounds_used"].as_u64().unwrap() + 1);
    let event =
        soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);

    assert_harness_error(&event, "rounds_used");
}

#[test]
fn soak_event_rejects_fault_state_relabeling() {
    let (summary, config, mut reports) = base_soak_reports();
    let preconditions =
        &mut report_mut(&mut reports, "quorum-only-leader-convergence")["preconditions"];
    preconditions["fault_requirement"] = json!("stopped");
    preconditions["faults_stopped"] = json!(true);
    preconditions["partition_active"] = json!(false);
    let event =
        soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);

    assert_harness_error(&event, "fault-state evidence");
}

#[test]
fn soak_event_requires_a_real_healed_fault_cycle() {
    for field in [
        "partition_observed",
        "partition_active_after_exercise",
        "heal_observed",
        "protocol_state_changed",
    ] {
        let (summary, config, mut reports) = base_soak_reports();
        report_mut(&mut reports, "leader-convergence")["fault_cycle"][field] = json!(false);
        let event =
            soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);

        assert_harness_error(&event, "fault-cycle evidence");
    }

    let (summary, config, mut reports) = base_soak_reports();
    report_mut(&mut reports, "leader-convergence")["fault_cycle"]["ticks_executed"] = json!(0);
    let event =
        soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);
    assert_harness_error(&event, "fault-cycle evidence");
}

#[test]
fn soak_event_requires_the_exact_optional_feature_set() {
    let config = SoakConfig::new(SimSeed(0x51_7e), 0)
        .with_max_read_indexes(1)
        .with_max_membership_changes(1)
        .with_max_transfers(1)
        .with_snapshot_catchup_probe();
    let summary = run_raft_random_soak(three_node_configs(2), config)
        .expect("all optional liveness fixtures should complete");
    let event = soak_event("raft-soak", &summary, config, &[], Duration::ZERO);

    assert_eq!(event["status"], "pass");
    assert_eq!(event["liveness_reports"].as_array().map(Vec::len), Some(10));
    assert_eq!(
        event["liveness_features"].as_array().map(Vec::len),
        Some(10)
    );
}

fn base_soak_reports() -> (SoakSummary, SoakConfig, Vec<serde_json::Value>) {
    let config = SoakConfig::new(SimSeed(0x51_7e), 0);
    let summary = run_raft_random_soak(three_node_configs(2), config)
        .expect("zero-step soak should complete measured liveness monitors");
    let reports = summary.liveness_reports_json();
    (summary, config, reports)
}

fn report_mut<'a>(
    reports: &'a mut [serde_json::Value],
    feature_id: &str,
) -> &'a mut serde_json::Value {
    reports
        .iter_mut()
        .find(|report| report["feature_id"] == feature_id)
        .unwrap_or_else(|| panic!("missing {feature_id} report"))
}

fn assert_harness_error(event: &serde_json::Value, message: &str) {
    assert_eq!(event["status"], "error");
    assert_eq!(event["classification"], "harness-error");
    assert!(event["message"]
        .as_str()
        .is_some_and(|actual| actual.contains(message)));
}
