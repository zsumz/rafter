//! Detector-level negative fixtures for bounded-liveness reports.

use serde_json::{json, Value};

use super::super::{verify_present_liveness_reports, LivenessReportErrorKind};
use super::fixture::{derive, fixture, report_array_mut, report_mut};

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
fn impossible_reachable_voter_count_is_malformed_without_panicking() {
    let (identity, contracts, mut events) = fixture();
    let report = report_mut(&mut events, "proposal-progress");
    report["preconditions"]["reachable_voters"] = json!(4);
    report["preconditions"]["unavailable_voters"] = json!(0);
    let error = derive(&identity, &contracts, &events)
        .expect_err("reachable voters above membership must fail");
    assert_eq!(error.kind, LivenessReportErrorKind::Malformed);
    assert!(error.message.contains("quorum"));
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
fn fault_cycle_endpoints_must_be_configured_voters() {
    let (identity, contracts, mut events) = fixture();
    let cycle = &mut report_mut(&mut events, "leader-convergence")["fault_cycle"];
    cycle["partition_a"] = json!(99);
    cycle["partition_b"] = json!(100);
    let error =
        derive(&identity, &contracts, &events).expect_err("invented partition endpoints must fail");
    assert_eq!(error.kind, LivenessReportErrorKind::Malformed);
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

#[test]
fn operation_evidence_is_feature_specific_and_fail_closed() {
    for feature in [
        "read-barrier",
        "membership-transition",
        "leadership-transfer",
        "snapshot-catch-up",
    ] {
        let (identity, contracts, mut events) = fixture();
        report_mut(&mut events, feature)["operation"]["terminal_outcome"] = json!("unknown");
        let error = derive(&identity, &contracts, &events)
            .expect_err("an invalid feature-specific terminal outcome must fail");
        assert!(error
            .message
            .contains("operation identity or terminal outcome"));
    }

    let (identity, contracts, mut events) = fixture();
    report_mut(&mut events, "read-barrier")["operation"] = Value::Null;
    let error = derive(&identity, &contracts, &events)
        .expect_err("missing operation evidence must fail closed");
    assert!(error.message.contains("operation"));
}

#[test]
fn nonpassing_events_cannot_hide_malformed_structured_reports() {
    let (identity, contracts, mut events) = fixture();
    events.get_mut("raft-soak").expect("soak events")[0]["status"] = json!("incomplete");
    report_mut(&mut events, "proposal-progress")["proposal"]["terminal_outcome"] = json!("pending");

    let error = verify_present_liveness_reports("pr", &identity, &contracts, &events)
        .expect_err("nonpassing structured report must still be verified");
    assert_eq!(error.kind, LivenessReportErrorKind::Malformed);
    assert!(error.message.contains("proposal terminal outcome"));

    let (_, _, mut empty_events) = fixture();
    empty_events.get_mut("raft-soak").expect("soak events")[0]["status"] = json!("incomplete");
    empty_events.get_mut("raft-soak").expect("soak events")[0]["liveness_reports"] = json!([]);
    verify_present_liveness_reports("pr", &identity, &contracts, &empty_events)
        .expect("an empty nonpassing report set is a legitimate coverage miss");
}
