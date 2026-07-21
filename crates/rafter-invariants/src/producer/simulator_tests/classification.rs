//! Failure classification and shared-check routing scenarios.

use std::collections::BTreeMap;

use super::super::{
    evaluate_descriptors, model_observations, simulator_event_issue, SimulatorIssue,
};
use super::support::{empty_detectors, model_fixture, standalone_safety_descriptor};
use crate::{EvidenceStatus, FailureClassification, SimulatorIdentity};
use serde_json::json;

#[test]
fn simulator_failure_classifications_remain_distinct() {
    let invariant = simulator_event_issue(
        "raft-soak",
        "CM-02",
        &json!({
            "event": "check-failure",
            "event_version": 2,
            "status": "fail",
            "classification": "invariant-violation",
            "invariant_id": "CM-02",
            "invariant": "CM-02 commit requires effective quorum",
        }),
    );
    assert!(matches!(
        invariant,
        Some(SimulatorIssue::InvariantViolation(_))
    ));

    let incomplete = simulator_event_issue(
        "raft-soak",
        "CM-02",
        &json!({"status": "incomplete", "classification": "coverage-not-reached"}),
    );
    assert!(matches!(
        incomplete,
        Some(SimulatorIssue::CoverageNotReached(_))
    ));

    let malformed = simulator_event_issue(
        "raft-soak",
        "CM-02",
        &json!({"status": "error", "classification": "harness-error"}),
    );
    assert!(matches!(malformed, Some(SimulatorIssue::HarnessError(_))));
}

#[test]
fn safety_model_events_preserve_their_structured_failure_classification() {
    let identity = SimulatorIdentity {
        checks: vec!["raft-commit".to_owned()],
        required_observation: "commit_floor_advances".to_owned(),
        minimum_observation: 1,
        minimum_protocol_states: Some(1),
        minimum_verifier_states: Some(1),
        minimum_runs_per_check: None,
        minimum_steps: None,
        liveness_report: None,
        negative_test: None,
    };
    let events = BTreeMap::from([(
        "raft-commit".to_owned(),
        vec![json!({
            "event": "check-failure",
            "event_version": 2,
            "status": "fail",
            "classification": "invariant-violation",
            "invariant_id": "CM-02",
            "invariant": "CM-02 commit requires effective quorum",
            "message": "commit witness violated"
        })],
    )]);

    let evidence = model_observations("pr", "CM-02", &identity, &BTreeMap::new(), &[], &events);

    assert!(matches!(
        evidence.issue,
        Some(SimulatorIssue::InvariantViolation(message))
            if message == "commit witness violated"
    ));
}

#[test]
fn shared_model_check_routes_a_counterexample_only_to_its_invariant() {
    let (catalog, _) = crate::tests::loaded();
    let mut matching = standalone_safety_descriptor(&catalog.evidence);
    matching.invariant_id = "LG-03".to_owned();
    matching.clause_id = "LG-03.a".to_owned();
    let mut sibling = matching.clone();
    sibling.invariant_id = "LG-04".to_owned();
    sibling.clause_id = "LG-04.a".to_owned();
    let check_id = matching
        .simulator
        .as_ref()
        .expect("simulator identity")
        .checks[0]
        .clone();
    let model = model_fixture(BTreeMap::from([(
        check_id.clone(),
        vec![json!({
            "event": "check-failure",
            "event_version": 2,
            "check_id": check_id,
            "status": "fail",
            "classification": "invariant-violation",
            "invariant_id": "LG-03",
            "invariant": "LG-03 log matching",
            "message": "shared check found a log-matching counterexample",
        })],
    )]));

    let (_, results) = evaluate_descriptors(
        &[matching, sibling],
        "pr",
        &BTreeMap::new(),
        &[],
        &model,
        &empty_detectors(),
    )
    .expect("route shared-check counterexample");

    let matching = results
        .iter()
        .find(|result| result.invariant_id == "LG-03")
        .expect("matching invariant result");
    assert_eq!(matching.status, EvidenceStatus::Fail);
    assert_eq!(
        matching.classification,
        Some(FailureClassification::InvariantViolation)
    );
    let sibling = results
        .iter()
        .find(|result| result.invariant_id == "LG-04")
        .expect("sibling invariant result");
    assert_eq!(sibling.status, EvidenceStatus::Incomplete);
    assert_eq!(
        sibling.classification,
        Some(FailureClassification::CoverageNotReached)
    );
}

#[test]
fn contradictory_pass_reduces_to_a_harness_error() {
    let (catalog, _) = crate::tests::loaded();
    let descriptor = standalone_safety_descriptor(&catalog.evidence);
    let check_id = descriptor
        .simulator
        .as_ref()
        .expect("simulator identity")
        .checks[0]
        .clone();
    let model = model_fixture(BTreeMap::from([(
        check_id.clone(),
        vec![json!({
            "check_id": check_id,
            "status": "pass",
            "classification": "invariant-violation",
        })],
    )]));

    let (_, results) = evaluate_descriptors(
        std::slice::from_ref(&descriptor),
        "pr",
        &BTreeMap::new(),
        &[],
        &model,
        &empty_detectors(),
    )
    .expect("reduce contradictory pass");

    assert_eq!(results[0].status, EvidenceStatus::Error);
    assert_eq!(
        results[0].classification,
        Some(FailureClassification::HarnessError)
    );
    assert!(results[0]
        .message
        .as_deref()
        .is_some_and(|message| message.contains("invalid status/classification pair")));
}

#[test]
fn unknown_pass_and_invariant_violation_reduce_to_the_same_deterministic_harness_error() {
    let (catalog, _) = crate::tests::loaded();
    let descriptor = standalone_safety_descriptor(&catalog.evidence);
    let mut messages = Vec::new();
    for event in [
        json!({"check_id": "unknown-check", "status": "pass"}),
        json!({
            "event": "check-failure",
            "event_version": 2,
            "check_id": "unknown-check",
            "status": "fail",
            "classification": "invariant-violation",
            "invariant_id": descriptor.invariant_id,
            "invariant": format!("{} test invariant", descriptor.invariant_id),
        }),
    ] {
        let model = model_fixture(BTreeMap::from([("unknown-check".to_owned(), vec![event])]));
        let (_, results) = evaluate_descriptors(
            std::slice::from_ref(&descriptor),
            "pr",
            &BTreeMap::new(),
            &[],
            &model,
            &empty_detectors(),
        )
        .expect("reduce unknown event");
        assert_eq!(results[0].status, EvidenceStatus::Error);
        assert_eq!(
            results[0].classification,
            Some(FailureClassification::HarnessError)
        );
        messages.push(results[0].message.clone());
    }

    assert_eq!(messages[0], messages[1]);
    assert_eq!(
        messages[0].as_deref(),
        Some("simulator emitted unclaimed machine event check IDs: unknown-check")
    );
}
