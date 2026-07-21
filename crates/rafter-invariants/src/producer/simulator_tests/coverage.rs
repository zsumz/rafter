//! Composite-witness and per-check coverage scenarios.

use std::collections::BTreeMap;

use super::super::{coverage_reached, model_observations, SimulatorIssue};
use crate::{SimulatorCheckContract, SimulatorIdentity};
use serde_json::json;

#[test]
fn composite_safety_evidence_allows_complementary_checks_to_supply_the_witness() {
    let identity = SimulatorIdentity {
        checks: vec!["primary".to_owned(), "variant".to_owned()],
        required_observation: "commit_floor_advances".to_owned(),
        minimum_observation: 1,
        minimum_protocol_states: Some(10),
        minimum_verifier_states: Some(10),
        minimum_runs_per_check: None,
        minimum_steps: None,
        liveness_report: None,
        negative_test: None,
    };
    let events = BTreeMap::from([
        (
            "primary".to_owned(),
            vec![json!({
                "event": "exhaustive-check",
                "check_id": "primary",
                "status": "pass",
                "classification": null,
                "unique_protocol_states": 10,
                "unique_verifier_states": 10,
                "observations": {"commit_floor_advances": 1},
            })],
        ),
        (
            "variant".to_owned(),
            vec![json!({
                "event": "exhaustive-check",
                "check_id": "variant",
                "status": "pass",
                "classification": null,
                "unique_protocol_states": 1,
                "unique_verifier_states": 1,
                "observations": {},
            })],
        ),
    ]);

    let evidence = model_observations("pr", "CM-02", &identity, &BTreeMap::new(), &[], &events);

    assert_eq!(evidence.observations["commit_floor_advances"], 1);
    assert!(coverage_reached(
        &identity,
        &evidence.observations,
        &evidence.per_check_required_observations,
    ));
}

#[test]
fn composite_safety_evidence_cannot_split_one_witness_minimum_across_checks() {
    let identity = SimulatorIdentity {
        checks: vec!["primary".to_owned(), "variant".to_owned()],
        required_observation: "commit_floor_advances".to_owned(),
        minimum_observation: 2,
        minimum_protocol_states: Some(10),
        minimum_verifier_states: Some(10),
        minimum_runs_per_check: None,
        minimum_steps: None,
        liveness_report: None,
        negative_test: None,
    };
    let event = |check: &str| {
        json!({
            "event": "exhaustive-check",
            "check_id": check,
            "status": "pass",
            "classification": null,
            "unique_protocol_states": 10,
            "unique_verifier_states": 10,
            "observations": {"commit_floor_advances": 1},
        })
    };
    let events = BTreeMap::from([
        ("primary".to_owned(), vec![event("primary")]),
        ("variant".to_owned(), vec![event("variant")]),
    ]);

    let evidence = model_observations("pr", "CM-02", &identity, &BTreeMap::new(), &[], &events);

    assert_eq!(evidence.observations["commit_floor_advances"], 2);
    assert!(!coverage_reached(
        &identity,
        &evidence.observations,
        &evidence.per_check_required_observations,
    ));
}

#[test]
fn malformed_passing_event_cannot_contribute_semantic_observations() {
    let identity = SimulatorIdentity {
        checks: vec!["primary".to_owned()],
        required_observation: "commit_floor_advances".to_owned(),
        minimum_observation: 2,
        minimum_protocol_states: Some(1),
        minimum_verifier_states: Some(1),
        minimum_runs_per_check: None,
        minimum_steps: None,
        liveness_report: None,
        negative_test: None,
    };
    let events = BTreeMap::from([(
        "primary".to_owned(),
        vec![
            json!({
                "event": "exhaustive-check",
                "check_id": "primary",
                "status": "pass",
                "classification": null,
                "unique_protocol_states": 1,
                "unique_verifier_states": 1,
                "observations": {"commit_floor_advances": 1},
            }),
            json!({
                "event": "unsupported-pass-event",
                "check_id": "primary",
                "status": "pass",
                "classification": null,
                "unique_protocol_states": 100,
                "unique_verifier_states": 100,
                "observations": {"commit_floor_advances": 100},
            }),
        ],
    )]);

    let evidence = model_observations(
        "nightly",
        "CM-02",
        &identity,
        &BTreeMap::new(),
        &[],
        &events,
    );

    assert_eq!(evidence.observations["commit_floor_advances"], 1);
    assert!(matches!(
        evidence.issue,
        Some(SimulatorIssue::HarnessError(_))
    ));
    assert!(!coverage_reached(
        &identity,
        &evidence.observations,
        &evidence.per_check_required_observations,
    ));
}

#[test]
fn per_check_profile_floor_cannot_be_borrowed_from_an_established_leg() {
    let identity = SimulatorIdentity {
        checks: vec!["primary".to_owned(), "variant".to_owned()],
        required_observation: "commit_floor_advances".to_owned(),
        minimum_observation: 1,
        minimum_protocol_states: Some(10),
        minimum_verifier_states: Some(10),
        minimum_runs_per_check: None,
        minimum_steps: None,
        liveness_report: None,
        negative_test: None,
    };
    let events = BTreeMap::from([
        (
            "primary".to_owned(),
            vec![json!({
                "event": "exhaustive-check",
                "check_id": "primary",
                "status": "pass",
                "unique_protocol_states": 100_000,
                "unique_verifier_states": 100_000,
                "observations": {
                    "commit_floor_advances": 1,
                    "primary_purpose": 1,
                },
            })],
        ),
        (
            "variant".to_owned(),
            vec![json!({
                "event": "exhaustive-check",
                "check_id": "variant",
                "status": "pass",
                "unique_protocol_states": 1,
                "unique_verifier_states": 1,
                "observations": {"commit_floor_advances": 1},
            })],
        ),
    ]);
    let contracts = BTreeMap::from([
        (
            "primary".to_owned(),
            SimulatorCheckContract {
                minimum_protocol_states: 10,
                minimum_verifier_states: 10,
                required_observations: vec!["primary_purpose".to_owned()],
            },
        ),
        (
            "variant".to_owned(),
            SimulatorCheckContract {
                minimum_protocol_states: 10,
                minimum_verifier_states: 10,
                required_observations: vec!["variant_purpose".to_owned()],
            },
        ),
    ]);

    let evidence = model_observations("pr", "CM-02", &identity, &contracts, &[], &events);

    assert!(coverage_reached(
        &identity,
        &evidence.observations,
        &evidence.per_check_required_observations,
    ));
    assert!(matches!(
        evidence.issue,
        Some(SimulatorIssue::CoverageNotReached(message))
            if message.contains("variant") && message.contains("variant_purpose")
    ));
    assert_eq!(
        evidence.observations[&crate::contract::profile::per_check_protocol_states_key("variant")],
        1
    );
}
