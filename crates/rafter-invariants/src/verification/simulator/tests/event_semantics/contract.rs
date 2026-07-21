//! Machine-event shape and status/classification contract tests.

use super::support::*;

#[test]
fn serialized_verifier_rejects_and_excludes_malformed_passing_events() {
    let (catalog, manifest) = crate::tests::loaded();
    let bundle = simulator_bundle(&catalog, &manifest);
    let identity = crate::SimulatorIdentity {
        checks: vec!["malformed-extra".to_owned()],
        required_observation: "semantic_witness".to_owned(),
        minimum_observation: 2,
        minimum_protocol_states: Some(1),
        minimum_verifier_states: Some(1),
        minimum_runs_per_check: None,
        minimum_steps: None,
        liveness_report: None,
        negative_test: None,
    };
    let malformed = json!({
        "event": "unsupported-pass-event",
        "check_id": "malformed-extra",
        "status": "pass",
        "classification": null,
        "unique_protocol_states": 100,
        "unique_verifier_states": 100,
        "observations": {"semantic_witness": 100},
    });
    let valid = json!({
        "event": "exhaustive-check",
        "check_id": "malformed-extra",
        "status": "pass",
        "classification": null,
        "unique_protocol_states": 1,
        "unique_verifier_states": 1,
        "observations": {"semantic_witness": 1},
    });
    let events = BTreeMap::from([("malformed-extra".to_owned(), vec![valid, malformed.clone()])]);

    let (issue, diagnostic) = raw_event_issue("malformed-extra", &malformed, None);
    assert_eq!(issue, Some(RawEventIssue::HarnessError));
    assert!(diagnostic.is_some());
    let (derived, _) = derive_simulator_observation_counts(&bundle, &identity, &events)
        .expect("derive serialized observations");
    assert_eq!(derived["semantic_witness"], 1);
}

#[test]
fn serialized_verifier_rejects_cross_kind_substitution() {
    let exhaustive = json!({
        "event": "exhaustive-check",
        "check_id": "raft-election",
        "status": "pass",
        "classification": null,
        "unique_protocol_states": 14_000,
        "unique_verifier_states": 18_000,
        "observations": {"election_certificates": 1},
    });
    let soak = json!({
        "event": "soak-check",
        "check_id": "raft-soak",
        "status": "pass",
        "classification": null,
        "seed": 1,
        "steps": 320,
        "duration_ms": 4,
        "execution_contract": {},
        "observed_actions": ["tick"],
        "liveness_features": ["proposal-progress"],
        "observations": {"accepted_completed_liveness_proposals": 1},
        "liveness_reports": [],
    });

    let mut substituted_soak = soak;
    substituted_soak["check_id"] = json!("raft-election");
    let error = verified_passing_simulator_event_contract("raft-election", &substituted_soak)
        .expect_err("exhaustive contract must reject a passing soak event");
    assert!(error.contains("expected exhaustive-check, found soak-check"));
    let (issue, diagnostic) = raw_event_issue("raft-election", &substituted_soak, None);
    assert_eq!(issue, Some(RawEventIssue::HarnessError));
    assert_eq!(diagnostic.as_deref(), Some(error.as_str()));

    let mut substituted_exhaustive = exhaustive;
    substituted_exhaustive["check_id"] = json!("raft-soak");
    let error = verified_passing_simulator_event_contract("raft-soak", &substituted_exhaustive)
        .expect_err("soak contract must reject a passing exhaustive event");
    assert!(error.contains("expected soak-check, found exhaustive-check"));
    let (issue, diagnostic) = raw_event_issue("raft-soak", &substituted_exhaustive, None);
    assert_eq!(issue, Some(RawEventIssue::HarnessError));
    assert_eq!(diagnostic.as_deref(), Some(error.as_str()));
}

#[test]
fn serialized_verifier_rejects_contradictory_and_missing_event_pairs() {
    let (catalog, manifest) = crate::tests::loaded();
    let bundle = simulator_bundle(&catalog, &manifest);
    let check = &bundle.execution.checks[0];
    let descriptor = catalog
        .evidence
        .iter()
        .find(|descriptor| descriptor.evidence_id() == check.evidence_ids[0])
        .expect("registered simulator descriptor");
    let identity = descriptor.simulator.as_ref().expect("simulator identity");
    let check_id = &identity.checks[0];

    for event in [
        json!({
            "check_id": check_id,
            "status": "pass",
            "classification": "invariant-violation",
        }),
        json!({"check_id": check_id, "status": "fail"}),
        json!({
            "check_id": check_id,
            "status": "incomplete",
            "classification": null,
        }),
        json!({
            "check_id": check_id,
            "status": "unknown",
            "classification": "harness-error",
        }),
    ] {
        let events = serialized_events("pr", &event);
        let inspection = inspect_machine_events("pr", std::slice::from_ref(descriptor), &events);
        assert_eq!(inspection.global_issue, None);
        assert_eq!(inspection.diagnostics.len(), 1);
        assert!(inspection.diagnostics[0].contains("invalid status/classification pair"));
        assert!(verify_nonpassing_event_classification(
            &bundle,
            check,
            &descriptor.invariant_id,
            identity,
            &events,
            inspection.global_issue,
        )
        .is_err());
    }
}
