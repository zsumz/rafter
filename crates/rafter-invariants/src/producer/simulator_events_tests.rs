//! Scenarios for simulator event-shape and cross-kind validation.

use serde_json::json;

use super::passing_simulator_event_contract;

#[test]
fn passing_soak_event_requires_its_complete_machine_shape() {
    let event = json!({
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
    passing_simulator_event_contract("raft-soak", &event)
        .expect("complete soak event is a passing machine receipt");

    for missing in [
        "seed",
        "steps",
        "duration_ms",
        "execution_contract",
        "observed_actions",
        "liveness_features",
        "observations",
        "liveness_reports",
    ] {
        let mut malformed = event.clone();
        malformed
            .as_object_mut()
            .expect("soak event object")
            .remove(missing);
        assert!(
            passing_simulator_event_contract("raft-soak", &malformed).is_err(),
            "accepted soak event without {missing}"
        );
    }
}

#[test]
fn passing_event_contract_rejects_cross_kind_substitution() {
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
    passing_simulator_event_contract("raft-election", &exhaustive)
        .expect("exhaustive safety check accepts its event kind");
    passing_simulator_event_contract("raft-soak", &soak)
        .expect("soak check accepts its event kind");

    let mut substituted_soak = soak;
    substituted_soak["check_id"] = json!("raft-election");
    let error = passing_simulator_event_contract("raft-election", &substituted_soak)
        .expect_err("exhaustive safety check must reject a passing soak event");
    assert!(error.contains("expected exhaustive-check, found soak-check"));

    let mut substituted_exhaustive = exhaustive;
    substituted_exhaustive["check_id"] = json!("raft-soak");
    let error = passing_simulator_event_contract("raft-soak", &substituted_exhaustive)
        .expect_err("soak check must reject a passing exhaustive event");
    assert!(error.contains("expected soak-check, found exhaustive-check"));
}
