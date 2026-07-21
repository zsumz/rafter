//! Composite and per-check observation-accounting tests.

use super::support::*;

#[test]
fn serialized_verifier_accepts_complementary_composite_witness_legs() {
    let (catalog, manifest) = crate::tests::loaded();
    let mut bundle = simulator_bundle(&catalog, &manifest);
    bundle
        .execution
        .plan
        .contract
        .runners
        .get_mut("simulator")
        .expect("simulator runner")
        .simulator_checks
        .clear();
    let descriptor = catalog
        .evidence
        .iter()
        .find(|descriptor| {
            descriptor.layer == "simulator"
                && descriptor.simulator.as_ref().is_some_and(|identity| {
                    identity.liveness_report.is_none() && identity.checks.len() > 1
                })
        })
        .expect("composite simulator safety descriptor");
    let identity = descriptor.simulator.as_ref().expect("simulator identity");
    let mut check = bundle
        .execution
        .checks
        .iter()
        .find(|check| check.evidence_ids == [descriptor.evidence_id()])
        .expect("receipt for composite simulator descriptor")
        .clone();
    check.observations.retain(|name, _| {
        !name.starts_with("unique_protocol_states:")
            && !name.starts_with("unique_verifier_states:")
            && !name.starts_with("observed:")
    });
    let mut events = BTreeMap::new();
    let mut observations = BTreeMap::from([("detector_qualified".to_owned(), 1)]);
    for (index, name) in identity.checks.iter().enumerate() {
        observations.extend([
            (format!("runs:{name}"), 1),
            (format!("passes:{name}"), 1),
            (format!("steps:{name}"), 0),
        ]);
        let event_observations = if index == 0 {
            json!({(identity.required_observation.clone()): identity.minimum_observation})
        } else {
            json!({})
        };
        events.insert(
            name.clone(),
            vec![json!({
                "event": "exhaustive-check",
                "check_id": name,
                "status": "pass",
                "classification": null,
                "unique_protocol_states": identity.minimum_protocol_states.unwrap_or_default(),
                "unique_verifier_states": identity.minimum_verifier_states.unwrap_or_default(),
                "observations": event_observations,
            })],
        );
    }
    observations.extend([
        (
            "unique_protocol_states".to_owned(),
            identity.minimum_protocol_states.unwrap_or_default() as u64,
        ),
        (
            "unique_verifier_states".to_owned(),
            identity.minimum_verifier_states.unwrap_or_default() as u64,
        ),
        (
            identity.required_observation.clone(),
            identity.minimum_observation as u64,
        ),
    ]);
    check.observations = observations;

    verify_simulator_observations(&bundle, &check, identity, &[], &events)
        .expect("one passing leg may supply the complete composite witness");
}

#[test]
fn serialized_verifier_rejects_a_witness_minimum_split_across_checks() {
    let (catalog, manifest) = crate::tests::loaded();
    let bundle = simulator_bundle(&catalog, &manifest);
    let check = &bundle.execution.checks[0];
    let identity = crate::SimulatorIdentity {
        checks: vec!["primary".to_owned(), "variant".to_owned()],
        required_observation: "semantic_witness".to_owned(),
        minimum_observation: 2,
        minimum_protocol_states: Some(1),
        minimum_verifier_states: Some(1),
        minimum_runs_per_check: None,
        minimum_steps: None,
        liveness_report: None,
        negative_test: None,
    };
    let events = BTreeMap::from([
        (
            "primary".to_owned(),
            vec![json!({
                "check_id": "primary",
                "status": "pass",
                "observations": {"semantic_witness": 1},
            })],
        ),
        (
            "variant".to_owned(),
            vec![json!({
                "check_id": "variant",
                "status": "pass",
                "observations": {"semantic_witness": 1},
            })],
        ),
    ]);

    let error = verify_composite_observation(&bundle, check, &identity, &events)
        .expect_err("split witness counts cannot satisfy one leg's minimum");

    assert!(error
        .to_string()
        .contains("no model check independently reached"));
}

#[test]
fn serialized_verifier_rederives_each_check_floor_and_purpose_witness() {
    let contract = crate::SimulatorCheckContract {
        minimum_protocol_states: 10,
        minimum_verifier_states: 20,
        required_observations: vec!["variant_purpose".to_owned()],
    };
    let event = json!({
        "event": "exhaustive-check",
        "check_id": "variant",
        "status": "pass",
        "unique_protocol_states": 100_000,
        "unique_verifier_states": 100_000,
        "observations": {"unrelated_purpose": 1},
    });
    let mut observations = BTreeMap::new();

    let issue = derive_check_contract_issue(
        "variant",
        std::slice::from_ref(&event),
        &contract,
        &mut observations,
    );

    assert_eq!(issue, Some(RawEventIssue::CoverageNotReached));
    assert_eq!(
        observations[&crate::contract::profile::per_check_protocol_states_key("variant")],
        100_000
    );
    assert_eq!(
        observations
            [&crate::contract::profile::per_check_observation_key("variant", "variant_purpose")],
        0
    );
}
