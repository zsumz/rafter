use std::{collections::BTreeMap, path::PathBuf};

use serde_json::{json, Value};

use super::{
    derive_check_contract_issue, derive_simulator_observation_counts, index_simulator_event,
    inspect_machine_events, raw_event_issue, verified_passing_simulator_event_contract,
    verify_composite_observation, verify_negative_detector_evidence,
    verify_negative_fixture_binding, verify_nonpassing_event_classification,
    verify_simulator_observations, RawEventIssue,
};

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
fn fabricated_named_witness_without_detector_call_is_rejected_end_to_end() {
    let (catalog, manifest) = crate::tests::loaded();
    let mut bundle = simulator_bundle(&catalog, &manifest);
    bundle.source_ref = format!("e2e{:09}-fabricated-witness", std::process::id());
    let fixture = "fabricated_detector_witness_without_invocation_subprocess_fixture";
    let detector = "token_bound_regression_detector";
    let (oracle_check_id, process_log) =
        crate::producer::test_exec::capture_fabricated_detector_witness_fixture_log(
            &bundle.source_ref,
            fixture,
        )
        .expect("capture the rejected fabricated marker through exact libtest framing");

    let processes = crate::producer::process::parse_combined_processes(&process_log)
        .expect("parse fabricated witness process log");
    assert_eq!(processes.len(), 1);
    assert_ne!(processes[0].exit_code, Some(0));

    let error = crate::artifact_verify::test_logs::require_detector_witness(
        &bundle,
        &process_log,
        &oracle_check_id,
        "rafter_invariant_test::tests::token_bound_regression_detector",
    )
    .expect_err("a textual marker without a post-invocation proof must be rejected");
    assert!(error.to_string().contains("proof"));

    let mut descriptor = catalog
        .evidence
        .iter()
        .find(|descriptor| descriptor.layer == "simulator" && descriptor.strength == "direct")
        .expect("registered direct simulator descriptor")
        .clone();
    descriptor.path = "crates/rafter-invariant-test/src/tests.rs".to_owned();
    descriptor.negative_fixture = Some(fixture.to_owned());
    descriptor.negative_fixture_path = Some(descriptor.path.clone());
    descriptor.negative_fixture_detector = Some(detector.to_owned());
    descriptor
        .simulator
        .as_mut()
        .expect("direct simulator identity")
        .negative_test = Some(crate::TestIdentity {
        package: "rafter-invariant-test".to_owned(),
        target_kind: "lib".to_owned(),
        target: "rafter_invariant_test".to_owned(),
        test_name: format!("tests::{fixture}"),
    });
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");

    let error = verify_negative_fixture_binding(
        &root,
        &descriptor,
        fixture,
        "adversarial-fabricated-witness",
    )
    .expect_err("source-bound verification must reject the self-attesting fixture");
    assert!(
        error
            .to_string()
            .contains("can emit an arbitrary detector witness"),
        "{error}"
    );
}

#[test]
fn qualified_helper_cannot_qualify_without_reaching_the_detector() {
    let (catalog, manifest) = crate::tests::loaded();
    let mut bundle = simulator_bundle(&catalog, &manifest);
    bundle.source_ref = format!("e2e{:09}-qualified-helper", std::process::id());
    let fixture = "qualified_helper_forged_transcript_subprocess_fixture";
    let detector = "token_bound_regression_detector";
    let (oracle_check_id, process_log) =
        crate::producer::test_exec::capture_qualified_helper_forged_transcript_fixture_log(
            &bundle.source_ref,
        )
        .expect("compile and execute the qualified-helper regression fixture");

    let processes = crate::producer::process::parse_combined_processes(&process_log)
        .expect("parse qualified-helper process log");
    let [exact] = processes.as_slice() else {
        panic!("expected one exact process receipt");
    };
    assert_eq!(exact.exit_code, Some(0));
    assert!(
        crate::producer::test_exec::exact_pass(
            exact.stdout.as_bytes(),
            &exact.invocation.arguments[0],
        ),
        "{}",
        exact.stdout
    );
    let token = crate::producer::test_exec::oracle_token(&bundle.source_ref, &oracle_check_id);
    assert_eq!(
        crate::producer::test_exec::classify_exact_execution(
            exact.stdout.as_bytes(),
            exact.stderr.as_bytes(),
            &exact.invocation.arguments[0],
            &token,
            exact.exit_code,
            exact.timed_out,
        ),
        crate::producer::test_exec::ExactTestExecution::Pass,
        "the regression must remain strong enough to satisfy the textual classifier"
    );

    let error = crate::artifact_verify::test_logs::require_detector_witness(
        &bundle,
        &process_log,
        &oracle_check_id,
        "rafter_invariant_test::tests::token_bound_regression_detector",
    )
    .expect_err("the final verifier must require the post-invocation proof");
    assert!(error.to_string().contains("proof"));

    let mut descriptor = catalog
        .evidence
        .iter()
        .find(|descriptor| descriptor.layer == "simulator" && descriptor.strength == "direct")
        .expect("registered direct simulator descriptor")
        .clone();
    descriptor.path = "crates/rafter-invariant-test/src/tests.rs".to_owned();
    descriptor.negative_fixture = Some(fixture.to_owned());
    descriptor.negative_fixture_path = Some(descriptor.path.clone());
    descriptor.negative_fixture_detector = Some(detector.to_owned());
    descriptor
        .simulator
        .as_mut()
        .expect("direct simulator identity")
        .negative_test = Some(crate::TestIdentity {
        package: "rafter-invariant-test".to_owned(),
        target_kind: "lib".to_owned(),
        target: "rafter_invariant_test".to_owned(),
        test_name: format!("tests::{fixture}"),
    });
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let error = verify_negative_fixture_binding(
        &root,
        &descriptor,
        fixture,
        "adversarial-qualified-helper",
    )
    .expect_err("source verification must recursively inspect the qualified helper");
    assert!(
        error
            .to_string()
            .contains("can emit an arbitrary detector witness"),
        "{error}"
    );
}

#[test]
fn proof_socket_is_not_visible_to_fixture_body_code() {
    let (catalog, manifest) = crate::tests::loaded();
    let mut bundle = simulator_bundle(&catalog, &manifest);
    bundle.source_ref = format!("e2e{:09}-hidden-proof-socket", std::process::id());
    let (oracle_check_id, process_log) =
        crate::producer::test_exec::capture_hidden_proof_socket_fixture_log(&bundle.source_ref)
            .expect("compile and execute the hidden-proof-socket fixture");

    crate::artifact_verify::test_logs::require_detector_witness(
        &bundle,
        &process_log,
        &oracle_check_id,
        "rafter_invariant_test::tests::token_bound_regression_detector",
    )
    .expect("the macro-held proof channel still validates after hiding the socket path");
}

#[test]
fn removing_the_gate_token_cannot_suppress_trusted_runtime_witnesses() {
    let source_ref = format!("e2e{:09}-removed-detector-token", std::process::id());
    let (_, process_log) =
        crate::producer::test_exec::capture_removed_token_detector_fixture_log(&source_ref)
            .expect("execute the token-removal fixture through exact libtest framing");
    let processes = crate::producer::process::parse_combined_processes(&process_log)
        .expect("parse token-removal process log");
    assert_eq!(processes.len(), 1);
    assert_ne!(processes[0].exit_code, Some(0));
    assert!(process_log.contains("detector test returned without its gate token"));
}

#[test]
fn registered_simulator_detector_is_invoked_with_its_compiler_identity() {
    let (catalog, manifest) = crate::tests::loaded();
    let mut bundle = simulator_bundle(&catalog, &manifest);
    bundle.source_ref = format!("e2e{:09}-registered-detector", std::process::id());
    let descriptor = catalog
        .evidence
        .iter()
        .find(|descriptor| {
            descriptor.layer == "simulator"
                && descriptor.negative_fixture.as_deref()
                    == Some("term_monotonicity_history_detects_regression_from_observation")
        })
        .expect("registered term-monotonicity detector fixture");
    let fixture = descriptor
        .negative_fixture
        .as_deref()
        .expect("registered fixture name");
    let identity = descriptor
        .simulator
        .as_ref()
        .and_then(|identity| identity.negative_test.as_ref())
        .expect("registered negative test identity");
    let (oracle_check_id, process_log) =
        crate::producer::test_exec::capture_registered_detector_fixture_log(
            &bundle.source_ref,
            identity,
        )
        .expect("compile and execute the registered detector fixture");
    assert_eq!(oracle_check_id, identity.check_id());

    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let contract =
        verify_negative_fixture_binding(&root, descriptor, fixture, "e2e-registered-detector")
            .expect("derive the registered source invocation contract");
    crate::artifact_verify::test_logs::require_detector_witness_contract(
        &bundle,
        &process_log,
        &oracle_check_id,
        contract.registered_identity(),
        contract.witnesses(),
    )
    .expect("runtime witnesses match the source and compiler identities");
}

#[test]
fn compile_failure_is_valid_red_detector_evidence_without_a_runtime_transcript() {
    let (catalog, manifest) = crate::tests::loaded();
    let mut bundle = simulator_bundle(&catalog, &manifest);
    let descriptor = catalog
        .evidence
        .iter()
        .find(|descriptor| {
            descriptor.layer == "simulator"
                && descriptor.negative_fixture.as_deref()
                    == Some("term_monotonicity_history_detects_regression_from_observation")
        })
        .expect("registered detector fixture");
    let identity = descriptor.simulator.as_ref().expect("simulator identity");
    let mut check = bundle
        .execution
        .checks
        .iter()
        .find(|check| check.evidence_ids == [descriptor.evidence_id()])
        .expect("detector receipt")
        .clone();
    set_result_outcome(
        &mut bundle,
        &check.execution_id,
        crate::EvidenceStatus::Error,
        crate::FailureClassification::HarnessError,
    );
    check
        .observations
        .insert("detector_qualified".to_owned(), 0);
    check
        .artifacts
        .retain(|artifact| artifact.kind != "test-log" && artifact.kind != "compile-log");
    let compile_log = crate::ArtifactRef {
        kind: "compile-log".to_owned(),
        path: "compile-only-detector.log".to_owned(),
        sha256: "0".repeat(64),
        size_bytes: 1,
    };
    check.artifacts.push(compile_log);
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");

    verify_negative_detector_evidence(
        &bundle,
        &root,
        &check,
        descriptor,
        identity,
        &mut super::super::detector_source::DetectorSourceCache::default(),
        &mut BTreeMap::new(),
    )
    .expect("compile failure is self-contained red detector evidence");

    check
        .artifacts
        .retain(|artifact| artifact.kind != "compile-log");
    let error = verify_negative_detector_evidence(
        &bundle,
        &root,
        &check,
        descriptor,
        identity,
        &mut super::super::detector_source::DetectorSourceCache::default(),
        &mut BTreeMap::new(),
    )
    .expect_err("a runtime detector failure cannot omit its runtime transcript");
    assert!(error.to_string().contains("detector log missing"));
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

#[test]
fn serialized_verifier_rejects_unknown_invariant_violation_as_harness_error() {
    let (catalog, manifest) = crate::tests::loaded();
    let bundle = simulator_bundle(&catalog, &manifest);
    let check = &bundle.execution.checks[0];
    let descriptor = catalog
        .evidence
        .iter()
        .find(|descriptor| descriptor.evidence_id() == check.evidence_ids[0])
        .expect("registered simulator descriptor");
    let identity = descriptor.simulator.as_ref().expect("simulator identity");
    let event = json!({
        "event": "check-failure",
        "event_version": 2,
        "check_id": "unknown-invariant",
        "status": "fail",
        "classification": "invariant-violation",
        "invariant_id": descriptor.invariant_id,
        "invariant": format!("{} test invariant", descriptor.invariant_id),
    });
    let events = serialized_events("pr", &event);
    let profile_descriptors = catalog
        .required_evidence(&bundle.execution.plan.contract)
        .into_values()
        .flatten()
        .filter(|descriptor| descriptor.layer == "simulator")
        .collect::<Vec<_>>();
    let inspection = inspect_machine_events("pr", &profile_descriptors, &events);

    assert_eq!(inspection.global_issue, Some(RawEventIssue::HarnessError));
    assert_eq!(
        inspection.diagnostics,
        ["simulator emitted unclaimed machine event check IDs: unknown-invariant"]
    );
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

#[test]
fn serialized_verifier_does_not_fan_out_a_shared_check_counterexample() {
    let (catalog, manifest) = crate::tests::loaded();
    let mut bundle = simulator_bundle(&catalog, &manifest);
    let (descriptors, matching, sibling, shared_check) = shared_log_descriptors(&catalog);
    let matching_identity = matching.simulator.as_ref().expect("LG-03 identity");
    let event = json!({
        "event": "check-failure",
        "event_version": 2,
        "check_id": shared_check,
        "status": "fail",
        "classification": "invariant-violation",
        "invariant_id": "LG-03",
        "invariant": "LG-03 log matching",
        "message": "serialized shared check found a log-matching counterexample",
    });
    let events = serialized_events("pr", &event);
    let inspection = inspect_machine_events("pr", &descriptors, &events);
    assert_eq!(inspection.global_issue, None);

    let matching_check = bundle
        .execution
        .checks
        .iter()
        .find(|check| check.evidence_ids == [matching.evidence_id()])
        .expect("LG-03 receipt")
        .clone();
    let sibling_check = bundle
        .execution
        .checks
        .iter()
        .find(|check| check.evidence_ids == [sibling.evidence_id()])
        .expect("LG-04 receipt")
        .clone();
    set_result_outcome(
        &mut bundle,
        &matching_check.execution_id,
        crate::EvidenceStatus::Fail,
        crate::FailureClassification::InvariantViolation,
    );
    set_result_outcome(
        &mut bundle,
        &sibling_check.execution_id,
        crate::EvidenceStatus::Incomplete,
        crate::FailureClassification::CoverageNotReached,
    );

    verify_nonpassing_event_classification(
        &bundle,
        &matching_check,
        &matching.invariant_id,
        matching_identity,
        &events,
        inspection.global_issue,
    )
    .expect("matching invariant preserves the counterexample");
    verify_nonpassing_event_classification(
        &bundle,
        &sibling_check,
        &sibling.invariant_id,
        sibling.simulator.as_ref().expect("LG-04 identity"),
        &events,
        inspection.global_issue,
    )
    .expect("sibling invariant remains an incomplete aborted check");

    set_result_outcome(
        &mut bundle,
        &sibling_check.execution_id,
        crate::EvidenceStatus::Fail,
        crate::FailureClassification::InvariantViolation,
    );
    assert!(verify_nonpassing_event_classification(
        &bundle,
        &sibling_check,
        &sibling.invariant_id,
        sibling.simulator.as_ref().expect("LG-04 identity"),
        &events,
        inspection.global_issue,
    )
    .is_err());
}

fn shared_log_descriptors(
    catalog: &crate::Catalog,
) -> (
    Vec<crate::EvidenceDescriptor>,
    crate::EvidenceDescriptor,
    crate::EvidenceDescriptor,
    String,
) {
    let descriptors = catalog
        .evidence
        .iter()
        .filter(|descriptor| descriptor.layer == "simulator")
        .cloned()
        .collect::<Vec<_>>();
    let matching = descriptors
        .iter()
        .find(|descriptor| descriptor.invariant_id == "LG-03")
        .expect("LG-03 simulator descriptor")
        .clone();
    let matching_identity = matching.simulator.as_ref().expect("LG-03 identity");
    let sibling = descriptors
        .iter()
        .find(|descriptor| {
            descriptor.invariant_id == "LG-04"
                && descriptor.simulator.as_ref().is_some_and(|identity| {
                    identity
                        .checks
                        .iter()
                        .any(|check| matching_identity.checks.contains(check))
                })
        })
        .expect("LG-04 descriptor sharing an LG-03 model check")
        .clone();
    let shared_check = matching_identity
        .checks
        .iter()
        .find(|check| {
            sibling
                .simulator
                .as_ref()
                .is_some_and(|identity| identity.checks.contains(check))
        })
        .expect("shared simulator check")
        .clone();
    (descriptors, matching, sibling, shared_check)
}

fn set_result_outcome(
    bundle: &mut crate::ResultBundle,
    execution_id: &str,
    status: crate::EvidenceStatus,
    classification: crate::FailureClassification,
) {
    let result = bundle
        .results
        .iter_mut()
        .find(|result| result.execution_id == execution_id)
        .expect("bound simulator result");
    result.status = status;
    result.classification = Some(classification);
}

fn simulator_bundle(
    catalog: &crate::Catalog,
    manifest: &crate::ProfileManifest,
) -> crate::ResultBundle {
    crate::tests::passing_bundles(catalog, manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "simulator")
        .expect("simulator bundle")
}

fn serialized_events(profile: &str, event: &Value) -> BTreeMap<String, Vec<Value>> {
    let source = format!("{}{}", crate::artifact_verify::EVENT_PREFIX, event);
    let (parsed, diagnostics) = super::super::simulator_schedule::scan_machine_events(
        &source,
        "serialized simulator fixture",
    );
    assert!(diagnostics.is_empty());
    let mut events = BTreeMap::new();
    for event in parsed {
        index_simulator_event(profile, event, &mut events).expect("index serialized event");
    }
    events
}
