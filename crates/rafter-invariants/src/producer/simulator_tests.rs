use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::path::Path;

use super::{
    coverage_reached, evaluate, evaluate_descriptors, execution_resource_metrics,
    liveness_contracts, model_observations, preflight_detector_sources_at, simulator_event_issue,
    DetectorRun, SimulatorIssue,
};
use crate::{
    CheckCompletion, EvidenceDescriptor, EvidenceStatus, FailureClassification,
    SimulatorCheckContract, SimulatorIdentity,
};
use serde_json::json;

#[test]
fn detector_source_preflight_rejects_the_compiled_qualified_helper_fixture() {
    let (catalog, _) = crate::tests::loaded();
    let mut descriptor = catalog
        .evidence
        .iter()
        .find(|descriptor| descriptor.layer == "simulator" && descriptor.strength == "direct")
        .expect("registered direct simulator descriptor")
        .clone();
    let fixture = "qualified_helper_forged_transcript_subprocess_fixture";
    descriptor.path = "crates/rafter-invariant-test/src/tests.rs".to_owned();
    descriptor.negative_fixture = Some(fixture.to_owned());
    descriptor.negative_fixture_path = Some(descriptor.path.clone());
    descriptor.negative_fixture_detector = Some("token_bound_regression_detector".to_owned());
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

    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let error = preflight_detector_sources_at(&root, &[descriptor])
        .expect_err("producer preflight must reject the invalid fixture source")
        .to_string();
    assert!(
        error.contains("can emit an arbitrary detector witness"),
        "{error}"
    );
}

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

#[cfg(unix)]
#[test]
fn timed_out_zero_exit_model_run_is_a_typed_harness_error_despite_passing_coverage() {
    let (catalog, _) = crate::tests::loaded();
    let descriptor = catalog
        .evidence
        .iter()
        .find(|descriptor| {
            descriptor.layer == "simulator"
                && descriptor
                    .simulator
                    .as_ref()
                    .is_some_and(|identity| identity.liveness_report.is_none())
        })
        .expect("safety simulator descriptor");
    let identity = descriptor.simulator.as_ref().expect("simulator identity");
    let stdout = passing_event_stream(identity);
    let output_dir = timeout_fixture_output_dir("passing");
    let (model, receipt) = crate::producer::simulator_model::timed_out_zero_exit_fixture(
        "pr",
        "abc123",
        &stdout,
        &output_dir,
    )
    .expect("run timeout fixture through the production reducer");
    assert_eq!(receipt.exit_code, Some(0));
    assert!(receipt.timed_out);
    assert!(!model.processes_succeeded);
    let observations = model_observations(
        "pr",
        &descriptor.invariant_id,
        identity,
        &BTreeMap::new(),
        &[],
        &model.events,
    );
    assert!(observations.issue.is_none());
    assert!(coverage_reached(
        identity,
        &observations.observations,
        &observations.per_check_required_observations,
    ));

    let result = evaluate(descriptor, "pr", &[], &model, &passing_detectors(identity))
        .expect("evaluate timed-out simulator evidence");

    assert_eq!(result.completion, CheckCompletion::HarnessError);
    assert_eq!(result.status, EvidenceStatus::Error);
    assert_eq!(
        result.classification,
        Some(FailureClassification::HarnessError)
    );
    assert!(result
        .message
        .as_deref()
        .is_some_and(|message| message.contains("did not complete successfully")));
    assert!(result.simulator_liveness.is_none());
    assert_eq!(result.artifacts, model.artifacts);
    assert!(result
        .artifacts
        .iter()
        .any(|artifact| artifact.kind == "simulator-log"));
    fs::remove_dir_all(output_dir).expect("remove timeout fixture artifacts");
}

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
fn detector_harness_failure_uses_metrics_for_its_attached_logs_only() {
    let (catalog, _) = crate::tests::loaded();
    let descriptor = safety_descriptor(&catalog.evidence);
    let identity = descriptor.simulator.as_ref().expect("simulator identity");
    let negative_test = identity
        .negative_test
        .as_ref()
        .expect("direct safety descriptor has a negative test");
    let check_id = &identity.checks[0];
    let mut model = model_fixture(BTreeMap::from([(
        check_id.clone(),
        vec![json!({
            "event": "exhaustive-check",
            "check_id": check_id,
            "status": "incomplete",
            "classification": "coverage-not-reached",
            "message": "model frontier was incomplete",
        })],
    )]));
    let model_log = crate::ArtifactRef {
        kind: "test-log".to_owned(),
        path: "artifacts/model.log".to_owned(),
        sha256: "0".repeat(64),
        size_bytes: 1,
    };
    model.artifacts.push(model_log.clone());
    model.duration_ms = 5;
    model.runtime_peak_rss_kib = 13;
    let detector_log = crate::ArtifactRef {
        kind: "test-log".to_owned(),
        path: "artifacts/detector.log".to_owned(),
        sha256: "0".repeat(64),
        size_bytes: 1,
    };
    let unrelated_detector_log = crate::ArtifactRef {
        kind: "test-log".to_owned(),
        path: "artifacts/unrelated-detector.log".to_owned(),
        sha256: "1".repeat(64),
        size_bytes: 1,
    };
    let detectors = DetectorRun {
        outcomes: BTreeMap::from([
            (
                negative_test.check_id(),
                super::test_exec::TestOutcome {
                    completion: CheckCompletion::HarnessError,
                    status: EvidenceStatus::Error,
                    classification: Some(FailureClassification::HarnessError),
                    message: Some("detector proof channel failed".to_owned()),
                    observations: BTreeMap::new(),
                    duration_ms: 7,
                    peak_rss_kib: 11,
                    artifacts: vec![detector_log.clone()],
                },
            ),
            (
                "unrelated::detector".to_owned(),
                super::test_exec::TestOutcome {
                    completion: CheckCompletion::Completed,
                    status: EvidenceStatus::Pass,
                    classification: None,
                    message: None,
                    observations: BTreeMap::new(),
                    duration_ms: 101,
                    peak_rss_kib: 211,
                    artifacts: vec![unrelated_detector_log.clone()],
                },
            ),
        ]),
        artifacts: Vec::new(),
        peak_rss_kib: 211,
        duration_ms: 108,
        harness_error: None,
    };

    let evaluated = evaluate(descriptor, "nightly", &[], &model, &detectors)
        .expect("fold model and detector outcomes");

    assert_eq!(evaluated.status, EvidenceStatus::Error);
    assert_eq!(
        evaluated.classification,
        Some(FailureClassification::HarnessError)
    );
    assert_eq!(evaluated.observations["detector_qualified"], 0);
    assert_eq!(evaluated.artifacts, vec![model_log, detector_log]);
    assert!(!evaluated.artifacts.contains(&unrelated_detector_log));
    assert_eq!(evaluated.duration_ms, 12);
    assert_eq!(evaluated.peak_rss_kib, 13);
}

include!("simulator_tests/compile_failure_metrics.rs");

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

#[cfg(unix)]
#[test]
fn recorded_counterexample_outranks_a_later_process_timeout() {
    let (catalog, _) = crate::tests::loaded();
    let descriptor = safety_descriptor(&catalog.evidence);
    let identity = descriptor.simulator.as_ref().expect("simulator identity");
    let mut stdout = passing_event_stream(identity);
    writeln!(
        stdout,
        "RAFTER_EVENT {}",
        json!({
            "event": "check-failure",
            "event_version": 2,
            "check_id": identity.checks[0],
            "status": "fail",
            "classification": "invariant-violation",
            "invariant_id": descriptor.invariant_id,
            "invariant": format!("{} test invariant", descriptor.invariant_id),
            "message": "timeout fixture found a real counterexample",
        })
    )
    .expect("append counterexample event");
    let output_dir = timeout_fixture_output_dir("counterexample");
    let (model, receipt) = crate::producer::simulator_model::timed_out_zero_exit_fixture(
        "pr",
        "abc123",
        &stdout,
        &output_dir,
    )
    .expect("run counterexample timeout fixture through the production reducer");
    assert_eq!(receipt.exit_code, Some(0));
    assert!(receipt.timed_out);

    let result = evaluate(descriptor, "pr", &[], &model, &passing_detectors(identity))
        .expect("evaluate recorded counterexample");

    assert_eq!(result.completion, CheckCompletion::Counterexample);
    assert_eq!(result.status, EvidenceStatus::Fail);
    assert_eq!(
        result.classification,
        Some(FailureClassification::InvariantViolation)
    );
    assert_eq!(
        result.message.as_deref(),
        Some("timeout fixture found a real counterexample")
    );
    fs::remove_dir_all(output_dir).expect("remove counterexample fixture artifacts");
}

#[cfg(unix)]
#[test]
fn counterexample_survives_a_later_event_error_and_run_launch_failure() {
    let (catalog, _) = crate::tests::loaded();
    let descriptors = catalog
        .evidence
        .iter()
        .filter(|descriptor| descriptor.layer == "simulator")
        .cloned()
        .collect::<Vec<_>>();
    let descriptor = safety_descriptor(&descriptors);
    let identity = descriptor.simulator.as_ref().expect("simulator identity");
    let mut stdout = passing_event_stream(identity);
    writeln!(
        stdout,
        "RAFTER_EVENT {}",
        json!({
            "event": "check-failure",
            "event_version": 2,
            "check_id": identity.checks[0],
            "status": "fail",
            "classification": "invariant-violation",
            "invariant_id": descriptor.invariant_id,
            "invariant": format!("{} test invariant", descriptor.invariant_id),
            "message": "first run found a real counterexample",
        })
    )
    .expect("append counterexample event");
    writeln!(
        stdout,
        "RAFTER_EVENT {}",
        json!({
            "event": "check-failure",
            "event_version": 2,
            "check_id": "unknown-later-check",
            "status": "fail",
            "classification": "invariant-violation",
            "invariant_id": descriptor.invariant_id,
            "invariant": format!("{} test invariant", descriptor.invariant_id),
            "message": "an unclaimed event cannot replace the known counterexample",
        })
    )
    .expect("append unclaimed event");
    let output_dir = timeout_fixture_output_dir("later-launch-error");
    let model = crate::producer::simulator_model::later_launch_error_fixture(
        "pr",
        "abc123",
        &stdout,
        &output_dir,
    );
    assert!(!model.processes_succeeded);
    assert!(model
        .harness_errors
        .iter()
        .any(|error| error.contains("raft-soak")));
    assert!(model.events.contains_key("unknown-later-check"));

    let contracts = liveness_contracts(&descriptors).expect("liveness contracts");
    let (_, results) = evaluate_descriptors(
        &descriptors,
        "pr",
        &BTreeMap::new(),
        &contracts,
        &model,
        &passing_detectors_for_descriptors(&descriptors),
    )
    .expect("produce simulator receipts after later launch failure");
    let result = results
        .iter()
        .find(|result| result.evidence_id == descriptor.evidence_id())
        .expect("counterexample result");
    assert_eq!(result.status, EvidenceStatus::Fail);
    assert_eq!(
        result.classification,
        Some(FailureClassification::InvariantViolation)
    );
    assert_eq!(
        result.message.as_deref(),
        Some("first run found a real counterexample")
    );
    fs::remove_dir_all(output_dir).expect("remove launch-error fixture artifacts");
}

fn safety_descriptor(descriptors: &[EvidenceDescriptor]) -> &EvidenceDescriptor {
    descriptors
        .iter()
        .find(|descriptor| {
            descriptor.layer == "simulator"
                && descriptor
                    .simulator
                    .as_ref()
                    .is_some_and(|identity| identity.liveness_report.is_none())
        })
        .expect("safety simulator descriptor")
}

fn standalone_safety_descriptor(descriptors: &[EvidenceDescriptor]) -> EvidenceDescriptor {
    let mut descriptor = safety_descriptor(descriptors).clone();
    descriptor
        .simulator
        .as_mut()
        .expect("simulator identity")
        .negative_test = None;
    descriptor
}

fn model_fixture(
    events: BTreeMap<String, Vec<serde_json::Value>>,
) -> super::simulator_model::SimulatorExecution {
    super::simulator_model::SimulatorExecution {
        events,
        artifacts: Vec::new(),
        runtime_peak_rss_kib: 0,
        build_peak_rss_kib: 0,
        duration_ms: 0,
        build_duration_ms: 0,
        processes_succeeded: true,
        harness_errors: Vec::new(),
    }
}

fn empty_detectors() -> DetectorRun {
    DetectorRun {
        outcomes: BTreeMap::new(),
        artifacts: Vec::new(),
        peak_rss_kib: 0,
        duration_ms: 0,
        harness_error: None,
    }
}

#[cfg(unix)]
fn passing_event_stream(identity: &SimulatorIdentity) -> String {
    identity
        .checks
        .iter()
        .fold(String::new(), |mut output, check| {
            writeln!(
                output,
                "RAFTER_EVENT {}",
                json!({
                    "event": "exhaustive-check",
                    "check_id": check,
                    "status": "pass",
                    "classification": null,
                    "unique_protocol_states": identity.minimum_protocol_states.unwrap_or_default(),
                    "unique_verifier_states": identity.minimum_verifier_states.unwrap_or_default(),
                    "observations": {
                        identity.required_observation.clone(): identity.minimum_observation,
                    },
                })
            )
            .expect("append passing simulator event");
            output
        })
}

#[cfg(unix)]
fn passing_detectors(identity: &SimulatorIdentity) -> DetectorRun {
    DetectorRun {
        outcomes: identity
            .negative_test
            .as_ref()
            .map(|test| (test.check_id(), passing_detector_outcome()))
            .into_iter()
            .collect(),
        artifacts: Vec::new(),
        peak_rss_kib: 0,
        duration_ms: 0,
        harness_error: None,
    }
}

#[cfg(unix)]
fn passing_detectors_for_descriptors(descriptors: &[EvidenceDescriptor]) -> DetectorRun {
    DetectorRun {
        outcomes: descriptors
            .iter()
            .filter_map(|descriptor| descriptor.simulator.as_ref())
            .filter_map(|identity| identity.negative_test.as_ref())
            .map(|test| (test.check_id(), passing_detector_outcome()))
            .collect(),
        artifacts: Vec::new(),
        peak_rss_kib: 0,
        duration_ms: 0,
        harness_error: None,
    }
}

#[cfg(unix)]
fn passing_detector_outcome() -> super::test_exec::TestOutcome {
    super::test_exec::TestOutcome {
        completion: CheckCompletion::Completed,
        status: EvidenceStatus::Pass,
        classification: None,
        message: None,
        observations: BTreeMap::new(),
        duration_ms: 1,
        peak_rss_kib: 1,
        artifacts: Vec::new(),
    }
}

#[cfg(unix)]
fn timeout_fixture_output_dir(suffix: &str) -> std::path::PathBuf {
    let path = Path::new("target/rafter-invariants/tests").join(format!(
        "simulator-producer-timeout-{suffix}-{}",
        std::process::id()
    ));
    if path.exists() {
        fs::remove_dir_all(&path).expect("remove stale timeout fixture artifacts");
    }
    path
}
