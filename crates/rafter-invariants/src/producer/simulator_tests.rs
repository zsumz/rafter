use std::collections::BTreeMap;
use std::fmt::Write as _;

#[cfg(unix)]
use std::{fs, path::Path};

use super::{
    coverage_reached, evaluate, evaluate_descriptors, liveness_contracts, model_observations,
    simulator_event_issue, DetectorRun, SimulatorIssue,
};
use crate::{
    CheckCompletion, EvidenceDescriptor, EvidenceStatus, FailureClassification, SimulatorIdentity,
};
use serde_json::json;

#[test]
fn simulator_failure_classifications_remain_distinct() {
    let invariant = simulator_event_issue(
        "raft-soak",
        &json!({"status": "fail", "classification": "invariant-violation"}),
    );
    assert!(matches!(
        invariant,
        Some(SimulatorIssue::InvariantViolation(_))
    ));

    let incomplete = simulator_event_issue(
        "raft-soak",
        &json!({"status": "incomplete", "classification": "coverage-not-reached"}),
    );
    assert!(matches!(
        incomplete,
        Some(SimulatorIssue::CoverageNotReached(_))
    ));

    let malformed = simulator_event_issue(
        "raft-soak",
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
            "status": "fail",
            "classification": "invariant-violation",
            "message": "commit witness violated"
        })],
    )]);

    let evidence = model_observations("pr", &identity, &[], &events);

    assert!(matches!(
        evidence.issue,
        Some(SimulatorIssue::InvariantViolation(message))
            if message == "commit witness violated"
    ));
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
            "check_id": "unknown-check",
            "status": "fail",
            "classification": "invariant-violation",
        }),
    ] {
        let model = model_fixture(BTreeMap::from([("unknown-check".to_owned(), vec![event])]));
        let (_, results) = evaluate_descriptors(
            std::slice::from_ref(&descriptor),
            "pr",
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
    let observations = model_observations("pr", identity, &[], &model.events);
    assert!(observations.issue.is_none());
    assert!(coverage_reached(identity, &observations.observations));

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
            "check_id": identity.checks[0],
            "status": "fail",
            "classification": "invariant-violation",
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
            "check_id": identity.checks[0],
            "status": "fail",
            "classification": "invariant-violation",
            "message": "first run found a real counterexample",
        })
    )
    .expect("append counterexample event");
    writeln!(
        stdout,
        "RAFTER_EVENT {}",
        json!({
            "check_id": "unknown-later-check",
            "status": "fail",
            "classification": "invariant-violation",
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
                    "check_id": check,
                    "status": "pass",
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
