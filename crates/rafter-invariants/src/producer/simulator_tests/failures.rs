//! Unix process-failure and counterexample-precedence scenarios.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;

use super::super::{
    coverage_reached, evaluate, evaluate_descriptors, liveness_contracts, model_observations,
};
use super::support::{
    passing_detectors, passing_detectors_for_descriptors, passing_event_stream, safety_descriptor,
    timeout_fixture_output_dir,
};
use crate::{CheckCompletion, EvidenceStatus, FailureClassification};
use serde_json::json;

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
    let (model, receipt) = crate::producer::simulator::model::timed_out_zero_exit_fixture(
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
    let (model, receipt) = crate::producer::simulator::model::timed_out_zero_exit_fixture(
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
    let model = crate::producer::simulator::model::later_launch_error_fixture(
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
