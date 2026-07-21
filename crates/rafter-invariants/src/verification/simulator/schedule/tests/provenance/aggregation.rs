//! End-to-end preservation of simulator failures through final aggregation.

use std::fs;

use super::fixtures::{materialize_fixture, RuntimeDefect, SimulatorFixture};

#[test]
fn real_timed_out_zero_exit_receipt_fails_closed_through_loading_and_aggregation() {
    let fixture = materialize_fixture(RuntimeDefect::Timeout);
    let (bundle, intake) = verify_fixture(&fixture);
    let counterexample = bundle
        .results
        .iter()
        .find(|result| {
            result.status == crate::EvidenceStatus::Fail
                && result.classification == Some(crate::FailureClassification::InvariantViolation)
        })
        .expect("serialized counterexample result")
        .clone();
    assert_eq!(intake.defects().len(), 2);
    let error = intake
        .defects()
        .iter()
        .map(crate::verification::IntakeDefect::message)
        .find(|error| error.contains("did not time out"))
        .expect("timeout diagnostic")
        .to_owned();
    assert!(intake.defects().iter().any(|error| error
        .message()
        .contains("did not run required profile raft-soak")));
    let report = crate::verdict::reduce(&fixture.catalog, &fixture.manifest, &intake)
        .expect("verified timeout error aggregates fail-closed");
    assert_eq!(report.summary.total, 44);
    assert_eq!(report.summary.green, 0);
    assert_eq!(report.summary.red, 44);
    assert!(report.invariants.iter().all(|verdict| {
        verdict.status == crate::VerdictStatus::Red
            && verdict.issues.iter().any(|issue| {
                issue.evidence_id == "aggregate/harness"
                    && issue.status == crate::EvidenceStatus::Error
                    && issue.classification == crate::FailureClassification::HarnessError
                    && issue.message == error
            })
    }));
    let verdict = report
        .invariants
        .iter()
        .find(|verdict| verdict.invariant_id == counterexample.invariant_id)
        .expect("counterexample invariant verdict");
    let issue = verdict
        .issues
        .iter()
        .find(|issue| {
            issue.classification == crate::FailureClassification::InvariantViolation
                && issue.message == "real timeout fixture found a counterexample"
        })
        .unwrap_or_else(|| {
            panic!("counterexample {counterexample:?} missing from final verdict: {verdict:?}")
        });
    assert_eq!(issue.evidence_id, counterexample.evidence_id);
    assert_eq!(issue.status, crate::EvidenceStatus::Fail);
    assert_eq!(issue.artifacts, counterexample.artifacts);
}

#[test]
fn malformed_event_after_a_counterexample_is_retained_as_a_separate_harness_error() {
    let fixture = materialize_fixture(RuntimeDefect::MalformedEvent);
    let (bundle, intake) = verify_fixture(&fixture);
    let counterexample = bundle
        .results
        .iter()
        .find(|result| {
            result.status == crate::EvidenceStatus::Fail
                && result.classification == Some(crate::FailureClassification::InvariantViolation)
        })
        .expect("serialized counterexample result")
        .clone();
    assert!(bundle.results.iter().any(|result| {
        result.status == crate::EvidenceStatus::Fail
            && result.classification == Some(crate::FailureClassification::InvariantViolation)
            && result.message.as_deref() == Some("real timeout fixture found a counterexample")
    }));
    assert!(intake
        .defects()
        .iter()
        .any(|error| error.message().contains("parse simulator log")));
    let report = aggregate_fixture(&fixture, &intake);
    assert_counterexample_survives(&report, &counterexample);
    assert!(report.invariants.iter().all(|verdict| {
        verdict.issues.iter().any(|issue| {
            issue.evidence_id == "aggregate/harness"
                && issue.message.contains("parse simulator log")
        })
    }));
}

#[test]
fn later_launch_failure_survives_loading_and_final_aggregation() {
    let fixture = materialize_fixture(RuntimeDefect::LaunchFailure);
    let (bundle, intake) = verify_fixture(&fixture);
    let counterexample = bundle
        .results
        .iter()
        .find(|result| result.status == crate::EvidenceStatus::Fail)
        .expect("first-run counterexample result")
        .clone();
    let launch_error = bundle
        .results
        .iter()
        .find(|result| {
            result.status == crate::EvidenceStatus::Error
                && result
                    .message
                    .as_deref()
                    .is_some_and(|message| message.contains("injected raft-soak launch failure"))
        })
        .expect("later launch failure result")
        .clone();
    assert!(intake.defects().iter().any(|error| error
        .message()
        .contains("did not run required profile raft-soak")));
    let report = aggregate_fixture(&fixture, &intake);
    assert_counterexample_survives(&report, &counterexample);
    let launch_verdict = report
        .invariants
        .iter()
        .find(|verdict| verdict.invariant_id == launch_error.invariant_id)
        .expect("launch-failure invariant verdict");
    let issue = launch_verdict
        .issues
        .iter()
        .find(|issue| issue.evidence_id == launch_error.evidence_id)
        .expect("launch failure survives final aggregation");
    assert_eq!(
        issue.classification,
        crate::FailureClassification::HarnessError
    );
    assert_eq!(issue.message, launch_error.message.expect("launch message"));
    assert_eq!(issue.artifacts, launch_error.artifacts);
}

#[test]
fn real_valid_looking_pass_then_exit_one_is_rejected_through_final_aggregation() {
    let fixture = materialize_fixture(RuntimeDefect::PassExitOne);
    let (bundle, intake) = verify_fixture(&fixture);
    let raw_log = fs::read_to_string(fixture.root.join("artifacts/invariants/fast.log"))
        .expect("read serialized exit-one simulator log");
    assert!(raw_log.lines().any(|line| line == "exit_code: Some(1)"));
    assert!(raw_log.contains("\"status\":\"pass\""));
    assert!(intake.defects().iter().any(|error| error
        .message()
        .contains("simulator log fast requires a zero-exit invocation")));
    assert!(bundle.results.iter().all(|result| {
        result.status == crate::EvidenceStatus::Error
            && result.classification == Some(crate::FailureClassification::HarnessError)
    }));

    let report = aggregate_fixture(&fixture, &intake);
    assert_eq!(report.summary.green, 0);
    assert_eq!(report.summary.red, report.summary.total);
    assert!(report.invariants.iter().all(|verdict| {
        verdict.issues.iter().any(|issue| {
            issue.evidence_id == "aggregate/harness"
                && issue
                    .message
                    .contains("simulator log fast requires a zero-exit invocation")
        })
    }));
}

#[test]
fn real_counterexample_then_exit_one_preserves_semantics_through_final_aggregation() {
    let fixture = materialize_fixture(RuntimeDefect::CounterexampleExitOne);
    let (bundle, intake) = verify_fixture(&fixture);
    let raw_log = fs::read_to_string(fixture.root.join("artifacts/invariants/fast.log"))
        .expect("read serialized counterexample exit-one simulator log");
    assert!(raw_log.lines().any(|line| line == "exit_code: Some(1)"));
    assert!(raw_log.contains("real exit-one fixture found a counterexample"));
    assert!(intake.defects().iter().any(|error| error
        .message()
        .contains("simulator log fast requires a zero-exit invocation")));
    let counterexample = bundle
        .results
        .iter()
        .find(|result| {
            result.status == crate::EvidenceStatus::Fail
                && result.classification == Some(crate::FailureClassification::InvariantViolation)
                && result.message.as_deref() == Some("real exit-one fixture found a counterexample")
        })
        .expect("serialized semantic counterexample")
        .clone();

    let report = aggregate_fixture(&fixture, &intake);
    assert_counterexample_survives(&report, &counterexample);
    assert_eq!(report.summary.green, 0);
    assert_eq!(report.summary.red, report.summary.total);
}

fn aggregate_fixture(
    fixture: &SimulatorFixture,
    intake: &crate::verification::EvidenceIntake,
) -> crate::VerdictReport {
    crate::verdict::reduce(&fixture.catalog, &fixture.manifest, intake)
        .expect("aggregate verified simulator fixture")
}

fn verify_fixture(
    fixture: &SimulatorFixture,
) -> (crate::ResultBundle, crate::verification::EvidenceIntake) {
    let bundle: crate::ResultBundle = serde_json::from_slice(
        &fs::read(&fixture.bundle_path).expect("read serialized simulator bundle"),
    )
    .expect("decode serialized simulator bundle");
    let request = crate::verification::VerificationRequest::new(
        &fixture.catalog,
        &fixture.manifest,
        &bundle.execution.plan,
        &bundle.source_ref,
        &fixture.root,
    );
    let intake =
        crate::verification::verify_layer_paths(request, "simulator", fixture.bundle_path.clone())
            .expect("verify serialized simulator fixture");
    (bundle, intake)
}

fn assert_counterexample_survives(
    report: &crate::VerdictReport,
    counterexample: &crate::EvidenceResult,
) {
    let verdict = report
        .invariants
        .iter()
        .find(|verdict| verdict.invariant_id == counterexample.invariant_id)
        .expect("counterexample invariant verdict");
    let issue = verdict
        .issues
        .iter()
        .find(|issue| issue.evidence_id == counterexample.evidence_id)
        .expect("semantic counterexample survives final aggregation");
    assert_eq!(
        issue.classification,
        crate::FailureClassification::InvariantViolation
    );
    assert_eq!(issue.message, counterexample.message.as_deref().unwrap());
    assert_eq!(issue.artifacts, counterexample.artifacts);
}
