//! Failure attribution and shared-check isolation tests.

use super::support::*;

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
