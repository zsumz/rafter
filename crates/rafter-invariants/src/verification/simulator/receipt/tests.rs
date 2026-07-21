//! Scenarios: every simulator receipt obligation fails closed independently.

use std::collections::BTreeMap;

use super::validate;
use crate::{
    contract::{catalog::EvidenceDescriptor, profile::RunnerContract},
    evidence::{EvidenceStatus, FailureClassification, ResultBundle},
};

#[test]
fn simulator_receipt_rejects_duplicate_checks_and_fanout_drift() {
    with_fixture(|bundle, expected, contract| {
        let duplicate = bundle.execution.checks[0].clone();
        bundle.execution.checks.push(duplicate);
        assert!(validate(bundle, expected, contract).is_err());

        bundle.execution.checks.pop();
        bundle.execution.checks[0]
            .evidence_ids
            .push("unknown/evidence".to_owned());
        assert!(validate(bundle, expected, contract).is_err());
    });
}

#[test]
fn simulator_receipt_rejects_conflicting_statuses_for_one_execution() {
    with_fixture(|bundle, expected, contract| {
        let execution_id = bundle.execution.checks[0].execution_id.clone();
        let mut conflict = bundle
            .results
            .iter()
            .find(|result| result.execution_id == execution_id)
            .expect("result for first simulator execution")
            .clone();
        conflict.status = EvidenceStatus::Fail;
        conflict.classification = Some(FailureClassification::InvariantViolation);
        bundle.results.push(conflict);
        assert!(validate(bundle, expected, contract).is_err());
    });
}

#[test]
fn simulator_receipt_requires_detector_qualification_artifacts() {
    with_fixture(|bundle, expected, contract| {
        let evidence_id = expected
            .iter()
            .find(|(_, descriptor)| {
                descriptor
                    .simulator
                    .as_ref()
                    .is_some_and(|identity| identity.negative_test.is_some())
            })
            .map(|(evidence_id, _)| evidence_id.as_str())
            .expect("simulator detector evidence");
        let check = check_for_evidence(bundle, evidence_id);
        check
            .artifacts
            .retain(|artifact| artifact.kind != "test-log");
        assert!(validate(bundle, expected, contract).is_err());
    });
}

#[test]
fn simulator_receipt_enforces_descriptor_and_profile_state_floors() {
    with_fixture(|bundle, expected, contract| {
        let (evidence_id, descriptor) = expected
            .iter()
            .find(|(_, descriptor)| {
                descriptor
                    .simulator
                    .as_ref()
                    .is_some_and(|identity| identity.minimum_protocol_states.is_some())
            })
            .expect("simulator safety evidence");
        let identity = descriptor.simulator.as_ref().unwrap();
        let minimum_protocol_states = identity.minimum_protocol_states.unwrap() as u64;
        let check_id = identity
            .checks
            .iter()
            .find(|check_id| contract.simulator_checks.contains_key(*check_id))
            .expect("profile-owned simulator check")
            .clone();
        check_for_evidence(bundle, evidence_id)
            .observations
            .insert("unique_protocol_states".to_owned(), 0);
        assert!(validate(bundle, expected, contract).is_err());

        let check = check_for_evidence(bundle, evidence_id);
        check
            .observations
            .insert("unique_protocol_states".to_owned(), minimum_protocol_states);
        check.observations.insert(
            crate::contract::profile::per_check_protocol_states_key(&check_id),
            0,
        );
        assert!(validate(bundle, expected, contract).is_err());
    });
}

#[test]
fn simulator_receipt_requires_exact_typed_liveness_binding() {
    with_fixture(|bundle, expected, contract| {
        let evidence_id = expected
            .iter()
            .find(|(_, descriptor)| {
                descriptor
                    .simulator
                    .as_ref()
                    .is_some_and(|identity| identity.liveness_report.is_some())
            })
            .map(|(evidence_id, _)| evidence_id.as_str())
            .expect("simulator liveness evidence");
        check_for_evidence(bundle, evidence_id).simulator_liveness = None;
        assert!(validate(bundle, expected, contract).is_err());
    });
}

fn with_fixture(
    test: impl FnOnce(&mut ResultBundle, &BTreeMap<String, &EvidenceDescriptor>, &RunnerContract),
) {
    let (catalog, manifest) = crate::tests::loaded();
    let profile = &manifest.profiles["pr"];
    let required = catalog.required_evidence(profile);
    let expected = required
        .values()
        .flatten()
        .map(|evidence| (evidence.evidence_id(), evidence))
        .collect::<BTreeMap<_, _>>();
    let mut bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "simulator")
        .expect("passing simulator bundle");
    validate(&bundle, &expected, &profile.runners["simulator"])
        .expect("fixture starts as a valid simulator receipt");
    test(&mut bundle, &expected, &profile.runners["simulator"]);
}

fn check_for_evidence<'a>(
    bundle: &'a mut ResultBundle,
    evidence_id: &str,
) -> &'a mut crate::CheckReceipt {
    bundle
        .execution
        .checks
        .iter_mut()
        .find(|check| check.evidence_ids.iter().any(|id| id == evidence_id))
        .expect("simulator check for evidence")
}
