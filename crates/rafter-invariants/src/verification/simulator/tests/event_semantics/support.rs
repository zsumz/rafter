//! Shared builders and adapters for simulator event-semantics tests.

pub(super) use std::{collections::BTreeMap, path::PathBuf};

pub(super) use serde_json::{json, Value};

pub(super) use super::super::{
    derive_check_contract_issue, derive_simulator_observation_counts, index_simulator_event,
    inspect_machine_events, raw_event_issue, verified_passing_simulator_event_contract,
    verify_composite_observation, verify_negative_detector_evidence_with,
    verify_negative_fixture_binding, verify_nonpassing_event_classification,
    verify_simulator_observations, RawEventIssue,
};
pub(super) use crate::verification::simulator::event_semantics_test_support::DetectorTestHarness;

pub(super) fn verify_negative_detector_evidence(
    bundle: &crate::ResultBundle,
    root: &std::path::Path,
    check: &crate::CheckReceipt,
    descriptor: &crate::EvidenceDescriptor,
    identity: &crate::SimulatorIdentity,
    detector_sources: &mut crate::verification::DetectorFixtureAnalysis,
    test_logs: &mut BTreeMap<String, String>,
) -> Result<(), crate::verification::AggregateError> {
    verify_negative_detector_evidence_with(
        bundle,
        root,
        check,
        descriptor,
        identity,
        DetectorTestHarness::new(
            detector_sources,
            test_logs,
            crate::artifact_verify::detector_log_verifier(),
        ),
    )
}

pub(super) fn shared_log_descriptors(
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

pub(super) fn set_result_outcome(
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

pub(super) fn simulator_bundle(
    catalog: &crate::Catalog,
    manifest: &crate::ProfileManifest,
) -> crate::ResultBundle {
    crate::tests::passing_bundles(catalog, manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "simulator")
        .expect("simulator bundle")
}

pub(super) fn serialized_events(profile: &str, event: &Value) -> BTreeMap<String, Vec<Value>> {
    let source = format!("{}{}", crate::artifact_verify::EVENT_PREFIX, event);
    let (parsed, diagnostics) = crate::artifact_verify::simulator_schedule::scan_machine_events(
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
