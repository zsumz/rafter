//! Detector identity, transcript, and compilation-evidence tests.

use super::support::*;

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
        &mut crate::verification::DetectorFixtureAnalysis::default(),
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
        &mut crate::verification::DetectorFixtureAnalysis::default(),
        &mut BTreeMap::new(),
    )
    .expect_err("a runtime detector failure cannot omit its runtime transcript");
    assert!(error.to_string().contains("detector log missing"));
}

#[test]
fn qualified_detector_rejects_duplicate_test_logs() {
    let (catalog, manifest) = crate::tests::loaded();
    let bundle = simulator_bundle(&catalog, &manifest);
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
    check
        .observations
        .insert("detector_qualified".to_owned(), 1);
    let duplicate = check
        .artifacts
        .iter()
        .find(|artifact| artifact.kind == "test-log")
        .expect("qualified detector test log")
        .clone();
    check.artifacts.push(duplicate);
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");

    let error = verify_negative_detector_evidence(
        &bundle,
        &root,
        &check,
        descriptor,
        identity,
        &mut crate::verification::DetectorFixtureAnalysis::default(),
        &mut BTreeMap::new(),
    )
    .expect_err("a qualified detector must bind exactly one runtime transcript");

    assert!(
        error
            .to_string()
            .contains("must bind exactly one detector test-log, found 2"),
        "{error}"
    );
}
