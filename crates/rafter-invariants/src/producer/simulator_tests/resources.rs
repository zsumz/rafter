//! Per-evidence artifact and execution-resource accounting scenarios.

use std::collections::BTreeMap;

use super::super::{evaluate, test_exec::TestOutcome, DetectorRun};
use super::support::{model_fixture, safety_descriptor};
use crate::{CheckCompletion, EvidenceStatus, FailureClassification};
use serde_json::json;

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
                TestOutcome {
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
                TestOutcome {
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
