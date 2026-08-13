//! Scenarios: aggregate replay qualifies only its bound evidence records.

use std::{collections::BTreeMap, path::Path};

use crate::{
    verification::detector_replay::{DetectorReplayAssessment, EvidenceReplayQualification},
    ArtifactRef, EvidenceResult, EvidenceStatus, FailureClassification,
};

use super::super::{verify_receipts_for_test, VerificationRequest};

#[test]
fn failed_fixture_turns_only_its_passing_evidence_into_a_harness_error() {
    let (catalog, manifest) = crate::tests::loaded();
    let plan = crate::tests::plan_receipt(&manifest, "pr");
    let request = VerificationRequest::new(
        &catalog,
        &manifest,
        &plan,
        "abc",
        Path::new("."),
        crate::verification::VerificationContext::ProducingJob,
    );
    let bundles = crate::tests::passing_bundles(&catalog, &manifest);
    let mut intake = verify_receipts_for_test(request, &bundles, Vec::new())
        .expect("passing receipts produce an intake");
    let evidence =
        crate::verification::detector_replay::required_evidence(&catalog, &manifest.profiles["pr"])
            .into_iter()
            .next()
            .expect("direct simulator evidence");
    let artifact = verifier_artifact();
    let assessment = DetectorReplayAssessment {
        qualifications: BTreeMap::from([(
            evidence.evidence_id.clone(),
            EvidenceReplayQualification::failed(
                evidence.invariant_id.clone(),
                "fixture transcript was malformed",
                vec![artifact.clone()],
            ),
        )]),
        artifacts: vec![artifact.clone()],
        artifact_guard: None,
    };

    intake
        .apply_detector_replay(assessment)
        .expect("replay qualification applies");

    let failed = &intake.accepted()[&evidence.evidence_id];
    assert_eq!(failed.status, EvidenceStatus::Error);
    assert_eq!(
        failed.classification,
        Some(FailureClassification::HarnessError)
    );
    assert!(failed
        .message
        .as_deref()
        .is_some_and(|message| message.contains("fixture transcript was malformed")));
    assert_eq!(failed.artifacts.as_slice(), std::slice::from_ref(&artifact));
    assert!(intake.artifacts().contains(&artifact));
    assert!(intake
        .accepted()
        .iter()
        .filter(|(id, _)| *id != &evidence.evidence_id)
        .all(|(_, result)| result.status == EvidenceStatus::Pass));
}

#[test]
fn identity_mismatch_is_rejected_before_any_evidence_is_changed() {
    let (catalog, manifest) = crate::tests::loaded();
    let plan = crate::tests::plan_receipt(&manifest, "pr");
    let request = VerificationRequest::new(
        &catalog,
        &manifest,
        &plan,
        "abc",
        Path::new("."),
        crate::verification::VerificationContext::ProducingJob,
    );
    let bundles = crate::tests::passing_bundles(&catalog, &manifest);
    let mut intake = verify_receipts_for_test(request, &bundles, Vec::new())
        .expect("passing receipts produce an intake");
    let evidence =
        crate::verification::detector_replay::required_evidence(&catalog, &manifest.profiles["pr"])
            .into_iter()
            .next()
            .expect("direct simulator evidence");
    let assessment = DetectorReplayAssessment {
        qualifications: BTreeMap::from([(
            evidence.evidence_id.clone(),
            EvidenceReplayQualification::failed(
                "wrong-invariant".to_owned(),
                "untrusted identity",
                vec![verifier_artifact()],
            ),
        )]),
        artifacts: vec![verifier_artifact()],
        artifact_guard: None,
    };

    assert!(intake.apply_detector_replay(assessment).is_err());
    assert!(intake
        .accepted()
        .values()
        .all(|result| result.status == EvidenceStatus::Pass));
}

#[test]
fn passing_qualification_without_an_artifact_guard_is_rejected() {
    let (catalog, manifest) = crate::tests::loaded();
    let plan = crate::tests::plan_receipt(&manifest, "pr");
    let request = VerificationRequest::new(
        &catalog,
        &manifest,
        &plan,
        "abc",
        Path::new("."),
        crate::verification::VerificationContext::ProducingJob,
    );
    let bundles = crate::tests::passing_bundles(&catalog, &manifest);
    let mut intake = verify_receipts_for_test(request, &bundles, Vec::new())
        .expect("passing receipts produce an intake");
    let evidence =
        crate::verification::detector_replay::required_evidence(&catalog, &manifest.profiles["pr"])
            .into_iter()
            .next()
            .expect("direct simulator evidence");
    let artifact = verifier_artifact();
    let assessment = DetectorReplayAssessment {
        qualifications: BTreeMap::from([(
            evidence.evidence_id.clone(),
            EvidenceReplayQualification::passed(evidence.invariant_id, vec![artifact.clone()]),
        )]),
        artifacts: vec![artifact],
        artifact_guard: None,
    };

    let error = intake
        .apply_detector_replay(assessment)
        .expect_err("unguarded passing replay must fail");
    assert!(
        error.contains("require a complete artifact guard"),
        "{error}"
    );
}

#[test]
fn coverage_mismatch_preserves_artifacts_and_fails_every_required_binding() {
    let inventory = vec![
        crate::verification::detector_replay::ReplayEvidence {
            invariant_id: "ST-01".to_owned(),
            evidence_id: "fixture/one".to_owned(),
        },
        crate::verification::detector_replay::ReplayEvidence {
            invariant_id: "ST-02".to_owned(),
            evidence_id: "fixture/two".to_owned(),
        },
    ];
    let artifact = verifier_artifact();
    let assessment = DetectorReplayAssessment {
        qualifications: BTreeMap::from([(
            inventory[0].evidence_id.clone(),
            EvidenceReplayQualification::passed(
                inventory[0].invariant_id.clone(),
                vec![artifact.clone()],
            ),
        )]),
        artifacts: vec![artifact.clone()],
        artifact_guard: None,
    };

    let failed = super::super::replay::validate_coverage(assessment, &inventory)
        .expect("coverage mismatch becomes evidence-local failure");

    assert_eq!(failed.artifacts.as_slice(), std::slice::from_ref(&artifact));
    assert_eq!(failed.qualifications.len(), inventory.len());
    assert!(failed.qualifications.values().all(|qualification| {
        !qualification.is_passed() && qualification.artifacts() == [artifact.clone()]
    }));
}

#[test]
fn failed_replay_never_erases_an_existing_invariant_violation() {
    let evidence = crate::verification::detector_replay::ReplayEvidence {
        invariant_id: "ST-01".to_owned(),
        evidence_id: "fixture/violation".to_owned(),
    };
    let original_artifact = ArtifactRef {
        kind: "counterexample".to_owned(),
        path: "artifacts/counterexample.json".to_owned(),
        sha256: "1".repeat(64),
        size_bytes: 1,
    };
    let replay_artifact = verifier_artifact();
    let mut intake = super::super::EvidenceIntake::new(
        "pr",
        "abc",
        BTreeMap::from([(
            evidence.evidence_id.clone(),
            EvidenceResult {
                invariant_id: evidence.invariant_id.clone(),
                evidence_id: evidence.evidence_id.clone(),
                execution_id: "run".to_owned(),
                status: EvidenceStatus::Fail,
                classification: Some(FailureClassification::InvariantViolation),
                message: Some("detector found a real counterexample".to_owned()),
                artifacts: vec![original_artifact.clone()],
            },
        )]),
        vec![original_artifact.clone()],
        Vec::new(),
    );
    let assessment = DetectorReplayAssessment {
        qualifications: BTreeMap::from([(
            evidence.evidence_id.clone(),
            EvidenceReplayQualification::failed(
                evidence.invariant_id.clone(),
                "replay harness failed",
                vec![replay_artifact.clone()],
            ),
        )]),
        artifacts: vec![replay_artifact.clone()],
        artifact_guard: None,
    };

    intake
        .apply_detector_replay(assessment)
        .expect("replay qualification applies");

    let result = &intake.accepted()[&evidence.evidence_id];
    assert_eq!(result.status, EvidenceStatus::Fail);
    assert_eq!(
        result.classification,
        Some(FailureClassification::InvariantViolation)
    );
    assert_eq!(
        result.message.as_deref(),
        Some("detector found a real counterexample")
    );
    assert!(result.artifacts.contains(&original_artifact));
    assert!(result.artifacts.contains(&replay_artifact));
}

#[test]
fn malformed_fallback_inventory_records_an_unverifiable_defect() {
    let duplicate = crate::verification::detector_replay::ReplayEvidence {
        invariant_id: "ST-01".to_owned(),
        evidence_id: "fixture/duplicate".to_owned(),
    };
    let mut intake =
        super::super::EvidenceIntake::new("pr", "abc", BTreeMap::new(), Vec::new(), Vec::new());

    super::super::replay::apply_fallback(
        &mut intake,
        vec![duplicate.clone(), duplicate],
        "primary overlay failed",
    );

    assert!(intake
        .defect_messages()
        .iter()
        .any(|message| message.contains("fallback could not be applied")));
}

fn verifier_artifact() -> ArtifactRef {
    ArtifactRef {
        kind: "verifier-replay-report".to_owned(),
        path: "target/rafter-invariants/detector-replay-artifacts/report".to_owned(),
        sha256: "0".repeat(64),
        size_bytes: 1,
    }
}
