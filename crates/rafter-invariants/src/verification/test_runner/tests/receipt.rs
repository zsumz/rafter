//! Scenarios: non-passing test receipts retain their exact structural matrix.

use super::validate;
use crate::{
    CheckCompletion, EvidenceStatus, FailureClassification, ProfileManifest, ResultBundle,
};
use std::collections::BTreeMap;

#[test]
fn nonpass_test_receipts_require_the_exact_status_matrix() {
    let (catalog, manifest): (_, ProfileManifest) = crate::tests::loaded();
    let required = catalog.required_evidence(&manifest.profiles["pr"]);
    let expected = required
        .values()
        .flatten()
        .map(|evidence| (evidence.evidence_id(), evidence))
        .collect::<BTreeMap<_, _>>();
    let passing = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "tests")
        .expect("tests bundle");
    validate(&passing, &expected).expect("passing receipt validates");

    let mut duplicate_binary = passing.clone();
    let binary = duplicate_binary.execution.checks[0]
        .artifacts
        .iter()
        .find(|artifact| artifact.kind == "test-binary")
        .expect("passing check preserves a test binary")
        .clone();
    duplicate_binary.execution.checks[0].artifacts.push(binary);
    assert!(validate(&duplicate_binary, &expected).is_err());

    let mut failed: ResultBundle = passing;
    let check = &mut failed.execution.checks[0];
    let execution_id = check.execution_id.clone();
    check.completion = CheckCompletion::Counterexample;
    check.observations = BTreeMap::from([
        ("discovered".to_owned(), 1),
        ("executed".to_owned(), 1),
        ("passed".to_owned(), 0),
    ]);
    let artifacts = check.artifacts.clone();
    for result in failed
        .results
        .iter_mut()
        .filter(|result| result.execution_id == execution_id)
    {
        result.status = EvidenceStatus::Fail;
        result.classification = Some(FailureClassification::InvariantViolation);
        result.message = Some("registered oracle rejected the revision".to_owned());
        result.artifacts.clone_from(&artifacts);
    }
    validate(&failed, &expected).expect("counterexample receipt validates");

    let mut forged = failed.clone();
    forged.execution.checks[0]
        .observations
        .insert("passed".to_owned(), 1);
    assert!(validate(&forged, &expected).is_err());
    let affected = forged.execution.checks[0].execution_id.clone();
    forged.execution.checks[0].observations = failed.execution.checks[0].observations.clone();
    forged
        .results
        .iter_mut()
        .find(|result| result.execution_id == affected)
        .expect("affected result")
        .classification = Some(FailureClassification::HarnessError);
    assert!(validate(&forged, &expected).is_err());
}
