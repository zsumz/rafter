//! Scenarios for independent Maelstrom completion classification.

use std::collections::{BTreeMap, BTreeSet};

use crate::{CheckCompletion, CheckReceipt, EvidenceStatus};

use super::{
    counterexample_statuses, expected_counterexample_invariants, has_harness_error,
    local_counterexample_agrees, LeaseArtifactStatus,
};

#[test]
fn combined_lease_violation_retains_secondary_harness_classification() {
    assert!(has_harness_error(
        &[true],
        &[true],
        &[LeaseArtifactStatus::ViolationWithHarnessError]
    ));
}

#[test]
fn independent_verifier_requires_both_rd05_and_rd06_failures_when_both_rederive() {
    let expected = expected_counterexample_invariants(true, true, true);
    assert_eq!(expected, BTreeSet::from(["RD-05", "RD-06"]));
    let check = check();
    let combined = [
        ("RD-05", EvidenceStatus::Fail),
        ("RD-06", EvidenceStatus::Fail),
        ("LG-04", EvidenceStatus::Incomplete),
    ];
    assert!(counterexample_statuses(&check, &combined, &expected));
    assert!(!counterexample_statuses(&check, &combined[..1], &expected));
}

#[test]
fn local_rd05_failure_survives_harness_faults_and_external_rd06_ownership() {
    let expected = BTreeSet::from(["RD-05"]);
    let check = check();
    let statuses = [("RD-05", EvidenceStatus::Fail)];

    assert!(local_counterexample_agrees(
        &check, &statuses, &expected, false, false, false
    ));
    assert!(local_counterexample_agrees(
        &check, &statuses, &expected, true, false, true
    ));
    assert!(!local_counterexample_agrees(
        &check, &statuses, &expected, true, false, false
    ));
}

fn check() -> CheckReceipt {
    CheckReceipt {
        execution_id: "lease".to_owned(),
        check_id: "maelstrom/lease-isolation".to_owned(),
        evidence_ids: Vec::new(),
        completion: CheckCompletion::Counterexample,
        observations: BTreeMap::new(),
        simulator_liveness: None,
        tla_continuation: None,
        duration_ms: 1,
        peak_rss_kib: 1,
        artifacts: Vec::new(),
    }
}
