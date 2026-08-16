//! Stable counterexample ownership and multi-violation scenarios.

use std::collections::BTreeMap;

use crate::{CheckCompletion, CheckReceipt, EvidenceResult, EvidenceStatus, FailureClassification};

use super::bind_counterexamples;

#[test]
fn non_base_counterexample_owns_the_single_rd06_failure() {
    let mut checks = vec![
        check("base", CheckCompletion::Completed, &["RD-06/evidence"]),
        check(
            "restart",
            CheckCompletion::Counterexample,
            &["PS-03/evidence"],
        ),
    ];
    checks[1]
        .observations
        .insert("invalid_trials".to_owned(), 1);
    let mut results = vec![result(
        "RD-06",
        "RD-06/evidence",
        "base",
        EvidenceStatus::Pass,
    )];

    bind_counterexamples(&mut checks, &mut results).expect("counterexample binds");

    assert!(!checks[0]
        .evidence_ids
        .contains(&"RD-06/evidence".to_owned()));
    assert!(checks[1]
        .evidence_ids
        .contains(&"RD-06/evidence".to_owned()));
    assert_eq!(results[0].execution_id, "restart");
    assert_eq!(results[0].status, EvidenceStatus::Fail);
    assert_eq!(
        results[0].classification,
        Some(FailureClassification::InvariantViolation)
    );
}

#[test]
fn lease_counterexample_remains_owned_by_rd05() {
    let mut checks = vec![check(
        "lease-isolation",
        CheckCompletion::Counterexample,
        &["RD-05/evidence"],
    )];
    let mut results = vec![result(
        "RD-05",
        "RD-05/evidence",
        "lease-isolation",
        EvidenceStatus::Fail,
    )];

    bind_counterexamples(&mut checks, &mut results).expect("counterexample remains bound");

    assert_eq!(checks[0].completion, CheckCompletion::Counterexample);
    assert_eq!(results[0].invariant_id, "RD-05");
    assert_eq!(results[0].status, EvidenceStatus::Fail);
}

#[test]
fn lease_and_linearizability_counterexamples_are_both_retained() {
    let mut checks = vec![
        check("base", CheckCompletion::Completed, &["RD-06/evidence"]),
        check(
            "lease-isolation",
            CheckCompletion::Counterexample,
            &["RD-05/evidence"],
        ),
    ];
    checks[1]
        .observations
        .insert("invalid_trials".to_owned(), 1);
    let mut results = vec![
        result("RD-06", "RD-06/evidence", "base", EvidenceStatus::Pass),
        result(
            "RD-05",
            "RD-05/evidence",
            "lease-isolation",
            EvidenceStatus::Fail,
        ),
    ];

    bind_counterexamples(&mut checks, &mut results).expect("combined counterexample binds");

    assert_eq!(checks[1].completion, CheckCompletion::Counterexample);
    assert!(checks[1]
        .evidence_ids
        .contains(&"RD-06/evidence".to_owned()));
    assert!(results
        .iter()
        .any(|result| result.invariant_id == "RD-05" && result.status == EvidenceStatus::Fail));
    assert!(results.iter().any(|result| {
        result.invariant_id == "RD-06"
            && result.execution_id == "lease-isolation"
            && result.status == EvidenceStatus::Fail
    }));
}

#[test]
fn lease_rd05_failure_remains_local_when_another_scenario_owns_rd06() {
    let mut checks = vec![
        check("base", CheckCompletion::Completed, &["RD-06/evidence"]),
        check(
            "restart",
            CheckCompletion::Counterexample,
            &["PS-03/evidence"],
        ),
        check(
            "lease-isolation",
            CheckCompletion::Counterexample,
            &["RD-05/evidence"],
        ),
    ];
    checks[1]
        .observations
        .insert("invalid_trials".to_owned(), 1);
    checks[2]
        .observations
        .insert("invalid_trials".to_owned(), 1);
    let mut results = vec![
        result("RD-06", "RD-06/evidence", "base", EvidenceStatus::Pass),
        result(
            "RD-05",
            "RD-05/evidence",
            "lease-isolation",
            EvidenceStatus::Fail,
        ),
    ];

    bind_counterexamples(&mut checks, &mut results).expect("counterexamples bind");

    assert_eq!(results[0].execution_id, "restart");
    assert_eq!(results[0].status, EvidenceStatus::Fail);
    assert_eq!(checks[2].completion, CheckCompletion::Counterexample);
    assert!(results.iter().any(|result| {
        result.invariant_id == "RD-05"
            && result.execution_id == "lease-isolation"
            && result.status == EvidenceStatus::Fail
    }));
}

fn check(id: &str, completion: CheckCompletion, evidence: &[&str]) -> CheckReceipt {
    CheckReceipt {
        execution_id: id.to_owned(),
        check_id: format!("maelstrom/{id}"),
        evidence_ids: evidence.iter().map(ToString::to_string).collect(),
        completion,
        observations: BTreeMap::new(),
        simulator_liveness: None,
        tla_continuation: None,
        duration_ms: 1,
        peak_rss_kib: 1,
        artifacts: Vec::new(),
    }
}

fn result(
    invariant: &str,
    evidence: &str,
    execution: &str,
    status: EvidenceStatus,
) -> EvidenceResult {
    EvidenceResult {
        invariant_id: invariant.to_owned(),
        evidence_id: evidence.to_owned(),
        execution_id: execution.to_owned(),
        status,
        classification: None,
        message: None,
        artifacts: Vec::new(),
    }
}
