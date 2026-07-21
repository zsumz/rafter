//! Evidence-domain outcomes for one exact libtest oracle execution.

use std::collections::BTreeMap;

use crate::evidence::{ArtifactRef, CheckCompletion, EvidenceStatus, FailureClassification};

use super::execution::ExactTestExecution;

pub(in crate::producer) struct TestOutcome {
    pub completion: CheckCompletion,
    pub status: EvidenceStatus,
    pub classification: Option<FailureClassification>,
    pub message: Option<String>,
    pub observations: BTreeMap<String, u64>,
    pub duration_ms: u64,
    pub peak_rss_kib: u64,
    pub artifacts: Vec<ArtifactRef>,
}

pub(super) struct ExactOutcomeEvidence<'a> {
    pub artifact: ArtifactRef,
    pub duration_ms: u64,
    pub peak_rss_kib: u64,
    pub exact_was_run: bool,
    pub exact_passed: bool,
    pub harness_error: Option<&'a str>,
}

pub(super) fn from_execution(
    execution: ExactTestExecution,
    test_name: &str,
    evidence: ExactOutcomeEvidence<'_>,
) -> TestOutcome {
    let ExactOutcomeEvidence {
        artifact,
        duration_ms,
        peak_rss_kib,
        exact_was_run,
        exact_passed,
        harness_error,
    } = evidence;
    match execution {
        ExactTestExecution::Pass => TestOutcome {
            completion: CheckCompletion::Completed,
            status: EvidenceStatus::Pass,
            classification: None,
            message: None,
            observations: observations(1, 1, 1),
            duration_ms,
            peak_rss_kib,
            artifacts: vec![artifact],
        },
        ExactTestExecution::CoverageNotReached => TestOutcome {
            completion: CheckCompletion::CoverageNotReached,
            status: EvidenceStatus::Incomplete,
            classification: Some(FailureClassification::CoverageNotReached),
            message: Some("libtest executed zero exact tests".to_owned()),
            observations: observations(1, usize::from(exact_was_run), usize::from(exact_passed)),
            duration_ms,
            peak_rss_kib,
            artifacts: vec![artifact],
        },
        ExactTestExecution::InvariantViolation => TestOutcome {
            completion: CheckCompletion::Counterexample,
            status: EvidenceStatus::Fail,
            classification: Some(FailureClassification::InvariantViolation),
            message: Some(format!(
                "direct oracle {test_name} reported an invariant violation"
            )),
            observations: observations(1, 1, 0),
            duration_ms,
            peak_rss_kib,
            artifacts: vec![artifact],
        },
        ExactTestExecution::HarnessError => TestOutcome {
            completion: CheckCompletion::HarnessError,
            status: EvidenceStatus::Error,
            classification: Some(FailureClassification::HarnessError),
            message: Some(harness_error.map_or_else(
                || {
                    format!(
                        "exact test process {test_name} failed without one canonical libtest verdict"
                    )
                },
                |error| format!("exact test process {test_name} failed: {error}"),
            )),
            observations: observations(1, usize::from(exact_was_run), 0),
            duration_ms,
            peak_rss_kib,
            artifacts: vec![artifact],
        },
    }
}

pub(super) fn error(
    message: String,
    artifact: ArtifactRef,
    peak_rss_kib: u64,
    duration_ms: u64,
    discovered: usize,
) -> TestOutcome {
    TestOutcome {
        completion: CheckCompletion::HarnessError,
        status: EvidenceStatus::Error,
        classification: Some(FailureClassification::HarnessError),
        message: Some(message),
        observations: observations(discovered, 0, 0),
        duration_ms,
        peak_rss_kib,
        artifacts: vec![artifact],
    }
}

pub(super) fn observations(
    discovered: usize,
    executed: usize,
    passed: usize,
) -> BTreeMap<String, u64> {
    BTreeMap::from([
        ("discovered".to_owned(), discovered as u64),
        ("executed".to_owned(), executed as u64),
        ("passed".to_owned(), passed as u64),
    ])
}
