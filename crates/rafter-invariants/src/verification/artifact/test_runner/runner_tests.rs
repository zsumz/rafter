//! Compile-failure binding fixtures for missing runtime transcripts.

use std::collections::BTreeSet;

use super::compile_failure_explains_missing_transcript;
use crate::{EvidenceStatus, FailureClassification};

#[test]
fn compile_only_harness_error_must_name_the_same_execution() {
    let mut compilation = super::super::super::compiler::CompilationEvidence::default();
    compilation.record_failures(BTreeSet::from(["failed-execution".to_owned()]));
    let harness_error = (
        EvidenceStatus::Error,
        Some(FailureClassification::HarnessError),
    );

    assert!(compile_failure_explains_missing_transcript(
        harness_error,
        &compilation,
        "failed-execution",
    ));
    assert!(!compile_failure_explains_missing_transcript(
        harness_error,
        &compilation,
        "unrelated-execution",
    ));
    assert!(!compile_failure_explains_missing_transcript(
        (EvidenceStatus::Pass, None),
        &compilation,
        "failed-execution",
    ));
}
