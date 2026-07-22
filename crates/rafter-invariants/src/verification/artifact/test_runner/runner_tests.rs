//! Compile-failure binding fixtures for missing runtime transcripts.

use std::collections::{BTreeMap, BTreeSet};

use super::{compile_failure_explains_missing_transcript, validate_canonical_test_transcript};
use crate::{
    evidence::{format::process::ProcessObservation, InvocationReceipt},
    EvidenceStatus, FailureClassification,
};

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

#[test]
fn canonical_detector_transcript_accepts_v4_discovery_and_v5_exact_execution() {
    let invocation = InvocationReceipt {
        program: "fixture".to_owned(),
        program_sha256: "0".repeat(64),
        arguments: Vec::new(),
        current_dir: "/workspace".to_owned(),
        environment: BTreeMap::default(),
        environment_sha256: "1".repeat(64),
        launchers: Vec::new(),
    };
    let observation = ProcessObservation {
        invocation: &invocation,
        exit_code: Some(0),
        timed_out: false,
        termination: None,
        duration_ms: 1,
        peak_rss_kib: 1,
        stdout: b"",
        stderr: b"",
    };
    let mut transcript =
        crate::evidence::format::process::encode_combined_v4("libtest discovery", observation)
            .expect("encode discovery receipt");
    transcript.extend(
        crate::evidence::format::process::encode_detector_v5(
            "exact libtest execution",
            observation,
            &"a".repeat(64),
        )
        .expect("encode detector receipt"),
    );

    validate_canonical_test_transcript(
        std::str::from_utf8(&transcript).expect("UTF-8 transcript"),
        "fixture.log",
    )
    .expect("mixed canonical detector transcript must parse");
}
