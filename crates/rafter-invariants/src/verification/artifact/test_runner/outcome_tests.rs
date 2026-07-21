//! Adversarial exact-test outcome fixtures.

use std::collections::BTreeMap;

use super::oracle_failure_processes;

fn process(label: &str) -> crate::evidence::format::process::LabeledProcess {
    crate::evidence::format::process::LabeledProcess {
        schema_version: crate::evidence::format::process::COMBINED_PROCESS_SCHEMA_VERSION,
        label: label.to_owned(),
        invocation: crate::InvocationReceipt {
            program: "/fixture/test".to_owned(),
            program_sha256: "0".repeat(64),
            arguments: Vec::new(),
            current_dir: "/fixture".to_owned(),
            environment_sha256: crate::provenance::invocation::digest_environment(&BTreeMap::new())
                .expect("fixture environment hashes"),
            environment: BTreeMap::new(),
            launchers: crate::receipt::fixture_launchers(false),
        },
        exit_code: Some(0),
        timed_out: false,
        metrics: crate::evidence::format::process::ProcessMetrics {
            duration_ms: 1,
            peak_rss_kib: 1,
        },
        stdout: String::new(),
        stderr: String::new(),
        detector_challenge: None,
    }
}

#[test]
fn truncated_oracle_failure_transcript_is_a_harness_error_not_a_panic() {
    let truncated = [
        process("libtest discovery"),
        process("libtest ignored discovery"),
    ];
    let error = oracle_failure_processes(&truncated)
        .expect_err("a failing receipt needs the exact failing invocation");
    assert!(error
        .to_string()
        .contains("missing its exact failing invocation"));
}
