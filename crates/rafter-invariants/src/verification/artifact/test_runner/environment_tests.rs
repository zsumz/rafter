//! Detector-schema environment reconciliation scenarios.

use std::collections::BTreeMap;

use super::extend_detector_environment;
use crate::evidence::{
    format::process::{LabeledProcess, ProcessMetrics},
    InvocationReceipt,
};

fn process(schema_version: u32, descriptor: Option<&str>) -> LabeledProcess {
    let mut environment = BTreeMap::new();
    if let Some(descriptor) = descriptor {
        environment.insert(
            crate::evidence::detector_proof::PROOF_DESCRIPTOR_ENV.to_owned(),
            descriptor.to_owned(),
        );
    }
    LabeledProcess {
        schema_version,
        label: "exact libtest execution".to_owned(),
        invocation: InvocationReceipt {
            program: "/fixture".to_owned(),
            program_sha256: "0".repeat(64),
            arguments: Vec::new(),
            current_dir: "/workspace".to_owned(),
            environment,
            environment_sha256: "1".repeat(64),
            launchers: Vec::new(),
        },
        exit_code: Some(0),
        timed_out: false,
        metrics: ProcessMetrics {
            duration_ms: 1,
            peak_rss_kib: 1,
        },
        stdout: String::new(),
        stderr: String::new(),
        detector_challenge: None,
    }
}

fn transcript(exact: LabeledProcess) -> Vec<LabeledProcess> {
    let discovery = process(
        crate::evidence::format::process::COMBINED_PROCESS_SCHEMA_VERSION,
        None,
    );
    vec![discovery.clone(), discovery, exact]
}

#[test]
fn detector_schema_extends_the_exact_environment_for_any_runner() {
    let mut environment = BTreeMap::new();
    extend_detector_environment(
        &transcript(process(
            crate::evidence::format::process::DETECTOR_PROCESS_SCHEMA_VERSION,
            Some("4"),
        )),
        &mut environment,
    )
    .expect("a detector transcript carries its inherited descriptor");

    assert_eq!(
        environment.get(crate::evidence::detector_proof::PROOF_DESCRIPTOR_ENV),
        Some(&"4".to_owned())
    );
}

#[test]
fn detector_schema_rejects_missing_or_noncanonical_descriptors() {
    for descriptor in [None, Some("0"), Some("04"), Some("not-a-descriptor")] {
        assert!(extend_detector_environment(
            &transcript(process(
                crate::evidence::format::process::DETECTOR_PROCESS_SCHEMA_VERSION,
                descriptor,
            )),
            &mut BTreeMap::new(),
        )
        .is_err());
    }
}

#[test]
fn ordinary_process_schema_does_not_acquire_a_detector_descriptor() {
    let mut environment = BTreeMap::new();
    extend_detector_environment(
        &transcript(process(
            crate::evidence::format::process::COMBINED_PROCESS_SCHEMA_VERSION,
            Some("4"),
        )),
        &mut environment,
    )
    .expect("ordinary process environment remains unchanged");

    assert!(environment.is_empty());
}
