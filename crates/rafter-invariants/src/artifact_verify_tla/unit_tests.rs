use super::{checkpoint::validate_inventory, checksum_matches, successful_detector};
use crate::producer::{
    tla_checkpoint::{CheckpointFile, CheckpointInventory},
    tla_output::TlcSummary,
    ProcessLog,
};
use crate::InvocationReceipt;
use std::collections::BTreeMap;

const SHA: &str = "cc4803dce2a8ffaf0f5920a9dc39df4b5ee34ab4cb53fb58ac557277a7e516b3";

#[test]
fn tool_checksum_binding_is_exact_and_unique() {
    assert!(checksum_matches(
        &format!("# pinned\n{SHA}  tla2tools.jar\n"),
        SHA
    ));
    assert!(!checksum_matches(
        &format!("{SHA}  tla2tools.jar\n{SHA}  tla2tools.jar\n"),
        SHA
    ));
    assert!(!checksum_matches(
        &format!("{}  tla2tools.jar\n", "0".repeat(64)),
        SHA
    ));
}

#[test]
fn detector_counterexample_identity_must_match_its_predicate() {
    let log = ProcessLog {
        schema_version: 2,
        label: "detector-negative-ElectionSafety".to_owned(),
        invocation: InvocationReceipt {
            program: "java".to_owned(),
            program_sha256: "0".repeat(64),
            arguments: Vec::new(),
            current_dir: ".".to_owned(),
            environment: BTreeMap::new(),
            environment_sha256: "0".repeat(64),
        },
        exit_code: Some(12),
        timed_out: false,
        termination: None,
        duration_ms: 1,
        peak_rss_kib: 1,
        stdout: String::new(),
        stderr: String::new(),
    };
    let mut summary = TlcSummary {
        distinct_states: 2,
        states_left: 0,
        search_depth: 2,
        process_finished: true,
        violated_invariant: Some("ElectionSafety".to_owned()),
        ..TlcSummary::default()
    };
    assert!(successful_detector(&log, &summary, "ElectionSafety"));
    assert!(!successful_detector(&log, &summary, "LogMatching"));
    summary.violated_invariant = Some("ExpectedViolation".to_owned());
    assert!(!successful_detector(&log, &summary, "ElectionSafety"));
}

#[test]
fn checkpoint_inventory_rejects_partial_or_multiple_run_directories() {
    let contract = "1".repeat(64);
    let complete = CheckpointInventory {
        schema_version: 1,
        contract_sha256: contract.clone(),
        latest_checkpoint: Some("run-a".to_owned()),
        files: vec![CheckpointFile {
            path: "run-a/queue.chkpt".to_owned(),
            sha256: "2".repeat(64),
            size_bytes: 0,
        }],
    };
    assert!(validate_inventory(&complete, &contract).is_ok());

    let mut partial = complete.clone();
    partial.files.push(CheckpointFile {
        path: "run-a/queue.tmp".to_owned(),
        sha256: "3".repeat(64),
        size_bytes: 1,
    });
    assert!(validate_inventory(&partial, &contract).is_err());

    let mut multiple = complete;
    multiple.files.push(CheckpointFile {
        path: "run-b/queue.chkpt".to_owned(),
        sha256: "4".repeat(64),
        size_bytes: 1,
    });
    assert!(validate_inventory(&multiple, &contract).is_err());
}
