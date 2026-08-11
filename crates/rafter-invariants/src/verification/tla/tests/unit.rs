//! Focused TLA+ artifact-policy unit scenarios.

use crate::verification::tla::{checksum_matches, successful_detector, validate_inventory};
use crate::InvocationReceipt;
use crate::{
    evidence::format::tla::checkpoint::{CheckpointFile, CheckpointInventory},
    evidence::format::{process::ProcessLog, tla::TlcSummary},
};
use std::collections::BTreeMap;

const SHA: &str = "ab323b79802aedc3203b3f9af37c6aca3ed43f4e0225b36f2aa77b26de46c05f";

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

/// Upstream v1.8.0 is a rolling channel whose assets are replaced and deleted,
/// so the checked-in checksum manifest and the profile contract are the only
/// things that pin an identity. They must agree exactly, and the manifest must
/// declare that digest once -- a second `tla2tools.jar` line would let a run
/// accept either jar.
#[test]
fn the_checked_in_tool_manifest_matches_every_profile_pin() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let checksums =
        std::fs::read_to_string(root.join("tools/tla/SHA256SUMS")).expect("read SHA256SUMS");
    let asset_id = std::fs::read_to_string(root.join("tools/tla/ASSET_ID")).expect("read ASSET_ID");
    let manifest = crate::ProfileManifest::load(&root.join("verification/raft-invariant-profiles.json"))
        .expect("load profile manifest");

    for profile in ["pr", "nightly", "weekly"] {
        let configuration = &manifest.profiles[profile].runners["tla"].configuration;
        assert!(
            checksum_matches(&checksums, &configuration["tool_sha256"]),
            "{profile} tool_sha256 is not the unique tla2tools.jar digest in SHA256SUMS"
        );
        assert_eq!(
            asset_id.trim(),
            configuration["tool_asset_id"],
            "{profile} tool_asset_id drifted from tools/tla/ASSET_ID"
        );
    }
}

#[test]
fn detector_counterexample_identity_must_match_its_predicate() {
    let log = ProcessLog {
        schema_version: 4,
        label: "detector-negative-ElectionSafety".to_owned(),
        invocation: InvocationReceipt {
            program: "java".to_owned(),
            program_sha256: "0".repeat(64),
            arguments: Vec::new(),
            current_dir: ".".to_owned(),
            environment: BTreeMap::new(),
            environment_sha256: "0".repeat(64),
            launchers: crate::receipt::fixture_launchers(false),
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
