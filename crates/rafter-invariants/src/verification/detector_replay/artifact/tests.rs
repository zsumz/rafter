//! Verifier replay report publication scenarios.

use std::path::PathBuf;

use sha2::{Digest, Sha256};

use super::{model::TargetReport, publish_preparation_failure, validation::validate_report_bytes};
use crate::verification::detector_replay::PreparationFailureRequest;
use crate::verification::{
    detector_replay::ReplayEvidence,
    source::{
        AuthenticatedSourceReceipt, ReplaySourceReceipts, ReplayToolchainProgramReceipt,
        ReplayToolchainReceipt, SourceMaterializationReceipt,
    },
};

#[test]
fn preparation_failure_publishes_machine_readable_evidence_local_results() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let manifest =
        crate::ProfileManifest::load(&root.join("verification/raft-invariant-profiles.json"))
            .expect("load profile manifest");
    let inventory = vec![
        ReplayEvidence {
            invariant_id: "ST-01".to_owned(),
            evidence_id: "fixture/one".to_owned(),
        },
        ReplayEvidence {
            invariant_id: "EL-01".to_owned(),
            evidence_id: "fixture/two".to_owned(),
        },
    ];
    let source_ref = "a".repeat(40);

    let assessment = publish_preparation_failure(PreparationFailureRequest {
        inventory,
        replay: None,
        receipts: receipts(),
        contract: &manifest.verifiers["pr"].detector_replay,
        profile: "pr",
        source_ref: &source_ref,
        registry: None,
        message: "authenticated registry unavailable",
        deadlines: crate::verification::detector_replay::deadlines(
            &manifest.verifiers["pr"].detector_replay,
        )
        .expect("derive replay deadlines"),
    })
    .expect("publish verifier failure report");

    assert_eq!(assessment.qualifications.len(), 2);
    assert!(assessment
        .qualifications
        .values()
        .all(|qualification| !qualification.is_passed()));
    assert_eq!(assessment.artifacts.len(), 2);
    let artifact = assessment
        .artifacts
        .iter()
        .find(|artifact| artifact.kind == "verifier-replay-report")
        .expect("preparation failure publishes its report");
    let manifest = assessment
        .artifacts
        .iter()
        .find(|artifact| artifact.kind == "verifier-artifact-manifest")
        .expect("preparation failure publishes its upload manifest");
    assert_eq!(artifact.kind, "verifier-replay-report");
    let bytes = std::fs::read(&artifact.path).expect("read verifier report");
    assert_eq!(artifact.size_bytes, bytes.len() as u64);
    assert_eq!(artifact.sha256, format!("{:x}", Sha256::digest(&bytes)));
    let report: serde_json::Value = serde_json::from_slice(&bytes).expect("parse verifier report");
    assert_eq!(report["schema_version"], 4);
    assert_eq!(report["profile"], "pr");
    assert_eq!(report["inventory"]["evidence_bindings"], 2);
    assert_eq!(report["compilation"]["status"], "harness_error");
    assert_eq!(
        report["compilation"]["message"],
        "authenticated registry unavailable"
    );
    let manifest_bytes = std::fs::read(&manifest.path).expect("read verifier manifest");
    assert_eq!(manifest.size_bytes, manifest_bytes.len() as u64);
    assert_eq!(
        manifest.sha256,
        format!("{:x}", Sha256::digest(&manifest_bytes))
    );
    assert!(String::from_utf8(manifest_bytes)
        .expect("manifest is UTF-8")
        .contains(&artifact.sha256));
}

#[test]
fn replay_report_validation_rejects_mutated_provenance_and_inventory() {
    let bytes = preparation_report_bytes('b');
    let original: serde_json::Value = serde_json::from_slice(&bytes).expect("parse replay report");

    let mut changed_source = original.clone();
    changed_source["source"]["tree"] = serde_json::Value::String("c".repeat(40));
    assert!(validate_report_bytes(&canonical(&changed_source)).is_err());

    let mut changed_toolchain = original.clone();
    changed_toolchain["toolchain"]["cargo"]["executable_sha256"] =
        serde_json::Value::String("d".repeat(64));
    assert!(validate_report_bytes(&canonical(&changed_toolchain)).is_err());

    let mut changed_source_ref = original.clone();
    changed_source_ref["source_ref"] = serde_json::Value::String("e".repeat(40));
    assert!(validate_report_bytes(&canonical(&changed_source_ref)).is_err());

    let mut false_inventory = original.clone();
    false_inventory["inventory"]["sha256"] =
        false_inventory["contract"]["required_inventory_sha256"].clone();
    assert!(validate_report_bytes(&canonical(&false_inventory)).is_err());

    let mut unknown = original;
    unknown["unreviewed"] = serde_json::Value::Bool(true);
    assert!(validate_report_bytes(&canonical(&unknown)).is_err());
}

#[test]
fn replay_report_recomputes_inventory_from_fixture_rows() {
    let original = preparation_report_bytes('c');
    let mut report: serde_json::Value =
        serde_json::from_slice(&original).expect("parse replay report");
    report["contract"]["required_unique_fixtures"] = 1.into();
    report["contract"]["required_evidence_bindings"] = 1.into();
    report["contract"]["required_targets"] = 1.into();
    report["contract"]["required_registry_packages"] = 1.into();
    report["contract"]["required_inventory_sha256"] = serde_json::Value::String("f".repeat(64));
    report["registry"] = serde_json::json!({
        "lock_sha256": report["source"]["cargo_lock_sha256"],
        "package_count": 1,
        "archive_bytes": 1,
        "expanded_bytes": 1,
        "entries": 1,
        "materialization_sha256": "e".repeat(64)
    });
    report["inventory"] = serde_json::json!({
        "fixtures": 1,
        "targets": 1,
        "evidence_bindings": 1,
        "sha256": "f".repeat(64)
    });
    let target = TargetReport {
        package: "pkg".to_owned(),
        kind: "test".to_owned(),
        name: "detectors".to_owned(),
    };
    let fixture_execution_id = super::process::fixture_execution_id(&target, "fixture_rejects");
    report["compilation"] = serde_json::json!({
        "status": "passed",
        "metadata_sha256": "d".repeat(64),
        "targets": [target],
        "processes": [
            completed_process("cargo-metadata", "cargo-metadata", '1'),
            completed_process("cargo-test-no-run", "cargo-test-no-run", '3')
        ]
    });
    report["fixtures"] = serde_json::json!([{
        "target": {"package": "pkg", "kind": "test", "name": "detectors"},
        "test_name": "fixture_rejects",
        "source": {
            "fixture_symbol": "fixture_rejects",
            "fixture_path": "src/fixture.rs",
            "fixture_sha256": "8".repeat(64),
            "detector_symbol": "detect",
            "detector_path": "src/detector.rs",
            "detector_sha256": "9".repeat(64),
            "source_graph_sha256": "a".repeat(64),
            "registered_identity": "detect",
            "expected_witnesses": {"expect-err:detect": 1}
        },
        "evidence": [{"invariant_id": "ST-01", "evidence_id": "simulator/direct"}],
        "status": "passed",
        "token": format!("replay-{}", "1".repeat(32)),
        "challenge": "2".repeat(64),
        "process": completed_process("detector-fixture", &fixture_execution_id, '5')
    }]);

    let mut impossible_runtime = report.clone();
    let total_timeout_ms = impossible_runtime["contract"]["total_timeout_seconds"]
        .as_u64()
        .expect("total timeout")
        * 1_000;
    impossible_runtime["compilation"]["processes"][0]["resources"]["duration_ms"] =
        (total_timeout_ms + 1).into();
    let bytes = crate::verification::detector_replay::canonical_report_value(impossible_runtime)
        .expect("render impossible runtime report");
    let error = validate_report_bytes(&bytes)
        .expect_err("aggregate observed runtime cannot exceed the absolute replay deadline");
    assert!(error.contains("total budget"), "unexpected error: {error}");

    let bytes = crate::verification::detector_replay::canonical_report_value(report)
        .expect("render self-consistent false inventory report");
    let error = validate_report_bytes(&bytes)
        .expect_err("report-supplied inventory digest must be recomputed from fixture rows");
    assert!(
        error.contains("inventory digest does not match its fixture rows"),
        "unexpected error: {error}"
    );
}

fn completed_process(role: &str, execution_id: &str, digest_digit: char) -> serde_json::Value {
    let artifact = |_stream: &str, digit: char| {
        let digest = digit.to_string().repeat(64);
        serde_json::json!({
            "kind": "verifier-replay-process-log",
            "path": format!("target/verifier/verifier-replay-process-log-{digest}"),
            "sha256": digest,
            "size_bytes": 1
        })
    };
    let next = char::from_u32(u32::from(digest_digit) + 1).expect("next digest digit");
    serde_json::json!({
        "status": "completed",
        "role": role,
        "execution_id": execution_id,
        "exit": {"success": true, "exit_code": 0, "timed_out": false},
        "resources": {"duration_ms": 1, "peak_rss_kib": 1},
        "termination": {
            "process_group": true,
            "term_signal_sent": false,
            "termination_grace_ms": 30000,
            "kill_signal_sent": false
        },
        "logs": [artifact("stdout", digest_digit), artifact("stderr", next)]
    })
}

fn preparation_report_bytes(source_digit: char) -> Vec<u8> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let manifest =
        crate::ProfileManifest::load(&root.join("verification/raft-invariant-profiles.json"))
            .expect("load profile manifest");
    let source_ref = source_digit.to_string().repeat(40);
    let mut receipts = receipts();
    receipts.source.commit.clone_from(&source_ref);
    receipts.source_sha256 =
        crate::verification::source::canonical_sha256(&receipts.source, "test source receipt")
            .expect("hash test source receipt");
    let assessment = publish_preparation_failure(PreparationFailureRequest {
        inventory: vec![ReplayEvidence {
            invariant_id: "ST-01".to_owned(),
            evidence_id: "fixture/one".to_owned(),
        }],
        replay: None,
        receipts,
        contract: &manifest.verifiers["pr"].detector_replay,
        profile: "pr",
        source_ref: &source_ref,
        registry: None,
        message: "preparation failed",
        deadlines: crate::verification::detector_replay::deadlines(
            &manifest.verifiers["pr"].detector_replay,
        )
        .expect("derive replay deadlines"),
    })
    .expect("publish preparation report");
    let report = assessment
        .artifacts
        .iter()
        .find(|artifact| artifact.kind == "verifier-replay-report")
        .expect("find replay report");
    std::fs::read(&report.path).expect("read replay report")
}

fn canonical(value: &serde_json::Value) -> Vec<u8> {
    let mut bytes = serde_json::to_vec_pretty(&value).expect("render changed report");
    bytes.push(b'\n');
    bytes
}

fn receipts() -> ReplaySourceReceipts {
    let program = |name: &str| ReplayToolchainProgramReceipt {
        identity: format!("{name} 1.88.0"),
        launcher_sha256: "1".repeat(64),
        executable_path: format!("/toolchain/bin/{name}"),
        executable_sha256: "2".repeat(64),
    };
    let source = AuthenticatedSourceReceipt {
        commit: "a".repeat(40),
        tree: "3".repeat(40),
        materialization: SourceMaterializationReceipt {
            contract: "git-tree-materialization-v1".to_owned(),
            sha256: "4".repeat(64),
            tracked_entries: 1,
            submodules: 0,
        },
        cargo_lock_sha256: "5".repeat(64),
        cargo_config_sha256: "6".repeat(64),
        environment_sha256: "7".repeat(64),
        target: "x86_64-unknown-linux-gnu".to_owned(),
    };
    let toolchain = ReplayToolchainReceipt {
        cargo: program("cargo"),
        rustc: program("rustc"),
    };
    ReplaySourceReceipts::from_parts(source, toolchain).expect("hash synthetic replay receipts")
}
