//! Adversarial archive process-log qualification scenarios.

use super::*;
use crate::{
    contract::profile::ProfileManifest,
    evidence::ArtifactRef,
    verification::{
        detector_replay::{
            artifact::model::{
                CompilationReport, CompilationStatus, FixtureReport, FixtureSourceReport,
                ProcessExitReport, ProcessResourceReport, ProcessTerminationReport,
                ReplayEvidenceReport, ReplayInventory, TargetReport, REPORT_SCHEMA_VERSION,
            },
            result::FixtureReplayStatus,
        },
        source::{
            AuthenticatedSourceReceipt, ReplayToolchainProgramReceipt, ReplayToolchainReceipt,
            SourceMaterializationReceipt,
        },
    },
};

#[test]
fn archived_passed_fixture_is_requalified_from_its_exact_transcript() {
    let target = TargetReport {
        package: "pkg".to_owned(),
        kind: "test".to_owned(),
        name: "detectors".to_owned(),
    };
    let execution_id =
        crate::verification::detector_replay::artifact::process::fixture_execution_id(
            &target,
            "fixture_rejects",
        );
    let stdout = envelope(
        "detector-fixture",
        &execution_id,
        "stdout",
        b"running 1 test\n",
    );
    let stderr = envelope("detector-fixture", &execution_id, "stderr", b"");
    let (stdout_ref, stdout_name) = artifact(&stdout);
    let (stderr_ref, stderr_name) = artifact(&stderr);
    let process = ProcessReport::Completed {
        role: "detector-fixture".to_owned(),
        execution_id,
        exit: ProcessExitReport {
            success: true,
            exit_code: Some(0),
            timed_out: false,
        },
        resources: ProcessResourceReport {
            duration_ms: 1,
            peak_rss_kib: 1,
        },
        termination: ProcessTerminationReport {
            process_group: true,
            term_signal_sent: false,
            termination_grace_ms: 30_000,
            kill_signal_sent: false,
        },
        logs: vec![stdout_ref, stderr_ref],
    };
    let report = report(target, process);
    let files = BTreeMap::from([(stdout_name, stdout), (stderr_name, stderr)]);

    let error = validate(&report, &files)
        .expect_err("a passed row with a malformed transcript must fail readback");
    assert!(
        error.contains("archived transcript does not qualify"),
        "{error}"
    );
}

fn report(target: TargetReport, process: ProcessReport) -> ReplayReport {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let manifest = ProfileManifest::load(&root.join("verification/raft-invariant-profiles.json"))
        .expect("load verifier contract");
    let program = |name: &str| ReplayToolchainProgramReceipt {
        identity: name.to_owned(),
        launcher_sha256: "1".repeat(64),
        executable_path: format!("/bin/{name}"),
        executable_sha256: "2".repeat(64),
    };
    ReplayReport {
        schema_version: REPORT_SCHEMA_VERSION,
        profile: "pr".to_owned(),
        source_ref: "a".repeat(40),
        source: AuthenticatedSourceReceipt {
            commit: "a".repeat(40),
            tree: "b".repeat(40),
            materialization: SourceMaterializationReceipt {
                contract: "test".to_owned(),
                sha256: "3".repeat(64),
                tracked_entries: 1,
                submodules: 0,
            },
            cargo_lock_sha256: "4".repeat(64),
            cargo_config_sha256: "5".repeat(64),
            environment_sha256: "6".repeat(64),
            target: "test-target".to_owned(),
        },
        source_sha256: "7".repeat(64),
        toolchain: ReplayToolchainReceipt {
            cargo: program("cargo"),
            rustc: program("rustc"),
        },
        toolchain_sha256: "8".repeat(64),
        contract: manifest.verifiers["pr"].detector_replay.clone(),
        registry: None,
        inventory: ReplayInventory {
            fixtures: 1,
            targets: 1,
            evidence_bindings: 1,
            sha256: None,
        },
        compilation: CompilationReport {
            status: CompilationStatus::HarnessError,
            message: Some("test fixture".to_owned()),
            metadata_sha256: None,
            targets: Vec::new(),
            processes: Vec::new(),
        },
        fixtures: vec![FixtureReport {
            target,
            test_name: "fixture_rejects".to_owned(),
            source: FixtureSourceReport {
                fixture_symbol: "fixture_rejects".to_owned(),
                fixture_path: "src/fixture.rs".to_owned(),
                fixture_sha256: "9".repeat(64),
                detector_symbol: "detect".to_owned(),
                detector_path: "src/detector.rs".to_owned(),
                detector_sha256: "a".repeat(64),
                source_graph_sha256: "b".repeat(64),
                registered_identity: "detect".to_owned(),
                expected_witnesses: BTreeMap::from([("expect-err:detect".to_owned(), 1)]),
            },
            evidence: vec![ReplayEvidenceReport {
                invariant_id: "ST-01".to_owned(),
                evidence_id: "simulator/direct".to_owned(),
            }],
            status: FixtureReplayStatus::Passed,
            token: Some(format!("replay-{}", "c".repeat(32))),
            challenge: Some("d".repeat(64)),
            message: None,
            process: Some(process),
        }],
    }
}

fn envelope(role: &str, execution_id: &str, stream: &str, payload: &[u8]) -> Vec<u8> {
    let mut bytes = format!(
        "rafter-verifier-process-log-v2\nrole:{role}\nexecution-id:{execution_id}\nstream:{stream}\npayload-bytes:{}\n\n",
        payload.len()
    )
    .into_bytes();
    bytes.extend_from_slice(payload);
    bytes
}

fn artifact(bytes: &[u8]) -> (ArtifactRef, String) {
    let digest = format!("{:x}", Sha256::digest(bytes));
    let name = format!("verifier-replay-process-log-{digest}");
    (
        ArtifactRef {
            kind: "verifier-replay-process-log".to_owned(),
            path: format!("target/verifier/{name}"),
            sha256: digest,
            size_bytes: bytes.len() as u64,
        },
        name,
    )
}
