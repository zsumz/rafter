//! Completed detector-replay report fixtures for publication boundary tests.

use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

use super::{artifact_ref, process_log, sha256, synthetic_report};
use crate::{
    contract::TestIdentity,
    verification::{
        detector_replay::{
            DetectorReplayPlan, ReplayEvidence, ReplayFixture, ReplayReportExpectation,
            ReplayTarget,
        },
        publication::VerifierArchiveExpectation,
        source::{RegistryReceipt, ReplaySourceReceipts},
    },
};

pub(in crate::verification::publication::tests) struct CompletedReport {
    pub(in crate::verification::publication::tests) bytes: Vec<u8>,
    pub(in crate::verification::publication::tests) expectation: VerifierArchiveExpectation,
    pub(in crate::verification::publication::tests) artifacts: Vec<(String, Vec<u8>)>,
}

pub(in crate::verification::publication::tests) fn completed_report(
    append_second_libtest_run: bool,
) -> CompletedReport {
    let (base, _) = synthetic_report();
    let mut report: serde_json::Value = serde_json::from_slice(&base).expect("parse base report");
    let (target, inventory_sha256) = one_fixture_inventory();
    report["contract"]["required_inventory_sha256"] = inventory_sha256.clone().into();
    report["contract"]["required_registry_packages"] = 1.into();
    report["contract"]["required_unique_fixtures"] = 1.into();
    report["contract"]["required_evidence_bindings"] = 1.into();
    report["contract"]["required_targets"] = 1.into();
    report["registry"]["package_count"] = 1.into();
    report["inventory"] = serde_json::json!({
        "fixtures": 1,
        "targets": 1,
        "evidence_bindings": 1,
        "sha256": inventory_sha256,
    });

    let token = format!("replay-{}", "c".repeat(32));
    let challenge = "d".repeat(64);
    let execution_id = fixture_execution_id(&target, "fixture_rejects");
    let mut fixture_stdout = format!(
        "running 1 test\nRAFTER_INVARIANT_ORACLE_OBSERVED:{token}\ntest fixture_rejects ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n"
    );
    if append_second_libtest_run {
        fixture_stdout.push_str(
            "running 1 test\ntest unrelated ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored\n",
        );
    }
    let fixture_stderr = format!(
        "RAFTER_INVARIANT_DETECTOR_WITNESS:{token}:expect-err:detect()\nRAFTER_INVARIANT_DETECTOR_PROOF:{token}:expect-err:detect():{challenge}\n"
    );
    let (metadata_process, mut artifacts) =
        completed_process_report("cargo-metadata", "cargo-metadata", b"metadata", b"");
    let (compile_process, compile_artifacts) =
        completed_process_report("cargo-test-no-run", "cargo-test-no-run", b"compiled", b"");
    artifacts.extend(compile_artifacts);
    let (fixture_process, fixture_artifacts) = completed_process_report(
        "detector-fixture",
        &execution_id,
        fixture_stdout.as_bytes(),
        fixture_stderr.as_bytes(),
    );
    artifacts.extend(fixture_artifacts);
    report["compilation"] = serde_json::json!({
        "status": "passed",
        "metadata_sha256": "b".repeat(64),
        "targets": [{"package": "pkg", "kind": "test", "name": "detectors"}],
        "processes": [metadata_process, compile_process],
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
            "expected_witnesses": {"expect-err:detect": 1},
        },
        "evidence": [{"invariant_id": "ST-01", "evidence_id": "simulator/direct"}],
        "status": "passed",
        "token": token,
        "challenge": challenge,
        "process": fixture_process,
    }]);

    let bytes = crate::verification::detector_replay::canonical_report_value(report.clone())
        .expect("render completed report");
    let receipts = ReplaySourceReceipts::from_parts(
        serde_json::from_value(report["source"].clone()).expect("decode source receipt"),
        serde_json::from_value(report["toolchain"].clone()).expect("decode toolchain receipt"),
    )
    .expect("rebuild completed receipts");
    let expectation = VerifierArchiveExpectation::from_replay(ReplayReportExpectation::new(
        "pr".to_owned(),
        receipts,
        serde_json::from_value(report["contract"].clone()).expect("decode replay contract"),
        Some(
            serde_json::from_value::<RegistryReceipt>(report["registry"].clone())
                .expect("decode registry receipt"),
        ),
    ));
    CompletedReport {
        bytes,
        expectation,
        artifacts,
    }
}

fn one_fixture_inventory() -> (ReplayTarget, String) {
    let target = ReplayTarget {
        package: "pkg".to_owned(),
        kind: "test".to_owned(),
        name: "detectors".to_owned(),
    };
    let fixture = ReplayFixture {
        identity: TestIdentity {
            package: target.package.clone(),
            target_kind: target.kind.clone(),
            target: target.name.clone(),
            test_name: "fixture_rejects".to_owned(),
        },
        fixture: "fixture_rejects".to_owned(),
        fixture_path: "src/fixture.rs".into(),
        fixture_sha256: "8".repeat(64),
        detector: "detect".to_owned(),
        detector_path: "src/detector.rs".into(),
        detector_sha256: "9".repeat(64),
        registered_identity: "detect".to_owned(),
        source_graph_sha256: "a".repeat(64),
        expected_witnesses: BTreeMap::from([("expect-err:detect".to_owned(), 1)]),
        evidence: vec![ReplayEvidence {
            invariant_id: "ST-01".to_owned(),
            evidence_id: "simulator/direct".to_owned(),
        }],
    };
    let replay =
        DetectorReplayPlan::from_test_targets(BTreeMap::from([(target.clone(), vec![fixture])]));
    let inventory_sha256 = replay.inventory_sha256().expect("hash one-fixture plan");
    (target, inventory_sha256)
}

fn completed_process_report(
    role: &str,
    execution_id: &str,
    stdout: &[u8],
    stderr: &[u8],
) -> (serde_json::Value, Vec<(String, Vec<u8>)>) {
    let stdout = process_log(role, execution_id, "stdout", stdout);
    let stderr = process_log(role, execution_id, "stderr", stderr);
    let process = serde_json::json!({
        "status": "completed",
        "role": role,
        "execution_id": execution_id,
        "exit": {"success": true, "exit_code": 0, "timed_out": false},
        "resources": {"duration_ms": 1, "peak_rss_kib": 1},
        "termination": {
            "process_group": true,
            "term_signal_sent": false,
            "termination_grace_ms": 30000,
            "kill_signal_sent": false,
        },
        "logs": [artifact_ref(&stdout), artifact_ref(&stderr)],
    });
    let artifacts = [stdout, stderr]
        .into_iter()
        .map(|bytes| {
            (
                format!("verifier-replay-process-log-{}", sha256(&bytes)),
                bytes,
            )
        })
        .collect();
    (process, artifacts)
}

fn fixture_execution_id(target: &ReplayTarget, test_name: &str) -> String {
    let identity = format!(
        "{}\0{}\0{}\0{test_name}",
        target.package, target.kind, target.name
    );
    format!("detector-fixture:{:x}", Sha256::digest(identity))
}
