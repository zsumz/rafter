//! Scenarios for exact report, provenance, inventory, and process-log binding.

use std::fs;

use super::{publish_verifier_archive, reviewed_profile_manifest, support::*};

#[test]
fn substituted_profile_manifest_fails_closed() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut supplied =
        crate::ProfileManifest::load(&root.join("verification/raft-invariant-profiles.json"))
            .expect("load reviewed profile manifest");
    supplied
        .verifiers
        .get_mut("pr")
        .expect("PR verifier contract")
        .detector_replay
        .total_timeout_seconds += 1;

    let error = reviewed_profile_manifest(&root, &supplied)
        .expect_err("substituted profile manifest must fail closed")
        .to_string();
    assert!(error.contains("reviewed checkout manifest"), "{error}");
}

#[test]
#[cfg(unix)]
fn reportless_and_orphaned_artifact_sets_fail_closed() {
    let (report, expectation) = synthetic_report();
    let orphan = process_log("orphan", "orphan", "stdout", b"unreferenced");
    let orphan_name = format!("verifier-replay-process-log-{}", sha256(&orphan));
    let orphaned = fixture_from_report(
        "orphaned",
        &report,
        expectation,
        vec![(orphan_name, orphan)],
    );
    let error = publish_verifier_archive(
        &orphaned.root,
        &orphaned.manifest,
        &orphaned.manifest_sha256,
        &orphaned.temp.path().join("orphaned.tar"),
        &orphaned.expectation,
    )
    .expect_err("orphaned process log must fail closed")
    .to_string();
    assert!(error.contains("process-log inventory"), "{error}");

    let temp = tempfile::tempdir().expect("temporary reportless root");
    let root = temp.path().join("reportless");
    fs::create_dir(&root).expect("create reportless root");
    let payload = b"not a replay report\n";
    let payload_sha256 = sha256(payload);
    let payload_name = format!("verifier-replay-process-log-{payload_sha256}");
    fs::write(root.join(&payload_name), payload).expect("write reportless payload");
    let manifest_bytes = format!("{payload_sha256}  {payload_name}\n");
    let manifest = root.join(format!(
        "verifier-artifact-manifest-{}",
        sha256(manifest_bytes.as_bytes())
    ));
    fs::write(&manifest, &manifest_bytes).expect("write reportless manifest");
    make_read_only(&root.join(payload_name));
    make_read_only(&manifest);
    make_read_only(&root);
    let error = publish_verifier_archive(
        &root,
        &manifest,
        &sha256(manifest_bytes.as_bytes()),
        &temp.path().join("reportless.tar"),
        &synthetic_report().1,
    )
    .expect_err("reportless set must fail closed")
    .to_string();
    assert!(error.contains("exactly one replay report"), "{error}");
}

#[test]
#[cfg(unix)]
fn unrecognized_artifact_kind_fails_closed() {
    let (report, expectation) = synthetic_report();
    let payload = b"unrecognized artifact".to_vec();
    let name = format!("verifier-extra-{}", sha256(&payload));
    let fixture = fixture_from_report("extra", &report, expectation, vec![(name, payload)]);
    let error = publish_verifier_archive(
        &fixture.root,
        &fixture.manifest,
        &fixture.manifest_sha256,
        &fixture.temp.path().join("extra.tar"),
        &fixture.expectation,
    )
    .expect_err("unrecognized verifier artifact must fail closed")
    .to_string();
    assert!(error.contains("unrecognized payload"), "{error}");
}

#[test]
#[cfg(unix)]
fn missing_report_referenced_process_log_fails_closed() {
    let (report, expectation) = synthetic_report();
    let mut value: serde_json::Value = serde_json::from_slice(&report).expect("parse report");
    let stdout = process_log(
        "cargo-process-lifecycle",
        "cargo-process-lifecycle",
        "stdout",
        b"partial stdout",
    );
    let stderr = process_log(
        "cargo-process-lifecycle",
        "cargo-process-lifecycle",
        "stderr",
        b"partial stderr",
    );
    value["compilation"]["processes"] = serde_json::json!([{
        "status": "lifecycle_error",
        "role": "cargo-process-lifecycle",
        "execution_id": "cargo-process-lifecycle",
        "message": "compilation failed",
        "logs": [artifact_ref(&stdout), artifact_ref(&stderr)]
    }]);
    let report = crate::verification::detector_replay::canonical_report_value(value)
        .expect("render report with process logs");
    let fixture = fixture_from_report(
        "missing-log",
        &report,
        expectation,
        vec![(
            format!("verifier-replay-process-log-{}", sha256(&stdout)),
            stdout,
        )],
    );
    let error = publish_verifier_archive(
        &fixture.root,
        &fixture.manifest,
        &fixture.manifest_sha256,
        &fixture.temp.path().join("missing-log.tar"),
        &fixture.expectation,
    )
    .expect_err("missing report-referenced log must fail closed")
    .to_string();
    assert!(
        error.contains("omits report-referenced artifact"),
        "{error}"
    );
}

#[test]
#[cfg(unix)]
fn completed_report_execution_and_stream_bindings_fail_closed() {
    assert_completed_mutation_rejected("execution-id", |report, _artifacts| {
        report["fixtures"][0]["process"]["execution_id"] =
            serde_json::Value::String("detector-fixture:substituted".to_owned());
    });
    assert_completed_mutation_rejected("reused-log", |report, artifacts| {
        let reused = report["fixtures"][0]["process"]["logs"][0].clone();
        report["fixtures"][0]["process"]["logs"][1] = reused;
        artifacts.pop().expect("remove replaced stderr artifact");
    });
    assert_completed_mutation_rejected("duplicate-stream", |report, artifacts| {
        let role = report["fixtures"][0]["process"]["role"]
            .as_str()
            .expect("fixture process role")
            .to_owned();
        let execution_id = report["fixtures"][0]["process"]["execution_id"]
            .as_str()
            .expect("fixture execution identity")
            .to_owned();
        let replacement = process_log(&role, &execution_id, "stdout", b"second stdout");
        report["fixtures"][0]["process"]["logs"][1] = artifact_ref(&replacement);
        let replacement_name = format!("verifier-replay-process-log-{}", sha256(&replacement));
        *artifacts.last_mut().expect("replace stderr artifact") = (replacement_name, replacement);
    });
}

#[cfg(unix)]
fn assert_completed_mutation_rejected(
    label: &str,
    mutate: impl FnOnce(&mut serde_json::Value, &mut Vec<(String, Vec<u8>)>),
) {
    let CompletedReport {
        bytes,
        expectation,
        mut artifacts,
    } = completed_report(false);
    let mut report: serde_json::Value = serde_json::from_slice(&bytes).expect("parse report");
    mutate(&mut report, &mut artifacts);
    let report = crate::verification::detector_replay::canonical_report_value(report)
        .expect("render mutated completed report");
    let fixture = fixture_from_report(label, &report, expectation, artifacts);
    let error = publish_verifier_archive(
        &fixture.root,
        &fixture.manifest,
        &fixture.manifest_sha256,
        &fixture.temp.path().join(format!("{label}.tar")),
        &fixture.expectation,
    )
    .expect_err("mutated completed report must fail closed")
    .to_string();
    assert!(error.contains("replay report"), "{label}: {error}");
}

#[test]
#[cfg(unix)]
fn self_consistent_report_context_substitutions_fail_closed() {
    assert_context_substitution_rejected("profile", |report| {
        report["profile"] = serde_json::Value::String("nightly".to_owned());
    });
    assert_context_substitution_rejected("source", |report| {
        report["source"]["tree"] = serde_json::Value::String("8".repeat(40));
        let receipt: crate::verification::source::AuthenticatedSourceReceipt =
            serde_json::from_value(report["source"].clone()).expect("decode source receipt");
        report["source_sha256"] = serde_json::Value::String(
            crate::verification::source::canonical_sha256(&receipt, "source receipt")
                .expect("hash source receipt"),
        );
    });
    assert_context_substitution_rejected("toolchain", |report| {
        report["toolchain"]["cargo"]["identity"] =
            serde_json::Value::String("cargo substituted".to_owned());
        let receipt: crate::verification::source::ReplayToolchainReceipt =
            serde_json::from_value(report["toolchain"].clone()).expect("decode toolchain receipt");
        report["toolchain_sha256"] = serde_json::Value::String(
            crate::verification::source::canonical_sha256(&receipt, "toolchain receipt")
                .expect("hash toolchain receipt"),
        );
    });
    assert_context_substitution_rejected("contract", |report| {
        let timeout = report["contract"]["total_timeout_seconds"]
            .as_u64()
            .expect("contract timeout");
        report["contract"]["total_timeout_seconds"] = (timeout + 1).into();
    });
    assert_context_substitution_rejected("registry", |report| {
        report["registry"]["materialization_sha256"] = serde_json::Value::String("9".repeat(64));
    });
}

#[cfg(unix)]
fn assert_context_substitution_rejected(label: &str, mutate: impl FnOnce(&mut serde_json::Value)) {
    let (report, expectation) = synthetic_report();
    let mut report: serde_json::Value = serde_json::from_slice(&report).expect("parse report");
    mutate(&mut report);
    let report = crate::verification::detector_replay::canonical_report_value(report)
        .expect("render substituted report");
    let fixture = fixture_from_report(label, &report, expectation, Vec::new());
    let error = publish_verifier_archive(
        &fixture.root,
        &fixture.manifest,
        &fixture.manifest_sha256,
        &fixture.temp.path().join(format!("{label}.tar")),
        &fixture.expectation,
    )
    .expect_err("self-consistent context substitution must fail closed")
    .to_string();
    assert!(error.contains("trusted expectation"), "{label}: {error}");
}
