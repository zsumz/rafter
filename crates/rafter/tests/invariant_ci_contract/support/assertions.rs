//! Reusable assertions for transport, upload, and launcher contracts.

use std::collections::BTreeSet;

use super::{job_block, workflow_step};

pub(crate) fn assert_transport_stage_contract(stage: &str, profile: &str, layers: &[&str]) {
    let inventory = layers.join(" ");
    for required in [
        "if: always()",
        "transport_root=\"$RUNNER_TEMP/$INVARIANT_EVIDENCE_DIR\"",
        "evidence_root=\"artifacts/invariants\"",
        &format!("for layer in {inventory}; do"),
        &format!("source=\"$transport_root/$layer/{profile}-$layer\""),
        &format!("destination=\"$evidence_root/{profile}-$layer\""),
        "test -s \"$source.json\"",
        "test -d \"$source\"",
        "[[ -e \"$destination\" || -e \"$destination.json\" ]]",
        "cp \"$source.json\" \"$destination.json\"",
        "cp -R \"$source\" \"$destination\"",
    ] {
        assert!(
            stage.contains(required),
            "{profile} transport staging omitted {required}"
        );
    }
}

pub(crate) fn assert_separate_aggregate_uploads(aggregate: &str, profile: &str) {
    let report_name = match profile {
        "nightly" => "Upload available nightly aggregate reports",
        "weekly" => "Upload available weekly aggregate reports",
        _ => panic!("unknown scheduled profile {profile}"),
    };
    let evidence_name = format!("Upload available {profile} aggregate evidence");
    let verifier_seal_name = format!("Seal {profile} aggregate verifier evidence");
    let verifier_upload_name = format!("Upload {profile} aggregate verifier evidence");
    let verifier_download_name =
        format!("Download {profile} aggregate verifier evidence for readback");
    let verifier_verify_name = format!("Verify downloaded {profile} aggregate verifier evidence");
    let diagnostics_name = format!("Upload {profile} aggregate process diagnostics");

    let report = workflow_step(aggregate, report_name);
    assert!(report.contains(&format!(
        "name: invariants-{profile}-report-${{{{ github.run_id }}}}-${{{{ github.run_attempt }}}}"
    )));
    assert!(!report.contains("INVARIANT_EVIDENCE_DIR"));
    assert!(!report.contains("telemetry"));

    let evidence = workflow_step(aggregate, &evidence_name);
    assert!(evidence.contains(&format!(
        "name: invariants-{profile}-aggregate-evidence-${{{{ github.run_id }}}}-${{{{ github.run_attempt }}}}"
    )));
    assert!(evidence.contains("${{ runner.temp }}/${{ env.INVARIANT_EVIDENCE_DIR }}/"));
    assert!(!evidence.contains("overwrite: true"));
    assert!(!evidence.contains("telemetry"));

    let verifier_seal = workflow_step(aggregate, &verifier_seal_name);
    assert!(verifier_seal.contains("seal-verifier-artifacts"));
    assert!(verifier_seal.contains("--manifest-sha256 \"$EXPECTED_MANIFEST_SHA256\""));
    assert!(verifier_seal.contains("--archive \"$archive\""));

    let verifier_upload = workflow_step(aggregate, &verifier_upload_name);
    assert!(verifier_upload.contains(&format!(
        "name: invariants-{profile}-aggregate-verifier-evidence-${{{{ github.run_id }}}}-${{{{ github.run_attempt }}}}"
    )));
    assert!(verifier_upload.contains("${{ steps.verifier_archive.outputs.archive }}"));
    assert!(verifier_upload.contains("if-no-files-found: error"));
    assert!(!verifier_upload.contains("telemetry"));

    let verifier_download = workflow_step(aggregate, &verifier_download_name);
    assert!(verifier_download
        .contains("actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093"));
    assert!(verifier_download.contains("steps.upload_verifier_archive.outcome == 'success'"));

    let verifier_verify = workflow_step(aggregate, &verifier_verify_name);
    assert!(verifier_verify.contains("verify-verifier-archive"));
    assert!(verifier_verify.contains("--archive-sha256 \"$ARCHIVE_SHA256\""));
    assert!(verifier_verify.contains("--manifest-sha256 \"$MANIFEST_SHA256\""));

    let diagnostics = workflow_step(aggregate, &diagnostics_name);
    assert!(diagnostics.contains(&format!(
        "name: invariants-{profile}-aggregate-process-diagnostics-${{{{ github.run_id }}}}-${{{{ github.run_attempt }}}}"
    )));
    assert!(diagnostics.contains("target/rafter-invariants/telemetry/"));
    assert!(!diagnostics.contains("INVARIANT_EVIDENCE_DIR"));
    assert!(!diagnostics.contains("verifier-evidence"));
}

pub(crate) fn assert_unique_paths(paths: &[String]) -> Result<(), String> {
    let unique = paths.iter().collect::<BTreeSet<_>>();
    if unique.len() == paths.len() {
        Ok(())
    } else {
        Err("artifact paths collide".to_owned())
    }
}

pub(crate) fn assert_pr_launcher_inventories(workflow: &str) {
    let launcher = job_block(workflow, "invariants-launcher-macos");
    for exact_inventory in [
        "scripts/cargo-test-exact 55 execution::process:: --locked -p rafter-invariants -- --test-threads=1",
        "scripts/cargo-test-exact 18 producer::process::tests --locked -p rafter-invariants -- --test-threads=1",
        "scripts/cargo-test-exact 8 artifact_verify::test_logs::tests --locked -p rafter-invariants -- --test-threads=1",
    ] {
        assert!(
            launcher.contains(exact_inventory),
            "macOS launcher validation omitted exact inventory: {exact_inventory}"
        );
    }
}
