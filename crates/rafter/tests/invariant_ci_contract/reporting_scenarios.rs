//! Reporting scenarios: complete reports always publish while failed evidence stays red.

use std::path::Path;

use super::support::*;

#[test]
fn complete_reports_publish_when_an_upstream_job_failed_but_the_gate_stays_red() {
    let root = workspace_root();
    for contract in [
        AggregateWorkflowContract {
            workflow: ".github/workflows/ci.yml",
            profile: "pr",
            job: "invariants-pr",
            validate_step: "Validate current-run aggregate reports",
            summary_step: "Render invariant table in the job summary",
            report_upload_step: "Upload available aggregate reports",
            evidence_upload_step: "Upload available aggregate evidence",
            verifier_seal_step: "Seal aggregate verifier evidence",
            verifier_upload_step: "Upload aggregate verifier evidence",
            verifier_download_step: "Download aggregate verifier evidence for readback",
            verifier_verify_step: "Verify downloaded aggregate verifier evidence",
            diagnostics_upload_step: "Upload aggregate process diagnostics",
            gate_step: "Require 44 of 44 green",
        },
        AggregateWorkflowContract {
            workflow: ".github/workflows/nightly.yml",
            profile: "nightly",
            job: "invariants-nightly",
            validate_step: "Validate current-run nightly reports",
            summary_step: "Render the 44-row nightly report",
            report_upload_step: "Upload available nightly aggregate reports",
            evidence_upload_step: "Upload available nightly aggregate evidence",
            verifier_seal_step: "Seal nightly aggregate verifier evidence",
            verifier_upload_step: "Upload nightly aggregate verifier evidence",
            verifier_download_step: "Download nightly aggregate verifier evidence for readback",
            verifier_verify_step: "Verify downloaded nightly aggregate verifier evidence",
            diagnostics_upload_step: "Upload nightly aggregate process diagnostics",
            gate_step: "Require 44 of 44 nightly invariants green",
        },
        AggregateWorkflowContract {
            workflow: ".github/workflows/weekly.yml",
            profile: "weekly",
            job: "invariants-weekly",
            validate_step: "Validate current-run weekly reports",
            summary_step: "Render the 44-row weekly report",
            report_upload_step: "Upload available weekly aggregate reports",
            evidence_upload_step: "Upload available weekly aggregate evidence",
            verifier_seal_step: "Seal weekly aggregate verifier evidence",
            verifier_upload_step: "Upload weekly aggregate verifier evidence",
            verifier_download_step: "Download weekly aggregate verifier evidence for readback",
            verifier_verify_step: "Verify downloaded weekly aggregate verifier evidence",
            diagnostics_upload_step: "Upload weekly aggregate process diagnostics",
            gate_step: "Require 44 of 44 weekly invariants green",
        },
    ] {
        verify_always_publish_failure_branch(&root, contract);
    }
}

fn verify_always_publish_failure_branch(root: &Path, contract: AggregateWorkflowContract) {
    let workflow = read(&root.join(contract.workflow));
    let aggregate = job_block(&workflow, contract.job);
    let fixture = AggregateReportFixture::new(root, contract.profile);

    assert_aggregate_timeout_budget(aggregate, contract.profile);
    let aggregate_step_name = if contract.profile == "pr" {
        "Aggregate exactly 44 invariant verdicts".to_owned()
    } else {
        format!(
            "Aggregate exactly 44 {} invariant verdicts",
            contract.profile
        )
    };
    assert!(
        workflow_step(aggregate, &aggregate_step_name).contains("timeout-minutes: 20"),
        "{} aggregate generation must leave time for report publication",
        contract.profile
    );
    assert!(aggregate.contains(&format!(
        "INVARIANT_VERIFIER_DIR: target/rafter-invariants/verifier-evidence/{}-${{{{ github.run_id }}}}-${{{{ github.run_attempt }}}}",
        contract.profile
    )));
    assert!(aggregate.contains(&format!(
        "RAFTER_INVARIANT_VERIFIER_EVIDENCE_DIR: target/rafter-invariants/verifier-evidence/{}-${{{{ github.run_id }}}}-${{{{ github.run_attempt }}}}",
        contract.profile
    )));
    let aggregate_step = workflow_step(aggregate, &aggregate_step_name);
    for required in [
        "verifier-artifact-manifest-*",
        "verifier_manifest=$manifest_path",
        "verifier_manifest_sha256=$(sha256sum",
    ] {
        assert!(
            aggregate_step.contains(required),
            "{} aggregate step omitted {required}",
            contract.profile
        );
    }

    let validate = workflow_step(aggregate, contract.validate_step);
    for required in [
        "timeout-minutes: 2",
        "cargo run --offline --locked -p rafter-invariants -- verify-report-set",
        &format!(
            "--profile {} --report-dir \"$report_dir\"",
            contract.profile
        ),
        "complete=true",
        "complete=false",
    ] {
        assert!(
            validate.contains(required),
            "{} report-set validation omitted {required}",
            contract.profile
        );
    }

    let summary = workflow_step(aggregate, contract.summary_step);
    assert!(summary.contains("timeout-minutes: 1"));
    let rendered = run_workflow_script(
        summary,
        root,
        &fixture.environment(&[("REPORT_READY", "true")]),
    );
    assert_success(&rendered, contract.summary_step);
    assert_eq!(read(&fixture.github_summary), fixture.markdown);

    assert_aggregate_upload_contracts(aggregate, contract);

    assert_gate_rejects_failed_evidence(root, aggregate, contract, &fixture);
}

fn assert_aggregate_timeout_budget(aggregate: &str, profile: &str) {
    let timeouts = aggregate
        .lines()
        .filter_map(|line| line.trim().strip_prefix("timeout-minutes: "))
        .map(|minutes| minutes.parse::<u64>().expect("numeric timeout"))
        .collect::<Vec<_>>();
    let steps = aggregate
        .split_once("\n    steps:\n")
        .expect("aggregate job steps")
        .1;
    let step_count = steps
        .lines()
        .filter(|line| line.starts_with("      - "))
        .count();
    let (&job_timeout, step_timeouts) = timeouts
        .split_first()
        .expect("aggregate job must declare a timeout");
    let declared_step_budget = step_timeouts.iter().sum::<u64>();

    assert_eq!(
        timeouts.len(),
        step_count + 1,
        "{profile} aggregate contains an uncapped step"
    );
    assert!(
        job_timeout >= declared_step_budget + 5,
        "{profile} aggregate allows {job_timeout} minutes but its steps reserve \
         {declared_step_budget}; at least five minutes of setup and publication reserve is required"
    );
}

#[test]
fn aggregate_job_timeout_is_a_minimum_budget_not_an_exact_identity() {
    let root = workspace_root();
    let workflow = read(&root.join(".github/workflows/ci.yml"));
    let aggregate = job_block(&workflow, "invariants-pr");
    let safer = aggregate.replacen("timeout-minutes: 95", "timeout-minutes: 100", 1);
    assert_ne!(
        safer, aggregate,
        "fixture must increase the aggregate timeout"
    );

    assert_aggregate_timeout_budget(&safer, "pr");
}

fn assert_gate_rejects_failed_evidence(
    root: &Path,
    aggregate: &str,
    contract: AggregateWorkflowContract,
    fixture: &AggregateReportFixture,
) {
    let gate_step = workflow_step(aggregate, contract.gate_step);
    assert!(gate_step.contains("timeout-minutes: 1"));
    let gate = run_workflow_script(
        gate_step,
        root,
        &fixture.environment(&[
            ("AGGREGATE_STATUS", "0"),
            ("REPORT_READY", "true"),
            ("TESTS_RESULT", "success"),
            ("LAUNCHER_MACOS_RESULT", "success"),
            ("SIMULATOR_RESULT", "failure"),
            ("TLA_RESULT", "success"),
            ("MAELSTROM_RESULT", "success"),
        ]),
    );
    assert!(
        !gate.status.success(),
        "{} aggregate gate accepted a failed upstream job",
        contract.profile
    );
    let invalid_report = run_workflow_script(
        gate_step,
        root,
        &fixture.environment(&[
            ("AGGREGATE_STATUS", "0"),
            ("REPORT_READY", "false"),
            ("TESTS_RESULT", "success"),
            ("LAUNCHER_MACOS_RESULT", "success"),
            ("SIMULATOR_RESULT", "success"),
            ("TLA_RESULT", "success"),
            ("MAELSTROM_RESULT", "success"),
        ]),
    );
    assert!(
        !invalid_report.status.success(),
        "{} aggregate gate accepted an invalid report set",
        contract.profile
    );
}

fn assert_aggregate_upload_contracts(aggregate: &str, contract: AggregateWorkflowContract) {
    assert_report_and_evidence_upload_contracts(aggregate, contract);
    assert_verifier_artifact_contracts(aggregate, contract);
    assert_diagnostics_upload_contract(aggregate, contract);
}

fn assert_report_and_evidence_upload_contracts(
    aggregate: &str,
    contract: AggregateWorkflowContract,
) {
    let upload = workflow_step(aggregate, contract.report_upload_step);
    for required in [
        "if: always()",
        "actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02",
        "timeout-minutes: 2",
        &format!("/{0}.json", contract.profile),
        &format!("/{0}.xml", contract.profile),
        &format!("/{0}.md", contract.profile),
        "if-no-files-found: error",
    ] {
        assert!(
            upload.contains(required),
            "{} upload step omitted {required}",
            contract.profile
        );
    }
    assert!(!upload.contains("INVARIANT_EVIDENCE_DIR"));
    assert!(!upload.contains("telemetry"));

    let evidence = workflow_step(aggregate, contract.evidence_upload_step);
    for required in [
        "if: always()",
        "actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02",
        "timeout-minutes: 2",
        "${{ runner.temp }}/${{ env.INVARIANT_EVIDENCE_DIR }}/",
        "if-no-files-found: ignore",
    ] {
        assert!(
            evidence.contains(required),
            "{} aggregate evidence upload omitted {required}",
            contract.profile
        );
    }
    assert!(!evidence.contains("telemetry"));
}

fn assert_verifier_artifact_contracts(aggregate: &str, contract: AggregateWorkflowContract) {
    let verifier_seal = workflow_step(aggregate, contract.verifier_seal_step);
    for required in [
        "if: always()",
        "timeout-minutes: 5",
        "EXPECTED_MANIFEST: ${{ steps.aggregate.outputs.verifier_manifest }}",
        "EXPECTED_MANIFEST_SHA256: ${{ steps.aggregate.outputs.verifier_manifest_sha256 }}",
        "seal-verifier-artifacts",
        &format!("--profile {}", contract.profile),
        "--root \"$run_root\"",
        "--manifest \"$EXPECTED_MANIFEST\"",
        "--manifest-sha256 \"$EXPECTED_MANIFEST_SHA256\"",
        "--archive \"$archive\"",
        "archive=$archive",
    ] {
        assert!(
            verifier_seal.contains(required),
            "{} verifier evidence seal omitted {required}",
            contract.profile
        );
    }

    let verifier_upload = workflow_step(aggregate, contract.verifier_upload_step);
    for required in [
        "if: always()",
        "actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02",
        "timeout-minutes: 2",
        "${{ steps.verifier_archive.outputs.archive }}",
        "if-no-files-found: error",
    ] {
        assert!(
            verifier_upload.contains(required),
            "{} verifier evidence upload omitted {required}",
            contract.profile
        );
    }
    assert!(!verifier_upload.contains("telemetry"));

    let verifier_download = workflow_step(aggregate, contract.verifier_download_step);
    for required in [
        "steps.upload_verifier_archive.outcome == 'success'",
        "actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093",
        "timeout-minutes: 2",
        &format!(
            "name: invariants-{}-aggregate-verifier-evidence-${{{{ github.run_id }}}}-${{{{ github.run_attempt }}}}",
            contract.profile
        ),
        &format!(
            "path: ${{{{ runner.temp }}}}/invariants-{}-aggregate-verifier-download-${{{{ github.run_id }}}}-${{{{ github.run_attempt }}}}",
            contract.profile
        ),
    ] {
        assert!(
            verifier_download.contains(required),
            "{} verifier evidence readback omitted {required}",
            contract.profile
        );
    }

    let verifier_verify = workflow_step(aggregate, contract.verifier_verify_step);
    for required in [
        "steps.download_verifier_archive.outcome == 'success'",
        "timeout-minutes: 5",
        "ARCHIVE_SHA256: ${{ steps.verifier_archive.outputs.archive_sha256 }}",
        "MANIFEST_SHA256: ${{ steps.aggregate.outputs.verifier_manifest_sha256 }}",
        "verify-verifier-archive",
        &format!("--profile {}", contract.profile),
        "--archive \"$archive\"",
        "--archive-sha256 \"$ARCHIVE_SHA256\"",
        "--manifest-sha256 \"$MANIFEST_SHA256\"",
    ] {
        assert!(
            verifier_verify.contains(required),
            "{} downloaded verifier evidence check omitted {required}",
            contract.profile
        );
    }
}

fn assert_diagnostics_upload_contract(aggregate: &str, contract: AggregateWorkflowContract) {
    let diagnostics = workflow_step(aggregate, contract.diagnostics_upload_step);
    for required in [
        "if: always()",
        "actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02",
        "timeout-minutes: 2",
        "target/rafter-invariants/telemetry/",
        "crates/rafter-invariants/target/rafter-invariants/telemetry/",
        "if-no-files-found: ignore",
    ] {
        assert!(
            diagnostics.contains(required),
            "{} aggregate diagnostics upload omitted {required}",
            contract.profile
        );
    }
    assert!(!diagnostics.contains("INVARIANT_EVIDENCE_DIR"));
    assert!(!diagnostics.contains("verifier-evidence"));
}
