//! Reporting scenarios: complete reports always publish while failed evidence stays red.

use std::{fs, path::Path};

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

    assert!(aggregate.contains("timeout-minutes: 20"));
    let aggregate_step_name = if contract.profile == "pr" {
        "Aggregate exactly 44 invariant verdicts".to_owned()
    } else {
        format!(
            "Aggregate exactly 44 {} invariant verdicts",
            contract.profile
        )
    };
    assert!(
        workflow_step(aggregate, &aggregate_step_name).contains("timeout-minutes: 8"),
        "{} aggregate generation must leave time for report publication",
        contract.profile
    );

    let validate = workflow_step(aggregate, contract.validate_step);
    assert!(validate.contains("timeout-minutes: 1"));
    let validation = run_workflow_script(validate, root, &fixture.environment(&[]));
    assert_success(&validation, contract.validate_step);
    let outputs = read(&fixture.github_output);
    assert!(
        outputs.lines().any(|line| line == "complete=true"),
        "{} did not recognize a complete 44-row report: {outputs}",
        contract.profile
    );

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

    verify_identity_mismatches_are_rejected(aggregate, validate, &fixture, contract, root);
}

fn assert_aggregate_upload_contracts(aggregate: &str, contract: AggregateWorkflowContract) {
    let upload = workflow_step(aggregate, contract.report_upload_step);
    for required in [
        "if: always()",
        "actions/upload-artifact@v4",
        "timeout-minutes: 2",
        &format!("/{0}.json", contract.profile),
        &format!("/{0}.xml", contract.profile),
        &format!("/{0}.md", contract.profile),
        "if-no-files-found: ignore",
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
        "actions/upload-artifact@v4",
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

    let diagnostics = workflow_step(aggregate, contract.diagnostics_upload_step);
    for required in [
        "if: always()",
        "actions/upload-artifact@v4",
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
}

fn verify_identity_mismatches_are_rejected(
    aggregate: &str,
    validate: &str,
    fixture: &AggregateReportFixture,
    contract: AggregateWorkflowContract,
    workspace: &Path,
) {
    let mut duplicate_ids = CANONICAL_INVARIANT_IDS;
    duplicate_ids[43] = "ST-01";
    assert_report_identity_rejected(
        validate,
        fixture,
        contract,
        workspace,
        &duplicate_ids,
        &CANONICAL_INVARIANT_IDS,
        &CANONICAL_INVARIANT_IDS,
        "duplicate JSON invariant",
    );
    assert_report_identity_rejected(
        validate,
        fixture,
        contract,
        workspace,
        &CANONICAL_INVARIANT_IDS,
        &duplicate_ids,
        &CANONICAL_INVARIANT_IDS,
        "duplicate Markdown invariant",
    );
    assert_report_identity_rejected(
        validate,
        fixture,
        contract,
        workspace,
        &CANONICAL_INVARIANT_IDS,
        &CANONICAL_INVARIANT_IDS,
        &duplicate_ids,
        "duplicate JUnit invariant",
    );

    let mut noncanonical_ids = CANONICAL_INVARIANT_IDS;
    noncanonical_ids[43] = "ZZ-99";
    assert_report_identity_rejected(
        validate,
        fixture,
        contract,
        workspace,
        &noncanonical_ids,
        &noncanonical_ids,
        &noncanonical_ids,
        "consistent noncanonical invariant",
    );

    fixture.write_reports(
        &CANONICAL_INVARIANT_IDS,
        &CANONICAL_INVARIANT_IDS,
        &CANONICAL_INVARIANT_IDS,
    );
    let invalid_gate = run_workflow_script(
        workflow_step(aggregate, contract.gate_step),
        workspace,
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
        !invalid_gate.status.success(),
        "{} aggregate gate accepted an invalid report",
        contract.profile
    );
}

#[allow(clippy::too_many_arguments)]
fn assert_report_identity_rejected(
    validate: &str,
    fixture: &AggregateReportFixture,
    contract: AggregateWorkflowContract,
    workspace: &Path,
    json_ids: &[&str],
    markdown_ids: &[&str],
    junit_ids: &[&str],
    case: &str,
) {
    fixture.write_reports(json_ids, markdown_ids, junit_ids);
    fs::write(&fixture.github_output, "").expect("reset aggregate outputs");
    let invalid_report = run_workflow_script(validate, workspace, &fixture.environment(&[]));
    assert_success(&invalid_report, contract.validate_step);
    assert!(
        read(&fixture.github_output)
            .lines()
            .any(|line| line == "complete=false"),
        "{} accepted {case}",
        contract.profile,
    );
}
