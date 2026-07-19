//! PR scenarios: source-bound producers feed one stable fail-closed aggregate.

use std::path::Path;

use super::support::*;

#[test]
fn pr_invariant_aggregate_is_stable_and_fail_closed() {
    let root = workspace_root();
    let workflow = read(&root.join(".github/workflows/ci.yml"));

    for (job, layer) in [
        ("invariants-tests", "tests"),
        ("invariants-simulator", "simulator"),
        ("invariants-tla", "tla"),
    ] {
        let block = job_block(&workflow, job);
        assert!(
            block.contains(&format!(
                "cargo run --locked -p rafter-invariants -- run --profile pr --layer {layer}"
            )),
            "{job} must invoke its source-bound producer"
        );
        assert!(
            block.contains("if: always()") && block.contains("actions/upload-artifact@v4"),
            "{job} must preserve evidence even when the producer fails"
        );
    }

    let maelstrom = job_block(&workflow, "invariants-maelstrom");
    assert!(maelstrom.contains("Validate scheduled Maelstrom evidence contract"));
    assert!(maelstrom.contains("cargo fetch --locked"));
    assert!(!maelstrom.contains("--profile pr --layer maelstrom"));

    assert_pr_launcher_inventories(&workflow);

    let tla = job_block(&workflow, "invariants-tla");
    for required in [
        "timeout-minutes: 360",
        "Check TLA host capacity",
        "required_kib=\"$((8 * 1024 * 1024))\"",
        "required_memory_kib=\"$((12 * 1024 * 1024))\"",
        "timeout-minutes: 350",
    ] {
        assert!(
            tla.contains(required),
            "PR TLA job omitted completion-capacity contract: {required}"
        );
    }
    let tla_validation = job_block(&workflow, "invariants-tla-validation");
    for exact_inventory in [
        "scripts/cargo-test-exact 34 producer::tla_exec::mutation_tests --locked -p rafter-invariants --lib -- --ignored --test-threads=1",
        "scripts/cargo-test-exact 4 artifact_verify_tla::full_bundle_tests::serialized_tests --locked -p rafter-invariants --lib -- --ignored --test-threads=1",
    ] {
        assert!(
            tla_validation.contains(exact_inventory),
            "TLA validation omitted exact inventory: {exact_inventory}"
        );
    }

    let profile = read(&root.join("verification/raft-invariant-profiles.json"));
    for required in [
        "\"soft_timeout\": \"300m\"",
        "\"total_timeout\": \"338m\"",
        "\"finalization_reserve\": \"2m\"",
        "\"max_heap\": \"8g\"",
        "\"fp_mem\": \"0.45\"",
        "\"minimum_generated_states\": \"120000000\"",
        "\"minimum_distinct_states\": \"16000000\"",
    ] {
        assert!(
            profile.contains(required),
            "PR TLA profile omitted completion contract: {required}"
        );
    }

    let aggregate = job_block(&workflow, "invariants-pr");
    for dependency in [
        "invariants-tests",
        "invariants-simulator",
        "invariants-tla",
        "invariants-tla-validation",
        "invariants-maelstrom",
    ] {
        assert!(aggregate.contains(&format!("- {dependency}")));
    }
    for required in [
        "if: always()",
        "actions/download-artifact@v4",
        "continue-on-error: true",
        "check --profile pr --source-ref \"$GITHUB_SHA\"",
        "GITHUB_STEP_SUMMARY",
        "actions/upload-artifact@v4",
        "needs.invariants-tests.result",
        "needs.invariants-simulator.result",
        "needs.invariants-tla.result",
        "needs.invariants-tla-validation.result",
        "needs.invariants-maelstrom.result",
        "timeout-minutes: 20",
        "verification/invariant-verdict-schema.json",
        "cmp -s \"$expected_ids\" \"$json_ids\"",
        "cmp -s \"$expected_ids\" \"$markdown_ids\"",
        "cmp -s \"$expected_ids\" \"$junit_ids\"",
        ".summary.total == 44",
        ".summary.green == 44",
        "(.invariants | length) == 44",
    ] {
        assert!(
            aggregate.contains(required),
            "invariants-pr omitted required contract fragment: {required}"
        );
    }

    assert_pr_documentation(&root);
}

fn assert_pr_documentation(root: &Path) {
    let readme = read(&root.join("README.md"));
    assert!(readme.contains("Branch protection on `main` requires the stable `invariants-pr`"));
    assert!(readme.contains("Evidence artifacts are isolated by workflow run attempt"));
}

#[test]
fn pr_invariant_evidence_is_isolated_by_run_attempt() {
    let root = workspace_root();
    let source = read(&root.join(".github/workflows/ci.yml"));

    for producer in &PR_EVIDENCE_PRODUCERS {
        let upload = workflow_step(job_block(&source, producer.job), producer.upload_step);
        assert!(upload.contains("if: always()"));
        assert!(upload.contains("overwrite: true"));
        assert!(upload.contains("if-no-files-found: error"));
        assert!(upload.contains(&format!("artifacts/invariants/pr-{}.json", producer.layer)));
        assert!(upload.contains(&format!("artifacts/invariants/pr-{}/", producer.layer)));
        assert!(
            !upload.contains("telemetry"),
            "{} evidence artifact must not contain process telemetry",
            producer.layer
        );
        assert!(upload.contains(&format!(
            "name: invariants-pr-evidence-{}-${{{{ github.run_id }}}}-${{{{ github.run_attempt }}}}",
            producer.layer
        )));

        let diagnostics =
            workflow_step(job_block(&source, producer.job), producer.diagnostics_step);
        for required in [
            "if: always()",
            "actions/upload-artifact@v4",
            "target/rafter-invariants/telemetry/",
            "crates/rafter-invariants/target/rafter-invariants/telemetry/",
            "if-no-files-found: ignore",
            "retention-days: 30",
        ] {
            assert!(
                diagnostics.contains(required),
                "{} diagnostics upload omitted {required}",
                producer.layer
            );
        }
        assert!(diagnostics.contains(&format!(
            "name: invariants-{}-process-diagnostics-${{{{ github.run_id }}}}-${{{{ github.run_attempt }}}}",
            producer.layer
        )));
        assert!(
            !diagnostics.contains("artifacts/invariants"),
            "{} diagnostics must remain separate from invariant evidence",
            producer.layer
        );
    }

    let aggregate = job_block(&source, "invariants-pr");
    assert!(aggregate.contains("if: always()"));
    assert!(!aggregate.contains("merge-multiple:"));
    assert!(!aggregate.contains("pattern: invariants-pr-evidence-"));
    let mut download_paths = Vec::new();
    for producer in &PR_EVIDENCE_PRODUCERS {
        let download = workflow_step(aggregate, producer.download_step);
        assert!(download.contains("continue-on-error: true"));
        assert!(download.contains("actions/download-artifact@v4"));
        assert!(download.contains(&format!(
            "name: invariants-pr-evidence-{}-${{{{ github.run_id }}}}-${{{{ github.run_attempt }}}}",
            producer.layer
        )));
        let expected_path = format!(
            "${{{{ runner.temp }}}}/${{{{ env.INVARIANT_EVIDENCE_DIR }}}}/{}",
            producer.layer
        );
        assert!(download.contains(&format!("path: {expected_path}")));
        download_paths.push(expected_path);

        let aggregate_step = workflow_step(aggregate, "Aggregate exactly 44 invariant verdicts");
        assert!(aggregate_step.contains(&format!(
            "--result \"$RUNNER_TEMP/$INVARIANT_EVIDENCE_DIR/{0}/pr-{0}.json\"",
            producer.layer
        )));
    }
    assert_unique_paths(&download_paths).expect("PR evidence downloads must not collide");

    let mut collision_fixture = download_paths.clone();
    collision_fixture[1] = collision_fixture[0].clone();
    assert!(
        assert_unique_paths(&collision_fixture).is_err(),
        "collision fixture must be rejected"
    );

    let stage = workflow_step(aggregate, "Stage current-run PR evidence artifacts");
    assert_transport_stage_contract(stage, "pr", &PR_LAYERS);

    let report = workflow_step(aggregate, "Upload available aggregate reports");
    assert!(report
        .contains("name: invariants-pr-report-${{ github.run_id }}-${{ github.run_attempt }}"));
    assert!(report.contains("overwrite: true"));
    assert!(!report.contains("artifacts/invariants"));
    assert!(!report.contains("telemetry"));

    let evidence = workflow_step(aggregate, "Upload available aggregate evidence");
    assert!(evidence.contains(
        "name: invariants-pr-aggregate-evidence-${{ github.run_id }}-${{ github.run_attempt }}"
    ));
    assert!(evidence.contains("${{ runner.temp }}/${{ env.INVARIANT_EVIDENCE_DIR }}/"));
    assert!(!evidence.contains("telemetry"));

    let diagnostics = workflow_step(aggregate, "Upload aggregate process diagnostics");
    assert!(diagnostics.contains(
        "name: invariants-pr-aggregate-process-diagnostics-${{ github.run_id }}-${{ github.run_attempt }}"
    ));
    assert!(diagnostics.contains("target/rafter-invariants/telemetry/"));
    assert!(!diagnostics.contains("INVARIANT_EVIDENCE_DIR"));
}
