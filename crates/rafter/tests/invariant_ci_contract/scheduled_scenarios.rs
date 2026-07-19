//! Scheduled scenarios: nightly and weekly transport every required evidence layer.

use super::support::*;

#[test]
fn scheduled_invariant_evidence_is_isolated_by_run_attempt() {
    let root = workspace_root();
    for (workflow, profile, aggregate_job) in [
        (
            ".github/workflows/nightly.yml",
            "nightly",
            "invariants-nightly",
        ),
        (
            ".github/workflows/weekly.yml",
            "weekly",
            "invariants-weekly",
        ),
    ] {
        let source = read(&root.join(workflow));
        for layer in SCHEDULED_LAYERS {
            let job = format!("invariants-{layer}");
            let block = job_block(&source, &job);
            let upload = workflow_step(block, &scheduled_upload_step(profile, layer));
            assert!(upload.contains(&format!(
                "name: invariants-{profile}-evidence-{layer}-${{{{ github.run_id }}}}-${{{{ github.run_attempt }}}}"
            )));
            for required in [
                format!("artifacts/invariants/{profile}-{layer}.json"),
                format!("artifacts/invariants/{profile}-{layer}/"),
            ] {
                assert!(
                    upload.contains(&required),
                    "{profile} {layer} evidence upload omitted {required}"
                );
            }
            assert!(upload.contains("if-no-files-found: error"));
            assert!(!upload.contains("telemetry"));

            let diagnostics = workflow_step(block, &scheduled_diagnostics_step(profile, layer));
            assert!(diagnostics.contains(&format!(
                "name: invariants-{profile}-{layer}-process-diagnostics-${{{{ github.run_id }}}}-${{{{ github.run_attempt }}}}"
            )));
            assert!(diagnostics.contains("target/rafter-invariants/telemetry/"));
            assert!(diagnostics.contains("if-no-files-found: ignore"));
            assert!(!diagnostics.contains("artifacts/invariants"));
        }

        let aggregate = job_block(&source, aggregate_job);
        assert!(!aggregate.contains("merge-multiple:"));
        assert!(!aggregate.contains(&format!("pattern: invariants-{profile}-evidence-")));
        let mut download_paths = Vec::new();
        for layer in SCHEDULED_LAYERS {
            let download = workflow_step(
                aggregate,
                &format!(
                    "Download available {profile} {} evidence",
                    display_layer(layer)
                ),
            );
            assert!(download.contains("continue-on-error: true"));
            assert!(download.contains(&format!(
                "name: invariants-{profile}-evidence-{layer}-${{{{ github.run_id }}}}-${{{{ github.run_attempt }}}}"
            )));
            let expected_path =
                format!("${{{{ runner.temp }}}}/${{{{ env.INVARIANT_EVIDENCE_DIR }}}}/{layer}");
            assert!(download.contains(&format!("path: {expected_path}")));
            download_paths.push(expected_path);

            let aggregate_step = workflow_step(
                aggregate,
                &format!("Aggregate exactly 44 {profile} invariant verdicts"),
            );
            assert!(aggregate_step.contains(&format!(
                "--result \"$RUNNER_TEMP/$INVARIANT_EVIDENCE_DIR/{layer}/{profile}-{layer}.json\""
            )));
        }
        assert_unique_paths(&download_paths)
            .unwrap_or_else(|error| panic!("{profile} evidence downloads: {error}"));

        let stage = workflow_step(
            aggregate,
            &format!("Stage current-run {profile} evidence artifacts"),
        );
        assert_transport_stage_contract(stage, profile, &SCHEDULED_LAYERS);

        assert_separate_aggregate_uploads(aggregate, profile);
    }
}

#[test]
fn scheduled_profiles_run_real_maelstrom_evidence() {
    let root = workspace_root();
    for (workflow, profile) in [
        (".github/workflows/nightly.yml", "nightly"),
        (".github/workflows/weekly.yml", "weekly"),
    ] {
        let source = read(&root.join(workflow));
        let block = job_block(&source, "invariants-maelstrom");
        assert!(block.contains(&format!(
            "cargo run --locked -p rafter-invariants -- run --profile {profile} --layer maelstrom"
        )));
        assert!(block.contains("cargo run --locked -p rafter-invariants -- verify-layer"));
        assert!(block.contains("if: always()"));
        assert!(block.contains("actions/upload-artifact@v4"));
        assert!(block.contains("if-no-files-found: error"));
        assert!(block.contains("retention-days: 30"));
    }
}

#[test]
fn scheduled_profiles_run_all_evidence_and_exact_aggregates() {
    let root = workspace_root();
    for (workflow, profile) in [
        (".github/workflows/nightly.yml", "nightly"),
        (".github/workflows/weekly.yml", "weekly"),
    ] {
        let source = read(&root.join(workflow));
        for layer in ["tests", "simulator", "tla", "maelstrom"] {
            let block = job_block(&source, &format!("invariants-{layer}"));
            assert!(block.contains(&format!(
                "cargo run --locked -p rafter-invariants -- run --profile {profile} --layer {layer}"
            )));
            assert!(block.contains("if: always()"));
            assert!(block.contains("actions/upload-artifact@v4"));
            assert!(block.contains("retention-days: 30"));
        }

        let aggregate = job_block(&source, &format!("invariants-{profile}"));
        assert!(
            aggregate.contains("cargo build --locked -p rafter-maelstrom --bins"),
            "{profile} aggregate must build the source-bound Maelstrom binaries it verifies"
        );
        for dependency in [
            "invariants-tests",
            "invariants-simulator",
            "invariants-tla",
            "invariants-maelstrom",
        ] {
            assert!(aggregate.contains(&format!("- {dependency}")));
            assert!(aggregate.contains(&format!("needs.{dependency}.result")));
        }
        for required in [
            "if: always()",
            "continue-on-error: true",
            "actions/download-artifact@v4",
            "actions/upload-artifact@v4",
            "GITHUB_STEP_SUMMARY",
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
                "{profile} aggregate omitted required contract fragment: {required}"
            );
        }
        assert!(aggregate.contains(&format!(
            "check --profile {profile} --source-ref \"$GITHUB_SHA\""
        )));
    }
}
