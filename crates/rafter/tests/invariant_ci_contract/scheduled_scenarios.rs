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
            if layer == "simulator" {
                let detector_artifacts =
                    format!("artifacts/invariants/{profile}-simulator-detectors-tests/");
                assert!(
                    upload.contains(&detector_artifacts),
                    "{profile} simulator evidence upload omitted {detector_artifacts}"
                );
            }
            assert!(upload.contains("if-no-files-found: error"));
            assert!(!upload.contains("overwrite: true"));
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
            assert!(
                aggregate_step.contains("evidence_root=\"$GITHUB_WORKSPACE/artifacts/invariants\"")
            );
            assert!(aggregate_step.contains(&format!(
                "--result \"$evidence_root/{profile}-{layer}.json\""
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

/// The continuation sampler reports whichever TLC it was pointed at, and this
/// job runs several: calibrated proof obligations for over two hours before
/// the primary starts, a trace sample, and a negative detector whose purpose is
/// to print a violation. Started without the primary configuration it reports
/// one of those instead, with nothing in the output to say so. The
/// configuration is read from the tier runner rather than restated here, so it
/// cannot drift from the run it claims to describe.
#[test]
fn scheduled_continuation_telemetry_is_bound_to_the_primary_config() {
    let root = workspace_root();
    for (workflow, profile, tier) in [
        (".github/workflows/nightly.yml", "nightly", "--nightly"),
        (".github/workflows/weekly.yml", "weekly", "--full"),
    ] {
        let source = read(&root.join(workflow));
        let block = job_block(&source, "invariants-tla");

        let start = workflow_step(block, "Start TLA+ continuation telemetry");
        for required in [
            format!("config=\"$(scripts/tla-model-check --print-config {tier})\""),
            "--config \"$config\"".to_owned(),
            format!("--checkpoint target/rafter-invariants/tla-checkpoint/{profile}"),
            format!("--output \"${{RUNNER_TEMP}}/tla-continuation-telemetry/{profile}.jsonl\""),
        ] {
            assert!(
                start.contains(&required),
                "{profile} continuation sampler omitted {required}"
            );
        }

        let classify = workflow_step(block, &format!("Classify {profile} TLA+ continuation"));
        assert!(classify.contains("if: always()"));
        assert!(classify.contains(&format!(
            "--input \"${{RUNNER_TEMP}}/tla-continuation-telemetry/{profile}.jsonl\""
        )));

        // Side channel: the sampler writes under RUNNER_TEMP, never into the
        // source-bound evidence tree the receipts cover.
        let upload = workflow_step(
            block,
            &format!("Upload {profile} TLA+ continuation telemetry"),
        );
        assert!(upload.contains("path: ${{ runner.temp }}/tla-continuation-telemetry/"));
        assert!(!upload.contains("artifacts/invariants"));
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
        assert!(block.contains("actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02"));
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
            assert!(
                block.contains("actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02")
            );
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
            "actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093",
            "actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02",
            "GITHUB_STEP_SUMMARY",
            "cargo run --offline --locked -p rafter-invariants -- verify-report-set",
            ".summary.total == 44",
            ".summary.green == 44",
            "(.invariants | length) == 44",
        ] {
            assert!(
                aggregate.contains(required),
                "{profile} aggregate omitted required contract fragment: {required}"
            );
        }
        assert!(workflow_step(
            aggregate,
            &format!("Aggregate exactly 44 {profile} invariant verdicts")
        )
        .contains("timeout-minutes: 35"));
        assert!(aggregate.contains(&format!(
            "check --profile {profile} --source-ref \"$GITHUB_SHA\""
        )));
    }
}
