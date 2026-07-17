use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT_AGGREGATE_FIXTURE_ID: AtomicU64 = AtomicU64::new(0);
const CANONICAL_INVARIANT_IDS: [&str; 44] = [
    "ST-01", "EL-01", "EL-02", "EL-03", "EL-04", "EL-05", "EL-06", "EL-07", "EL-08", "LG-01",
    "LG-02", "LG-03", "LG-04", "LG-05", "CM-01", "CM-02", "CM-03", "AP-01", "AP-02", "MB-01",
    "MB-02", "MB-03", "MB-04", "MB-05", "MB-06", "MB-07", "RD-01", "RD-02", "RD-03", "RD-04",
    "RD-05", "RD-06", "PS-01", "PS-02", "PS-03", "PS-04", "SS-01", "SS-02", "SS-03", "SS-04",
    "SS-05", "LV-01", "LV-02", "LV-03",
];

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
    assert!(!maelstrom.contains("--profile pr --layer maelstrom"));

    let tla = job_block(&workflow, "invariants-tla");
    for required in [
        "timeout-minutes: 205",
        "Check TLA state capacity",
        "required_kib=\"$((8 * 1024 * 1024))\"",
        "timeout-minutes: 170",
        "cargo test --locked -p rafter-invariants --lib -- --ignored --test-threads=1",
    ] {
        assert!(
            tla.contains(required),
            "PR TLA job omitted completion-capacity contract: {required}"
        );
    }

    let profile = read(&root.join("verification/raft-invariant-profiles.json"));
    for required in [
        "\"soft_timeout\": \"115m\"",
        "\"total_timeout\": \"155m\"",
        "\"finalization_reserve\": \"2m\"",
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

    let readme = read(&root.join("README.md"));
    assert!(readme.contains("Branch protection on `main` requires the stable `invariants-pr`"));
    assert!(readme.contains("Evidence artifacts are isolated by workflow run attempt"));
}

#[test]
fn pr_invariant_evidence_is_isolated_by_run_attempt() {
    let root = workspace_root();
    let source = read(&root.join(".github/workflows/ci.yml"));
    let producers = [
        ArtifactProducerContract {
            job: "invariants-tests",
            layer: "tests",
            upload_step: "Upload test evidence",
        },
        ArtifactProducerContract {
            job: "invariants-simulator",
            layer: "simulator",
            upload_step: "Upload simulator evidence",
        },
        ArtifactProducerContract {
            job: "invariants-tla",
            layer: "tla",
            upload_step: "Upload TLA+ evidence",
        },
    ];

    for producer in producers {
        let upload = workflow_step(job_block(&source, producer.job), producer.upload_step);
        assert!(upload.contains("if: always()"));
        assert!(upload.contains("overwrite: true"));
        assert!(upload.contains("if-no-files-found: error"));
        assert!(upload.contains(&format!(
            "name: invariants-pr-evidence-{}-${{{{ github.run_id }}}}-${{{{ github.run_attempt }}}}",
            producer.layer
        )));
    }

    let aggregate = job_block(&source, "invariants-pr");
    assert!(aggregate.contains("if: always()"));
    let download = workflow_step(aggregate, "Download available evidence");
    assert!(download.contains(
        "pattern: invariants-pr-evidence-*-${{ github.run_id }}-${{ github.run_attempt }}"
    ));

    let report = workflow_step(aggregate, "Upload available aggregate reports and evidence");
    assert!(report
        .contains("name: invariants-pr-report-${{ github.run_id }}-${{ github.run_attempt }}"));
    assert!(report.contains("overwrite: true"));
}

#[test]
fn scheduled_invariant_evidence_is_isolated_by_run_attempt() {
    let root = workspace_root();
    for (workflow, profile, aggregate_job, download_step) in [
        (
            ".github/workflows/nightly.yml",
            "nightly",
            "invariants-nightly",
            "Download available nightly evidence",
        ),
        (
            ".github/workflows/weekly.yml",
            "weekly",
            "invariants-weekly",
            "Download available weekly evidence",
        ),
    ] {
        let source = read(&root.join(workflow));
        for (job, layer) in [
            ("invariants-tests", "tests"),
            ("invariants-simulator", "simulator"),
            ("invariants-tla", "tla"),
            ("invariants-maelstrom", "maelstrom"),
        ] {
            let block = job_block(&source, job);
            assert!(block.contains(&format!(
                "name: invariants-{profile}-evidence-{layer}-${{{{ github.run_id }}}}-${{{{ github.run_attempt }}}}"
            )));
            assert!(block.contains("if-no-files-found: error"));
        }

        let aggregate = job_block(&source, aggregate_job);
        let download = workflow_step(aggregate, download_step);
        assert!(download.contains(&format!(
            "pattern: invariants-{profile}-evidence-*-${{{{ github.run_id }}}}-${{{{ github.run_attempt }}}}"
        )));
    }
}

#[derive(Clone, Copy)]
struct ArtifactProducerContract {
    job: &'static str,
    layer: &'static str,
    upload_step: &'static str,
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
            upload_step: "Upload available aggregate reports and evidence",
            gate_step: "Require 44 of 44 green",
        },
        AggregateWorkflowContract {
            workflow: ".github/workflows/nightly.yml",
            profile: "nightly",
            job: "invariants-nightly",
            validate_step: "Validate current-run nightly reports",
            summary_step: "Render the 44-row nightly report",
            upload_step: "Upload available nightly aggregate reports and evidence",
            gate_step: "Require 44 of 44 nightly invariants green",
        },
        AggregateWorkflowContract {
            workflow: ".github/workflows/weekly.yml",
            profile: "weekly",
            job: "invariants-weekly",
            validate_step: "Validate current-run weekly reports",
            summary_step: "Render the 44-row weekly report",
            upload_step: "Upload available weekly aggregate reports and evidence",
            gate_step: "Require 44 of 44 weekly invariants green",
        },
    ] {
        verify_always_publish_failure_branch(&root, contract);
    }
}

#[test]
fn weekly_full_tlc_is_source_bound_checkpointed_and_fail_closed() {
    let root = workspace_root();
    let workflow = read(&root.join(".github/workflows/weekly.yml"));
    assert!(!workflow.contains("\n  tlc-full:\n"));
    assert!(!workflow.contains("best-effort"));

    let tla = job_block(&workflow, "invariants-tla");
    for required in [
        "timeout-minutes: 400",
        "timeout-minutes: 360",
        "runs-on: [self-hosted, linux, X64]",
        "actions/cache/restore@v4",
        "Restore exact-compatible weekly TLC checkpoint",
        "target/rafter-invariants/tla-checkpoint/weekly",
        "tla-weekly-checkpoint-v1-",
        "cargo run --locked -p rafter-invariants -- run --profile weekly --layer tla",
        "actions/cache/save@v4",
        "Save exact-compatible weekly TLC checkpoint",
        "if: always()",
    ] {
        assert!(
            tla.contains(required),
            "weekly source-bound TLA job omitted: {required}"
        );
    }
    for implementation_glob in [
        "'crates/rafter-invariants/src/producer/*.rs'",
        "'crates/rafter-invariants/src/producer/filesystem/**/*.rs'",
        "'crates/rafter-invariants/src/producer/process/**/*.rs'",
        "'crates/rafter-invariants/src/producer/tla_checkpoint/**/*.rs'",
    ] {
        assert_eq!(
            tla.matches(implementation_glob).count(),
            3,
            "weekly checkpoint restore/save keys must all hash {implementation_glob}"
        );
    }

    let profile = read(&root.join("verification/raft-invariant-profiles.json"));
    for required in [
        "\"config\": \"Raft.cfg\"",
        "\"soft_timeout\": \"295m\"",
        "\"total_timeout\": \"350m\"",
        "\"finalization_reserve\": \"10m\"",
        "\"workers\": \"auto\"",
        "\"checkpoint_minutes\": \"30\"",
        "\"checkpoint_gzip\": \"required\"",
        "\"max_heap\": \"4g\"",
        "\"checkpoint_recovery\": \"strict-compatible-if-present\"",
        "\"unsymmetrized_exploration\": \"required\"",
    ] {
        assert!(
            profile.contains(required),
            "weekly TLA profile omitted: {required}"
        );
    }
}

#[test]
fn model_check_overhead_evidence_is_repeated_and_durable() {
    let root = workspace_root();
    let workflow = read(&root.join(".github/workflows/benchmarks.yml"));
    let smoke = job_block(&workflow, "smoke");
    assert!(smoke.contains("python3 -m unittest scripts/tests/test_model_check_profile_report.py"));
    assert!(smoke.contains("test -x scripts/model-check-profile-compare"));

    let evidence = job_block(&workflow, "model-check-evidence");
    for required in [
        "fetch-depth: 0",
        "MODEL_CHECK_PROFILES: fast",
        "MODEL_CHECK_RUNS: \"6\"",
        "scripts/model-check-profile-compare",
        "if: always()",
        "GITHUB_STEP_SUMMARY",
        "actions/upload-artifact@v4",
        "if-no-files-found: error",
        "retention-days: 30",
    ] {
        assert!(
            evidence.contains(required),
            "model-check evidence omitted required contract fragment: {required}"
        );
    }
}

#[derive(Clone, Copy)]
struct AggregateWorkflowContract {
    workflow: &'static str,
    profile: &'static str,
    job: &'static str,
    validate_step: &'static str,
    summary_step: &'static str,
    upload_step: &'static str,
    gate_step: &'static str,
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

    let upload = workflow_step(aggregate, contract.upload_step);
    for required in [
        "if: always()",
        "actions/upload-artifact@v4",
        "timeout-minutes: 2",
        &format!("/{0}.json", contract.profile),
        &format!("/{0}.xml", contract.profile),
        &format!("/{0}.md", contract.profile),
        "artifacts/invariants/",
        "target/rafter-invariants/telemetry/",
        "if-no-files-found: ignore",
    ] {
        assert!(
            upload.contains(required),
            "{} upload step omitted {required}",
            contract.profile
        );
    }

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

struct AggregateReportFixture {
    root: PathBuf,
    runner_temp: PathBuf,
    report_dir: String,
    github_output: PathBuf,
    github_summary: PathBuf,
    profile: String,
    markdown: String,
}

impl AggregateReportFixture {
    fn new(workspace: &Path, profile: &str) -> Self {
        let id = NEXT_AGGREGATE_FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
        let root = workspace
            .join("target/ci-contract")
            .join(format!("always-publish-{}-{id}", std::process::id()));
        if root.exists() {
            fs::remove_dir_all(&root).expect("remove stale aggregate fixture");
        }
        let runner_temp = root.join("runner-temp");
        let report_dir = format!("reports-{profile}");
        let reports = runner_temp.join(&report_dir);
        fs::create_dir_all(&reports).expect("create aggregate report fixture");

        fs::create_dir_all(root.join("artifacts/invariants"))
            .expect("create aggregate evidence fixture");
        fs::write(root.join("artifacts/invariants/evidence.json"), "{}\n")
            .expect("write aggregate evidence fixture");
        fs::create_dir_all(root.join("target/rafter-invariants/telemetry"))
            .expect("create aggregate telemetry fixture");
        fs::write(
            root.join("target/rafter-invariants/telemetry/process.log"),
            "fixture\n",
        )
        .expect("write aggregate telemetry fixture");

        let fixture = Self {
            github_output: root.join("github-output"),
            github_summary: root.join("github-summary"),
            root,
            runner_temp,
            report_dir,
            profile: profile.to_owned(),
            markdown: render_markdown(profile, &CANONICAL_INVARIANT_IDS),
        };
        fixture.write_reports(
            &CANONICAL_INVARIANT_IDS,
            &CANONICAL_INVARIANT_IDS,
            &CANONICAL_INVARIANT_IDS,
        );
        fixture
    }

    fn write_reports(&self, json_ids: &[&str], markdown_ids: &[&str], junit_ids: &[&str]) {
        let reports = self.runner_temp.join(&self.report_dir);
        let invariants = json_ids
            .iter()
            .map(|id| format!(r#"{{"invariant_id":"{id}"}}"#))
            .collect::<Vec<_>>()
            .join(",");
        fs::write(
            reports.join(format!("{}.json", self.profile)),
            format!(
                r#"{{"profile":"{}","summary":{{"total":44,"green":44,"red":0}},"invariants":[{invariants}]}}"#,
                self.profile
            ),
        )
        .expect("write aggregate JSON fixture");
        fs::write(
            reports.join(format!("{}.md", self.profile)),
            render_markdown(&self.profile, markdown_ids),
        )
        .expect("write aggregate Markdown fixture");
        let junit_rows = junit_ids
            .iter()
            .map(|id| {
                format!("  <testcase classname=\"rafter.invariants\" name=\"{id}\">\n  </testcase>")
            })
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(
            reports.join(format!("{}.xml", self.profile)),
            format!("<testsuite tests=\"44\" failures=\"0\">\n{junit_rows}\n</testsuite>\n"),
        )
        .expect("write aggregate JUnit fixture");
    }

    fn environment<'a>(&'a self, extra: &[(&'a str, &'a str)]) -> Vec<(&'a str, &'a str)> {
        let mut environment = vec![
            (
                "RUNNER_TEMP",
                self.runner_temp.to_str().expect("UTF-8 runner temp"),
            ),
            ("INVARIANT_REPORT_DIR", self.report_dir.as_str()),
            (
                "GITHUB_OUTPUT",
                self.github_output.to_str().expect("UTF-8 output path"),
            ),
            (
                "GITHUB_STEP_SUMMARY",
                self.github_summary.to_str().expect("UTF-8 summary path"),
            ),
        ];
        environment.extend_from_slice(extra);
        environment
    }
}

fn render_markdown(profile: &str, ids: &[&str]) -> String {
    let rows = ids
        .iter()
        .map(|id| format!("| `{id}` | GREEN | 1/1 | 1/1 | |"))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "# Rafter invariant report: {profile}\n\n| Invariant | Verdict | Clauses | Evidence | Detail |\n| --- | --- | ---: | ---: | --- |\n{rows}\n"
    )
}

impl Drop for AggregateReportFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn workflow_step<'a>(job: &'a str, name: &str) -> &'a str {
    let marker = format!("      - name: {name}\n");
    let start = job
        .find(&marker)
        .unwrap_or_else(|| panic!("workflow step {name} is missing"));
    let tail = &job[start..];
    let end = tail[marker.len()..]
        .find("\n      - name: ")
        .map_or(tail.len(), |offset| marker.len() + offset);
    &tail[..end]
}

fn run_workflow_script(step: &str, current_dir: &Path, environment: &[(&str, &str)]) -> Output {
    let marker = "        run: |\n";
    let script = step
        .split_once(marker)
        .unwrap_or_else(|| panic!("workflow step omitted a shell script: {step}"))
        .1
        .lines()
        .take_while(|line| line.is_empty() || line.starts_with("          "))
        .map(|line| line.strip_prefix("          ").unwrap_or(line))
        .collect::<Vec<_>>()
        .join("\n");
    let mut command = Command::new("bash");
    command
        .args(["-eu", "-o", "pipefail", "-c", &script])
        .current_dir(current_dir);
    for (name, value) in environment {
        command.env(name, value);
    }
    command.output().expect("execute workflow shell fixture")
}

fn assert_success(output: &Output, label: &str) {
    assert!(
        output.status.success(),
        "{label} failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn job_block<'a>(workflow: &'a str, id: &str) -> &'a str {
    let marker = format!("\n  {id}:\n");
    let start = workflow
        .find(&marker)
        .unwrap_or_else(|| panic!("workflow job {id} is missing"))
        + marker.len();
    let tail = &workflow[start..];
    let end = tail
        .match_indices("\n  ")
        .find_map(|(offset, _)| {
            let line = tail[offset + 1..].lines().next()?;
            (line.starts_with("  ") && !line.starts_with("    ") && line.trim_end().ends_with(':'))
                .then_some(offset)
        })
        .unwrap_or(tail.len());
    &tail[..end]
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("rafter crate should live under <workspace>/crates/rafter")
        .to_path_buf()
}
