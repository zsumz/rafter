use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use crate::{
    ArtifactRef, CheckCompletion, CheckReceipt, EvidenceResult, EvidenceStatus,
    ExecutionPlanReceipt, ExecutionReceipt, FailureClassification, InvocationReceipt, PlanInput,
    ProfileContract, ResultBundle, RunnerContract, SourceMaterializationReceipt, SourceReceipt,
    ToolReceipt, PLAN_SCHEMA_VERSION,
};
use sha2::{Digest, Sha256};

const VALID: &str = r"{
  :stats {:count 9 :ok-count 6 :by-f {
    :read {:ok-count 2} :write {:ok-count 3} :cas {:ok-count 1}}}
  :workload {:valid? true :failures [] :results {
    0 {:linearizable {:valid? true}}}}
  :valid? true}";

#[test]
fn aggregate_rederives_maelstrom_semantics_from_trial_artifacts(
) -> Result<(), Box<dyn std::error::Error>> {
    let root = temporary_root()?;
    write(&root, "scripts/maelstrom-lin-kv", "runner")?;
    prepare_state(&root)?;
    write(&root, "target/debug/rafter-maelstrom", "binary")?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/runner",
        "runner",
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/binary",
        "binary",
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/results.edn",
        VALID,
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/process.json",
        &process_log(&root, 0, "")?,
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/node.log",
        "role=leader",
    )?;
    let bundle = bundle();

    crate::artifact_verify_maelstrom::verify(&bundle, &root)?;

    let mut wrong_invocation: serde_json::Value =
        serde_json::from_str(&process_log(&root, 0, "")?)?;
    wrong_invocation["invocation"]["arguments"] = serde_json::json!(["--test-count", "2"]);
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/process.json",
        &wrong_invocation.to_string(),
    )?;
    assert!(crate::artifact_verify_maelstrom::verify(&bundle, &root)
        .expect_err("wrong complete invocation is rejected")
        .to_string()
        .contains("arguments"));
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/process.json",
        &process_log(&root, 0, "")?,
    )?;

    let mut forged = bundle.clone();
    forged.execution.checks[0]
        .observations
        .insert("read_ok".to_owned(), 99);
    assert!(crate::artifact_verify_maelstrom::verify(&forged, &root)
        .expect_err("forged observation is rejected")
        .to_string()
        .contains("observations disagree"));

    let mut stale: serde_json::Value = serde_json::from_str(&process_log(&root, 0, "")?)?;
    stale["schema_version"] = serde_json::json!(1);
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/process.json",
        &stale.to_string(),
    )?;
    assert!(crate::artifact_verify_maelstrom::verify(&bundle, &root)
        .expect_err("stale process schema is rejected")
        .to_string()
        .contains("exact invocation"));

    let mut incomplete: serde_json::Value = serde_json::from_str(&process_log(&root, 0, "")?)?;
    incomplete["invocation"]["program"] = serde_json::json!("");
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/process.json",
        &incomplete.to_string(),
    )?;
    assert!(crate::artifact_verify_maelstrom::verify(&bundle, &root)
        .expect_err("incomplete invocation is rejected")
        .to_string()
        .contains("exact invocation"));
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/process.json",
        &process_log(&root, 0, "")?,
    )?;

    let mut missing = bundle;
    missing.execution.checks[0]
        .artifacts
        .retain(|artifact| artifact.kind != "maelstrom-process-log");
    assert!(crate::artifact_verify_maelstrom::verify(&missing, &root)
        .expect_err("missing process evidence is rejected")
        .to_string()
        .contains("process-log is missing"));

    fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
fn invalid_history_can_only_fail_client_linearizability() -> Result<(), Box<dyn std::error::Error>>
{
    let root = temporary_root()?;
    write(&root, "scripts/maelstrom-lin-kv", "runner")?;
    prepare_state(&root)?;
    write(&root, "target/debug/rafter-maelstrom", "binary")?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/runner",
        "runner",
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/binary",
        "binary",
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/results.edn",
        &VALID.replace(
            ":linearizable {:valid? true}",
            ":linearizable {:valid? false}",
        ),
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/process.json",
        &process_log(&root, 0, "")?,
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/node.log",
        "role=leader",
    )?;
    let mut bundle = bundle();
    bundle.execution.checks[0]
        .observations
        .insert("valid_trials".to_owned(), 0);
    bundle.execution.checks[0]
        .observations
        .insert("invalid_trials".to_owned(), 1);
    bundle.execution.checks[0].completion = CheckCompletion::Counterexample;
    bundle.results[0].status = EvidenceStatus::Fail;

    let diagnostics = crate::artifact_verify_maelstrom::verify(&bundle, &root)?;
    assert!(diagnostics.is_empty());

    write(
        &root,
        "artifacts/invariants/evidence/trial-0/process.json",
        &process_log(&root, 1, "checker exited after reporting a counterexample")?,
    )?;
    let diagnostics = crate::artifact_verify_maelstrom::verify(&bundle, &root)?;
    assert_eq!(diagnostics.len(), 1);
    assert!(diagnostics[0].contains("counterexample alongside a harness error"));

    bundle.results[0].invariant_id = "LG-04".to_owned();
    assert!(crate::artifact_verify_maelstrom::verify(&bundle, &root).is_err());
    fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
fn nonzero_process_exit_is_always_a_harness_error() -> Result<(), Box<dyn std::error::Error>> {
    let root = temporary_root()?;
    write(&root, "scripts/maelstrom-lin-kv", "runner")?;
    prepare_state(&root)?;
    write(&root, "target/debug/rafter-maelstrom", "binary")?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/runner",
        "runner",
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/binary",
        "binary",
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/results.edn",
        VALID,
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/process.json",
        &process_log(&root, 1, "failed")?,
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/node.log",
        "role=leader",
    )?;
    let mut bundle = bundle();
    bundle.execution.checks[0].completion = CheckCompletion::HarnessError;
    bundle.results[0].status = EvidenceStatus::Error;

    crate::artifact_verify_maelstrom::verify(&bundle, &root)?;

    bundle.execution.checks[0].completion = CheckCompletion::CoverageNotReached;
    bundle.results[0].status = EvidenceStatus::Incomplete;
    assert!(crate::artifact_verify_maelstrom::verify(&bundle, &root).is_err());
    fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
fn serialized_counterexample_retains_secondary_harness_diagnostic(
) -> Result<(), Box<dyn std::error::Error>> {
    let root = temporary_root()?;
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    materialize_checkout(&root, &workspace)?;
    write(&root, "scripts/maelstrom-lin-kv", "runner")?;
    prepare_state(&root)?;
    write(&root, "target/debug/rafter-maelstrom", "binary")?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/runner",
        "runner",
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/binary",
        "binary",
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/maelstrom.jar",
        "jar",
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/results.edn",
        &VALID.replace(
            ":linearizable {:valid? true}",
            ":linearizable {:valid? false}",
        ),
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/process.json",
        &process_log(&root, 1, "checker exited after reporting a counterexample")?,
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/node.log",
        "role=leader",
    )?;
    write(&root, "artifacts/invariants/evidence/producer", "producer")?;
    initialize_fixture_repository(&root)?;

    let mut bundle = bundle();
    bind_serialized_bundle(&mut bundle, &root, &workspace)?;
    let result_path = root.with_extension("result.json");
    fs::write(&result_path, serde_json::to_vec_pretty(&bundle)?)?;

    let loaded = crate::aggregate::load_evidence_at(std::slice::from_ref(&result_path), &root);
    assert_eq!(loaded.bundles.len(), 1, "{:?}", loaded.harness_errors);
    assert_eq!(loaded.harness_errors.len(), 1);
    assert!(loaded.harness_errors[0].contains("counterexample alongside a harness error"));
    assert_eq!(
        loaded.bundles[0].results[0].classification,
        Some(FailureClassification::InvariantViolation)
    );

    let (catalog, manifest) = crate::tests::loaded();
    let report = crate::aggregate_with_harness_errors(
        &catalog,
        &manifest,
        "nightly",
        &bundle.source_ref,
        &loaded.bundles,
        &loaded.harness_errors,
    )?;
    assert_eq!(report.summary.total, 44);
    assert_eq!(report.summary.green, 0);
    assert!(report
        .invariants
        .iter()
        .all(|verdict| verdict.issues.iter().any(|issue| {
            issue.classification == FailureClassification::HarnessError
                && issue
                    .message
                    .contains("counterexample alongside a harness error")
        })));

    let _ = fs::remove_file(result_path);
    fs::remove_dir_all(root)?;
    Ok(())
}

fn materialize_checkout(root: &Path, workspace: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let tracked = Command::new("git")
        .args([
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
        ])
        .current_dir(workspace)
        .output()?;
    if !tracked.status.success() {
        return Err(format!(
            "git source inventory failed while materializing Maelstrom fixture: {}",
            String::from_utf8_lossy(&tracked.stderr)
        )
        .into());
    }
    for path in tracked.stdout.split(|byte| *byte == 0) {
        if path.is_empty() {
            continue;
        }
        let path = std::str::from_utf8(path)?;
        let destination = root.join(path);
        fs::create_dir_all(destination.parent().ok_or("plan input has no parent")?)?;
        fs::copy(workspace.join(path), destination)?;
    }
    write(root, ".gitignore", "/artifacts/invariants/\n/target/\n")?;
    Ok(())
}

fn initialize_fixture_repository(root: &Path) -> Result<(), Box<dyn std::error::Error>> {
    for arguments in [
        vec!["init", "-q"],
        vec!["add", "."],
        vec![
            "-c",
            "user.name=Rafter Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-q",
            "-m",
            "test: materialize Maelstrom evidence fixture",
        ],
    ] {
        let output = Command::new("git")
            .args(&arguments)
            .current_dir(root)
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "git {arguments:?} failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
    }
    Ok(())
}

fn bind_serialized_bundle(
    bundle: &mut ResultBundle,
    root: &Path,
    workspace: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    for (path, input) in [
        (
            "verification/raft-invariants.yaml",
            &mut bundle.execution.plan.registry,
        ),
        (
            "verification/raft-invariant-profiles.json",
            &mut bundle.execution.plan.manifest,
        ),
        (
            "verification/invariant-result-schema.json",
            &mut bundle.execution.plan.result_schema,
        ),
        (
            "verification/invariant-verdict-schema.json",
            &mut bundle.execution.plan.verdict_schema,
        ),
    ] {
        let bytes = fs::read(workspace.join(path))?;
        input.path = path.to_owned();
        input.sha256 = format!("{:x}", Sha256::digest(&bytes));
        input.size_bytes = bytes.len() as u64;
    }

    let mut source = crate::producer::source::capture_for_layer_at("tests", root)?;
    source.build_profile = "maelstrom-debug".to_owned();
    source.features.clear();
    source.tools = ["java", "maelstrom", "dot", "gnuplot"]
        .into_iter()
        .map(|name| {
            (
                name.to_owned(),
                ToolReceipt {
                    version: format!("{name} fixture"),
                    sha256: "0".repeat(64),
                },
            )
        })
        .collect();
    bundle.execution.source = source;

    let producer = bound_artifact(
        root,
        "artifacts/invariants/evidence/producer",
        "producer-binary",
    )?;
    bundle.execution.artifacts = vec![producer.clone()];
    bundle.execution.producer.executable = producer.clone();
    let repository = fs::canonicalize(root)?;
    bundle.execution.invocation.current_dir = repository.to_string_lossy().into_owned();
    bundle.execution.invocation.program_sha256 = producer.sha256.clone();
    bundle.execution.invocation.program =
        crate::producer_image::image_path(&repository, &producer.sha256)
            .to_string_lossy()
            .into_owned();
    bundle.execution.invocation.environment = crate::producer::process::base_environment();
    bundle.execution.invocation.environment_sha256 =
        crate::producer::process::digest_environment(&bundle.execution.invocation.environment);

    let check = &mut bundle.execution.checks[0];
    check.completion = CheckCompletion::Counterexample;
    check.observations.insert("valid_trials".to_owned(), 0);
    check.observations.insert("invalid_trials".to_owned(), 1);
    for artifact in &mut check.artifacts {
        *artifact = bound_artifact(root, &artifact.path, &artifact.kind)?;
    }
    let jar = check
        .artifacts
        .iter()
        .find(|artifact| artifact.kind == "maelstrom-tool-jar")
        .ok_or("Maelstrom fixture omitted jar")?;
    bundle
        .execution
        .plan
        .contract
        .runners
        .get_mut("maelstrom")
        .ok_or("Maelstrom fixture omitted runner contract")?
        .configuration
        .insert("maelstrom_jar_sha256".to_owned(), jar.sha256.clone());

    let replay = check
        .artifacts
        .iter()
        .find(|artifact| artifact.kind == "maelstrom-process-log")
        .ok_or("Maelstrom fixture omitted process log")?
        .clone();
    let result = &mut bundle.results[0];
    result.status = EvidenceStatus::Fail;
    result.classification = Some(FailureClassification::InvariantViolation);
    result.message = Some("Maelstrom reported a non-linearizable client history".to_owned());
    result.artifacts = vec![replay];
    Ok(())
}

fn bound_artifact(
    root: &Path,
    path: &str,
    kind: &str,
) -> Result<ArtifactRef, Box<dyn std::error::Error>> {
    let bytes = fs::read(root.join(path))?;
    Ok(ArtifactRef {
        kind: kind.to_owned(),
        path: path.to_owned(),
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        size_bytes: bytes.len() as u64,
    })
}

fn bundle() -> ResultBundle {
    let execution_id = "maelstrom-base".to_owned();
    ResultBundle {
        schema_version: crate::evidence::RESULT_SCHEMA_VERSION,
        runner: "maelstrom".to_owned(),
        profile: "nightly".to_owned(),
        source_ref: "abc".to_owned(),
        execution: ExecutionReceipt {
            plan: ExecutionPlanReceipt {
                schema_version: PLAN_SCHEMA_VERSION,
                profile: "nightly".to_owned(),
                registry: plan_input("verification/raft-invariants.yaml"),
                manifest: plan_input("verification/raft-invariant-profiles.json"),
                result_schema: plan_input("verification/invariant-result-schema.json"),
                verdict_schema: plan_input("verification/invariant-verdict-schema.json"),
                contract: maelstrom_contract(),
            },
            invocation: InvocationReceipt {
                program: format!(
                    "/workspace/rafter/target/rafter-invariants/producer-images/{}/rafter-invariants",
                    "0".repeat(64)
                ),
                program_sha256: "0".repeat(64),
                arguments: vec!["run".to_owned()],
                current_dir: "/workspace/rafter".to_owned(),
                environment: BTreeMap::new(),
                environment_sha256: crate::producer::process::digest_environment(&BTreeMap::new()),
            },
            producer: crate::ProducerBindingReceipt {
                binding: crate::producer_image::PRODUCER_BINDING.to_owned(),
                executable: artifact(
                    "artifacts/invariants/evidence/producer",
                    "producer-binary",
                ),
            },
            source: source(),
            checks: vec![maelstrom_check(&execution_id)],
            duration_ms: 1,
            peak_rss_kib: 1,
            artifacts: Vec::new(),
        },
        results: vec![EvidenceResult {
            invariant_id: "RD-06".to_owned(),
            evidence_id: "RD-06/test".to_owned(),
            execution_id,
            status: EvidenceStatus::Pass,
            classification: None,
            message: None,
            artifacts: Vec::new(),
        }],
    }
}

fn maelstrom_contract() -> ProfileContract {
    let configuration = BTreeMap::from([
        ("trials".to_owned(), "1".to_owned()),
        ("maelstrom_jar_sha256".to_owned(), "0".repeat(64)),
        ("duration_seconds".to_owned(), "5".to_owned()),
        ("rate".to_owned(), "10".to_owned()),
    ]);
    ProfileContract {
        description: "test".to_owned(),
        evidence_policy: "all_matching_registry_evidence".to_owned(),
        clause_policy: "all_required_clauses".to_owned(),
        required_clause_strength: "direct".to_owned(),
        required_layers: vec!["maelstrom".to_owned()],
        required_strengths: vec!["e2e".to_owned()],
        canonical_minimum_independent_layers: 2,
        runners: BTreeMap::from([(
            "maelstrom".to_owned(),
            RunnerContract {
                producer: "test".to_owned(),
                command: vec!["test".to_owned()],
                configuration,
                simulator_checks: BTreeMap::new(),
                minimum_observed_checks: 1,
                require_peak_rss: true,
            },
        )]),
    }
}

fn maelstrom_check(execution_id: &str) -> CheckReceipt {
    CheckReceipt {
        execution_id: execution_id.to_owned(),
        check_id: "maelstrom/base".to_owned(),
        evidence_ids: vec!["RD-06/test".to_owned()],
        completion: CheckCompletion::Completed,
        observations: observations(),
        simulator_liveness: None,
        duration_ms: 1,
        peak_rss_kib: 1,
        artifacts: [
            ("runner", "maelstrom-runner"),
            ("binary", "maelstrom-binary"),
            ("maelstrom.jar", "maelstrom-tool-jar"),
            ("results.edn", "maelstrom-results"),
            ("process.json", "maelstrom-process-log"),
            ("node.log", "maelstrom-node-log"),
        ]
        .into_iter()
        .map(|(path, kind)| {
            artifact(
                &format!("artifacts/invariants/evidence/trial-0/{path}"),
                kind,
            )
        })
        .collect(),
    }
}

fn process_log(
    root: &std::path::Path,
    exit_code: i32,
    stderr: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let state_dir =
        fs::canonicalize(root.join("target/rafter-invariants/maelstrom/abc/nightly/base/trial-0"))?;
    let durable = state_dir.join("durable");
    let environment = crate::producer::process::base_environment()
        .into_iter()
        .chain(BTreeMap::from([
            (
                "RAFTER_MAELSTROM_ROOT".to_owned(),
                durable.to_string_lossy().into_owned(),
            ),
            (
                "RAFTER_MAELSTROM_SCRIPT_DIR".to_owned(),
                fs::canonicalize(root.join("scripts"))?
                    .to_string_lossy()
                    .into_owned(),
            ),
            ("RAFTER_MAELSTROM_TIME_LIMIT".to_owned(), "5".to_owned()),
            ("RAFTER_MAELSTROM_RATE".to_owned(), "10".to_owned()),
            ("RAFTER_MAELSTROM_CONCURRENCY".to_owned(), "6".to_owned()),
        ]))
        .collect::<BTreeMap<_, _>>();
    let invocation = crate::producer::process::expected_invocation(
        fs::canonicalize(root.join("scripts/maelstrom-lin-kv"))?
            .to_str()
            .ok_or("script path is not UTF-8")?,
        &["--test-count".into(), "1".into()],
        &environment,
        &state_dir,
    )?;
    Ok(serde_json::json!({
        "schema_version": 2,
        "label": "base",
        "invocation": invocation,
        "exit_code": exit_code,
        "timed_out": false,
        "duration_ms": 1,
        "peak_rss_kib": 1,
        "stdout": "",
        "stderr": stderr,
    })
    .to_string())
}

fn prepare_state(root: &std::path::Path) -> Result<(), std::io::Error> {
    fs::create_dir_all(
        root.join("target/rafter-invariants/maelstrom/abc/nightly/base/trial-0/durable"),
    )
}

fn plan_input(path: &str) -> PlanInput {
    PlanInput {
        path: path.to_owned(),
        sha256: "0".repeat(64),
        size_bytes: 1,
    }
}

fn observations() -> BTreeMap<String, u64> {
    BTreeMap::from([
        ("trials".to_owned(), 1),
        ("valid_trials".to_owned(), 1),
        ("invalid_trials".to_owned(), 0),
        ("operation_count".to_owned(), 9),
        ("ok_count".to_owned(), 6),
        ("read_ok".to_owned(), 2),
        ("write_ok".to_owned(), 3),
        ("cas_ok".to_owned(), 1),
        ("membership_enter".to_owned(), 0),
        ("membership_leave".to_owned(), 0),
        ("membership_complete".to_owned(), 0),
        ("restarts".to_owned(), 0),
        ("post_restart_progress".to_owned(), 0),
        ("crashpoints".to_owned(), 0),
        ("post_crash_progress".to_owned(), 0),
        ("snapshots_compacted".to_owned(), 0),
        ("snapshots_applied".to_owned(), 0),
        ("post_restart_snapshots_applied".to_owned(), 0),
        ("lease_fast_path_read_ok".to_owned(), 0),
        ("lease_read_buffered".to_owned(), 0),
        ("lease_expired_while_leader".to_owned(), 0),
        ("lease_post_expiry_released".to_owned(), 0),
        ("lease_post_expiry_handler".to_owned(), 0),
        ("lease_post_expiry_unavailable".to_owned(), 0),
        ("lease_post_expiry_read_served".to_owned(), 0),
        ("lease_post_expiry_renewed".to_owned(), 0),
        ("lease_post_expiry_unexpected_error".to_owned(), 0),
        ("lease_duplicate_terminal".to_owned(), 0),
        ("lease_coverage_lost".to_owned(), 0),
        ("lease_history_probe_matches".to_owned(), 0),
        ("lease_history_probe_mismatches".to_owned(), 0),
        ("lease_sequence_complete".to_owned(), 0),
        ("lease_sequence_invalid".to_owned(), 0),
    ])
}

fn artifact(path: &str, kind: &str) -> ArtifactRef {
    ArtifactRef {
        kind: kind.to_owned(),
        path: path.to_owned(),
        sha256: if kind == "maelstrom-runner" {
            format!("{:x}", Sha256::digest(b"runner"))
        } else {
            "0".repeat(64)
        },
        size_bytes: 1,
    }
}

fn source() -> SourceReceipt {
    SourceReceipt {
        commit: "abc".to_owned(),
        tree: "tree".to_owned(),
        materialization: SourceMaterializationReceipt {
            contract: "git-head-worktree-raw-v1".to_owned(),
            sha256: "0".repeat(64),
            tracked_entries: 1,
            submodules: 0,
        },
        cargo_lock_sha256: "0".repeat(64),
        cargo: "cargo".to_owned(),
        cargo_sha256: "0".repeat(64),
        cargo_config_sha256: "0".repeat(64),
        rustc: "rustc".to_owned(),
        rustc_sha256: "0".repeat(64),
        target: "target".to_owned(),
        build_profile: "test".to_owned(),
        features: Vec::new(),
        tools: BTreeMap::new(),
        environment_sha256: crate::producer::process::digest_environment(
            &crate::producer::process::base_environment(),
        ),
        clean: true,
    }
}

fn temporary_root() -> Result<PathBuf, std::io::Error> {
    let root = Path::new("target/rafter-invariants/tests").join(format!(
        "maelstrom-artifact-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    if root.exists() {
        fs::remove_dir_all(&root)?;
    }
    fs::create_dir_all(&root)?;
    Ok(root)
}

fn write(root: &std::path::Path, path: &str, source: &str) -> Result<(), std::io::Error> {
    let path = root.join(path);
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "artifact has no parent")
    })?;
    fs::create_dir_all(parent)?;
    fs::write(path, source)
}
