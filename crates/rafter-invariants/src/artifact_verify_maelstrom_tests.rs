use std::{collections::BTreeMap, fs, path::PathBuf};

use crate::{
    ArtifactRef, CheckCompletion, CheckReceipt, EvidenceResult, EvidenceStatus, ExecutionReceipt,
    ResultBundle, SourceReceipt,
};

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
    write(&root, "target/debug/rafter-maelstrom", "binary")?;
    write(&root, "evidence/trial-0/runner", "runner")?;
    write(&root, "evidence/trial-0/binary", "binary")?;
    write(&root, "evidence/trial-0/results.edn", VALID)?;
    write(
        &root,
        "evidence/trial-0/process.json",
        r#"{"schema_version":1,"label":"base","exit_code":0,"timed_out":false,"duration_ms":1,"peak_rss_kib":1,"stdout":"","stderr":""}"#,
    )?;
    write(&root, "evidence/trial-0/node.log", "role=leader")?;
    let bundle = bundle();

    crate::artifact_verify_maelstrom::verify(&bundle, &root)?;

    let mut forged = bundle.clone();
    forged.execution.checks[0]
        .observations
        .insert("read_ok".to_owned(), 99);
    assert!(crate::artifact_verify_maelstrom::verify(&forged, &root)
        .expect_err("forged observation is rejected")
        .to_string()
        .contains("observations disagree"));

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
    write(&root, "target/debug/rafter-maelstrom", "binary")?;
    write(&root, "evidence/trial-0/runner", "runner")?;
    write(&root, "evidence/trial-0/binary", "binary")?;
    write(
        &root,
        "evidence/trial-0/results.edn",
        &VALID.replace(
            ":linearizable {:valid? true}",
            ":linearizable {:valid? false}",
        ),
    )?;
    write(
        &root,
        "evidence/trial-0/process.json",
        r#"{"schema_version":1,"label":"base","exit_code":0,"timed_out":false,"duration_ms":1,"peak_rss_kib":1,"stdout":"","stderr":""}"#,
    )?;
    write(&root, "evidence/trial-0/node.log", "role=leader")?;
    let mut bundle = bundle();
    bundle.execution.checks[0]
        .observations
        .insert("valid_trials".to_owned(), 0);
    bundle.execution.checks[0].completion = CheckCompletion::Counterexample;
    bundle.results[0].status = EvidenceStatus::Fail;

    crate::artifact_verify_maelstrom::verify(&bundle, &root)?;
    bundle.results[0].invariant_id = "LG-04".to_owned();
    assert!(crate::artifact_verify_maelstrom::verify(&bundle, &root).is_err());
    fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
fn nonzero_process_exit_is_always_a_harness_error() -> Result<(), Box<dyn std::error::Error>> {
    let root = temporary_root()?;
    write(&root, "scripts/maelstrom-lin-kv", "runner")?;
    write(&root, "target/debug/rafter-maelstrom", "binary")?;
    write(&root, "evidence/trial-0/runner", "runner")?;
    write(&root, "evidence/trial-0/binary", "binary")?;
    write(&root, "evidence/trial-0/results.edn", VALID)?;
    write(
        &root,
        "evidence/trial-0/process.json",
        r#"{"schema_version":1,"label":"base","exit_code":1,"timed_out":false,"duration_ms":1,"peak_rss_kib":1,"stdout":"","stderr":"failed"}"#,
    )?;
    write(&root, "evidence/trial-0/node.log", "role=leader")?;
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

fn bundle() -> ResultBundle {
    let execution_id = "maelstrom-base".to_owned();
    ResultBundle {
        schema_version: 6,
        runner: "maelstrom".to_owned(),
        profile: "nightly".to_owned(),
        source_ref: "abc".to_owned(),
        execution: ExecutionReceipt {
            producer: "test".to_owned(),
            command: Vec::new(),
            configuration: BTreeMap::from([
                ("trials".to_owned(), "1".to_owned()),
                ("maelstrom_jar_sha256".to_owned(), "0".repeat(64)),
            ]),
            source: source(),
            checks: vec![CheckReceipt {
                execution_id: execution_id.clone(),
                check_id: "maelstrom/base".to_owned(),
                evidence_ids: vec!["RD-06/test".to_owned()],
                completion: CheckCompletion::Completed,
                observations: observations(),
                duration_ms: 1,
                peak_rss_kib: 1,
                artifacts: vec![
                    artifact("evidence/trial-0/runner", "maelstrom-runner"),
                    artifact("evidence/trial-0/binary", "maelstrom-binary"),
                    artifact("evidence/trial-0/maelstrom.jar", "maelstrom-tool-jar"),
                    artifact("evidence/trial-0/results.edn", "maelstrom-results"),
                    artifact("evidence/trial-0/process.json", "maelstrom-process-log"),
                    artifact("evidence/trial-0/node.log", "maelstrom-node-log"),
                ],
            }],
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

fn observations() -> BTreeMap<String, u64> {
    BTreeMap::from([
        ("trials".to_owned(), 1),
        ("valid_trials".to_owned(), 1),
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
    ])
}

fn artifact(path: &str, kind: &str) -> ArtifactRef {
    ArtifactRef {
        kind: kind.to_owned(),
        path: path.to_owned(),
        sha256: "0".repeat(64),
        size_bytes: 1,
    }
}

fn source() -> SourceReceipt {
    SourceReceipt {
        commit: "abc".to_owned(),
        tree: "tree".to_owned(),
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
        environment_sha256: "0".repeat(64),
        clean: true,
    }
}

fn temporary_root() -> Result<PathBuf, std::io::Error> {
    let root = std::env::temp_dir().join(format!(
        "rafter-maelstrom-artifact-test-{}-{:?}",
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
