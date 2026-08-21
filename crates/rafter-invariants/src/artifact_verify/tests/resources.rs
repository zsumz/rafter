use super::{
    validate_simulator_schedule, verify_liveness_observations, verify_producer_invocation_paths,
    verify_resource_metrics, verify_simulator_observations, EVENT_PREFIX,
};
use crate::contract::profile::scheduled_simulator_seeds;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fmt::Write as _};

#[test]
fn producer_paths_bind_to_the_recorded_checkout_and_preserved_binary() {
    let root = scratch("producer-paths");
    std::fs::create_dir_all(&root).expect("create aggregate checkout B");
    let root = std::fs::canonicalize(root).expect("canonical aggregate checkout B");
    let bytes = b"preserved producer";
    let digest = format!("{:x}", Sha256::digest(bytes));
    let producer_root = root.with_extension("producer-root-a");
    assert!(!producer_root.exists());
    let (catalog, manifest) = crate::tests::loaded();
    let mut bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "tests")
        .expect("tests bundle");
    bundle.execution.invocation.program =
        crate::provenance::image::image_path(&producer_root, &digest)
            .to_string_lossy()
            .into_owned();
    bundle.execution.invocation.current_dir = producer_root.to_string_lossy().into_owned();
    bundle
        .execution
        .invocation
        .program_sha256
        .clone_from(&digest);
    let producer = crate::ArtifactRef {
        kind: "producer-binary".to_owned(),
        path: "preserved-producer".to_owned(),
        sha256: digest.clone(),
        size_bytes: bytes.len() as u64,
    };
    bundle.execution.producer = crate::ProducerBindingReceipt {
        binding: crate::provenance::image::PRODUCER_BINDING.to_owned(),
        executable: producer.clone(),
    };
    bundle.execution.artifacts = vec![producer];

    verify_producer_invocation_paths(&bundle, &root)
        .expect("nonexistent producer checkout remains verifiable from checkout B");

    let mut forged = bundle.clone();
    forged.execution.artifacts[0].sha256 = "f".repeat(64);
    assert!(verify_producer_invocation_paths(&forged, &root).is_err());
    forged = bundle.clone();
    forged.execution.invocation.program_sha256 = "f".repeat(64);
    assert!(verify_producer_invocation_paths(&forged, &root).is_err());
    forged = bundle.clone();
    forged.execution.invocation.current_dir = "producer-root-a".to_owned();
    assert!(verify_producer_invocation_paths(&forged, &root).is_err());
    forged = bundle.clone();
    let current_root = producer_root.join(".");
    forged.execution.invocation.current_dir = current_root.to_string_lossy().into_owned();
    forged.execution.invocation.program =
        crate::provenance::image::image_path(&current_root, &digest)
            .to_string_lossy()
            .into_owned();
    assert!(verify_producer_invocation_paths(&forged, &root).is_err());
    forged = bundle.clone();
    let parent_root = producer_root.join("nested/..");
    forged.execution.invocation.current_dir = parent_root.to_string_lossy().into_owned();
    forged.execution.invocation.program =
        crate::provenance::image::image_path(&parent_root, &digest)
            .to_string_lossy()
            .into_owned();
    assert!(verify_producer_invocation_paths(&forged, &root).is_err());
    forged = bundle.clone();
    forged.execution.invocation.current_dir = root.to_string_lossy().into_owned();
    assert!(verify_producer_invocation_paths(&forged, &root).is_err());
    forged = bundle.clone();
    forged.execution.invocation.program = crate::provenance::image::image_path(&root, &digest)
        .to_string_lossy()
        .into_owned();
    assert!(verify_producer_invocation_paths(&forged, &root).is_err());
    forged = bundle.clone();
    forged.execution.invocation.program = "/absolute/substituted/rafter-invariants".to_owned();
    assert!(verify_producer_invocation_paths(&forged, &root).is_err());
    forged = bundle.clone();
    forged.execution.invocation.program = producer_root
        .join("target/rafter-invariants/producer-images")
        .join(&digest)
        .join("nested/../rafter-invariants")
        .to_string_lossy()
        .into_owned();
    assert!(verify_producer_invocation_paths(&forged, &root).is_err());
    forged = bundle.clone();
    forged.execution.producer.binding = "mutable-path-v0".to_owned();
    assert!(verify_producer_invocation_paths(&forged, &root).is_err());
    forged = bundle.clone();
    forged.execution.producer.executable.sha256 = "f".repeat(64);
    assert!(verify_producer_invocation_paths(&forged, &root).is_err());
    forged = bundle.clone();
    forged.execution.artifacts.clear();
    assert!(verify_producer_invocation_paths(&forged, &root).is_err());
    forged = bundle;
    forged
        .execution
        .artifacts
        .push(forged.execution.producer.executable.clone());
    assert!(verify_producer_invocation_paths(&forged, &root).is_err());
    std::fs::remove_dir_all(root).expect("remove scratch root");
}

#[test]
fn process_logs_bind_check_and_execution_resource_metrics() {
    let root = scratch("resource-metrics");
    std::fs::create_dir_all(&root).expect("create scratch root");
    let relative = "process.log";
    let source = concat!(
        "schema_version: 4\n",
        "label: exact\n",
        "invocation: {\"program\":\"/bin/test\",\"program_sha256\":\"0000000000000000000000000000000000000000000000000000000000000000\",\"arguments\":[\"test\"],\"current_dir\":\"/workspace\",\"environment\":{},\"environment_sha256\":\"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\",\"launchers\":[]}\n",
        "exit_code: Some(0)\n",
        "timed_out: false\n",
        "duration_ms: 7\n",
        "peak_rss_kib: 13\n",
        "stdout_bytes: 2\n",
        "stderr_bytes: 0\n\n",
        "ok",
    );
    std::fs::write(root.join(relative), source).expect("write process log");
    let artifact = crate::ArtifactRef {
        kind: "test-log".to_owned(),
        path: relative.to_owned(),
        sha256: format!("{:x}", Sha256::digest(source)),
        size_bytes: source.len() as u64,
    };
    let (catalog, manifest) = crate::tests::loaded();
    let mut bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "tests")
        .expect("tests bundle");
    bundle.execution.checks.truncate(1);
    bundle.execution.checks[0].artifacts = vec![artifact.clone()];
    bundle.execution.checks[0].duration_ms = 7;
    bundle.execution.checks[0].peak_rss_kib = 13;
    bundle.execution.artifacts = vec![artifact];
    bundle.execution.duration_ms = 7;
    bundle.execution.peak_rss_kib = 13;

    verify_resource_metrics(&bundle, &root).expect("log-derived metrics verify");
    let mut forged = bundle.clone();
    forged.execution.checks[0].duration_ms = u64::MAX;
    assert!(verify_resource_metrics(&forged, &root).is_err());
    forged = bundle;
    forged.execution.peak_rss_kib = 1;
    assert!(verify_resource_metrics(&forged, &root).is_err());
    std::fs::remove_dir_all(root).expect("remove scratch root");
}

#[test]
fn simulator_check_metrics_exclude_compile_resources() {
    let root = scratch("simulator-resource-metrics");
    std::fs::create_dir_all(&root).expect("create scratch root");
    let process_log = |label: &str, duration_ms: u64, peak_rss_kib: u64| {
        format!(
            concat!(
                "schema_version: 4\n",
                "label: {label}\n",
                "invocation: {{\"program\":\"/bin/test\",\"program_sha256\":\"0000000000000000000000000000000000000000000000000000000000000000\",\"arguments\":[\"test\"],\"current_dir\":\"/workspace\",\"environment\":{{}},\"environment_sha256\":\"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\",\"launchers\":[]}}\n",
                "exit_code: Some(0)\n",
                "timed_out: false\n",
                "duration_ms: {duration_ms}\n",
                "peak_rss_kib: {peak_rss_kib}\n",
                "stdout_bytes: 2\n",
                "stderr_bytes: 0\n\n",
                "ok",
            ),
            label = label,
            duration_ms = duration_ms,
            peak_rss_kib = peak_rss_kib,
        )
    };
    let artifact = |kind: &str, relative: &str, source: &str| {
        std::fs::write(root.join(relative), source).expect("write process log");
        crate::ArtifactRef {
            kind: kind.to_owned(),
            path: relative.to_owned(),
            sha256: format!("{:x}", Sha256::digest(source)),
            size_bytes: source.len() as u64,
        }
    };
    let compile = process_log("compile", 5, 100);
    let runtime = process_log("runtime", 7, 13);
    let compile = artifact("compile-log", "simulator-compile.log", &compile);
    let runtime = artifact("simulator-log", "simulator-runtime.log", &runtime);
    let (catalog, manifest) = crate::tests::loaded();
    let mut bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "simulator")
        .expect("simulator bundle");
    bundle.execution.checks.truncate(1);
    bundle.execution.checks[0].artifacts = vec![compile.clone(), runtime.clone()];
    bundle.execution.checks[0].duration_ms = 7;
    bundle.execution.checks[0].peak_rss_kib = 13;
    bundle.execution.artifacts = vec![compile, runtime];
    bundle.execution.duration_ms = 12;
    bundle.execution.peak_rss_kib = 100;

    verify_resource_metrics(&bundle, &root).expect("compile and runtime metrics stay distinct");
    std::fs::remove_dir_all(root).expect("remove scratch root");
}

#[test]
fn tla_execution_metrics_include_the_mutation_process_log() {
    let root = scratch("tla-resource-metrics");
    std::fs::create_dir_all(&root).expect("create scratch root");
    let main = write_tla_process_log(&root, "tla-process.json", "tla-log", 7, 13);
    let mutation = write_tla_process_log(
        &root,
        "tla-mutation-process.json",
        "tla-mutation-log",
        5,
        29,
    );
    let (catalog, manifest) = crate::tests::loaded();
    let mut bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "tla")
        .expect("TLA bundle");
    bundle.execution.checks.truncate(1);
    bundle.execution.checks[0].artifacts = vec![main.clone(), mutation.clone()];
    bundle.execution.checks[0].duration_ms = 12;
    bundle.execution.checks[0].peak_rss_kib = 29;
    bundle.execution.artifacts = vec![main, mutation];
    bundle.execution.duration_ms = 12;
    bundle.execution.peak_rss_kib = 29;

    verify_resource_metrics(&bundle, &root).expect("exact TLA metrics verify");

    let mut duration_tampered = bundle.clone();
    let mutation = write_tla_process_log(
        &root,
        "tla-mutation-process.json",
        "tla-mutation-log",
        6,
        29,
    );
    duration_tampered.execution.checks[0].artifacts[1] = mutation.clone();
    duration_tampered.execution.artifacts[1] = mutation;
    assert!(verify_resource_metrics(&duration_tampered, &root).is_err());

    let mut peak_tampered = bundle.clone();
    let mutation = write_tla_process_log(
        &root,
        "tla-mutation-process.json",
        "tla-mutation-log",
        5,
        31,
    );
    peak_tampered.execution.checks[0].artifacts[1] = mutation.clone();
    peak_tampered.execution.artifacts[1] = mutation;
    assert!(verify_resource_metrics(&peak_tampered, &root).is_err());

    let mut omitted = bundle;
    omitted.execution.checks[0].artifacts.pop();
    omitted.execution.artifacts.pop();
    assert!(verify_resource_metrics(&omitted, &root).is_err());
    std::fs::remove_dir_all(root).expect("remove scratch root");
}

fn write_tla_process_log(
    root: &std::path::Path,
    relative: &str,
    kind: &str,
    duration_ms: u64,
    peak_rss_kib: u64,
) -> crate::ArtifactRef {
    let source = serde_json::to_vec(&json!({
        "schema_version": 4,
        "label": "model-check",
        "invocation": {
            "program": "/bin/java",
            "program_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
            "arguments": ["java", "tlc2.TLC"],
            "current_dir": "/workspace",
            "environment": {},
            "environment_sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "launchers": []
        },
        "exit_code": 0,
        "timed_out": false,
        "termination": {
            "process_group": true,
            "term_signal_sent": false,
            "grace_ms": 30000,
            "kill_signal_sent": false
        },
        "duration_ms": duration_ms,
        "peak_rss_kib": peak_rss_kib,
        "stdout": "ok",
        "stderr": ""
    }))
    .expect("serialize process log");
    std::fs::write(root.join(relative), &source).expect("write process log");
    crate::ArtifactRef {
        kind: kind.to_owned(),
        path: relative.to_owned(),
        sha256: format!("{:x}", Sha256::digest(&source)),
        size_bytes: source.len() as u64,
    }
}

fn scratch(label: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("target/rafter-invariants/tests")
        .join(format!("{label}-{}", std::process::id()))
}
