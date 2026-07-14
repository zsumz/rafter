use super::{
    validate_simulator_schedule, verify_producer_invocation_paths, verify_resource_metrics,
    verify_simulator_observations, EVENT_PREFIX,
};
use crate::producer::expected_scheduled_seeds;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fmt::Write as _};

#[test]
fn producer_paths_bind_to_the_checkout_and_preserved_binary() {
    let root = scratch("producer-paths");
    let program = root.join("target/debug/rafter-invariants");
    std::fs::create_dir_all(program.parent().expect("program parent")).expect("create program dir");
    let bytes = b"preserved producer";
    std::fs::write(&program, bytes).expect("write producer");
    let digest = format!("{:x}", Sha256::digest(bytes));
    let (catalog, manifest) = crate::tests::loaded();
    let mut bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "tests")
        .expect("tests bundle");
    bundle.execution.invocation.program = std::fs::canonicalize(&program)
        .expect("canonical program")
        .to_string_lossy()
        .into_owned();
    bundle.execution.invocation.current_dir = std::fs::canonicalize(&root)
        .expect("canonical root")
        .to_string_lossy()
        .into_owned();
    bundle
        .execution
        .invocation
        .program_sha256
        .clone_from(&digest);
    bundle.execution.artifacts = vec![crate::ArtifactRef {
        kind: "producer-binary".to_owned(),
        path: "preserved-producer".to_owned(),
        sha256: digest,
        size_bytes: bytes.len() as u64,
    }];

    verify_producer_invocation_paths(&bundle, &root).expect("bound paths verify");
    let mut forged = bundle.clone();
    forged.execution.invocation.current_dir = std::env::temp_dir().to_string_lossy().into_owned();
    assert!(verify_producer_invocation_paths(&forged, &root).is_err());
    forged = bundle;
    forged.execution.invocation.program = root.join("fabricated").to_string_lossy().into_owned();
    assert!(verify_producer_invocation_paths(&forged, &root).is_err());
    std::fs::remove_dir_all(root).expect("remove scratch root");
}

#[test]
fn process_logs_bind_check_and_execution_resource_metrics() {
    let root = scratch("resource-metrics");
    std::fs::create_dir_all(&root).expect("create scratch root");
    let relative = "process.log";
    let source = concat!(
        "schema_version: 3\n",
        "label: exact\n",
        "invocation: {\"program\":\"/bin/test\",\"program_sha256\":\"0000000000000000000000000000000000000000000000000000000000000000\",\"arguments\":[\"test\"],\"current_dir\":\"/workspace\",\"environment\":{},\"environment_sha256\":\"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\"}\n",
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
                "schema_version: 3\n",
                "label: {label}\n",
                "invocation: {{\"program\":\"/bin/test\",\"program_sha256\":\"0000000000000000000000000000000000000000000000000000000000000000\",\"arguments\":[\"test\"],\"current_dir\":\"/workspace\",\"environment\":{{}},\"environment_sha256\":\"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\"}}\n",
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
fn tla_execution_duration_must_exactly_match_process_logs() {
    let root = scratch("tla-resource-metrics");
    std::fs::create_dir_all(&root).expect("create scratch root");
    let relative = "tla-process.json";
    let source = serde_json::to_vec(&json!({
        "schema_version": 1,
        "label": "model-check",
        "invocation": {
            "program": "/bin/java",
            "program_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
            "arguments": ["java", "tlc2.TLC"],
            "current_dir": "/workspace",
            "environment": {},
            "environment_sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        },
        "exit_code": 0,
        "timed_out": false,
        "duration_ms": 7,
        "peak_rss_kib": 13,
        "stdout": "ok",
        "stderr": ""
    }))
    .expect("serialize process log");
    std::fs::write(root.join(relative), &source).expect("write process log");
    let artifact = crate::ArtifactRef {
        kind: "tla-log".to_owned(),
        path: relative.to_owned(),
        sha256: format!("{:x}", Sha256::digest(&source)),
        size_bytes: source.len() as u64,
    };
    let (catalog, manifest) = crate::tests::loaded();
    let mut bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "tla")
        .expect("TLA bundle");
    bundle.execution.checks.truncate(1);
    bundle.execution.checks[0].artifacts = vec![artifact.clone()];
    bundle.execution.checks[0].duration_ms = 7;
    bundle.execution.checks[0].peak_rss_kib = 13;
    bundle.execution.artifacts = vec![artifact];
    bundle.execution.duration_ms = 7;
    bundle.execution.peak_rss_kib = 13;

    verify_resource_metrics(&bundle, &root).expect("exact TLA metrics verify");
    bundle.execution.duration_ms = 8;
    assert!(verify_resource_metrics(&bundle, &root).is_err());
    std::fs::remove_dir_all(root).expect("remove scratch root");
}

fn scratch(label: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("target/rafter-invariants/tests")
        .join(format!("{label}-{}", std::process::id()))
}

#[test]
fn scheduled_simulator_log_proves_exact_source_derived_plan() {
    let source_ref = "abc123";
    let log = scheduled_log(source_ref);
    let configuration = simulator_configuration("nightly");

    assert!(validate_simulator_schedule(
        "nightly",
        source_ref,
        &configuration,
        std::slice::from_ref(&log),
    )
    .is_ok());
    assert!(validate_simulator_schedule(
        "nightly",
        "different",
        &configuration,
        std::slice::from_ref(&log),
    )
    .is_err());
    assert!(validate_simulator_schedule("nightly", source_ref, &configuration, &[]).is_err());

    let mut changed = configuration.clone();
    changed.soak_steps += 1;
    assert!(validate_simulator_schedule(
        "nightly",
        source_ref,
        &changed,
        std::slice::from_ref(&log),
    )
    .is_err());
    changed = configuration.clone();
    changed.seed_count = Some(7);
    assert!(validate_simulator_schedule(
        "nightly",
        source_ref,
        &changed,
        std::slice::from_ref(&log),
    )
    .is_err());
    changed = configuration;
    changed.state_floors = crate::catalog::SimulatorStateFloors::Aggregate {
        protocol: 100_000_001,
        verifier: 100_000_001,
    };
    assert!(validate_simulator_schedule("nightly", source_ref, &changed, &[log]).is_err());
}

#[test]
fn scheduled_simulator_rejects_fabricated_totals_and_executed_seeds() {
    let source_ref = "abc123";
    let log = scheduled_log(source_ref);
    let configuration = simulator_configuration("nightly");
    let fabricated_total = log.replace(
        "\"unique_protocol_states\":40000000",
        "\"unique_protocol_states\":4",
    );
    assert!(validate_simulator_schedule(
        "nightly",
        source_ref,
        &configuration,
        &[fabricated_total],
    )
    .is_err());

    let wrong_seed = log.replacen("\"seed\":", "\"seed\":999,\"ignored_seed\":", 1);
    assert!(
        validate_simulator_schedule("nightly", source_ref, &configuration, &[wrong_seed]).is_err()
    );

    let wrong_steps = log.replace("\"steps\":1024", "\"steps\":1023");
    assert!(
        validate_simulator_schedule("nightly", source_ref, &configuration, &[wrong_steps]).is_err()
    );
}

#[test]
fn scheduled_seed_banner_uses_simulator_canonical_hex() {
    let seeds = expected_scheduled_seeds("weekly", "abc123").expect("weekly seeds");
    assert!(seeds.contains("0xe00e6256b8bdd15"));
    assert!(!seeds.contains("0x0e00e6256b8bdd15"));
}

#[test]
fn pr_simulator_log_proves_exact_curated_seed_inventory() {
    let logs = pr_logs();
    let configuration = simulator_configuration("pr");
    assert!(validate_simulator_schedule("pr", "unused", &configuration, &logs).is_ok());

    let mut substituted = logs.clone();
    substituted[1] = substituted[1].replacen("\"seed\":37123", "\"seed\":999", 1);
    assert!(validate_simulator_schedule("pr", "unused", &configuration, &substituted).is_err());

    let mut extra = logs.clone();
    write!(
        extra[1],
        "\n{EVENT_PREFIX}{}",
        serde_json::to_string(&json!({
            "event": "soak-check",
            "check_id": "raft-soak",
            "status": "pass",
            "seed": 0x9107_u64,
            "steps": 320,
        }))
        .expect("serialize extra event")
    )
    .expect("append extra event");
    assert!(validate_simulator_schedule("pr", "unused", &configuration, &extra).is_err());

    let mut unknown = logs;
    write!(
        unknown[0],
        "\n{EVENT_PREFIX}{}",
        serde_json::to_string(&json!({
            "event": "soak-check",
            "check_id": "unreviewed-soak",
            "status": "pass",
            "seed": 0x9103_u64,
            "steps": 320,
        }))
        .expect("serialize unknown event")
    )
    .expect("append unknown event");
    assert!(validate_simulator_schedule("pr", "unused", &configuration, &unknown).is_err());
}

fn pr_logs() -> Vec<String> {
    let mut soak = vec![
        "label: raft-soak".to_owned(),
        "exit_code: Some(0)".to_owned(),
        "model-check raft-soak seeds source=curated seeds=0x9103,0x9104,0x9105,0x9106".to_owned(),
    ];
    for check_id in ["raft-soak", "raft-soak-lease", "raft-soak-membership"] {
        for seed in 0x9103_u64..=0x9106 {
            soak.push(event(&json!({
                "event": "soak-check",
                "check_id": check_id,
                "status": "pass",
                "seed": seed,
                "steps": 320,
            })));
        }
    }
    vec![
        "label: fast\nexit_code: Some(0)".to_owned(),
        soak.join("\n"),
    ]
}

#[test]
fn raw_reports_reject_coordinated_receipt_binding_tampering() {
    let (catalog, manifest) = crate::tests::loaded();
    let descriptor = catalog
        .evidence
        .iter()
        .find(|descriptor| {
            descriptor.simulator.as_ref().is_some_and(|identity| {
                identity.liveness_report.as_ref().is_some_and(|contract| {
                    contract.feature_id == "proposal-progress" && descriptor.clause_id == "LV-02.a"
                })
            })
        })
        .expect("proposal progress descriptor");
    let mut bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "simulator")
        .expect("simulator bundle");
    let check_index = bundle
        .execution
        .checks
        .iter()
        .position(|check| check.evidence_ids == [descriptor.evidence_id()])
        .expect("proposal progress check");
    let (fixture_identity, contracts, events) = crate::catalog::liveness_report_tests::fixture();
    let binding =
        crate::catalog::derive_liveness_binding("pr", &fixture_identity, &contracts, &events)
            .expect("valid raw reports bind");
    let check = &mut bundle.execution.checks[check_index];
    check.observations = BTreeMap::from([
        ("runs:raft-soak".to_owned(), 1),
        ("passes:raft-soak".to_owned(), 1),
        ("steps:raft-soak".to_owned(), 320),
        (
            fixture_identity.required_observation.clone(),
            binding.reports.len() as u64,
        ),
    ]);
    check.simulator_liveness = Some(binding);
    verify_simulator_observations(
        &bundle,
        &bundle.execution.checks[check_index],
        &fixture_identity,
        &contracts,
        &events,
    )
    .expect("raw report binding verifies");

    let mut tampered = bundle.execution.checks[check_index].clone();
    let binding = tampered
        .simulator_liveness
        .as_mut()
        .expect("liveness binding remains");
    binding.reports[0].report_sha256 = "f".repeat(64);
    binding.reports_sha256 = crate::catalog::liveness_reports_digest(&binding.reports);
    let error =
        verify_simulator_observations(&bundle, &tampered, &fixture_identity, &contracts, &events)
            .expect_err("coordinated binding tamper must fail");
    assert!(error.to_string().contains("disagrees with raw logs"));
}

#[test]
fn raw_reports_reject_coordinated_execution_contract_tampering() {
    let (catalog, manifest) = crate::tests::loaded();
    let descriptor = proposal_progress_descriptor(&catalog);
    let mut bundle = simulator_bundle(&catalog, &manifest);
    let check_index = proposal_progress_check_index(&bundle, descriptor);
    let (identity, contracts, mut events) = crate::catalog::liveness_report_tests::fixture();
    let binding = crate::catalog::derive_liveness_binding("pr", &identity, &contracts, &events)
        .expect("valid raw reports bind");
    prepare_fixture_check(
        &mut bundle.execution.checks[check_index],
        &identity,
        binding,
    );

    events.get_mut("raft-soak").expect("soak event")[0]["execution_contract"]["max_proposals"] =
        json!(25);
    let receipt = bundle.execution.checks[check_index]
        .simulator_liveness
        .as_mut()
        .expect("liveness binding");
    for report in &mut receipt.reports {
        report.execution_contract.max_proposals = 25;
        report.execution_contract_sha256 =
            crate::catalog::execution_contract_digest(&report.execution_contract);
    }
    receipt.reports_sha256 = crate::catalog::liveness_reports_digest(&receipt.reports);

    let error = verify_simulator_observations(
        &bundle,
        &bundle.execution.checks[check_index],
        &identity,
        &contracts,
        &events,
    )
    .expect_err("coordinated execution-contract tamper must fail");
    assert!(error.to_string().contains("execution contract"));
}

#[test]
fn raw_reports_reject_complete_report_set_substitution() {
    let (catalog, manifest) = crate::tests::loaded();
    let descriptor = proposal_progress_descriptor(&catalog);
    let mut bundle = simulator_bundle(&catalog, &manifest);
    let check_index = proposal_progress_check_index(&bundle, descriptor);
    let (identity, contracts, mut events) = crate::catalog::liveness_report_tests::fixture();
    let binding = crate::catalog::derive_liveness_binding("pr", &identity, &contracts, &events)
        .expect("valid raw reports bind");
    prepare_fixture_check(
        &mut bundle.execution.checks[check_index],
        &identity,
        binding,
    );

    let reports = events.get_mut("raft-soak").expect("soak event")[0]["liveness_reports"]
        .as_array_mut()
        .expect("liveness report array");
    reports
        .iter_mut()
        .find(|report| report["feature_id"] == "snapshot-catch-up")
        .expect("snapshot report")["feature_id"] = json!("invented-feature");

    let error = verify_simulator_observations(
        &bundle,
        &bundle.execution.checks[check_index],
        &identity,
        &contracts,
        &events,
    )
    .expect_err("complete report-set substitution must fail");
    assert!(error.to_string().contains("unknown"));
}

fn proposal_progress_descriptor(catalog: &crate::Catalog) -> &crate::EvidenceDescriptor {
    catalog
        .evidence
        .iter()
        .find(|descriptor| {
            descriptor.simulator.as_ref().is_some_and(|identity| {
                identity.liveness_report.as_ref().is_some_and(|contract| {
                    contract.feature_id == "proposal-progress" && descriptor.clause_id == "LV-02.a"
                })
            })
        })
        .expect("proposal progress descriptor")
}

fn simulator_bundle(
    catalog: &crate::Catalog,
    manifest: &crate::ProfileManifest,
) -> crate::ResultBundle {
    crate::tests::passing_bundles(catalog, manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "simulator")
        .expect("simulator bundle")
}

fn proposal_progress_check_index(
    bundle: &crate::ResultBundle,
    descriptor: &crate::EvidenceDescriptor,
) -> usize {
    bundle
        .execution
        .checks
        .iter()
        .position(|check| check.evidence_ids == [descriptor.evidence_id()])
        .expect("proposal progress check")
}

fn prepare_fixture_check(
    check: &mut crate::CheckReceipt,
    identity: &crate::SimulatorIdentity,
    binding: crate::types::SimulatorLivenessBinding,
) {
    check.observations = BTreeMap::from([
        ("runs:raft-soak".to_owned(), 1),
        ("passes:raft-soak".to_owned(), 1),
        ("steps:raft-soak".to_owned(), 320),
        (
            identity.required_observation.clone(),
            binding.reports.len() as u64,
        ),
    ]);
    check.simulator_liveness = Some(binding);
}

fn scheduled_log(source_ref: &str) -> String {
    let seeds = expected_scheduled_seeds("nightly", source_ref).expect("nightly seeds");
    let mut lines = vec![
        "label: raft-nightly".to_owned(),
        "exit_code: Some(0)".to_owned(),
        "model-check profile=raft-nightly expected_runtime=scheduled".to_owned(),
        format!("model-check raft-nightly-soak seeds source=replay seeds={seeds}"),
        event(&json!({
            "event": "exhaustive-check",
            "check_id": "raft-election-nightly",
            "status": "pass",
            "unique_protocol_states": 40_000_000,
            "unique_verifier_states": 40_000_000,
        })),
        event(&json!({
            "event": "exhaustive-check",
            "check_id": "raft-commit-nightly",
            "status": "pass",
            "unique_protocol_states": 60_000_000,
            "unique_verifier_states": 60_000_000,
        })),
        event(&json!({
            "event": "profile-total",
            "check_id": "raft-profile-total-nightly",
            "profile": "raft-nightly",
            "status": "pass",
            "unique_protocol_states": 100_000_000,
            "unique_verifier_states": 100_000_000,
            "target_protocol_states": 100_000_000,
            "target_verifier_states": 100_000_000,
        })),
    ];
    for seed in seeds.split(',') {
        let seed = u64::from_str_radix(seed.trim_start_matches("0x"), 16).expect("hex seed");
        for check_id in [
            "raft-nightly-soak",
            "raft-nightly-soak-lease",
            "raft-nightly-soak-membership",
        ] {
            lines.push(event(&json!({
                "event": "soak-check",
                "check_id": check_id,
            "status": "pass",
            "seed": seed,
            "steps": 1024,
            })));
        }
    }
    format!("{}\n", lines.join("\n"))
}

fn simulator_configuration(profile: &str) -> crate::catalog::SimulatorRunnerConfiguration {
    let (_, manifest) = crate::tests::loaded();
    manifest.profiles[profile].runners["simulator"]
        .simulator_configuration()
        .expect("typed simulator configuration")
}

fn event(value: &serde_json::Value) -> String {
    format!("{}{}", super::EVENT_PREFIX, value)
}
