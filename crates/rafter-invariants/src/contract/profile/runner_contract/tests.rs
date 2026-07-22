//! Scenarios: each runner contract rejects policy and configuration drift.

use super::super::RunnerContract;
use super::validate_runner;

fn runner(layer: &str, configuration: serde_json::Value) -> RunnerContract {
    let producer = match layer {
        "tests" => "rafter-invariants-tests-v14",
        "tla" => "rafter-invariants-tla-v15",
        "maelstrom" => "rafter-invariants-maelstrom-v10",
        _ => unreachable!(),
    };
    let minimum_observed_checks = match layer {
        "tests" => 82,
        "tla" => 1,
        "maelstrom" => 6,
        _ => unreachable!(),
    };
    RunnerContract {
        producer: producer.to_owned(),
        command: [
            "cargo",
            "run",
            "--locked",
            "-p",
            "rafter-invariants",
            "--",
            "run",
            "--profile",
            "pr",
            "--layer",
            layer,
        ]
        .map(str::to_owned)
        .to_vec(),
        configuration: serde_json::from_value(configuration).expect("string map"),
        simulator_checks: std::collections::BTreeMap::new(),
        minimum_observed_checks,
        require_peak_rss: true,
    }
}

#[test]
fn tests_contract_rejects_unknown_configuration() {
    let contract = runner(
        "tests",
        serde_json::json!({
            "build": "locked-no-default-features",
            "compile_timeout": "10m",
            "discovery": "libtest-executable",
            "discovery_timeout": "2m",
            "execution": "exact-single-threaded",
            "execution_timeout": "5m",
            "failure_contract": "typed-oracle-libtest-v4",
            "finalization_reserve": "3m",
            "kill_confirmation_timeout": "5s",
            "layer_timeout": "40m",
            "receipt_finalization_allowance": "5s",
            "scale": "pr",
            "termination_grace": "30s",
            "unreviewed": "true"
        }),
    );
    assert!(validate_runner("pr", "tests", &contract).is_err());
}

#[test]
fn simulator_producer_contract_is_exact_and_profile_specific() {
    let (_, manifest) = crate::tests::loaded();
    let pr = &manifest.profiles["pr"].runners["simulator"];
    validate_runner("pr", "simulator", pr).expect("PR simulator v19 contract");

    let mut stale_pr = pr.clone();
    stale_pr.producer = "rafter-invariants-simulator-v18".to_owned();
    assert!(validate_runner("pr", "simulator", &stale_pr).is_err());

    let nightly = &manifest.profiles["nightly"].runners["simulator"];
    validate_runner("nightly", "simulator", nightly).expect("nightly simulator v19 contract");

    let mut stale_nightly = nightly.clone();
    stale_nightly.producer = "rafter-invariants-simulator-v18".to_owned();
    assert!(validate_runner("nightly", "simulator", &stale_nightly).is_err());

    let mut stale_proof = pr.clone();
    stale_proof.configuration.remove("detector_proof");
    assert!(validate_runner("pr", "simulator", &stale_proof).is_err());
}

#[test]
fn tla_contract_rejects_weakened_state_floor() {
    let contract = runner(
        "tla",
        serde_json::json!({
            "config": "RaftRefactor.cfg",
            "detector_negative": "required",
            "finalization_reserve": "2m",
            "fp": "0",
            "java_major": "21",
            "kill_confirmation_timeout": "5s",
            "minimum_distinct_states": "1",
            "minimum_generated_states": "900000",
            "module": "Raft.tla",
            "receipt_finalization_allowance": "5s",
            "seed": "2026071101",
            "soft_timeout": "2m",
            "symmetry": "nodes-values-read-requests-product",
            "termination_grace": "30s",
            "tool_asset_id": "481553986",
            "tool_mode": "required",
            "tool_sha256": "cc4803dce2a8ffaf0f5920a9dc39df4b5ee34ab4cb53fb58ac557277a7e516b3",
            "total_timeout": "15m",
            "trace_sample": "required",
            "workers": "4"
        }),
    );
    assert!(validate_runner("pr", "tla", &contract).is_err());
}

#[test]
fn maelstrom_contract_rejects_pr_profile() {
    let contract = runner(
        "maelstrom",
        serde_json::json!({
            "build": "locked-debug",
            "duration_seconds": "45",
            "evidence_semantics": "nondeterministic-sampled-e2e",
            "fault_markers": "required",
            "finalization_reserve": "5m",
            "java_major": "21",
            "kill_confirmation_timeout": "5s",
            "layer_timeout": "25m",
            "lease_election_timeout_ticks": "20",
            "lease_heartbeat_interval_ticks": "2",
            "lease_history_binding": "ordered-process-invoke-terminal-client-msg-value-code11",
            "lease_probe_selection": "second-post-expiry-read-per-client",
            "lease_probe_source": "real-direct-maelstrom-read",
            "lease_same_node_term": "required",
            "lease_tick_interval_ms": "50",
            "lease_window_ms": "500",
            "lease_window_ticks": "10",
            "maelstrom_archive_sha256": "301ec71d6b12af0d765edb413f5cf5aa1046b5609bd4e31376a0b549548e5799",
            "maelstrom_executable_sha256": "aba82f628ca088d25e8952c2c49834565406b9239d1c79953a54bf2c26cfdf20",
            "maelstrom_jar_sha256": "7d35db06546a737134a4dd4eb3b7dfb0955537df992d922d18cc716080853f67",
            "maelstrom_version": "v0.2.4",
            "operation_floor": "read-write-cas-per-trial",
            "rate": "100",
            "receipt_finalization_allowance": "5s",
            "replay": "retained-store",
            "scenarios": "base,membership,restart,app-crash,snapshot,lease-isolation",
            "scheduler_seed": "unavailable",
            "structural_edn": "required",
            "termination_grace": "30s",
            "trial_timeout_overhead": "2m",
            "trials": "1"
        }),
    );
    assert!(validate_runner("pr", "maelstrom", &contract).is_err());
}
