//! Scenarios: each runner contract rejects policy and configuration drift.

use super::super::{ObligationCompletion, ProofObligationContract, RunnerContract};
use super::validate_runner;

fn runner(layer: &str, configuration: serde_json::Value) -> RunnerContract {
    let producer = match layer {
        "tests" => "rafter-invariants-tests-v14",
        "tla" => "rafter-invariants-tla-v16",
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
        obligations: Vec::new(),
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
            "config": "RaftCi.cfg",
            "detector_negative": "required",
            "finalization_reserve": "2m",
            "fp": "0",
            "java_major": "21",
            "kill_confirmation_timeout": "5s",
            "minimum_distinct_states": "1",
            "minimum_generated_states": "120000000",
            "module": "Raft.tla",
            "receipt_finalization_allowance": "5s",
            "seed": "2026071101",
            "soft_timeout": "115m",
            "symmetry": "nodes-values-read-requests-product",
            "termination_grace": "30s",
            "tool_asset_id": "510140106",
            "tool_mode": "required",
            "tool_sha256": "ab323b79802aedc3203b3f9af37c6aca3ed43f4e0225b36f2aa77b26de46c05f",
            "total_timeout": "155m",
            "trace_sample": "required",
            "workers": "4"
        }),
    );
    assert!(validate_runner("pr", "tla", &contract).is_err());
}

fn canonical_pr_tla_configuration() -> serde_json::Value {
    serde_json::json!({
        "config": "RaftCi.cfg",
        "detector_negative": "required",
        "finalization_reserve": "2m",
        "fp": "0",
        "fp_mem": "0.45",
        "java_major": "21",
        "kill_confirmation_timeout": "5s",
        "max_heap": "8g",
        "minimum_distinct_states": "16000000",
        "minimum_generated_states": "120000000",
        "module": "Raft.tla",
        "receipt_finalization_allowance": "5s",
        "seed": "2026071101",
        "soft_timeout": "325m",
        "symmetry": "nodes-values-read-requests-product",
        "termination_grace": "30s",
        "tool_asset_id": "510140106",
        "tool_mode": "required",
        "tool_sha256": "ab323b79802aedc3203b3f9af37c6aca3ed43f4e0225b36f2aa77b26de46c05f",
        "total_timeout": "338m",
        "trace_sample": "required",
        "workers": "4"
    })
}

fn obligation(id: &str, config: &str, soft_timeout: &str) -> ProofObligationContract {
    ProofObligationContract {
        id: id.to_owned(),
        config: config.to_owned(),
        completion: ObligationCompletion::FrontierExhausted,
        minimum_generated_states: 4_000,
        minimum_distinct_states: 900,
        soft_timeout: soft_timeout.to_owned(),
        seed: "2026081101".to_owned(),
    }
}

fn tla_runner_with(obligations: Vec<ProofObligationContract>) -> RunnerContract {
    let mut contract = runner("tla", canonical_pr_tla_configuration());
    contract.obligations = obligations;
    contract
}

/// The canonical PR configuration with no obligations must stay valid: the
/// empty list is the identity, and the deterministic gate depends on it.
#[test]
fn tla_contract_accepts_an_empty_obligation_list() {
    validate_runner("pr", "tla", &tla_runner_with(Vec::new()))
        .expect("empty obligations must remain canonical");
}

#[test]
fn tla_contract_accepts_sorted_focused_obligations() {
    validate_runner(
        "pr",
        "tla",
        &tla_runner_with(vec![
            obligation("joint-quorum-focused-init", "RaftJointQuorumFocusedInit.cfg", "4m"),
            obligation("joint-quorum-focused-next", "RaftJointQuorumFocusedNext.cfg", "6m"),
        ]),
    )
    .expect("sorted, budgeted obligations are canonical");
}

#[test]
fn tla_contract_rejects_duplicate_and_unsorted_obligation_identities() {
    let duplicate = tla_runner_with(vec![
        obligation("focused", "RaftJointQuorumFocusedInit.cfg", "4m"),
        obligation("focused", "RaftJointQuorumFocusedNext.cfg", "4m"),
    ]);
    assert!(validate_runner("pr", "tla", &duplicate).is_err());

    let unsorted = tla_runner_with(vec![
        obligation("zebra", "RaftJointQuorumFocusedNext.cfg", "4m"),
        obligation("alpha", "RaftJointQuorumFocusedInit.cfg", "4m"),
    ]);
    assert!(validate_runner("pr", "tla", &unsorted).is_err());

    let shouting = tla_runner_with(vec![obligation(
        "Focused_Init",
        "RaftJointQuorumFocusedInit.cfg",
        "4m",
    )]);
    assert!(validate_runner("pr", "tla", &shouting).is_err());
}

/// An obligation may not smuggle a profile's primary configuration back in
/// under a shorter timeout, and may not escape the specification directory.
#[test]
fn tla_contract_rejects_primary_and_escaping_obligation_configs() {
    for config in [
        "RaftCi.cfg",
        "RaftNightly.cfg",
        "Raft.cfg",
        "../secrets/Raft.cfg",
        "RaftJointQuorumFocusedInit.tla",
    ] {
        let contract = tla_runner_with(vec![obligation("focused", config, "4m")]);
        assert!(
            validate_runner("pr", "tla", &contract).is_err(),
            "obligation config {config} must be rejected"
        );
    }
}

/// Obligations are paid out of the same window as the primary run. A set that
/// would starve the continuation is a contract error, not a runtime surprise.
#[test]
fn tla_contract_rejects_obligations_that_starve_the_primary_run() {
    // The PR window is 338m total less a 2m reserve; the primary run already
    // claims 325m, so 11m is the entire remaining budget.
    let affordable = tla_runner_with(vec![obligation(
        "focused",
        "RaftJointQuorumFocusedInit.cfg",
        "11m",
    )]);
    validate_runner("pr", "tla", &affordable).expect("11m fits the PR window exactly");

    let overcommitted = tla_runner_with(vec![obligation(
        "focused",
        "RaftJointQuorumFocusedInit.cfg",
        "12m",
    )]);
    assert!(validate_runner("pr", "tla", &overcommitted).is_err());
}

#[test]
fn tla_contract_rejects_vacuous_obligation_floors_and_budgets() {
    let mut zero_floor = obligation("focused", "RaftJointQuorumFocusedInit.cfg", "4m");
    zero_floor.minimum_distinct_states = 0;
    assert!(validate_runner("pr", "tla", &tla_runner_with(vec![zero_floor])).is_err());

    let mut inverted = obligation("focused", "RaftJointQuorumFocusedInit.cfg", "4m");
    inverted.minimum_generated_states = 10;
    inverted.minimum_distinct_states = 11;
    assert!(validate_runner("pr", "tla", &tla_runner_with(vec![inverted])).is_err());

    let mut instant = obligation("focused", "RaftJointQuorumFocusedInit.cfg", "0m");
    instant.seed = "2026081101".to_owned();
    assert!(validate_runner("pr", "tla", &tla_runner_with(vec![instant])).is_err());

    let mut unseeded = obligation("focused", "RaftJointQuorumFocusedInit.cfg", "4m");
    unseeded.seed = "auto".to_owned();
    assert!(validate_runner("pr", "tla", &tla_runner_with(vec![unseeded])).is_err());
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
