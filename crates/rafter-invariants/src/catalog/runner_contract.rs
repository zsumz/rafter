use std::collections::BTreeMap;

use serde::Deserialize;

use super::RunnerContract;

const TLA_TOOL_SHA256: &str = "33de7da9ce1b7fffb9d1c184021178dbb051747be48504e65c584c423721a32e";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TestsConfiguration {
    build: String,
    compile_timeout: String,
    discovery: String,
    discovery_timeout: String,
    execution: String,
    execution_timeout: String,
    failure_contract: String,
    finalization_reserve: String,
    kill_confirmation_timeout: String,
    layer_timeout: String,
    receipt_finalization_allowance: String,
    scale: String,
    termination_grace: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TlaConfiguration {
    config: String,
    detector_negative: String,
    fp: String,
    java_major: String,
    kill_confirmation_timeout: String,
    minimum_distinct_states: String,
    minimum_generated_states: String,
    module: String,
    receipt_finalization_allowance: String,
    seed: String,
    soft_timeout: String,
    tool_asset_id: String,
    tool_mode: String,
    tool_sha256: String,
    trace_sample: String,
    termination_grace: String,
    workers: String,
    #[serde(default)]
    checkpoint_gzip: Option<String>,
    #[serde(default)]
    checkpoint_minutes: Option<String>,
    #[serde(default)]
    checkpoint_recovery: Option<String>,
    #[serde(default)]
    finalization_reserve: Option<String>,
    #[serde(default)]
    max_heap: Option<String>,
    #[serde(default)]
    symmetry: Option<String>,
    #[serde(default)]
    total_timeout: Option<String>,
    #[serde(default)]
    unsymmetrized_exploration: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MaelstromConfiguration {
    build: String,
    duration_seconds: String,
    evidence_semantics: String,
    fault_markers: String,
    finalization_reserve: String,
    java_major: String,
    kill_confirmation_timeout: String,
    layer_timeout: String,
    lease_election_timeout_ticks: String,
    lease_heartbeat_interval_ticks: String,
    lease_history_binding: String,
    lease_probe_selection: String,
    lease_probe_source: String,
    lease_same_node_term: String,
    lease_tick_interval_ms: String,
    lease_window_ms: String,
    lease_window_ticks: String,
    maelstrom_archive_sha256: String,
    maelstrom_executable_sha256: String,
    maelstrom_jar_sha256: String,
    maelstrom_version: String,
    operation_floor: String,
    rate: String,
    receipt_finalization_allowance: String,
    replay: String,
    scenarios: String,
    scheduler_seed: String,
    structural_edn: String,
    termination_grace: String,
    trial_timeout_overhead: String,
    trials: String,
}

pub(super) fn validate_runner(
    profile: &str,
    layer: &str,
    runner: &RunnerContract,
) -> Result<(), String> {
    let (producer, minimum_observed_checks) = match layer {
        "tests" => ("rafter-invariants-tests-v13", 82),
        "simulator" => ("rafter-invariants-simulator-v16", 79),
        "tla" => ("rafter-invariants-tla-v15", 1),
        "maelstrom" => ("rafter-invariants-maelstrom-v10", 6),
        _ => return Err(format!("unsupported runner layer {layer}")),
    };
    let expected_command = [
        "cargo",
        "run",
        "--locked",
        "-p",
        "rafter-invariants",
        "--",
        "run",
        "--profile",
        profile,
        "--layer",
        layer,
    ]
    .map(str::to_owned)
    .to_vec();
    if runner.producer != producer
        || runner.command != expected_command
        || runner.minimum_observed_checks != minimum_observed_checks
        || !runner.require_peak_rss
    {
        return Err(
            "producer, command, coverage floor, or resource contract is not canonical".to_owned(),
        );
    }

    match layer {
        "tests" => validate_tests(profile, &runner.configuration),
        "simulator" => Ok(()),
        "tla" => validate_tla(profile, &runner.configuration),
        "maelstrom" => validate_maelstrom(profile, &runner.configuration),
        _ => unreachable!("layer was matched above"),
    }
}

fn typed<T>(configuration: &BTreeMap<String, String>) -> Result<T, String>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_value(serde_json::to_value(configuration).map_err(|error| error.to_string())?)
        .map_err(|error| error.to_string())
}

fn validate_tests(profile: &str, configuration: &BTreeMap<String, String>) -> Result<(), String> {
    let contract: TestsConfiguration = typed(configuration)?;
    if contract.build == "locked-no-default-features"
        && contract.compile_timeout == "10m"
        && contract.discovery == "libtest-executable"
        && contract.discovery_timeout == "2m"
        && contract.execution == "exact-single-threaded"
        && contract.execution_timeout == "5m"
        && contract.failure_contract == "typed-oracle-libtest-v4"
        && contract.finalization_reserve == "3m"
        && contract.kill_confirmation_timeout == "5s"
        && contract.layer_timeout == "40m"
        && contract.receipt_finalization_allowance == "5s"
        && contract.scale == profile
        && contract.termination_grace == "30s"
    {
        Ok(())
    } else {
        Err("tests runner configuration is not canonical".to_owned())
    }
}

fn validate_tla(profile: &str, configuration: &BTreeMap<String, String>) -> Result<(), String> {
    let contract: TlaConfiguration = typed(configuration)?;
    if contract.detector_negative != "required"
        || contract.fp != "0"
        || contract.java_major != "21"
        || contract.minimum_distinct_states != "16000000"
        || contract.minimum_generated_states != "120000000"
        || contract.module != "Raft.tla"
        || contract.kill_confirmation_timeout != "5s"
        || contract.receipt_finalization_allowance != "5s"
        || contract.termination_grace != "30s"
        || contract.tool_asset_id != "471380474"
        || contract.tool_mode != "required"
        || contract.tool_sha256 != TLA_TOOL_SHA256
        || contract.trace_sample != "required"
    {
        return Err("shared TLA+ runner configuration is not canonical".to_owned());
    }

    let valid = match profile {
        "pr" => {
            contract.config == "RaftCi.cfg"
                && contract.seed == "2026071101"
                && contract.soft_timeout == "115m"
                && contract.workers == "4"
                && contract.finalization_reserve.as_deref() == Some("2m")
                && contract.symmetry.as_deref() == Some("nodes-values-read-requests-product")
                && contract.total_timeout.as_deref() == Some("155m")
                && no_checkpoint_configuration(&contract)
        }
        "nightly" => {
            contract.config == "RaftNightly.cfg"
                && contract.seed == "2026071102"
                && contract.soft_timeout == "115m"
                && contract.workers == "4"
                && contract.symmetry.as_deref() == Some("nodes-values-read-requests-product")
                && contract.finalization_reserve.as_deref() == Some("10m")
                && contract.total_timeout.as_deref() == Some("165m")
                && no_checkpoint_configuration(&contract)
        }
        "weekly" => {
            contract.config == "Raft.cfg"
                && contract.seed == "2026071103"
                && contract.soft_timeout == "295m"
                && contract.workers == "auto"
                && contract.checkpoint_gzip.as_deref() == Some("required")
                && contract.checkpoint_minutes.as_deref() == Some("30")
                && contract.checkpoint_recovery.as_deref() == Some("strict-compatible-if-present")
                && contract.max_heap.as_deref() == Some("4g")
                && contract.unsymmetrized_exploration.as_deref() == Some("required")
                && contract.finalization_reserve.as_deref() == Some("10m")
                && contract.symmetry.is_none()
                && contract.total_timeout.as_deref() == Some("350m")
        }
        _ => false,
    };
    valid
        .then_some(())
        .ok_or_else(|| "profile-specific TLA+ runner configuration is not canonical".to_owned())
}

fn no_checkpoint_configuration(contract: &TlaConfiguration) -> bool {
    contract.checkpoint_gzip.is_none()
        && contract.checkpoint_minutes.is_none()
        && contract.checkpoint_recovery.is_none()
        && contract.max_heap.is_none()
        && contract.unsymmetrized_exploration.is_none()
}

fn validate_maelstrom(
    profile: &str,
    configuration: &BTreeMap<String, String>,
) -> Result<(), String> {
    let contract: MaelstromConfiguration = typed(configuration)?;
    let profile_bounds = match profile {
        "nightly" => {
            contract.duration_seconds == "45"
                && contract.trials == "1"
                && contract.layer_timeout == "25m"
        }
        "weekly" => {
            contract.duration_seconds == "60"
                && contract.trials == "3"
                && contract.layer_timeout == "40m"
        }
        _ => false,
    };
    if profile_bounds
        && contract.build == "locked-debug"
        && contract.evidence_semantics == "nondeterministic-sampled-e2e"
        && contract.fault_markers == "required"
        && contract.finalization_reserve == "5m"
        && contract.java_major == "21"
        && contract.kill_confirmation_timeout == "5s"
        && contract.lease_election_timeout_ticks == "20"
        && contract.lease_heartbeat_interval_ticks == "2"
        && contract.lease_history_binding
            == "ordered-process-invoke-terminal-client-msg-value-code11"
        && contract.lease_probe_selection == "second-post-expiry-read-per-client"
        && contract.lease_probe_source == "real-direct-maelstrom-read"
        && contract.lease_same_node_term == "required"
        && contract.lease_tick_interval_ms == "50"
        && contract.lease_window_ms == "500"
        && contract.lease_window_ticks == "10"
        && contract.maelstrom_archive_sha256
            == "301ec71d6b12af0d765edb413f5cf5aa1046b5609bd4e31376a0b549548e5799"
        && contract.maelstrom_executable_sha256
            == "aba82f628ca088d25e8952c2c49834565406b9239d1c79953a54bf2c26cfdf20"
        && contract.maelstrom_jar_sha256
            == "7d35db06546a737134a4dd4eb3b7dfb0955537df992d922d18cc716080853f67"
        && contract.maelstrom_version == "v0.2.4"
        && contract.operation_floor == "read-write-cas-per-trial"
        && contract.rate == "100"
        && contract.receipt_finalization_allowance == "5s"
        && contract.replay == "retained-store"
        && contract.scenarios == "base,membership,restart,app-crash,snapshot,lease-isolation"
        && contract.scheduler_seed == "unavailable"
        && contract.structural_edn == "required"
        && contract.termination_grace == "30s"
        && contract.trial_timeout_overhead == "2m"
    {
        Ok(())
    } else {
        Err("Maelstrom runner configuration is not canonical".to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::{validate_runner, RunnerContract};

    fn runner(layer: &str, configuration: serde_json::Value) -> RunnerContract {
        let producer = match layer {
            "tests" => "rafter-invariants-tests-v13",
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
                "tool_asset_id": "471380474",
                "tool_mode": "required",
                "tool_sha256": "33de7da9ce1b7fffb9d1c184021178dbb051747be48504e65c584c423721a32e",
                "total_timeout": "155m",
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
}
