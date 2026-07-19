//! Canonical configuration for the Maelstrom evidence runner.

use std::collections::BTreeMap;

use serde::Deserialize;

use super::validate::decode_configuration;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
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

pub(super) fn validate(
    profile: &str,
    configuration: &BTreeMap<String, String>,
) -> Result<(), String> {
    let contract: Configuration = decode_configuration(configuration)?;
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
