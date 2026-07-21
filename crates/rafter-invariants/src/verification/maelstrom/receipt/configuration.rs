//! Reviewed sampled-E2E Maelstrom profile contract.

use crate::contract::profile::RunnerContract;

pub(super) fn validate(contract: &RunnerContract) -> Result<(), &'static str> {
    let values = &contract.configuration;
    let fixed = [
        ("build", "locked-debug"),
        ("evidence_semantics", "nondeterministic-sampled-e2e"),
        ("fault_markers", "required"),
        ("java_major", "21"),
        (
            "maelstrom_archive_sha256",
            "301ec71d6b12af0d765edb413f5cf5aa1046b5609bd4e31376a0b549548e5799",
        ),
        (
            "maelstrom_executable_sha256",
            "aba82f628ca088d25e8952c2c49834565406b9239d1c79953a54bf2c26cfdf20",
        ),
        (
            "maelstrom_jar_sha256",
            "7d35db06546a737134a4dd4eb3b7dfb0955537df992d922d18cc716080853f67",
        ),
        ("maelstrom_version", "v0.2.4"),
        ("operation_floor", "read-write-cas-per-trial"),
        ("lease_tick_interval_ms", "50"),
        ("lease_election_timeout_ticks", "20"),
        ("lease_heartbeat_interval_ticks", "2"),
        ("lease_window_ticks", "10"),
        ("lease_window_ms", "500"),
        ("lease_probe_source", "real-direct-maelstrom-read"),
        (
            "lease_history_binding",
            "ordered-process-invoke-terminal-client-msg-value-code11",
        ),
        (
            "lease_probe_selection",
            "second-post-expiry-read-per-client",
        ),
        ("lease_same_node_term", "required"),
        ("replay", "retained-store"),
        (
            "scenarios",
            "base,membership,restart,app-crash,snapshot,lease-isolation",
        ),
        ("scheduler_seed", "unavailable"),
        ("structural_edn", "required"),
    ];
    if fixed
        .iter()
        .any(|(key, expected)| values.get(*key).map(String::as_str) != Some(*expected))
        || values.get("rate").map(String::as_str) != Some("100")
        || invalid_positive_integer(values.get("duration_seconds"))
        || invalid_positive_integer(values.get("trials"))
    {
        return Err("Maelstrom profile configuration is not the reviewed sampled-E2E contract");
    }
    Ok(())
}

fn invalid_positive_integer(value: Option<&String>) -> bool {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .is_none_or(|value| value == 0)
}
