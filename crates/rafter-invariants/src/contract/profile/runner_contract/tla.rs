//! Canonical configuration for the TLA+ evidence runner.

use std::collections::BTreeMap;

use serde::Deserialize;

use super::validate::decode_configuration;

const TLA_TOOL_SHA256: &str = "cc4803dce2a8ffaf0f5920a9dc39df4b5ee34ab4cb53fb58ac557277a7e516b3";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
    config: String,
    detector_negative: String,
    fp: String,
    #[serde(default)]
    fp_mem: Option<String>,
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

pub(super) fn validate(
    profile: &str,
    configuration: &BTreeMap<String, String>,
) -> Result<(), String> {
    let contract: Configuration = decode_configuration(configuration)?;
    if contract.detector_negative != "required"
        || contract.fp != "0"
        || contract.java_major != "21"
        || contract.minimum_distinct_states != "16000000"
        || contract.minimum_generated_states != "120000000"
        || contract.module != "Raft.tla"
        || contract.kill_confirmation_timeout != "5s"
        || contract.receipt_finalization_allowance != "5s"
        || contract.termination_grace != "30s"
        || contract.tool_asset_id != "481553986"
        || contract.tool_mode != "required"
        || contract.tool_sha256 != TLA_TOOL_SHA256
        || contract.trace_sample != "required"
    {
        return Err("shared TLA+ runner configuration is not canonical".to_owned());
    }

    let valid = match profile {
        "pr" => valid_pr(&contract),
        "nightly" => valid_nightly(&contract),
        "weekly" => valid_weekly(&contract),
        _ => false,
    };
    valid
        .then_some(())
        .ok_or_else(|| "profile-specific TLA+ runner configuration is not canonical".to_owned())
}

fn valid_pr(contract: &Configuration) -> bool {
    contract.config == "RaftCi.cfg"
        && contract.seed == "2026071101"
        && contract.soft_timeout == "325m"
        && contract.workers == "4"
        && contract.finalization_reserve.as_deref() == Some("2m")
        && contract.max_heap.as_deref() == Some("8g")
        && contract.fp_mem.as_deref() == Some("0.45")
        && contract.symmetry.as_deref() == Some("nodes-values-read-requests-product")
        && contract.total_timeout.as_deref() == Some("338m")
        && no_checkpoint_configuration(contract)
}

fn valid_nightly(contract: &Configuration) -> bool {
    contract.config == "RaftNightly.cfg"
        && contract.seed == "2026071102"
        && contract.soft_timeout == "265m"
        && contract.workers == "auto"
        && contract.symmetry.as_deref() == Some("nodes-values-read-requests-product")
        && contract.checkpoint_gzip.as_deref() == Some("required")
        && contract.checkpoint_minutes.as_deref() == Some("30")
        && contract.checkpoint_recovery.as_deref() == Some("strict-compatible-if-present")
        && contract.max_heap.as_deref() == Some("8g")
        && contract.fp_mem.as_deref() == Some("0.45")
        && contract.finalization_reserve.as_deref() == Some("10m")
        && contract.total_timeout.as_deref() == Some("320m")
        && contract.unsymmetrized_exploration.is_none()
}

fn valid_weekly(contract: &Configuration) -> bool {
    contract.config == "Raft.cfg"
        && contract.seed == "2026071103"
        && contract.soft_timeout == "265m"
        && contract.workers == "auto"
        && contract.checkpoint_gzip.as_deref() == Some("required")
        && contract.checkpoint_minutes.as_deref() == Some("30")
        && contract.checkpoint_recovery.as_deref() == Some("strict-compatible-if-present")
        && contract.max_heap.as_deref() == Some("4g")
        && contract.fp_mem.as_deref() == Some("0.45")
        && contract.unsymmetrized_exploration.as_deref() == Some("required")
        && contract.finalization_reserve.as_deref() == Some("10m")
        && contract.symmetry.is_none()
        && contract.total_timeout.as_deref() == Some("320m")
}

fn no_checkpoint_configuration(contract: &Configuration) -> bool {
    contract.checkpoint_gzip.is_none()
        && contract.checkpoint_minutes.is_none()
        && contract.checkpoint_recovery.is_none()
        && contract.unsymmetrized_exploration.is_none()
}
