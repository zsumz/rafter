//! Canonical configuration for the TLA+ evidence runner.

use std::collections::BTreeSet;

use serde::Deserialize;

use super::super::{ObligationCompletion, ProofObligationContract, RunnerContract};
use super::validate::decode_configuration;

const TLA_TOOL_SHA256: &str = "ab323b79802aedc3203b3f9af37c6aca3ed43f4e0225b36f2aa77b26de46c05f";
const TLA_TOOL_ASSET_ID: &str = "510788686";

/// Configurations that are the profile-owned monolith for some profile. An
/// obligation may never name one: obligations exist to state theorems the
/// primary run cannot reach, and a "focused" run of the monolith under a
/// shorter timeout would only ever fail.
const PRIMARY_CONFIGS: [&str; 3] = ["RaftCi.cfg", "RaftNightly.cfg", "Raft.cfg"];

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

pub(super) fn validate(profile: &str, runner: &RunnerContract) -> Result<(), String> {
    let configuration = &runner.configuration;
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
        || contract.tool_asset_id != TLA_TOOL_ASSET_ID
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
    if !valid {
        return Err("profile-specific TLA+ runner configuration is not canonical".to_owned());
    }
    validate_obligations(&contract, &runner.obligations)
}

/// Validates the obligation list structurally rather than by value.
///
/// The reviewed obligation set is profile data, not source: calibrated floors
/// arrive as a profiles-manifest edit. What source owns is the shape -- unique
/// sorted kebab-case identities, a resolvable non-primary configuration, a
/// positive whole-minute budget, positive ratchets, and a layer budget that
/// still leaves the primary continuation the time it was promised.
fn validate_obligations(
    contract: &Configuration,
    obligations: &[ProofObligationContract],
) -> Result<(), String> {
    let identities = obligations
        .iter()
        .map(|obligation| obligation.id.as_str())
        .collect::<Vec<_>>();
    if identities.iter().collect::<BTreeSet<_>>().len() != identities.len()
        || !identities.windows(2).all(|pair| pair[0] < pair[1])
    {
        return Err("TLA+ proof obligations must have unique, sorted identities".to_owned());
    }
    for obligation in obligations {
        validate_obligation(obligation)?;
    }
    validate_obligation_budget(contract, obligations)
}

fn validate_obligation(obligation: &ProofObligationContract) -> Result<(), String> {
    if !is_kebab_case(&obligation.id) {
        return Err(format!(
            "TLA+ proof obligation {} must use a kebab-case identity",
            obligation.id
        ));
    }
    if obligation.completion != ObligationCompletion::FrontierExhausted {
        return Err(format!(
            "TLA+ proof obligation {} must require frontier exhaustion",
            obligation.id
        ));
    }
    if !obligation.config.ends_with(".cfg")
        || obligation.config.contains('/')
        || obligation.config.contains('\\')
        || obligation.config.starts_with('.')
        || PRIMARY_CONFIGS.contains(&obligation.config.as_str())
    {
        return Err(format!(
            "TLA+ proof obligation {} must name a non-primary configuration under specs/tla/raft",
            obligation.id
        ));
    }
    if obligation.minimum_generated_states == 0
        || obligation.minimum_distinct_states == 0
        || obligation.minimum_generated_states < obligation.minimum_distinct_states
    {
        return Err(format!(
            "TLA+ proof obligation {} must ratchet positive, ordered state floors",
            obligation.id
        ));
    }
    if obligation.seed.is_empty() || !obligation.seed.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!(
            "TLA+ proof obligation {} must pin a numeric seed",
            obligation.id
        ));
    }
    match whole_minutes(&obligation.soft_timeout) {
        Some(minutes) if minutes > 0 => Ok(()),
        _ => Err(format!(
            "TLA+ proof obligation {} must budget positive whole minutes",
            obligation.id
        )),
    }
}

/// Obligations are paid out of the same layer budget as the primary run, and
/// they run first. Requiring the whole sequence to fit inside the execution
/// window keeps that ordering honest: an obligation set that would starve the
/// primary continuation is rejected at contract time rather than silently
/// truncating the monolith at runtime.
fn validate_obligation_budget(
    contract: &Configuration,
    obligations: &[ProofObligationContract],
) -> Result<(), String> {
    let (Some(total), Some(reserve)) = (
        contract.total_timeout.as_deref().and_then(whole_minutes),
        contract
            .finalization_reserve
            .as_deref()
            .and_then(whole_minutes),
    ) else {
        return Ok(());
    };
    let primary = whole_minutes(&contract.soft_timeout)
        .ok_or_else(|| "TLA+ soft_timeout must use whole minutes".to_owned())?;
    let obligated = obligations
        .iter()
        .try_fold(0_u64, |sum, obligation| {
            sum.checked_add(whole_minutes(&obligation.soft_timeout)?)
        })
        .ok_or_else(|| "TLA+ proof obligation budget overflows".to_owned())?;
    let window = total
        .checked_sub(reserve)
        .ok_or_else(|| "TLA+ total_timeout must exceed finalization_reserve".to_owned())?;
    if obligated.saturating_add(primary) > window {
        return Err(
            "TLA+ proof obligations and the primary run must fit the execution window".to_owned(),
        );
    }
    Ok(())
}

fn whole_minutes(value: &str) -> Option<u64> {
    value.strip_suffix('m')?.parse::<u64>().ok()
}

fn is_kebab_case(value: &str) -> bool {
    !value.is_empty()
        && !value.starts_with('-')
        && !value.ends_with('-')
        && !value.contains("--")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
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
