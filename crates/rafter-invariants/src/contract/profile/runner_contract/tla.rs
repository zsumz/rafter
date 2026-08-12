//! Canonical configuration for the TLA+ evidence runner.

use serde::Deserialize;

use super::super::RunnerContract;
use super::tla_obligations::{self, PrimaryBudget};
use super::validate::decode_configuration;

const TLA_TOOL_SHA256: &str = "ab323b79802aedc3203b3f9af37c6aca3ed43f4e0225b36f2aa77b26de46c05f";
const TLA_TOOL_ASSET_ID: &str = "510788686";

/// Configurations that are the profile-owned monolith for some profile. An
/// obligation may never name one: obligations exist to state theorems the
/// primary run cannot reach, and a "focused" run of the monolith under a
/// shorter timeout would only ever fail.
pub(super) const PRIMARY_CONFIGS: [&str; 3] = ["RaftCi.cfg", "RaftNightly.cfg", "Raft.cfg"];

/// Exhaustion floors for the PR primary configuration.
///
/// These are the only state floors that gate anything. `RaftCi.cfg` genuinely
/// drains its queue, so its continuation decides the PR layer and these two
/// numbers are the calibrated bar it must clear. They are the exact counts of
/// a measured post-reduction exhaustion (255,177,640 generated, 36,058,645
/// distinct, queue drained; TLC 2026.08.11.125311, seed 2026071101, fp 0,
/// 4 workers, symmetric), matching the obligation-floor philosophy: counts
/// are deterministic for a fixed spec, config, and symmetry, so a deviation
/// is a spec change and wants deliberate recalibration, not slack. A `RaftCi`
/// recalibration is exactly this edit and nothing else.
const PR_MINIMUM_GENERATED_STATES: &str = "255177640";
const PR_MINIMUM_DISTINCT_STATES: &str = "36058645";

/// Accumulation bar the scheduled continuations report progress against.
///
/// Nightly and weekly run their primary in reporting mode, where these are
/// published as context beside the observed counters rather than enforced as a
/// terminal condition. They document the bar the lineage is accumulating
/// toward; they do not decide a verdict.
const REPORTING_MINIMUM_GENERATED_STATES: &str = "120000000";
const REPORTING_MINIMUM_DISTINCT_STATES: &str = "16000000";

/// Contract vocabulary for the primary-continuation policy. Re-derived here
/// rather than imported: `contract` sits upstream of `evidence` in the domain
/// graph, and pinning the wire strings independently keeps this gate honest
/// even if the producer-side vocabulary drifts.
const GATING_FRONTIER_EXHAUSTED: &str = "gating-frontier-exhausted";
const REPORTING_CONTINUATION: &str = "reporting-continuation";

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
    primary_completion: String,
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
    tla_obligations::validate(
        PrimaryBudget {
            reporting: contract.primary_completion == REPORTING_CONTINUATION,
            soft_timeout: &contract.soft_timeout,
            total_timeout: contract.total_timeout.as_deref(),
            finalization_reserve: contract.finalization_reserve.as_deref(),
        },
        &runner.obligations,
    )
}

fn valid_pr(contract: &Configuration) -> bool {
    contract.config == "RaftCi.cfg"
        && contract.primary_completion == GATING_FRONTIER_EXHAUSTED
        && contract.minimum_generated_states == PR_MINIMUM_GENERATED_STATES
        && contract.minimum_distinct_states == PR_MINIMUM_DISTINCT_STATES
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
        && contract.primary_completion == REPORTING_CONTINUATION
        && contract.minimum_generated_states == REPORTING_MINIMUM_GENERATED_STATES
        && contract.minimum_distinct_states == REPORTING_MINIMUM_DISTINCT_STATES
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
        && contract.primary_completion == REPORTING_CONTINUATION
        && contract.minimum_generated_states == REPORTING_MINIMUM_GENERATED_STATES
        && contract.minimum_distinct_states == REPORTING_MINIMUM_DISTINCT_STATES
        && contract.seed == "2026071103"
        // 200m, not nightly's 265m: with the continuation reporting rather
        // than gating, an hour of its budget buys the unsymmetrized
        // obligation family instead -- the symmetry audit the weekly tier
        // exists to provide, applied to the theorems that actually gate.
        && contract.soft_timeout == "200m"
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
