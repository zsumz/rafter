//! Contract-pinned continuation policy and counter consistency.

use crate::contract::profile::RunnerContract;
use std::collections::BTreeSet;

use crate::evidence::{CheckReceipt, PrimaryCompletionPolicy, ResultBundle, PRIMARY_COMPLETION_KEY};

/// Resolves the contract-pinned continuation policy and refuses any receipt
/// that disagrees with it.
///
/// Two things are enforced here rather than trusted. The deterministic PR gate
/// may never demote its primary: `RaftCi.cfg` genuinely drains, and a PR
/// receipt claiming reporting mode would be quietly relaxing the only lane that
/// blocks a merge. And the receipt's own declared policy must match the profile
/// it claims to come from, so the mode is contract state rather than something
/// a producer chooses about itself.
pub(super) fn pinned_policy(
    bundle: &ResultBundle,
    contract: &RunnerContract,
) -> Result<PrimaryCompletionPolicy, &'static str> {
    let policy = contract
        .configuration
        .get(PRIMARY_COMPLETION_KEY)
        .map(String::as_str)
        .and_then(PrimaryCompletionPolicy::parse)
        .ok_or("TLA profile pins no reviewed primary_completion policy")?;
    if bundle.profile == "pr" && !policy.gates() {
        return Err("the PR TLA profile may not demote its primary configuration to reporting");
    }
    let declared = bundle.execution.checks[0]
        .tla_continuation
        .ok_or("TLA receipt omitted its continuation binding")?;
    if declared.policy != policy {
        return Err("TLA receipt declares a continuation policy its profile does not pin");
    }
    Ok(policy)
}

/// Counter consistency for whichever frame the continuation actually produced.
///
/// Under a gating policy the run drained and cleared its calibrated floors.
/// Under a reporting policy the floors are context rather than a condition, so
/// only internal consistency is enforced -- generated states cannot be fewer
/// than distinct ones, and a frame claiming an open frontier must show one.
pub(super) fn counters_are_consistent(
    check: &CheckReceipt,
    policy: PrimaryCompletionPolicy,
    elapsed: bool,
    minimum_generated: u64,
    minimum_distinct: u64,
) -> bool {
    if elapsed {
        return observed(check, "progress_generated_states")
            >= observed(check, "progress_distinct_states")
            && observed(check, "progress_states_left") > 0
            && observed(check, "progress_depth") > 0;
    }
    let consistent = observed(check, "generated_states") >= observed(check, "distinct_states")
        && observed(check, "states_left_on_queue") == 0
        && observed(check, "search_depth") > 0;
    if !policy.gates() {
        return consistent;
    }
    consistent
        && observed(check, "generated_states") >= minimum_generated
        && observed(check, "distinct_states") >= minimum_distinct
}

fn observed(check: &CheckReceipt, name: &str) -> u64 {
    check.observations.get(name).copied().unwrap_or_default()
}

/// Observation keys a passing receipt must carry for whichever frame the
/// continuation produced. An elapsed run publishes progress counters; a run
/// that reached a terminal state publishes terminal ones. Never both.
pub(super) fn expected_counter_frame(elapsed: bool) -> BTreeSet<String> {
    let mut frame = BTreeSet::from([
        "configured_invariants".to_owned(),
        "tool_pin_verified".to_owned(),
        "trace_sample_passed".to_owned(),
    ]);
    frame.extend(
        if elapsed {
            [
                "progress_generated_states",
                "progress_distinct_states",
                "progress_states_left",
                "progress_depth",
            ]
        } else {
            [
                "generated_states",
                "distinct_states",
                "states_left_on_queue",
                "search_depth",
            ]
        }
        .into_iter()
        .map(str::to_owned),
    );
    frame
}
