//! Structural completion, status, and artifact-cardinality validation.

use std::collections::BTreeSet;

use crate::evidence::{CheckCompletion, CheckReceipt, EvidenceStatus, ResultBundle};

use super::super::scenario::Scenario;

pub(super) fn validate(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    scenario: Scenario,
    trials: u64,
) -> Result<(), &'static str> {
    let trials_usize = usize::try_from(trials).map_err(|_| "Maelstrom trial count is too large")?;
    let statuses = bundle
        .results
        .iter()
        .filter(|result| result.execution_id == check.execution_id)
        .map(|result| (result.invariant_id.as_str(), result.status))
        .collect::<Vec<_>>();
    match check.completion {
        CheckCompletion::Completed => {
            if statuses
                .iter()
                .any(|(_, status)| *status != EvidenceStatus::Pass)
                || observed(check, "valid_trials") != trials
                || observed(check, "read_ok") < trials
                || observed(check, "write_ok") < trials
                || observed(check, "cas_ok") < trials
                || !markers_cover(check, scenario, trials)
                || artifact_count(check, "maelstrom-results") != trials_usize
                || artifact_count(check, "maelstrom-process-log") != trials_usize
                // Tool inputs are captured once per source tree and shared by
                // every trial, so a check names each of them exactly once
                // however many trials referenced it. Requiring one per trial
                // was the same duplication the producer used to emit and the
                // trial grouping already refuses as ambiguous; single-trial
                // profiles could not tell the two rules apart.
                || artifact_count(check, "maelstrom-runner") != 1
                || artifact_count(check, "maelstrom-binary") != 1
                || artifact_count(check, "maelstrom-tool-jar") != 1
                || artifact_count(check, "maelstrom-node-log") == 0
                || (scenario.requires_proxy()
                    && artifact_count(check, "maelstrom-proxy-binary") != 1)
                || (scenario.requires_durable_state()
                    && artifact_count(check, "maelstrom-durable-file") < trials_usize)
            {
                return Err(
                    "passing Maelstrom scenario lacks checker, operation, or fault coverage",
                );
            }
        }
        CheckCompletion::Counterexample => {
            if !valid_counterexample_attribution(&statuses) {
                return Err("Maelstrom counterexample has an invalid invariant attribution");
            }
        }
        CheckCompletion::CoverageNotReached
        | CheckCompletion::BudgetExhausted
        | CheckCompletion::Timeout => {
            if statuses
                .iter()
                .any(|(_, status)| *status != EvidenceStatus::Incomplete)
            {
                return Err("incomplete Maelstrom scenario must leave every result incomplete");
            }
        }
        CheckCompletion::HarnessError => {
            if statuses
                .iter()
                .any(|(_, status)| *status != EvidenceStatus::Error)
            {
                return Err("Maelstrom harness error must mark every result errored");
            }
        }
        // Both are frontier vocabulary and Maelstrom has no frontier: it runs a
        // fixed number of randomized trials and checks their histories. Listed
        // separately from the arms above so a future completion cannot join
        // this rejection by accident.
        CheckCompletion::FrontierExhausted => {
            return Err("Maelstrom scenario cannot claim exhaustive frontier completion");
        }
        CheckCompletion::BudgetElapsedFrontierOpen => {
            return Err("Maelstrom scenario cannot claim a model-checking frontier at all");
        }
    }
    Ok(())
}

pub(crate) fn valid_counterexample_attribution(statuses: &[(&str, EvidenceStatus)]) -> bool {
    let failed = statuses
        .iter()
        .filter(|(_, status)| *status == EvidenceStatus::Fail)
        .map(|(invariant, _)| *invariant)
        .collect::<BTreeSet<_>>();
    !failed.is_empty()
        && failed.is_subset(&BTreeSet::from(["RD-05", "RD-06"]))
        && statuses.iter().all(|(invariant, status)| {
            (*status != EvidenceStatus::Fail || matches!(*invariant, "RD-05" | "RD-06"))
                && matches!(status, EvidenceStatus::Fail | EvidenceStatus::Incomplete)
        })
}

pub(super) fn observed(check: &CheckReceipt, name: &str) -> u64 {
    check.observations.get(name).copied().unwrap_or_default()
}

fn artifact_count(check: &CheckReceipt, kind: &str) -> usize {
    check
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == kind)
        .count()
}

fn markers_cover(check: &CheckReceipt, scenario: Scenario, trials: u64) -> bool {
    match scenario {
        Scenario::Base => true,
        Scenario::Membership => {
            observed(check, "membership_enter") >= trials
                && observed(check, "membership_leave") >= trials
                && observed(check, "membership_complete") >= trials
        }
        Scenario::Restart => {
            observed(check, "restarts") >= 3 * trials
                && observed(check, "post_restart_progress") >= trials
        }
        Scenario::ApplicationCrash => {
            observed(check, "crashpoints") >= trials
                && observed(check, "post_crash_progress") >= trials
        }
        Scenario::Snapshot => {
            observed(check, "restarts") >= trials
                && observed(check, "snapshots_compacted") >= trials
                && observed(check, "snapshots_applied") >= trials
                && observed(check, "post_restart_snapshots_applied") >= trials
        }
        Scenario::LeaseIsolation => lease_markers_cover(check, trials),
    }
}

fn lease_markers_cover(check: &CheckReceipt, trials: u64) -> bool {
    observed(check, "lease_sequence_complete") == trials
        && observed(check, "lease_sequence_invalid") == 0
        && observed(check, "lease_fast_path_read_ok") == trials
        && observed(check, "lease_read_buffered") == trials
        && observed(check, "lease_expired_while_leader") == trials
        && observed(check, "lease_post_expiry_released") == trials
        && observed(check, "lease_post_expiry_handler") == trials
        && observed(check, "lease_post_expiry_unavailable") == trials
        && observed(check, "lease_post_expiry_read_served") == 0
        && observed(check, "lease_post_expiry_renewed") == 0
        && observed(check, "lease_post_expiry_unexpected_error") == 0
        && observed(check, "lease_duplicate_terminal") == 0
        && observed(check, "lease_coverage_lost") == 0
        && observed(check, "lease_history_probe_matches") == trials
        && observed(check, "lease_history_probe_mismatches") == 0
}
