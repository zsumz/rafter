//! The post-heal soak liveness check the registry pins by name.
//!
//! LV-01 owes one bounded run: inject and heal a real fault, converge on a
//! leader, and commit under that leader, each stage within a budget derived
//! from the configuration rather than a constant. The fault cycle, the report
//! rendering, and the per-feature checks live in the submodules.

use std::collections::BTreeSet;

use super::{
    catalog,
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
    state::ExplorationState,
};

pub(in crate::model_check::liveness) mod driver;
mod fault_cycle;
mod features;
mod post_heal;
mod reports;

use driver::{drive_until_stable_leader, soak_liveness_invariant_failure, LivenessRoundBudget};
use fault_cycle::create_and_heal_post_heal_fault;
use features::run_feature_liveness_checks;
pub(in crate::model_check) use features::LivenessFeatureReport;
use post_heal::{drive_until_stable_leader_commits, PostHealBudgets};
use reports::{successful_post_heal_convergence_report, successful_post_heal_usability_report};

#[cfg(test)]
pub(super) use features::run_snapshot_catchup_liveness_check;

const MIN_SOAK_LIVENESS_ROUNDS: usize = 128;
const POST_HEAL_FAULT_EXERCISE_ROUNDS: usize = 1;

pub(super) fn run_soak_liveness_check(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
) -> Result<Vec<LivenessFeatureReport>, SoakFailure> {
    run_soak_liveness_check_with_budget_overrides(
        state,
        config,
        trace,
        observed_actions,
        None,
        None,
    )
}

pub(in crate::model_check) fn run_soak_liveness_check_with_budget_overrides(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    convergence_budget_override: Option<usize>,
    usability_budget_override: Option<usize>,
) -> Result<Vec<LivenessFeatureReport>, SoakFailure> {
    let fault_cycle = create_and_heal_post_heal_fault(state, config, trace, observed_actions)?;
    let convergence_round_budget = LivenessRoundBudget::capture(state, config, 1)
        .with_fixed_rounds(fault_cycle.partitioned_rounds);
    let usability_round_budget = LivenessRoundBudget::capture(state, config, 1);
    let convergence_budget =
        convergence_budget_override.unwrap_or(convergence_round_budget.base_rounds);
    let usability_budget = usability_budget_override.unwrap_or(usability_round_budget.base_rounds);

    let Some(convergence) =
        drive_until_stable_leader(state, config, trace, observed_actions, convergence_budget)?
    else {
        return Err(soak_liveness_invariant_failure(
            state,
            config,
            trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            format!("no leader elected within {convergence_budget} post-heal convergence rounds"),
        ));
    };
    let usability = drive_until_stable_leader_commits(
        state,
        config,
        trace,
        observed_actions,
        convergence,
        PostHealBudgets {
            schedule_round_offset: convergence.rounds_used,
            convergence_rounds: convergence_budget,
            usability_rounds: usability_budget,
        },
    )?;

    let convergence_report = successful_post_heal_convergence_report(
        state,
        convergence_round_budget,
        usability.convergence,
        fault_cycle,
    );

    let usability_report = successful_post_heal_usability_report(
        state,
        usability_round_budget,
        usability.convergence,
        usability.completion,
        usability.proposal_id,
        usability.accepted_proposal,
    );
    let mut reports = vec![convergence_report, usability_report];
    reports.extend(run_feature_liveness_checks(config, observed_actions)?);
    Ok(reports)
}
