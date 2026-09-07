//! The bounded liveness feature suite run by every soak.
//!
//! Each configured feature gets its own production monitor state and round
//! budget, so one feature's execution can never shorten or extend the bound
//! another feature's report is validated against.

use std::collections::BTreeSet;

use crate::model_check::{
    catalog,
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
};

use super::super::driver::soak_liveness_round_budget;
use super::leader::run_quorum_only_leader_liveness_check;
use super::membership::run_membership_transition_liveness_check;
use super::proposal::{
    run_proposal_progress_liveness_check, run_proposal_termination_liveness_check,
};
use super::read::run_read_barrier_liveness_check;
use super::snapshot::snapshot_liveness_round_budget;
use super::transfer::run_leadership_transfer_liveness_check;
use super::{production_monitor_state, run_snapshot_catchup_liveness_check, LivenessFeatureReport};

pub(in crate::model_check::liveness) fn run_feature_liveness_checks(
    config: SoakConfig,
    observed_actions: &mut BTreeSet<SoakActionKind>,
) -> Result<Vec<LivenessFeatureReport>, SoakFailure> {
    let budget_state =
        production_monitor_state(config, catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE)?;
    let budget = soak_liveness_round_budget(&budget_state, config);
    let mut reports = run_quorum_only_leader_liveness_check(config, budget)?;
    reports.push(run_proposal_progress_liveness_check(config, budget)?);
    reports.push(run_proposal_termination_liveness_check(
        config, budget, budget,
    )?);
    if config.max_read_indexes > 0 {
        let mut state =
            production_monitor_state(config, catalog::LV_03_FEATURE_OPERATION_PROGRESS)?;
        let mut trace = Vec::<SoakAction>::new();
        let mut feature_actions = BTreeSet::new();
        let budget = soak_liveness_round_budget(&state, config);
        reports.push(run_read_barrier_liveness_check(
            &mut state,
            config,
            &mut trace,
            &mut feature_actions,
            budget,
            budget,
        )?);
        observed_actions.extend(feature_actions);
    }
    if config.max_membership_changes > 0 {
        let mut state =
            production_monitor_state(config, catalog::LV_03_FEATURE_OPERATION_PROGRESS)?;
        let mut trace = Vec::<SoakAction>::new();
        let mut feature_actions = BTreeSet::new();
        let budget = soak_liveness_round_budget(&state, config);
        reports.push(run_membership_transition_liveness_check(
            &mut state,
            config,
            &mut trace,
            &mut feature_actions,
            budget,
            budget,
        )?);
        observed_actions.extend(feature_actions);
    }
    if config.max_transfers > 0 {
        let mut state =
            production_monitor_state(config, catalog::LV_03_FEATURE_OPERATION_PROGRESS)?;
        let mut trace = Vec::<SoakAction>::new();
        let mut feature_actions = BTreeSet::new();
        let budget = soak_liveness_round_budget(&state, config);
        reports.push(run_leadership_transfer_liveness_check(
            &mut state,
            config,
            &mut trace,
            &mut feature_actions,
            budget,
            budget,
        )?);
        observed_actions.extend(feature_actions);
    }
    if config.snapshot_catchup_probe {
        let budget = snapshot_liveness_round_budget(config);
        reports.push(run_snapshot_catchup_liveness_check(config, budget)?);
    }
    Ok(reports)
}
