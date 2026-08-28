//! The membership-transition liveness detector the registry pins by name.
//!
//! LV-03 owes a stable leader, a removable voter, and then a joint-consensus
//! removal that reaches an explicit terminal outcome inside its round budget;
//! a missing premise is reported as uncovered rather than as a violation. The
//! monitor and the driving loop live in the private submodules.

use std::collections::BTreeSet;

use super::super::driver::{
    drive_until_stable_leader, soak_liveness_coverage_failure, LivenessRoundBudget,
};
use super::{
    FaultStateRequirement, LivenessFeatureReport, LivenessPreconditionProbe, LivenessPreconditions,
    TerminalRecorderMode,
};
use crate::model_check::{
    catalog,
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
    state::ExplorationState,
};

mod monitor;
mod operation;

use monitor::{membership_liveness_target, MembershipMonitor};
use operation::run_membership_operation;

#[cfg(test)]
use monitor::membership_rejection_observed;
#[cfg(test)]
use operation::operation_rounds;

pub(super) fn run_membership_transition_liveness_check(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    convergence_budget: usize,
    operation_budget: usize,
) -> Result<LivenessFeatureReport, SoakFailure> {
    run_membership_transition_liveness_detector(
        state,
        config,
        trace,
        observed_actions,
        convergence_budget,
        operation_budget,
        TerminalRecorderMode::Production,
    )
}

pub(super) fn run_membership_transition_liveness_detector(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    convergence_budget: usize,
    operation_budget: usize,
    recorder_mode: TerminalRecorderMode,
) -> Result<LivenessFeatureReport, SoakFailure> {
    let round_budget = LivenessRoundBudget::capture(state, config, 2);
    let Some(convergence) =
        drive_until_stable_leader(state, config, trace, observed_actions, convergence_budget)?
    else {
        return Err(soak_liveness_coverage_failure(
            state,
            config,
            trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            format!(
                "no leader elected within {convergence_budget} membership-transition liveness rounds"
            ),
        ));
    };
    let leader = convergence.leader;
    let preconditions = LivenessPreconditions::capture(
        state,
        LivenessPreconditionProbe {
            leader: Some(leader),
            fault_requirement: FaultStateRequirement::Stopped,
            stable_leader_observed: None,
            accepted_proposal_observed: None,
            authority_loss_observed: None,
        },
    );

    let Some((removed_voter, target)) = membership_liveness_target(state, leader) else {
        return Err(soak_liveness_coverage_failure(
            state,
            config,
            trace,
            catalog::LV_03_FEATURE_OPERATION_PROGRESS,
            "membership liveness precondition was not reached: no removable voter".to_owned(),
        ));
    };

    let monitor = MembershipMonitor {
        leader,
        removed_voter,
        target,
        rejection_floor: state.cluster().proposal_rejections().len(),
        convergence_budget,
        operation_budget,
        round_budget,
        convergence_rounds: convergence.rounds_used,
        preconditions,
        recorder_mode,
    };
    run_membership_operation(state, config, trace, observed_actions, &monitor)
}

#[cfg(test)]
#[path = "membership_test.rs"]
mod tests;
