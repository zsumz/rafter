//! Bounded round loops driven by the fair scheduler.
//!
//! Each loop runs at most its budgeted rounds, checks commit safety after
//! every round, and reports whether the premise held throughout, so a monitor
//! can distinguish a real completion from an abandoned observation.

use std::collections::BTreeSet;

use crate::model_check::{
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
    state::ExplorationState,
};
use crate::SimSeed;

use super::fair_round::drive_soak_liveness_round_observed;
use super::{
    check_soak_safety, soak_liveness_harness_error, BoundedFairnessMonitor, BoundedRun,
    FairRoundDriver, FairRoundExecution, LivenessScheduleWindow, FAIR_DELIVERY_BOUND_ROUNDS,
    FAIR_TICK_BOUND_ROUNDS,
};

impl LivenessScheduleWindow {
    pub(in crate::model_check::liveness) const fn new(round_offset: usize, budget: usize) -> Self {
        Self {
            round_offset,
            budget,
        }
    }
}

impl FairRoundDriver {
    pub(in crate::model_check::liveness) fn new(schedule_seed: SimSeed) -> Self {
        Self {
            fairness: BoundedFairnessMonitor::new(
                FAIR_TICK_BOUND_ROUNDS,
                FAIR_DELIVERY_BOUND_ROUNDS,
            ),
            schedule_seed,
        }
    }

    pub(super) fn drive(
        &mut self,
        state: &mut ExplorationState,
        trace: &mut Vec<SoakAction>,
        observed_actions: &mut BTreeSet<SoakActionKind>,
        round: usize,
        observe: &mut dyn FnMut(&ExplorationState) -> bool,
    ) -> Result<FairRoundExecution, &'static str> {
        drive_soak_liveness_round_observed(
            state,
            trace,
            observed_actions,
            round,
            observe,
            &mut self.fairness,
            self.schedule_seed,
        )
    }
}

pub(in crate::model_check::liveness) fn drive_liveness_rounds_until_observed(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    budget: usize,
    mut complete: impl FnMut(&ExplorationState) -> bool,
    mut observe: impl FnMut(&ExplorationState) -> bool,
) -> Result<BoundedRun, SoakFailure> {
    drive_liveness_rounds_until_observed_from_round(
        state,
        config,
        trace,
        observed_actions,
        LivenessScheduleWindow::new(0, budget),
        &mut complete,
        &mut observe,
    )
}

pub(in crate::model_check::liveness) fn drive_liveness_rounds_until_observed_from_round(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    schedule: LivenessScheduleWindow,
    mut complete: impl FnMut(&ExplorationState) -> bool,
    mut observe: impl FnMut(&ExplorationState) -> bool,
) -> Result<BoundedRun, SoakFailure> {
    if complete(state) {
        return Ok(BoundedRun {
            completed: true,
            rounds_used: 0,
            observer_held: true,
        });
    }
    let mut fair_rounds = FairRoundDriver::new(config.seed);
    for elapsed_round in 0..schedule.budget {
        let schedule_round = schedule.round_offset.saturating_add(elapsed_round);
        let mut completion_latched = false;
        let mut premise_held = true;
        let mut observe_until_terminal = |state: &ExplorationState| {
            if completion_latched {
                return true;
            }
            if complete(state) {
                completion_latched = true;
                return true;
            }
            let held = observe(state);
            premise_held &= held;
            held
        };
        let execution = fair_rounds
            .drive(
                state,
                trace,
                observed_actions,
                schedule_round,
                &mut observe_until_terminal,
            )
            .map_err(|message| soak_liveness_harness_error(state, config, trace, message))?;
        check_soak_safety(state, config, trace)?;
        if !premise_held || !execution.observer_held {
            return Ok(BoundedRun {
                completed: false,
                rounds_used: elapsed_round + 1,
                observer_held: false,
            });
        }
        if completion_latched || complete(state) {
            return Ok(BoundedRun {
                completed: true,
                rounds_used: elapsed_round + 1,
                observer_held: true,
            });
        }
    }
    Ok(BoundedRun {
        completed: false,
        rounds_used: schedule.budget,
        observer_held: true,
    })
}

pub(in crate::model_check::liveness) fn drive_soak_liveness_round(
    fair_rounds: &mut FairRoundDriver,
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    round: usize,
) -> Result<(), SoakFailure> {
    fair_rounds
        .drive(state, trace, observed_actions, round, &mut always_observe)
        .map(|_| ())
        .map_err(|message| soak_liveness_harness_error(state, config, trace, message))
}

pub(in crate::model_check::liveness) fn drive_soak_liveness_round_until_terminal(
    fair_rounds: &mut FairRoundDriver,
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    round: usize,
    mut terminal: impl FnMut(&ExplorationState) -> bool,
) -> Result<bool, SoakFailure> {
    let mut terminal_latched = terminal(state);
    let mut latch_terminal = |state: &ExplorationState| {
        terminal_latched |= terminal(state);
        true
    };
    fair_rounds
        .drive(state, trace, observed_actions, round, &mut latch_terminal)
        .map_err(|message| soak_liveness_harness_error(state, config, trace, message))?;
    Ok(terminal_latched)
}

pub(super) fn always_observe(_: &ExplorationState) -> bool {
    true
}
