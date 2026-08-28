use std::collections::{BTreeMap, BTreeSet};

use rafter::NodeId;

use crate::model_check::{
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
    state::ExplorationState,
};
use crate::SimSeed;

mod budget;
mod failures;
mod fair_round;
mod leader;
mod proposals;
mod rounds;
mod schedule;

pub(in crate::model_check::liveness) use budget::soak_liveness_round_budget;
pub(in crate::model_check::liveness) use failures::{
    check_soak_safety, soak_liveness_coverage_failure, soak_liveness_harness_error,
    soak_liveness_invariant_failure, soak_transition_failure,
};
#[cfg(test)]
pub(in crate::model_check::liveness) use leader::drive_until_quiescent_leader;
pub(in crate::model_check::liveness) use leader::{
    drive_until_stable_leader, drive_until_stable_leader_from_round, has_partition,
    quiescent_leader, single_leader,
};
pub(in crate::model_check::liveness) use proposals::{
    issue_liveness_proposal, liveness_proposal_accepted, liveness_proposal_completed,
    liveness_proposal_terminal_outcome,
};
pub(in crate::model_check::liveness) use rounds::{
    drive_liveness_rounds_until_observed, drive_liveness_rounds_until_observed_from_round,
    drive_soak_liveness_round, drive_soak_liveness_round_until_terminal,
};

pub(in crate::model_check::liveness) const FAIR_SCHEDULER_POLICY_ID: &str =
    "seeded-rotating-all-node-ticks-ready-wave-permutations-v1";
pub(in crate::model_check::liveness) const FAIR_TICK_BOUND_ROUNDS: usize = 1;
pub(in crate::model_check::liveness) const FAIR_DELIVERY_BOUND_ROUNDS: usize = 1;
pub(in crate::model_check::liveness) const STABLE_LEADER_WINDOW_ROUNDS: usize = 2;
pub(in crate::model_check::liveness) const FAIR_MAX_DELIVERY_WAVES_PER_TICK: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::model_check::liveness) struct LivenessRoundBudget {
    pub(in crate::model_check::liveness) minimum_rounds: usize,
    pub(in crate::model_check::liveness) node_count: usize,
    pub(in crate::model_check::liveness) queued_messages: usize,
    pub(in crate::model_check::liveness) max_proposals: usize,
    pub(in crate::model_check::liveness) max_membership_changes: usize,
    pub(in crate::model_check::liveness) max_partitions: usize,
    pub(in crate::model_check::liveness) snapshot_catchup_probe: bool,
    pub(in crate::model_check::liveness) base_rounds: usize,
    pub(in crate::model_check::liveness) phase_count: usize,
    pub(in crate::model_check::liveness) fixed_rounds: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::model_check::liveness) struct BoundedRun {
    pub(in crate::model_check::liveness) completed: bool,
    pub(in crate::model_check::liveness) rounds_used: usize,
    pub(in crate::model_check::liveness) observer_held: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::model_check::liveness) struct LeaderConvergence {
    pub(in crate::model_check::liveness) leader: NodeId,
    pub(in crate::model_check::liveness) rounds_used: usize,
    pub(in crate::model_check::liveness) stable_rounds: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::model_check::liveness) enum ProposalTerminalOutcome {
    Committed,
    Rejected,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FairRoundExecution {
    expected_ticks: usize,
    ticks_executed: usize,
    ready_at_boundary: usize,
    boundary_deliveries: usize,
    observer_held: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BoundedFairnessMonitor {
    tick_bound: usize,
    delivery_bound: usize,
    unticked_rounds: BTreeMap<NodeId, usize>,
    undelivered_rounds: usize,
}

impl BoundedFairnessMonitor {
    fn new(tick_bound: usize, delivery_bound: usize) -> Self {
        assert!(tick_bound > 0, "tick fairness bound must be positive");
        assert!(
            delivery_bound > 0,
            "delivery fairness bound must be positive"
        );
        Self {
            tick_bound,
            delivery_bound,
            unticked_rounds: BTreeMap::new(),
            undelivered_rounds: 0,
        }
    }

    fn observe_round(
        &mut self,
        expected_ticks: &[NodeId],
        executed_ticks: &[NodeId],
        ready_at_boundary: usize,
        boundary_deliveries: usize,
    ) -> Result<(), &'static str> {
        let executed = executed_ticks.iter().copied().collect::<BTreeSet<_>>();
        for node_id in expected_ticks {
            let age = self.unticked_rounds.entry(*node_id).or_default();
            if executed.contains(node_id) {
                *age = 0;
            } else {
                *age = age.saturating_add(1);
                if *age >= self.tick_bound {
                    return Err("tick starvation exceeded the bounded-fair round limit");
                }
            }
        }

        if boundary_deliveries >= ready_at_boundary {
            self.undelivered_rounds = 0;
        } else {
            self.undelivered_rounds = self.undelivered_rounds.saturating_add(1);
            if self.undelivered_rounds >= self.delivery_bound {
                return Err("delivery starvation exceeded the bounded-fair round limit");
            }
        }
        Ok(())
    }
}

fn observe_bounded_fairness_round(
    monitor: &mut BoundedFairnessMonitor,
    expected_ticks: &[NodeId],
    executed_ticks: &[NodeId],
    ready_at_boundary: usize,
    boundary_deliveries: usize,
) -> Result<(), &'static str> {
    monitor.observe_round(
        expected_ticks,
        executed_ticks,
        ready_at_boundary,
        boundary_deliveries,
    )
}

pub(in crate::model_check::liveness) struct FairRoundDriver {
    fairness: BoundedFairnessMonitor,
    schedule_seed: SimSeed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::model_check::liveness) struct LivenessScheduleWindow {
    pub(in crate::model_check::liveness) round_offset: usize,
    pub(in crate::model_check::liveness) budget: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::model_check::liveness) struct StableLeaderGuard {
    expected: NodeId,
    round_limit: usize,
    observations: usize,
}

#[allow(
    dead_code,
    reason = "held as the documented driver entry for liveness scenarios that attach on demand"
)]
pub(in crate::model_check::liveness) fn drive_liveness_rounds_until(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    budget: usize,
    complete: impl FnMut(&ExplorationState) -> bool,
) -> Result<bool, SoakFailure> {
    Ok(drive_liveness_rounds_until_observed(
        state,
        config,
        trace,
        observed_actions,
        budget,
        complete,
        |_| true,
    )?
    .completed)
}

fn ensure_delivery_frontier_drained(ready_after_final_wave: usize) -> Result<(), &'static str> {
    if ready_after_final_wave == 0 {
        Ok(())
    } else {
        Err("delivery-wave cap exhausted with ready messages still queued")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rafter_invariant_test::{oracle_assert, oracle_expect_err};

    #[::rafter_invariant_test::detector_test]
    fn bounded_fairness_detector_rejects_positive_bound_tick_starvation() {
        let mut monitor = BoundedFairnessMonitor::new(2, 3);
        observe_bounded_fairness_round(&mut monitor, &[NodeId(1), NodeId(2)], &[NodeId(1)], 0, 0)
            .expect("one missed round remains inside the positive bound");
        let error = oracle_expect_err!(
            observe_bounded_fairness_round(
                &mut monitor,
                &[NodeId(1), NodeId(2)],
                &[NodeId(1)],
                0,
                0,
            ),
            "the second missed tick must exhaust the positive bound"
        );

        oracle_assert!(error.contains("tick starvation"));
    }
}

#[cfg(test)]
#[path = "driver/tests.rs"]
mod unit_tests;
