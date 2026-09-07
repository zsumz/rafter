//! Leader convergence monitors over bounded-fair rounds.
//!
//! A leader counts as stable only when the same node holds authority through
//! every observation of a full window, so a heartbeat landing mid-window
//! cannot be mistaken for a re-election.

use std::collections::BTreeSet;

use rafter::NodeId;

use crate::model_check::{
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
    state::ExplorationState,
};
use crate::Cluster;

use super::{
    check_soak_safety, soak_liveness_harness_error, FairRoundDriver, LeaderConvergence,
    StableLeaderGuard, STABLE_LEADER_WINDOW_ROUNDS,
};

impl StableLeaderGuard {
    pub(in crate::model_check::liveness) const fn new(
        expected: NodeId,
        round_limit: usize,
    ) -> Self {
        Self {
            expected,
            round_limit,
            observations: 0,
        }
    }

    pub(in crate::model_check::liveness) fn observe(
        &mut self,
        observed: Option<NodeId>,
    ) -> Result<(), String> {
        self.observations = self.observations.saturating_add(1);
        if observed == Some(self.expected) {
            return Ok(());
        }
        Err(format!(
            "stable leader {} was replaced by {:?} during observation {} of {}",
            self.expected, observed, self.observations, self.round_limit
        ))
    }
}

#[cfg(test)]
pub(in crate::model_check::liveness) fn drive_until_quiescent_leader(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    budget: usize,
) -> Result<Option<NodeId>, SoakFailure> {
    Ok(
        drive_until_stable_leader(state, config, trace, observed_actions, budget)?
            .map(|evidence| evidence.leader),
    )
}

pub(in crate::model_check::liveness) fn drive_until_stable_leader(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    budget: usize,
) -> Result<Option<LeaderConvergence>, SoakFailure> {
    drive_until_stable_leader_from_round(state, config, trace, observed_actions, 0, budget)
}

pub(in crate::model_check::liveness) fn drive_until_stable_leader_from_round(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    schedule_round_offset: usize,
    budget: usize,
) -> Result<Option<LeaderConvergence>, SoakFailure> {
    let mut stable_leader = None;
    let mut stable_rounds = 0usize;
    let mut fair_rounds = FairRoundDriver::new(config.seed);
    for elapsed_round in 0..budget {
        let schedule_round = schedule_round_offset.saturating_add(elapsed_round);
        let round_candidate = single_leader(state);
        let mut candidate_held = round_candidate.is_some();
        let mut observe_candidate = |state: &ExplorationState| {
            let held = round_candidate.is_some() && single_leader(state) == round_candidate;
            candidate_held &= held;
            held
        };
        let execution = fair_rounds
            .drive(
                state,
                trace,
                observed_actions,
                schedule_round,
                &mut observe_candidate,
            )
            .map_err(|message| soak_liveness_harness_error(state, config, trace, message))?;
        check_soak_safety(state, config, trace)?;

        if candidate_held && execution.observer_held {
            if stable_leader == round_candidate {
                stable_rounds = stable_rounds.saturating_add(1);
            } else {
                stable_leader = round_candidate;
                stable_rounds = 1;
            }
        } else {
            stable_leader = single_leader(state);
            stable_rounds = 0;
        }

        if stable_rounds >= STABLE_LEADER_WINDOW_ROUNDS {
            return Ok(stable_leader.map(|leader| LeaderConvergence {
                leader,
                rounds_used: elapsed_round + 1,
                stable_rounds,
            }));
        }
    }
    Ok(None)
}

pub(in crate::model_check::liveness) fn quiescent_leader(
    state: &ExplorationState,
) -> Option<NodeId> {
    state
        .cluster()
        .network
        .is_empty()
        .then(|| single_leader(state))?
}

pub(in crate::model_check::liveness) fn single_leader(state: &ExplorationState) -> Option<NodeId> {
    let leaders = state.cluster().leaders();
    (leaders.len() == 1).then(|| leaders[0])
}

pub(in crate::model_check::liveness) fn has_partition(cluster: &Cluster) -> bool {
    cluster
        .nodes
        .keys()
        .any(|a| cluster.nodes.keys().any(|b| cluster.partitioned(*a, *b)))
}
