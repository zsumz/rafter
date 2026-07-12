use std::collections::BTreeSet;

use rafter::NodeId;

use super::super::driver::{
    check_soak_safety, drive_liveness_rounds_until_observed, drive_until_stable_leader,
    quiescent_leader, soak_liveness_failure, LivenessRoundBudget,
};
use super::{
    FaultStateRequirement, LivenessFeatureReport, LivenessPreconditionProbe, LivenessPreconditions,
};
use crate::model_check::{
    catalog,
    scheduling::Operation,
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
    state::apply_to_state,
    state::ExplorationState,
};

pub(super) fn run_leadership_transfer_liveness_check(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    budget: usize,
) -> Result<LivenessFeatureReport, SoakFailure> {
    let round_budget = LivenessRoundBudget::capture(state, config, 2);
    let Some(convergence) =
        drive_until_stable_leader(state, config, trace, observed_actions, budget)?
    else {
        return Err(soak_liveness_failure(
            state,
            config,
            trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            format!("no leader elected within {budget} leadership-transfer liveness rounds"),
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

    let Some(target) = leadership_transfer_liveness_target(state, leader) else {
        return Err(soak_liveness_failure(
            state,
            config,
            trace,
            catalog::LV_03_FEATURE_OPERATION_PROGRESS,
            "leadership-transfer liveness precondition was not reached: no caught-up voter"
                .to_owned(),
        ));
    };

    issue_liveness_transfer(state, leader, target);
    trace.push(SoakAction::Transfer {
        from: leader,
        target,
    });
    observed_actions.insert(SoakActionKind::Transfer);
    check_soak_safety(state, config, trace)?;

    let completion = drive_liveness_rounds_until_observed(
        state,
        config,
        trace,
        observed_actions,
        budget,
        |state| quiescent_leader(state) == Some(target),
        |_| true,
    )?;
    if completion.completed {
        Ok(LivenessFeatureReport {
            invariant_id: "LV-03",
            feature_id: "leadership-transfer",
            scenario_id: "caught-up-voter-transfer-v1",
            observation_id: "completed_target_leadership_transfers",
            preconditions,
            round_budget,
            round_limit: budget.saturating_mul(2),
            rounds_used: convergence
                .rounds_used
                .saturating_add(completion.rounds_used),
            fault_cycle: None,
            stable_leader: None,
            proposal: None,
        })
    } else {
        Err(soak_liveness_failure(
            state,
            config,
            trace,
            catalog::LV_03_FEATURE_OPERATION_PROGRESS,
            format!(
                "leadership transfer {leader}->{target} did not make the target the quiescent leader within {budget} post-heal rounds"
            ),
        ))
    }
}

fn leadership_transfer_liveness_target(state: &ExplorationState, leader: NodeId) -> Option<NodeId> {
    let membership = state.cluster().effective_membership(leader);
    let leader_last_log_index = state.cluster().last_log_index(leader);
    state
        .cluster()
        .leader_replication_progress(leader)
        .into_iter()
        .filter(|progress| {
            progress.follower_id != leader
                && membership.contains_voter(progress.follower_id)
                && progress.match_index == leader_last_log_index
        })
        .map(|progress| progress.follower_id)
        .min_by_key(|node_id| node_id.0)
}

fn issue_liveness_transfer(state: &mut ExplorationState, leader: NodeId, target: NodeId) {
    apply_to_state(
        state,
        Operation::Transfer {
            from: leader,
            target,
        },
    );
}
