use std::collections::BTreeSet;

use rafter::NodeId;

use super::super::driver::{
    check_soak_safety, drive_liveness_rounds_until, drive_until_quiescent_leader, quiescent_leader,
    soak_liveness_failure,
};
use crate::model_check::{
    catalog,
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
    state::ExplorationState,
};

pub(super) fn run_leadership_transfer_liveness_check(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    budget: usize,
) -> Result<(), SoakFailure> {
    let Some(leader) =
        drive_until_quiescent_leader(state, config, trace, observed_actions, budget)?
    else {
        return Err(soak_liveness_failure(
            state,
            config,
            trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            format!("no leader elected within {budget} leadership-transfer liveness rounds"),
        ));
    };

    let Some(target) = leadership_transfer_liveness_target(state, leader) else {
        return Ok(());
    };

    issue_liveness_transfer(state, leader, target);
    trace.push(SoakAction::Transfer {
        from: leader,
        target,
    });
    observed_actions.insert(SoakActionKind::Transfer);
    check_soak_safety(state, config, trace)?;

    if drive_liveness_rounds_until(state, config, trace, observed_actions, budget, |state| {
        quiescent_leader(state) == Some(target)
    })? {
        Ok(())
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
    let membership = state.cluster.effective_membership(leader);
    let leader_last_log_index = state.cluster.last_log_index(leader);
    state
        .cluster
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
    state.cluster.transfer_leadership(leader, target);
    state.transfers_issued += 1;
    state.refresh_commit_floors();
    state.refresh_client_history();
    state.refresh_log_history();
    state.refresh_committed_prefixes();
    state.record_leader_completeness_observation();
}
