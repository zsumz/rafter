use std::collections::BTreeSet;

use super::{
    catalog,
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
    state::ExplorationState,
};

pub(in crate::model_check::liveness) mod driver;
mod features;

use driver::{
    check_soak_safety, drive_soak_liveness_round, drive_until_quiescent_leader, has_partition,
    issue_liveness_proposal, liveness_proposal_completed, quiescent_leader, soak_liveness_failure,
    soak_liveness_round_budget,
};
use features::run_feature_liveness_checks;

#[cfg(test)]
pub(super) use features::run_snapshot_catchup_liveness_check;

const MIN_SOAK_LIVENESS_ROUNDS: usize = 128;

pub(super) fn run_soak_liveness_check(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
) -> Result<(), SoakFailure> {
    if has_partition(&state.cluster) {
        state.cluster.heal_partitions();
        state.refresh_commit_floors();
        state.refresh_client_history();
        trace.push(SoakAction::Heal);
        observed_actions.insert(SoakActionKind::Heal);
        check_soak_safety(state, config, trace)?;
    }

    let budget = soak_liveness_round_budget(state, config);
    let Some(leader) =
        drive_until_quiescent_leader(state, config, trace, observed_actions, budget)?
    else {
        return Err(soak_liveness_failure(
            state,
            config,
            trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            format!("no leader elected within {budget} post-heal convergence rounds"),
        ));
    };

    let mut accepted_proposal = issue_liveness_proposal(state, leader, trace, observed_actions);
    check_soak_safety(state, config, trace)?;
    for round in 0..budget {
        if accepted_proposal
            .is_some_and(|proposal_id| liveness_proposal_completed(state, proposal_id))
            && !state.cluster.leaders().is_empty()
        {
            run_feature_liveness_checks(state, config, trace, observed_actions, budget)?;
            return Ok(());
        }
        if accepted_proposal.is_none() {
            if let Some(leader) = quiescent_leader(state) {
                accepted_proposal = issue_liveness_proposal(state, leader, trace, observed_actions);
                check_soak_safety(state, config, trace)?;
                if accepted_proposal
                    .is_some_and(|proposal_id| liveness_proposal_completed(state, proposal_id))
                {
                    run_feature_liveness_checks(state, config, trace, observed_actions, budget)?;
                    return Ok(());
                }
            }
        }
        drive_soak_liveness_round(state, trace, observed_actions, round);
        check_soak_safety(state, config, trace)?;
    }

    let message = match (state.cluster.leaders().is_empty(), accepted_proposal) {
        (true, _) => format!("no leader remained after {budget} liveness proposal rounds"),
        (false, Some(proposal_id)) => format!(
            "accepted liveness proposal {} did not commit within {budget} post-heal rounds",
            proposal_id.0
        ),
        (false, None) => {
            format!("no liveness proposal was accepted within {budget} post-heal rounds")
        }
    };
    let invariant = if state.cluster.leaders().is_empty() {
        catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE
    } else {
        catalog::LV_02_PROPOSAL_PROGRESS
    };
    Err(soak_liveness_failure(
        state, config, trace, invariant, message,
    ))
}
