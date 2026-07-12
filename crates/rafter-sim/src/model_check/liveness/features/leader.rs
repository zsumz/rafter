use std::collections::BTreeSet;

use rafter::NodeId;

use super::{
    super::driver::{
        check_soak_safety, drive_liveness_rounds_until, drive_until_quiescent_leader,
        issue_liveness_proposal, liveness_proposal_completed, soak_liveness_failure,
    },
    production_monitor_state,
};
use crate::model_check::{
    application::apply_soak_action,
    catalog,
    scheduling::SoakOperation,
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
};

pub(super) fn run_quorum_only_leader_liveness_check(
    config: SoakConfig,
    budget: usize,
) -> Result<(), SoakFailure> {
    let mut state = production_monitor_state(config, catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE)?;
    let mut trace = Vec::new();
    let mut observed_actions = BTreeSet::new();
    for peer in [NodeId(1), NodeId(2)] {
        apply_soak_action(
            &mut state,
            SoakOperation::Partition {
                a: peer,
                b: NodeId(3),
            },
        );
        trace.push(SoakAction::Partition {
            a: peer,
            b: NodeId(3),
        });
        observed_actions.insert(SoakActionKind::Partition);
    }
    check_soak_safety(&state, config, &trace)?;

    let Some(leader) = drive_until_quiescent_leader(
        &mut state,
        config,
        &mut trace,
        &mut observed_actions,
        budget,
    )?
    else {
        return Err(soak_liveness_failure(
            &state,
            config,
            &trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            format!(
                "reachable quorum did not elect a stable leader within {budget} fair-schedule rounds"
            ),
        ));
    };
    let Some(proposal_id) =
        issue_liveness_proposal(&mut state, leader, &mut trace, &mut observed_actions)
    else {
        return Err(soak_liveness_failure(
            &state,
            config,
            &trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            "quorum-only leader rejected its usability probe".to_owned(),
        ));
    };
    let completed = drive_liveness_rounds_until(
        &mut state,
        config,
        &mut trace,
        &mut observed_actions,
        budget,
        |state| liveness_proposal_completed(state, proposal_id),
    )?;
    if completed && !state.cluster.leaders().is_empty() {
        return Ok(());
    }

    Err(soak_liveness_failure(
        &state,
        config,
        &trace,
        catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
        format!(
            "reachable-quorum leader did not complete its usability probe within {budget} fair-schedule rounds"
        ),
    ))
}
