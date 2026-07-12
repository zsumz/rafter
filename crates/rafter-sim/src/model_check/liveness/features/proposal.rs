use std::collections::BTreeSet;

use rafter::NodeId;

use super::{
    super::driver::{
        check_soak_safety, issue_liveness_proposal, liveness_proposal_terminated,
        soak_liveness_failure,
    },
    production_monitor_state,
};
use crate::model_check::{
    application::apply_soak_action,
    catalog,
    helpers::{deliver_all_in_state, elect_node_one_in_state},
    scheduling::SoakOperation,
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
};

pub(super) fn run_proposal_termination_liveness_check(
    config: SoakConfig,
    budget: usize,
) -> Result<(), SoakFailure> {
    let mut state = production_monitor_state(config, catalog::LV_02_PROPOSAL_PROGRESS)?;
    elect_node_one_in_state(&mut state);
    deliver_all_in_state(&mut state);

    let mut trace = Vec::new();
    let mut observed_actions = BTreeSet::new();
    let Some(proposal_id) =
        issue_liveness_proposal(&mut state, NodeId(1), &mut trace, &mut observed_actions)
    else {
        return Err(soak_liveness_failure(
            &state,
            config,
            &trace,
            catalog::LV_02_PROPOSAL_PROGRESS,
            "proposal-termination monitor could not establish an accepted proposal".to_owned(),
        ));
    };

    for peer in [NodeId(2), NodeId(3)] {
        apply_soak_action(
            &mut state,
            SoakOperation::Partition {
                a: NodeId(1),
                b: peer,
            },
        );
        trace.push(SoakAction::Partition {
            a: NodeId(1),
            b: peer,
        });
        observed_actions.insert(SoakActionKind::Partition);
    }
    check_soak_safety(&state, config, &trace)?;

    for _ in 0..budget {
        if liveness_proposal_terminated(&state, proposal_id) {
            return Ok(());
        }
        apply_soak_action(&mut state, SoakOperation::Tick(NodeId(1)));
        trace.push(SoakAction::Tick(NodeId(1)));
        observed_actions.insert(SoakActionKind::Tick);
        check_soak_safety(&state, config, &trace)?;
    }
    if liveness_proposal_terminated(&state, proposal_id) {
        return Ok(());
    }

    Err(soak_liveness_failure(
        &state,
        config,
        &trace,
        catalog::LV_02_PROPOSAL_PROGRESS,
        format!(
            "accepted proposal {} did not reach an explicit terminal state within {budget} authority-loss rounds",
            proposal_id.0
        ),
    ))
}
