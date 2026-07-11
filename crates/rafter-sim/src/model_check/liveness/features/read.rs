use std::collections::BTreeSet;

use super::super::driver::{
    check_soak_safety, drive_liveness_rounds_until, drive_until_quiescent_leader,
    soak_liveness_failure,
};
use crate::model_check::{
    application::apply_to_state,
    catalog,
    scheduling::Operation,
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
    state::{ClientReadOutcome, ExplorationState},
};

pub(super) fn run_read_barrier_liveness_check(
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
            format!("no leader elected within {budget} read-barrier liveness rounds"),
        ));
    };

    let request_id = state.read_indexes_issued + 1;
    apply_to_state(
        state,
        Operation::ReadIndex {
            to: leader,
            request_id,
        },
    );
    trace.push(SoakAction::ReadIndex {
        to: leader,
        request_id,
    });
    observed_actions.insert(SoakActionKind::ReadIndex);
    check_soak_safety(state, config, trace)?;

    if drive_liveness_rounds_until(state, config, trace, observed_actions, budget, |state| {
        liveness_read_completed(state, request_id)
    })? {
        Ok(())
    } else {
        Err(soak_liveness_failure(
            state,
            config,
            trace,
            catalog::LV_03_FEATURE_OPERATION_PROGRESS,
            format!(
                "read-index request {request_id} to leader {leader} did not complete within {budget} post-heal rounds"
            ),
        ))
    }
}

fn liveness_read_completed(state: &ExplorationState, request_id: u64) -> bool {
    state
        .client_history
        .reads
        .get(&request_id)
        .is_some_and(|read| matches!(read.outcome, ClientReadOutcome::Completed { .. }))
}
