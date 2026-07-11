use std::collections::BTreeSet;

use crate::model_check::{
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
    state::ExplorationState,
};

mod membership;
mod read;
mod snapshot;
mod transfer;

use membership::run_membership_transition_liveness_check;
use read::run_read_barrier_liveness_check;
pub(in crate::model_check) use snapshot::run_snapshot_catchup_liveness_check;
use transfer::run_leadership_transfer_liveness_check;

pub(super) fn run_feature_liveness_checks(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    budget: usize,
) -> Result<(), SoakFailure> {
    if config.max_read_indexes > 0 {
        run_read_barrier_liveness_check(state, config, trace, observed_actions, budget)?;
    }
    if config.max_membership_changes > 0 {
        run_membership_transition_liveness_check(state, config, trace, observed_actions, budget)?;
    }
    if config.max_transfers > 0 {
        run_leadership_transfer_liveness_check(state, config, trace, observed_actions, budget)?;
    }
    if config.snapshot_catchup_probe {
        run_snapshot_catchup_liveness_check(config, budget)?;
    }
    Ok(())
}
