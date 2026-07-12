use std::collections::BTreeSet;

use rafter::{NodeConfig, NodeConfigError, NodeId};

use crate::model_check::{
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
    state::ExplorationState,
};
use crate::Cluster;

use super::driver::soak_liveness_failure;

mod leader;
#[cfg(test)]
mod leader_tests;
mod membership;
mod proposal;
#[cfg(test)]
mod proposal_tests;
mod read;
mod snapshot;
mod transfer;

use leader::run_quorum_only_leader_liveness_check;
use membership::run_membership_transition_liveness_check;
use proposal::run_proposal_termination_liveness_check;
use read::run_read_barrier_liveness_check;
pub(in crate::model_check) use snapshot::run_snapshot_catchup_liveness_check;
use transfer::run_leadership_transfer_liveness_check;

fn production_monitor_state(
    config: SoakConfig,
    invariant: &'static str,
) -> Result<ExplorationState, SoakFailure> {
    match production_configs() {
        Ok(configs) => Ok(ExplorationState::new(Cluster::new_with_seed(
            configs,
            config.seed,
        ))),
        Err(error) => {
            let empty = ExplorationState::new(Cluster::new_with_seed(Vec::new(), config.seed));
            Err(soak_liveness_failure(
                &empty,
                config,
                &[],
                invariant,
                format!("invalid production liveness configuration: {error}"),
            ))
        }
    }
}

fn production_configs() -> Result<Vec<NodeConfig>, NodeConfigError> {
    [1_u64, 2, 3]
        .into_iter()
        .map(|id| {
            NodeConfig::new(
                NodeId(id),
                [1_u64, 2, 3]
                    .into_iter()
                    .filter(|peer| *peer != id)
                    .map(NodeId)
                    .collect(),
                3,
            )
        })
        .collect()
}

pub(super) fn run_feature_liveness_checks(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    budget: usize,
) -> Result<(), SoakFailure> {
    run_quorum_only_leader_liveness_check(config, budget)?;
    run_proposal_termination_liveness_check(config, budget)?;
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
