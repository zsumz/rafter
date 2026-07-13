use std::collections::BTreeSet;

use rafter::NodeConfig;

use crate::Cluster;

use super::{
    invariants::run_replay_check,
    liveness::run_soak_liveness_check,
    scheduling::{enabled_soak_actions, soak_preferred_kind},
    soak::{SoakAction, SoakConfig, SoakFailure, SoakSummary},
    state::try_apply_soak_action,
    state::ExplorationState,
    ReplayCheck,
};

/// Runs a deterministic randomized Raft simulator soak.
///
/// # Errors
///
/// Returns [`SoakFailure`] when any step violates the commit-safety invariant
/// suite.
pub fn run_raft_random_soak(
    configs: Vec<NodeConfig>,
    config: SoakConfig,
) -> Result<SoakSummary, SoakFailure> {
    let liveness_configs = configs.clone();
    let mut state = ExplorationState::new(Cluster::new_with_seed(configs, config.seed));
    let mut trace = Vec::new();
    let mut observed_actions = BTreeSet::new();

    for step in 0..config.steps {
        let actions = enabled_soak_actions(&state, config).map_err(|error| SoakFailure {
            seed: config.seed,
            step: step + 1,
            trace: trace.clone(),
            failure: Box::new(error.into_failure(state.cluster(), &[])),
        })?;
        let preferred_kind = soak_preferred_kind(step);
        let candidates = actions
            .iter()
            .enumerate()
            .filter_map(|(index, action)| (action.trace.kind() == preferred_kind).then_some(index))
            .collect::<Vec<_>>();
        let mut action_index = if candidates.is_empty() {
            state.scheduler_index(actions.len())
        } else {
            candidates[state.scheduler_index(candidates.len())]
        };
        // Tick-rate skew: re-draw tick targets so the skewed node ticks
        // `weight`-to-one against each peer, deterministically.
        if let (Some((skewed, weight)), SoakAction::Tick(_)) =
            (config.tick_skew, &actions[action_index].trace)
        {
            let peers = state.cluster().nodes.len().saturating_sub(1);
            if state.scheduler_index(weight as usize + peers) < weight as usize {
                if let Some(skewed_index) = actions.iter().position(
                    |action| matches!(action.trace, SoakAction::Tick(node) if node == skewed),
                ) {
                    action_index = skewed_index;
                }
            }
        }
        let action = actions[action_index].clone();
        try_apply_soak_action(&mut state, action.operation).map_err(|failure| {
            let mut failure_trace = trace.clone();
            failure_trace.push(action.trace.clone());
            SoakFailure {
                seed: config.seed,
                step: step + 1,
                trace: failure_trace,
                failure: Box::new(failure),
            }
        })?;
        observed_actions.insert(action.trace.kind());
        trace.push(action.trace);

        if let Err(failure) = run_replay_check(&state, ReplayCheck::CommitSafety, &[]) {
            return Err(SoakFailure {
                seed: config.seed,
                step: step + 1,
                trace,
                failure: Box::new(failure),
            });
        }
    }

    let mut liveness_state =
        ExplorationState::new(Cluster::new_with_seed(liveness_configs, config.seed));
    let mut liveness_trace = Vec::new();
    let mut liveness_actions = BTreeSet::new();
    let liveness_reports = run_soak_liveness_check(
        &mut liveness_state,
        config,
        &mut liveness_trace,
        &mut liveness_actions,
    )?;
    trace.extend(liveness_trace);
    observed_actions.extend(liveness_actions);

    Ok(SoakSummary::from_trace(
        config,
        &trace,
        &observed_actions,
        liveness_reports,
    ))
}
