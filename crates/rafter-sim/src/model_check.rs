use std::collections::{BTreeMap, BTreeSet};

use rafter::{LogIndex, NodeConfig, NodeId};

use crate::Cluster;

mod types;

pub use types::{
    Action, Bounds, Failure, MessageKind, NodeSummary, ProposalId, StateSummary, Summary,
};

mod tla;

pub use tla::{
    project_raft_trace_to_tla, render_tla_trace_spec, require_tla_projectable_raft_trace,
    TlaAbstractionGap, TlaAction, TlaProjection, TlaProjectionFailure, TlaTraceRenderError,
    TlaTraceSpec, TlaTraceStep,
};

mod replay;

pub use replay::{replay_raft_trace, ReplayCheck, ReplayError, ReplayExpectation, ReplayReport};

mod helpers;

use helpers::{proposal_payload, summarize, three_node_configs};

mod invariants;

use invariants::run_replay_check;

mod linearizability;

mod scheduling;

use scheduling::{enabled_soak_actions, soak_preferred_kind, Operation, SoakOperation};

mod state;

use state::{ClientWriteStatus, ExplorationState, RestartSnapshotState};

mod application;

use application::{apply_soak_action, apply_to_state, restart_node};

mod soak;

pub use soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure, SoakSummary};

mod explorers;

use explorers::{
    CommitSafetyExplorer, ElectionSafetyExplorer, ReadIndexSafetyExplorer, RestartSafetyExplorer,
};

const SOAK_LIVENESS_INVARIANT: &str = "raft randomized soak liveness";
const MIN_SOAK_LIVENESS_ROUNDS: usize = 128;

/// Exhaustively explores bounded Raft tick and ready-message delivery schedules.
///
/// # Errors
///
/// Returns [`Failure`] when any explored state violates election safety.
pub fn check_raft_election_safety(
    configs: Vec<NodeConfig>,
    bounds: Bounds,
) -> Result<Summary, Failure> {
    let cluster = Cluster::new(configs);
    let mut explorer = ElectionSafetyExplorer::new(bounds);
    let mut trace = Vec::new();
    explorer.explore(&cluster, &mut trace, 0)?;
    Ok(explorer.summary())
}

/// Exhaustively explores bounded Raft proposal, tick, and delivery schedules.
///
/// # Errors
///
/// Returns [`Failure`] when any explored state violates commit safety.
pub fn check_raft_commit_safety(
    configs: Vec<NodeConfig>,
    bounds: Bounds,
) -> Result<Summary, Failure> {
    let state = ExplorationState::new(Cluster::new(configs));
    let mut explorer = CommitSafetyExplorer::new(bounds);
    let mut trace = Vec::new();
    explorer.explore(&state, &mut trace, 0)?;
    Ok(explorer.summary())
}

/// Exhaustively explores bounded dynamic-membership proposals mixed with
/// ticks, message delivery, and client proposals.
///
/// # Errors
///
/// Returns [`Failure`] when any explored state violates commit safety or a
/// membership-specific invariant.
pub fn check_raft_membership_safety(
    configs: Vec<NodeConfig>,
    bounds: Bounds,
) -> Result<Summary, Failure> {
    let mut cluster = Cluster::new(configs);
    helpers::elect_node_one(&mut cluster);
    let state = ExplorationState::new(cluster);
    let mut explorer = CommitSafetyExplorer::new(bounds);
    let mut trace = Vec::new();
    explorer.explore(&state, &mut trace, 0)?;
    Ok(explorer.summary())
}

/// Exhaustively explores restart and snapshot-transfer schedules while the
/// snapshot carries a committed joint membership.
///
/// # Errors
///
/// Returns [`Failure`] when any explored state violates restart, snapshot, or
/// committed-prefix safety.
pub fn check_raft_joint_membership_restart_and_snapshot_safety(
    bounds: Bounds,
) -> Result<Summary, Failure> {
    let mut bounds = bounds;
    bounds.depth = if bounds.depth > 12 { bounds.depth } else { 12 };
    let state = RestartSnapshotState::joint_snapshot_transfer();
    let mut explorer = RestartSafetyExplorer::new(bounds);
    let mut trace = Vec::new();
    explorer.explore(&state, &mut trace, 0)?;
    if !explorer.observed_restart {
        return Err(Failure {
            invariant: RestartSafetyExplorer::INVARIANT,
            message: "bounded joint-membership exploration did not reach a restart action"
                .to_string(),
            trace: Vec::new(),
            state: summarize(&state.state.cluster),
        });
    }
    if !explorer.observed_pending_snapshot {
        return Err(Failure {
            invariant: RestartSafetyExplorer::INVARIANT,
            message:
                "bounded joint-membership exploration did not reach a pending snapshot transfer"
                    .to_string(),
            trace: Vec::new(),
            state: summarize(&state.state.cluster),
        });
    }
    if !explorer.observed_installed_snapshot {
        return Err(Failure {
            invariant: RestartSafetyExplorer::INVARIANT,
            message: "bounded joint-membership exploration did not reach an installed snapshot"
                .to_string(),
            trace: Vec::new(),
            state: summarize(&state.state.cluster),
        });
    }
    Ok(explorer.summary())
}

/// Explores hand-seeded commit-safety states that previously required long,
/// unlikely prefixes before the critical action was reachable.
///
/// # Errors
///
/// Returns [`Failure`] when any seeded state violates commit safety.
pub fn check_raft_seeded_commit_safety(
    configs: Vec<NodeConfig>,
    bounds: Bounds,
) -> Result<Summary, Failure> {
    let seeds = [
        ExplorationState::seeded_low_empty_probe(configs.clone()),
        ExplorationState::seeded_divergent_suffix_probe(configs),
    ];
    let mut explorer = CommitSafetyExplorer::new(bounds);
    for state in seeds {
        let mut trace = Vec::new();
        explorer.explore(&state, &mut trace, 0)?;
    }
    Ok(explorer.summary())
}

/// Explores hand-seeded leadership no-op states.
///
/// These seeds pin the cases where a newly elected leader's no-op can
/// immediately commit prior-term application or configuration entries. The
/// checker fails both on safety violations and on bounds too shallow to reach
/// the seeded commit points.
///
/// # Errors
///
/// Returns [`Failure`] when any seeded state violates election or commit
/// safety, or when the bound does not reach every required seeded observation.
pub fn check_raft_leadership_noop_safety(bounds: Bounds) -> Result<Summary, Failure> {
    let seeds = vec![
        ExplorationState::seeded_single_voter_prior_application_noop(),
        ExplorationState::seeded_single_voter_prior_configuration_noop(),
        ExplorationState::seeded_joint_self_quorum_prior_application_noop(),
        ExplorationState::seeded_leadership_transfer_noop_commit(),
    ];
    let required_applies = required_state_summaries(seeds.iter().flat_map(|state| {
        state
            .required_applied_payloads
            .keys()
            .copied()
            .map(move |key| (key, state))
    }));
    let required_configurations = required_state_summaries(seeds.iter().flat_map(|state| {
        state
            .required_committed_configurations
            .keys()
            .copied()
            .map(move |key| (key, state))
    }));
    let required_commits = required_state_summaries(seeds.iter().flat_map(|state| {
        state
            .required_commit_indexes
            .iter()
            .copied()
            .map(move |key| (key, state))
    }));

    let mut explorer = CommitSafetyExplorer::new(bounds);
    for state in seeds {
        let mut trace = Vec::new();
        explorer.explore(&state, &mut trace, 0)?;
    }

    for (key, summary) in &required_applies {
        if !explorer.observed_required_applies().contains(key) {
            return Err(Failure {
                invariant: CommitSafetyExplorer::INVARIANT,
                message: format!(
                    "leadership no-op seed did not reach required Apply for {} at {} within depth {}",
                    key.0,
                    key.1,
                    bounds.max_depth()
                ),
                trace: Vec::new(),
                state: summary.clone(),
            });
        }
    }
    for (key, summary) in &required_configurations {
        if !explorer.observed_required_configurations().contains(key) {
            return Err(Failure {
                invariant: CommitSafetyExplorer::INVARIANT,
                message: format!(
                    "leadership no-op seed did not reach required committed configuration for {} at {} within depth {}",
                    key.0,
                    key.1,
                    bounds.max_depth()
                ),
                trace: Vec::new(),
                state: summary.clone(),
            });
        }
    }
    for (key, summary) in &required_commits {
        if !explorer.observed_required_commits().contains(key) {
            return Err(Failure {
                invariant: CommitSafetyExplorer::INVARIANT,
                message: format!(
                    "leadership no-op seed did not reach required commit for {} at {} within depth {}",
                    key.0,
                    key.1,
                    bounds.max_depth()
                ),
                trace: Vec::new(),
                state: summary.clone(),
            });
        }
    }

    Ok(explorer.summary())
}

fn required_state_summaries<'a>(
    required: impl IntoIterator<Item = ((NodeId, LogIndex), &'a ExplorationState)>,
) -> BTreeMap<(NodeId, LogIndex), StateSummary> {
    required
        .into_iter()
        .map(|(key, state)| (key, summarize(&state.cluster)))
        .collect()
}

/// Exhaustively explores bounded schedules that mix proposals, message
/// faults, and read barriers, checking that every granted barrier covers the
/// cluster-wide committed floor at its registration (thesis 6.4).
///
/// The initial state is an elected leader with one committed current-term
/// entry, so barriers are grantable within shallow depths.
///
/// # Errors
///
/// Returns [`Failure`] when any explored state violates election safety,
/// commit safety, or the read-barrier committed-floor invariant.
pub fn check_raft_read_index_safety(
    configs: Vec<NodeConfig>,
    bounds: Bounds,
) -> Result<Summary, Failure> {
    let mut cluster = Cluster::new(configs);
    helpers::elect_node_one(&mut cluster);
    cluster.propose(rafter::NodeId(1), b"read-index-seed".to_vec());
    cluster.deliver_all();
    let state = ExplorationState::new(cluster);
    let mut explorer = ReadIndexSafetyExplorer::new(bounds);
    let mut trace = Vec::new();
    explorer.explore(&state, &mut trace, 0)?;
    Ok(explorer.summary())
}

/// Exhaustively explores bounded Raft restart and snapshot-transfer schedules.
///
/// # Errors
///
/// Returns [`Failure`] when any explored state violates restart or snapshot
/// safety.
pub fn check_raft_restart_and_snapshot_safety(bounds: Bounds) -> Result<Summary, Failure> {
    let mut restart_explorer = RestartSafetyExplorer::new(bounds);
    let mut restart_trace = Vec::new();
    let restart_state =
        RestartSnapshotState::new(ExplorationState::new(Cluster::new(three_node_configs())));
    restart_explorer.explore(&restart_state, &mut restart_trace, 0)?;
    if !restart_explorer.observed_restart {
        return Err(Failure {
            invariant: RestartSafetyExplorer::INVARIANT,
            message: "bounded exploration did not reach a restart action".to_string(),
            trace: Vec::new(),
            state: summarize(&restart_state.state.cluster),
        });
    }

    let mut snapshot_bounds = bounds;
    snapshot_bounds.depth = if bounds.depth > 12 { bounds.depth } else { 12 };
    let mut snapshot_explorer = RestartSafetyExplorer::new(snapshot_bounds);
    let mut snapshot_trace = Vec::new();
    let snapshot_state = RestartSnapshotState::snapshot_transfer();
    snapshot_explorer.explore(&snapshot_state, &mut snapshot_trace, 0)?;
    if !snapshot_explorer.observed_pending_snapshot {
        return Err(Failure {
            invariant: RestartSafetyExplorer::INVARIANT,
            message: "bounded exploration did not reach a pending snapshot transfer".to_string(),
            trace: Vec::new(),
            state: summarize(&snapshot_state.state.cluster),
        });
    }
    if !snapshot_explorer.observed_installed_snapshot {
        return Err(Failure {
            invariant: RestartSafetyExplorer::INVARIANT,
            message: "bounded exploration did not reach an installed snapshot".to_string(),
            trace: Vec::new(),
            state: summarize(&snapshot_state.state.cluster),
        });
    }

    Ok(restart_explorer
        .summary()
        .combined(snapshot_explorer.summary()))
}

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
    let mut state = ExplorationState::new(Cluster::new_with_seed(configs, config.seed));
    let mut trace = Vec::new();
    let mut observed_actions = BTreeSet::new();

    for step in 0..config.steps {
        let actions = enabled_soak_actions(&state, config);
        let preferred_kind = soak_preferred_kind(step);
        let candidates = actions
            .iter()
            .enumerate()
            .filter_map(|(index, action)| (action.trace.kind() == preferred_kind).then_some(index))
            .collect::<Vec<_>>();
        let mut action_index = if candidates.is_empty() {
            state.cluster.rng.index(actions.len())
        } else {
            candidates[state.cluster.rng.index(candidates.len())]
        };
        // Tick-rate skew: re-draw tick targets so the skewed node ticks
        // `weight`-to-one against each peer, deterministically.
        if let (Some((skewed, weight)), SoakAction::Tick(_)) =
            (config.tick_skew, &actions[action_index].trace)
        {
            let peers = state.cluster.nodes.len().saturating_sub(1);
            if state.cluster.rng.index(weight as usize + peers) < weight as usize {
                if let Some(skewed_index) = actions.iter().position(
                    |action| matches!(action.trace, SoakAction::Tick(node) if node == skewed),
                ) {
                    action_index = skewed_index;
                }
            }
        }
        let action = actions[action_index].clone();
        apply_soak_action(&mut state, action.operation);
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

    run_soak_liveness_check(&mut state, config, &mut trace, &mut observed_actions)?;

    Ok(SoakSummary {
        seed: config.seed,
        steps_executed: config.steps,
        observed_actions,
    })
}

fn run_soak_liveness_check(
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
            return Ok(());
        }
        if accepted_proposal.is_none() {
            if let Some(leader) = quiescent_leader(state) {
                accepted_proposal = issue_liveness_proposal(state, leader, trace, observed_actions);
                check_soak_safety(state, config, trace)?;
                if accepted_proposal
                    .is_some_and(|proposal_id| liveness_proposal_completed(state, proposal_id))
                {
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
    Err(soak_liveness_failure(state, config, trace, message))
}

fn drive_until_quiescent_leader(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    budget: usize,
) -> Result<Option<NodeId>, SoakFailure> {
    let mut stable_leader = None;
    let mut stable_observations = 0usize;
    for round in 0..budget {
        if let Some(leader) = quiescent_leader(state) {
            if stable_leader == Some(leader) {
                stable_observations += 1;
            } else {
                stable_leader = Some(leader);
                stable_observations = 1;
            }
            if stable_observations >= 2 {
                return Ok(Some(leader));
            }
        } else {
            stable_leader = None;
            stable_observations = 0;
        }

        drive_soak_liveness_round(state, trace, observed_actions, round);
        check_soak_safety(state, config, trace)?;
    }
    Ok(quiescent_leader(state))
}

fn drive_soak_liveness_round(
    state: &mut ExplorationState,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    round: usize,
) {
    if let Some(envelope) = state.cluster.deliver_random_ready() {
        trace.push(SoakAction::Deliver {
            from: envelope.from,
            to: envelope.to,
            message: MessageKind::from(&envelope.message),
        });
        observed_actions.insert(SoakActionKind::Deliver);
    } else {
        let node_ids = state.cluster.nodes.keys().copied().collect::<Vec<_>>();
        let node_id = node_ids[round % node_ids.len()];
        state.cluster.tick(node_id);
        trace.push(SoakAction::Tick(node_id));
        observed_actions.insert(SoakActionKind::Tick);
    }
    state.refresh_commit_floors();
    state.refresh_client_history();
}

fn check_soak_safety(
    state: &ExplorationState,
    config: SoakConfig,
    trace: &[SoakAction],
) -> Result<(), SoakFailure> {
    if let Err(failure) = run_replay_check(state, ReplayCheck::CommitSafety, &[]) {
        return Err(SoakFailure {
            seed: config.seed,
            step: trace.len(),
            trace: trace.to_vec(),
            failure: Box::new(failure),
        });
    }
    Ok(())
}

fn issue_liveness_proposal(
    state: &mut ExplorationState,
    leader: NodeId,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
) -> Option<ProposalId> {
    let proposal_id = ProposalId(state.proposals_issued + 1);
    let payload = proposal_payload(proposal_id);
    state.cluster.propose(leader, payload.clone());
    state.refresh_commit_floors();
    state.refresh_client_history();
    trace.push(SoakAction::Propose {
        to: leader,
        proposal_id,
    });
    observed_actions.insert(SoakActionKind::Propose);

    if !liveness_payload_visible(state, &payload) {
        return None;
    }

    state.record_client_proposal(leader, proposal_id, false);
    state.proposals_issued += 1;
    state.refresh_client_history();
    Some(proposal_id)
}

fn liveness_payload_visible(state: &ExplorationState, payload: &[u8]) -> bool {
    state
        .cluster
        .applied()
        .iter()
        .any(|applied| applied.payload.as_slice() == payload)
        || state.cluster.nodes.keys().any(|node_id| {
            state
                .cluster
                .bootstrap_state(*node_id)
                .log
                .iter()
                .any(|entry| entry.kind.application_payload() == Some(payload))
        })
}

fn soak_liveness_round_budget(state: &ExplorationState, config: SoakConfig) -> usize {
    MIN_SOAK_LIVENESS_ROUNDS
        .saturating_add(state.cluster.nodes.len().saturating_mul(16))
        .saturating_add(state.cluster.network.len().saturating_mul(4))
        .saturating_add(config.max_proposals.saturating_mul(8))
        .saturating_add(config.max_membership_changes.saturating_mul(16))
        .saturating_add(config.max_partitions.saturating_mul(16))
}

fn quiescent_leader(state: &ExplorationState) -> Option<NodeId> {
    let leaders = state.cluster.leaders();
    (leaders.len() == 1 && state.cluster.network.is_empty()).then(|| leaders[0])
}

fn has_partition(cluster: &Cluster) -> bool {
    cluster
        .nodes
        .keys()
        .any(|a| cluster.nodes.keys().any(|b| cluster.partitioned(*a, *b)))
}

fn liveness_proposal_completed(state: &ExplorationState, proposal_id: ProposalId) -> bool {
    state
        .client_history
        .writes
        .get(&proposal_id)
        .is_some_and(|write| matches!(write.status, ClientWriteStatus::Completed { .. }))
}

fn soak_liveness_failure(
    state: &ExplorationState,
    config: SoakConfig,
    trace: &[SoakAction],
    message: String,
) -> SoakFailure {
    SoakFailure {
        seed: config.seed,
        step: trace.len(),
        trace: trace.to_vec(),
        failure: Box::new(Failure {
            invariant: SOAK_LIVENESS_INVARIANT,
            message,
            trace: Vec::new(),
            state: summarize(&state.cluster),
        }),
    }
}

#[cfg(test)]
mod tests;
