use std::collections::BTreeSet;

use rafter::NodeId;

use crate::Cluster;

use super::MIN_SOAK_LIVENESS_ROUNDS;
use crate::model_check::{
    helpers::{proposal_payload, summarize},
    invariants::run_replay_check,
    scheduling::Operation,
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
    state::apply_to_state,
    state::{ClientWriteStatus, ExplorationState},
    Failure, MessageKind, ProposalId, ReplayCheck,
};

pub(in crate::model_check::liveness) fn drive_liveness_rounds_until(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    budget: usize,
    mut complete: impl FnMut(&ExplorationState) -> bool,
) -> Result<bool, SoakFailure> {
    if complete(state) {
        return Ok(true);
    }
    for round in 0..budget {
        drive_soak_liveness_round(state, trace, observed_actions, round);
        check_soak_safety(state, config, trace)?;
        if complete(state) {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(in crate::model_check::liveness) fn drive_until_quiescent_leader(
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
        } else if let Some(leader) = single_leader(state) {
            if stable_leader != Some(leader) {
                stable_leader = Some(leader);
                stable_observations = 0;
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

pub(in crate::model_check::liveness) fn drive_soak_liveness_round(
    state: &mut ExplorationState,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    round: usize,
) {
    if let Some(position) = state.random_ready_position() {
        let Some(envelope) = state.cluster().pending_envelope_at(position).cloned() else {
            return;
        };
        apply_to_state(state, Operation::DeliverReadyAt(position));
        trace.push(SoakAction::Deliver {
            from: envelope.from,
            to: envelope.to,
            message: MessageKind::from(&envelope.message),
        });
        observed_actions.insert(SoakActionKind::Deliver);
    } else {
        let node_ids = state.cluster().nodes.keys().copied().collect::<Vec<_>>();
        let node_id = node_ids[round % node_ids.len()];
        apply_to_state(state, Operation::Tick(node_id));
        trace.push(SoakAction::Tick(node_id));
        observed_actions.insert(SoakActionKind::Tick);
    }
}

pub(in crate::model_check::liveness) fn check_soak_safety(
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

pub(in crate::model_check::liveness) fn issue_liveness_proposal(
    state: &mut ExplorationState,
    leader: NodeId,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
) -> Option<ProposalId> {
    let proposal_id = ProposalId(state.proposals_issued() + 1);
    let payload = proposal_payload(proposal_id);
    apply_to_state(
        state,
        Operation::Propose {
            to: leader,
            proposal_id,
            stale_leader: false,
        },
    );
    trace.push(SoakAction::Propose {
        to: leader,
        proposal_id,
    });
    observed_actions.insert(SoakActionKind::Propose);

    if !state
        .client_history()
        .writes
        .get(&proposal_id)
        .is_some_and(|write| {
            matches!(
                write.status,
                ClientWriteStatus::Accepted { .. } | ClientWriteStatus::Completed { .. }
            )
        })
        || !liveness_payload_visible(state, &payload)
    {
        return None;
    }

    Some(proposal_id)
}

pub(in crate::model_check::liveness) fn soak_liveness_round_budget(
    state: &ExplorationState,
    config: SoakConfig,
) -> usize {
    MIN_SOAK_LIVENESS_ROUNDS
        .saturating_add(state.cluster().nodes.len().saturating_mul(16))
        .saturating_add(state.cluster().network.len().saturating_mul(4))
        .saturating_add(config.max_proposals.saturating_mul(8))
        .saturating_add(config.max_membership_changes.saturating_mul(16))
        .saturating_add(config.max_partitions.saturating_mul(16))
        .saturating_add(usize::from(config.snapshot_catchup_probe).saturating_mul(64))
}

pub(in crate::model_check::liveness) fn quiescent_leader(
    state: &ExplorationState,
) -> Option<NodeId> {
    state
        .cluster()
        .network
        .is_empty()
        .then(|| single_leader(state))?
}

fn single_leader(state: &ExplorationState) -> Option<NodeId> {
    let leaders = state.cluster().leaders();
    (leaders.len() == 1).then(|| leaders[0])
}

pub(in crate::model_check::liveness) fn has_partition(cluster: &Cluster) -> bool {
    cluster
        .nodes
        .keys()
        .any(|a| cluster.nodes.keys().any(|b| cluster.partitioned(*a, *b)))
}

pub(in crate::model_check::liveness) fn liveness_proposal_completed(
    state: &ExplorationState,
    proposal_id: ProposalId,
) -> bool {
    state
        .client_history()
        .writes
        .get(&proposal_id)
        .is_some_and(|write| matches!(write.status, ClientWriteStatus::Completed { .. }))
}

pub(in crate::model_check::liveness) fn liveness_proposal_terminated(
    state: &ExplorationState,
    proposal_id: ProposalId,
) -> bool {
    state
        .client_history()
        .writes
        .get(&proposal_id)
        .is_some_and(|write| {
            matches!(
                write.status,
                ClientWriteStatus::Completed { .. }
                    | ClientWriteStatus::Rejected
                    | ClientWriteStatus::Dropped { .. }
                    | ClientWriteStatus::Unknown { .. }
            )
        })
}

pub(in crate::model_check::liveness) fn soak_liveness_failure(
    state: &ExplorationState,
    config: SoakConfig,
    trace: &[SoakAction],
    invariant: &'static str,
    message: String,
) -> SoakFailure {
    SoakFailure {
        seed: config.seed,
        step: trace.len(),
        trace: trace.to_vec(),
        failure: Box::new(Failure {
            kind: crate::model_check::FailureKind::CoverageNotReached,
            invariant,
            message,
            trace: Vec::new(),
            state: summarize(state.cluster()),
        }),
    }
}

fn liveness_payload_visible(state: &ExplorationState, payload: &[u8]) -> bool {
    state
        .cluster()
        .applied()
        .iter()
        .any(|applied| applied.payload.as_slice() == payload)
        || state.cluster().nodes.keys().any(|node_id| {
            state
                .cluster()
                .bootstrap_state(*node_id)
                .log
                .iter()
                .any(|entry| entry.kind.application_payload() == Some(payload))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        model_check::{
            helpers::{config, deliver_all_in_state, elect_node_one_in_state},
            state::ExplorationState,
        },
        Cluster, SimSeed,
    };

    fn three_node_fast_configs() -> Vec<rafter::NodeConfig> {
        vec![
            config(1, &[2, 3], 2),
            config(2, &[1, 3], 2),
            config(3, &[1, 2], 2),
        ]
    }

    #[test]
    fn quiescent_leader_monitor_survives_heartbeat_between_observations() {
        let config = SoakConfig::new(SimSeed(0x1ead), 0);
        let mut state = ExplorationState::new(Cluster::new_with_seed(
            three_node_fast_configs(),
            config.seed,
        ));
        elect_node_one_in_state(&mut state);
        deliver_all_in_state(&mut state);
        assert_eq!(quiescent_leader(&state), Some(NodeId(1)));

        let mut trace = Vec::new();
        let mut observed_actions = BTreeSet::new();
        let leader =
            drive_until_quiescent_leader(&mut state, config, &mut trace, &mut observed_actions, 12)
                .expect("leader convergence monitor should preserve same-leader observations");

        assert_eq!(leader, Some(NodeId(1)));
        assert!(
            trace
                .iter()
                .any(|action| matches!(action, SoakAction::Tick(NodeId(1)))),
            "regression should exercise a leader tick between quiescent observations"
        );
    }
}
