use super::super::{
    helpers::proposal_payload, observations::Observation, scheduling::Operation, Action,
    ExplorationState, Failure, RestartSnapshotState,
};
use super::cluster::apply_to_cluster;
use super::restart::restart_node;

pub(in crate::model_check) fn apply_to_state(state: &mut ExplorationState, operation: Operation) {
    let commit_context = state.commit_transition_context();
    let configuration_proposer = match &operation {
        Operation::AddLearner { to, .. }
        | Operation::RemoveLearner { to, .. }
        | Operation::PromoteLearner { to, .. }
        | Operation::RemoveVoter { to, .. }
        | Operation::EnterJoint { to, .. }
        | Operation::LeaveJoint { to } => Some(*to),
        Operation::Tick(_)
        | Operation::Restart(_)
        | Operation::Propose { .. }
        | Operation::ReadIndex { .. }
        | Operation::Transfer { .. }
        | Operation::DeliverReadyAt(_) => None,
    };
    let delivered = match &operation {
        Operation::DeliverReadyAt(position) => state
            .cluster
            .network
            .get(*position)
            .map(|queued| queued.envelope.clone()),
        _ => None,
    };
    let needs_transition_context = matches!(&operation, Operation::Tick(_)) || delivered.is_some();
    let transition_context =
        needs_transition_context.then(|| (state.cluster.clone(), delivered.clone()));

    if let Operation::Propose {
        to,
        proposal_id,
        stale_leader,
    } = &operation
    {
        state.record_client_proposal(*to, *proposal_id, *stale_leader);
        state.proposals_issued += 1;
        if *stale_leader {
            state
                .forbidden_applied_payloads
                .insert(proposal_payload(*proposal_id).into());
        }
    }
    if let Operation::ReadIndex { to, request_id } = &operation {
        state.record_client_read(*to, *request_id, state.cluster.committed_floor());
        state.read_indexes_issued += 1;
    }
    if matches!(
        operation,
        Operation::AddLearner { .. }
            | Operation::RemoveLearner { .. }
            | Operation::PromoteLearner { .. }
            | Operation::RemoveVoter { .. }
            | Operation::EnterJoint { .. }
            | Operation::LeaveJoint { .. }
    ) {
        state.membership_changes_issued += 1;
    }
    if matches!(operation, Operation::Transfer { .. }) {
        state.transfers_issued += 1;
    }
    let effects = apply_to_cluster(&mut state.cluster, operation);
    if let Some((before, delivered)) = transition_context {
        state.observe_election_authority();
        state.record_election_observation(&before, delivered.as_ref(), &effects.emitted);
        state.record_log_transition(&before, delivered.as_ref(), &effects.emitted);
    }
    state.observe_election_authority();
    state.refresh_log_history();
    state.record_commit_observation(&commit_context, configuration_proposer);
    state.record_leader_completeness_observation();
    state.refresh_commit_floors();
    state.refresh_client_history();
    state.observe_state_coverage();
}

pub(in crate::model_check) fn apply_to_restart_snapshot_state(
    state: &mut RestartSnapshotState,
    operation: Operation,
    trace: &[Action],
) -> Result<(), Failure> {
    match operation {
        Operation::Restart(node_id) => {
            restart_node(&mut state.state, node_id, trace)?;
            state.state.restarts_issued += 1;
            state.state.reset_commit_floor(node_id);
            state.state.refresh_log_history();
            state.state.refresh_committed_prefixes();
        }
        operation => {
            apply_to_state(&mut state.state, operation);
        }
    }
    if let Some(expected) = &state.expected_snapshot {
        let installed_expected = state.state.cluster.nodes.keys().any(|node_id| {
            let bootstrap = state.state.cluster.bootstrap_state(*node_id);
            bootstrap.snapshot.as_ref() == Some(&expected.snapshot)
                && state
                    .state
                    .cluster
                    .snapshot_payload(*node_id, &expected.snapshot)
                    == Some(expected.payload.as_slice())
        });
        if installed_expected {
            state
                .state
                .mark_observation(Observation::ExpectedSnapshotInstallsChecked);
        }
    }
    state.state.observe_state_coverage();
    Ok(())
}
